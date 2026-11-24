"""Ingestion and incremental rerun pipeline.

Workflow
--------
1. ``ingest`` one (source, partition) file: parse/validate, copy the raw
   bytes into the content-addressed blob store, and either accept the whole
   partition or, if any record is rejected, keep the previous partition and
   write the rejected rows to quarantine (the replacement is void).
2. ``rerun`` rebuilds the in-memory indices from the accepted blobs, forms
   the allowed pairs and union-find groups, and recomputes ONLY the groups
   whose inputs changed. Groups whose input hash is unchanged keep their
   previously published result (business result and lineage stay identical).
3. Publication is fenced by the monotonic input epoch captured when the run
   started. A slow run whose inputs went stale while it was solving is
   refused at commit time instead of overwriting a newer published version;
   identical-input reruns collapse to the already-published version. The
   two-phase commit (staged ledger row + lineage, version tree + atomic
   pointer swap, then ``current`` marker) keeps every readable snapshot
   whole even if the process stops mid-publish.
"""
from __future__ import annotations

import os
import time
import uuid

from . import parsers
from .parsers import (SOURCE_DEVICE, SOURCE_SCHEDULE, SOURCE_LEDGER,
                      ParsedPartition)
from .reconcile import build_pairs, reconcile_groups
from .storage import Lake, PublicationSuperseded, canonical_json, sha256_bytes, utc_now


# ---------------------------------------------------------------------- #
# fault-injection hooks (offline tests only; driven by RECON_FAULT_* env)
# ---------------------------------------------------------------------- #
def maybe_fault(point: str) -> None:
    """Trigger a test fault when RECON_FAULT=<point>[,...] names this point.

    Supported actions via RECON_FAULT_ACTION:
      * ``exit``     hard process stop (os._exit, no cleanup) -> crash
      * ``sleep:N``  block N seconds -> a slow, long-running solve
    """
    wanted = os.environ.get("RECON_FAULT", "")
    if point not in {p.strip() for p in wanted.split(",") if p.strip()}:
        return
    action = os.environ.get("RECON_FAULT_ACTION", "exit")
    if action == "exit":
        # emulate a hard kill mid-publish: no finally/atexit runs
        os._exit(91)
    if action.startswith("sleep:"):
        time.sleep(float(action.split(":", 1)[1]))


class IngestionReport:
    def __init__(self, run_id):
        self.run_id = run_id
        self.accepted = []        # (source, partition, blob_sha, n_records)
        self.voided = []          # (source, partition, n_quarantined)
        self.quarantine = []      # dicts
        self.affected_groups = []
        self.version_id = None
        self.status = "running"
        # "ok": newly published; "unchanged": inputs already published;
        # "superseded": stale inputs, publication refused
        self.outcome = "ok"
        self.detail = ""
        self.input_epoch = None

    def to_dict(self):
        return {
            "run_id": self.run_id,
            "status": self.status,
            "outcome": self.outcome,
            "detail": self.detail,
            "version_id": self.version_id,
            "input_epoch": self.input_epoch,
            "accepted": self.accepted,
            "voided": self.voided,
            "quarantine_count": len(self.quarantine),
            "affected_groups": self.affected_groups,
        }


class Pipeline:
    def __init__(self, lake: Lake, receiver_map: dict) -> None:
        self.lake = lake
        self.receiver_map = receiver_map

    # ------------------------------------------------------------------ #
    # single partition ingestion (whole-partition replacement)
    # ------------------------------------------------------------------ #
    def ingest(self, source: str, partition: str, path: str,
               run_id: str, report: IngestionReport) -> ParsedPartition | None:
        if source == SOURCE_DEVICE:
            parsed = parsers.parse_device(path, partition, self.receiver_map)
        elif source == SOURCE_SCHEDULE:
            parsed = parsers.parse_schedule(path, partition)
        elif source == SOURCE_LEDGER:
            parsed = parsers.parse_ledger(path, partition)
        else:
            raise ValueError(f"unknown source {source!r}")

        if not parsed.ok:
            # replacement is void: do NOT touch the existing accepted
            # partition; persist rejected rows to durable quarantine at once
            # so they survive even if no rerun follows.
            for q in parsed.quarantine:
                doc = q.to_dict()
                qid = _quarantine_id(doc)
                doc["quarantine_id"] = qid
                doc["run_id"] = run_id
                self.lake.add_quarantine(qid, doc)
                report.quarantine.append(doc)
            report.voided.append((source, partition, len(parsed.quarantine)))
            return None

        blob_sha, _, byte_size = self.lake.store_file_blob(path)
        self.lake.record_ingest(source, partition, blob_sha, path,
                                byte_size, run_id)
        report.accepted.append((source, partition, blob_sha,
                                len(parsed.records)))
        return parsed

    # ------------------------------------------------------------------ #
    # rebuild indices from accepted blobs and recompute affected groups
    # ------------------------------------------------------------------ #
    def load_accepted(self):
        """Re-parse every accepted partition blob into normalized records."""
        device_recs, schedule_recs, ledger_recs = [], [], []
        for row in self.lake.all_ingests():
            source, partition = row["source"], row["partition"]
            blob_path = self.lake.blob_path(row["blob_sha"])
            if source == SOURCE_DEVICE:
                p = parsers.parse_device(blob_path, partition,
                                         self.receiver_map)
                for r in p.records:
                    r["_source_path"] = row["source_path"]
                    r["_blob_sha"] = row["blob_sha"]
                device_recs.extend(p.records)
            elif source == SOURCE_SCHEDULE:
                p = parsers.parse_schedule(blob_path, partition)
                for r in p.records:
                    r["_source_path"] = row["source_path"]
                    r["_blob_sha"] = row["blob_sha"]
                schedule_recs.extend(p.records)
            elif source == SOURCE_LEDGER:
                p = parsers.parse_ledger(blob_path, partition)
                for r in p.records:
                    r["_source_path"] = row["source_path"]
                    r["_blob_sha"] = row["blob_sha"]
                ledger_recs.extend(p.records)
        return device_recs, schedule_recs, ledger_recs

    def rerun(self, changed_partitions: dict | None = None,
              note: str = "") -> IngestionReport:
        """Recompute and publish with epoch fencing.

        ``changed_partitions`` optionally maps source -> set(partition) that
        were touched in this run; only groups depending on them are rebuilt.
        When None, every group is recomputed (but unchanged groups still
        produce identical hashes/results).

        Outcomes:
          * ``ok``          - a new immutable version was published;
          * ``unchanged``   - the exact input state is already published;
          * ``superseded``  - newer inputs landed during the solve, so the
            stale result was refused and the current version stays in place.
        """
        run_id = uuid.uuid4().hex
        report = IngestionReport(run_id)
        self.lake.start_run(run_id, note)
        try:
            # fence: pin the accepted input state this run computes from.
            # No write lock is held while the (potentially long) solve runs,
            # so ingestion and another rerun can complete concurrently.
            start_epoch = self.lake.current_input_epoch()
            report.input_epoch = start_epoch
            maybe_fault("before_solve")

            device_recs, schedule_recs, ledger_recs = self.load_accepted()

            device_index, project_index, pair_lineage, night_period = \
                self._build_indices(device_recs, schedule_recs, ledger_recs)
            pairs = build_pairs(schedule_recs)

            all_docs = reconcile_groups(
                pairs, device_index, project_index, pair_lineage,
                night_period)
            maybe_fault("after_solve")

            # decide which groups are affected by the changed partitions
            changed_keys = self._changed_group_keys(
                all_docs, changed_partitions)
            report.affected_groups = sorted(changed_keys)

            # reuse prior results for unaffected groups; recompute the rest.
            # group_version() only sees lineage attached to the CURRENT
            # publication, so staged/superseded runs can never poison reuse.
            prior_docs = {d["group_id"]: d
                          for d in self.lake.read_all_group_docs()}
            group_docs, quarantine_docs = {}, {}
            for doc in all_docs:
                gid = doc["group_id"]
                input_hash = self._input_hash(doc)
                prior_meta = self.lake.group_version(gid)
                if gid not in changed_keys and prior_meta and \
                        prior_meta["input_hash"] == input_hash and \
                        gid in prior_docs:
                    # identical inputs -> keep identical published result
                    group_docs[gid] = prior_docs[gid]
                    continue
                doc["input_hash"] = input_hash
                doc["result_hash"] = sha256_bytes(canonical_json(
                    self._hashable(doc)))
                doc["run_id"] = run_id
                group_docs[gid] = doc

            # Quarantine is an append-only, content-deduplicated history held
            # durably in SQLite; every published version snapshots the current
            # full set so rejected records remain queryable offline.
            quarantine_docs = {}
            for q in self.lake.all_quarantine():
                qid = q.get("quarantine_id") or _quarantine_id(q)
                q["quarantine_id"] = qid
                quarantine_docs[qid] = q
            for q in report.quarantine:
                qid = q.get("quarantine_id") or _quarantine_id(q)
                q["quarantine_id"] = qid
                quarantine_docs[qid] = q

            fingerprint = self.lake.input_fingerprint()

            # phase 1: fenced reservation in a single SQLite transaction
            try:
                reservation = self.lake.reserve_publication(
                    run_id, start_epoch, fingerprint)
            except PublicationSuperseded as exc:
                report.status = "superseded"
                report.outcome = "superseded"
                report.detail = exc.reason
                report.version_id = exc.current_version_id
                self.lake.finish_run(run_id, "superseded")
                return report

            if reservation["status"] == "unchanged":
                # identical input state is already published: idempotent
                # no-op, keep the existing complete version untouched.
                report.status = "unchanged"
                report.outcome = "unchanged"
                report.version_id = reservation["version_id"]
                report.detail = (
                    "input fingerprint already published as version "
                    f"v{reservation['version_id']}; no new version written")
                self.lake.finish_run(run_id, "unchanged")
                return report

            version_id = reservation["version_id"]
            manifest = self._manifest(run_id, group_docs, report,
                                      start_epoch, fingerprint)
            maybe_fault("after_reserve")
            try:
                # durable lineage rows bound to the reserved (staged) version
                for gid, doc in group_docs.items():
                    self.lake.record_group_version(
                        gid, version_id, doc["input_hash"],
                        doc["result_hash"], run_id)

                # phase 2a: full version tree, then atomic pointer swap
                self.lake.write_version_tree(
                    version_id, manifest, group_docs, quarantine_docs)
                maybe_fault("after_tree_write")

                # phase 2b: durable commit marker
                self.lake.commit_publication(version_id)
            except Exception:
                self.lake.abort_publication(version_id)
                raise

            report.version_id = version_id
            report.status = "ok"
            report.outcome = "ok"
            self.lake.finish_run(run_id, "ok")
        except Exception as exc:
            report.status = "error"
            report.outcome = "error"
            report.detail = str(exc)
            self.lake.finish_run(run_id, "error")
            raise
        return report

    # ------------------------------------------------------------------ #
    def _build_indices(self, device_recs, schedule_recs, ledger_recs):
        # device totals keyed by (receiver, night)
        device_index = {}
        for r in device_recs:
            device_index[(r["receiver"], r["night"])] = r

        # night -> settlement_period, derived from ledger rows
        night_period = {}
        for r in ledger_recs:
            night = r.get("night")
            if night is not None:
                night_period.setdefault(night, r["settlement_period"])

        # project totals keyed by (project, settlement_period). Repeated rows
        # for the same key are accumulated into one total.
        project_index = {}
        for r in ledger_recs:
            key = (r["project"], r["settlement_period"])
            if key in project_index:
                project_index[key]["occupied_ms"] += r["occupy_ms"]
            else:
                merged = dict(r)
                merged["occupied_ms"] = r["occupy_ms"]
                project_index[key] = merged

        # lineage: which schedule line created each allowed pair
        pair_lineage: dict = {}
        for rec in schedule_recs:
            for night in rec["nights"]:
                for receiver in rec["receivers"]:
                    triple = (rec["project"], receiver, night)
                    pair_lineage.setdefault(triple, []).append({
                        "source": SOURCE_SCHEDULE,
                        "source_path": rec.get("_source_path"),
                        "location": rec.get("_line"),
                        "ref": list(triple),
                    })
        return device_index, project_index, pair_lineage, night_period

    def _changed_group_keys(self, all_docs, changed_partitions):
        if not changed_partitions:
            return {d["group_id"] for d in all_docs}
        touched_files = self._files_for_partitions(changed_partitions)
        keys = set()
        for d in all_docs:
            files = set(d.get("lineage", {}).get("files", []))
            if files & touched_files:
                keys.add(d["group_id"])
        return keys

    def _files_for_partitions(self, changed_partitions) -> set:
        files = set()
        for source, partitions in changed_partitions.items():
            rows = self.lake.ingests_for_partitions(source, set(partitions))
            for r in rows:
                files.add(r["source_path"])
        return files

    @staticmethod
    def _input_hash(doc) -> str:
        material = {
            "pairs": doc["pairs"],
            "totals": doc.get("totals"),
            "missing_totals": doc.get("missing_totals"),
            "lineage_files": doc.get("lineage", {}).get("files"),
        }
        return sha256_bytes(canonical_json(material))

    @staticmethod
    def _hashable(doc) -> dict:
        # strip run/version bookkeeping so the result hash is business-only
        return {k: v for k, v in doc.items()
                if k not in ("run_id", "input_hash", "result_hash")}

    def _manifest(self, run_id, group_docs, report, input_epoch,
                  input_fingerprint):
        statuses = {}
        for d in group_docs.values():
            statuses[d["status"]] = statuses.get(d["status"], 0) + 1
        return {
            "run_id": run_id,
            "published_at": utc_now(),
            "input_epoch": input_epoch,
            "input_fingerprint": input_fingerprint,
            "group_count": len(group_docs),
            "status_counts": statuses,
            "groups": sorted(group_docs.keys()),
            "ingestion": report.to_dict(),
            "schema_version": 2,
        }


def _quarantine_id(q: dict) -> str:
    """Stable content id for a rejected record (independent of run)."""
    material = canonical_json({
        "source": q.get("source"),
        "partition": q.get("partition"),
        "source_path": q.get("source_path"),
        "location": q.get("location"),
        "raw": q.get("raw"),
        "reason": q.get("reason"),
    })
    return "q" + sha256_bytes(material)[:16]
