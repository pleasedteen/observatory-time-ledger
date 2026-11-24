"""Retention and confirmable garbage collection for the reconciliation lake.

The policy is deliberately conservative:

* keep the newest ``keep_versions`` complete published versions;
* additionally keep versions protected by an active read lease;
* never delete the current pointer target;
* protect every currently accepted partition blob, including an ingest that
  has not been included in a rerun yet;
* protect blobs referenced by any retained/leased version and explicit
  write/blob leases;
* delete an old original blob only when it has confirmed ingest attribution
  and no surviving version/current partition/lease reference;
* leave blobs with unknown attribution untouched.

Deletion is idempotent and journaled in SQLite.  A second retention run
reconciles the journal and finishes work left behind by an interrupted run.
"""
from __future__ import annotations

import json
import os
import shutil
import uuid

from .storage import Lake

DEFAULT_KEEP_VERSIONS = 3


class RetentionReport(dict):
    """Dict report that also supports attribute access."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setdefault("status", "ok")

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def to_dict(self) -> dict:
        return dict(self)


GCReport = RetentionReport
RetentionResult = RetentionReport


class RetentionInterrupted(RuntimeError):
    """Raised when a deletion step is interrupted.

    The original deletion error is preserved as ``__cause__``; ``report`` and
    ``gc_id`` describe the durable journal run that can be resumed.
    """

    def __init__(self, message, gc_id=None, report=None):
        super().__init__(message)
        self.gc_id = gc_id
        self.report = report


CleanupInterrupted = RetentionInterrupted
GarbageCollectionInterrupted = RetentionInterrupted


def as_lake(lake_or_root: "Lake | str | os.PathLike[str]") -> Lake:
    if isinstance(lake_or_root, Lake):
        return lake_or_root
    if isinstance(lake_or_root, (str, os.PathLike)):
        return Lake(os.fspath(lake_or_root))
    raise TypeError(f"expected Lake or lake root, got {type(lake_or_root)!r}")


def _remove_path(path: str) -> None:
    """Single deletion seam used by retention and failure-injection tests."""
    if os.path.islink(path) or os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.lexists(path):
        os.remove(path)


class GarbageCollector:
    """Plan and execute conservative snapshot/blob reclamation."""

    def __init__(self, lake: Lake | str, keep_versions: int | None = None,
                 *, keep: int | None = None,
                 retain: int | None = None,
                 retain_versions: int | None = None,
                 keep_releases: int | None = None,
                 retain_count: int | None = None,
                 releases: int | None = None,
                 n: int | None = None,
                 dry_run: bool = False,
                 delete: bool = True) -> None:
        self.lake = as_lake(lake)
        for candidate in (keep_versions, keep, retain, retain_versions,
                          keep_releases, retain_count, releases, n):
            if candidate is not None:
                keep_versions = candidate
                break
        self.keep_versions = int(
            DEFAULT_KEEP_VERSIONS if keep_versions is None else keep_versions)
        if self.keep_versions < 0:
            raise ValueError("keep_versions must be non-negative")
        self.dry_run = dry_run
        self.delete = delete

    # ------------------------------------------------------------------ #
    # planning / protection
    # ------------------------------------------------------------------ #
    def _lease_protection(self):
        lake = self.lake
        pinned_versions: set[int] = set()
        pinned_blobs: set[str] = set()
        global_write = False
        active = lake.active_leases()
        for lease in active:
            kind = lease.get("kind")
            tt = lease.get("target_type")
            tid = str(lease.get("target_id"))
            if tt in ("lake", "root") or (tt == "*" and tid == "*"):
                if kind in ("write", "gc"):
                    global_write = True
                else:
                    pinned_versions.update(lake.list_version_ids(False))
                continue
            if tt in ("version", "snapshot"):
                vid = Lake.parse_version_id(tid)
                if vid is None:
                    try:
                        vid = lake.version_id_from_dir(lake.version_dir(tid))
                    except (ValueError, FileNotFoundError):
                        vid = None
                if vid is not None:
                    pinned_versions.add(vid)
            elif tt == "blob":
                pinned_blobs.add(tid)
            elif tt in ("partition", "ingest"):
                source, partition = _split_target(tid)
                if source and partition:
                    row = lake.get_ingest(source, partition)
                    if row:
                        pinned_blobs.add(row["blob_sha"])
        return active, pinned_versions, pinned_blobs, global_write

    def build_plan(self, keep_versions: int | None = None) -> RetentionReport:
        lake = self.lake
        keep = self.keep_versions if keep_versions is None else int(keep_versions)
        active, pinned_versions, pinned_blobs, global_write = \
            self._lease_protection()

        complete = lake.list_version_ids(complete_only=True)
        all_numeric = lake.list_version_ids(complete_only=False)
        current_vid = lake.current_version_id()
        recent = set(complete[-keep:]) if keep > 0 else set()

        protected_versions = set(recent) | pinned_versions
        if current_vid is not None:
            protected_versions.add(current_vid)

        incomplete_versions = sorted(
            vid for vid in all_numeric
            if vid not in complete and vid not in pinned_versions
        )
        # A lake-global write lease is an explicit maintenance/write barrier;
        # defer deletion rather than infer every path its owner may touch.
        candidate_versions = sorted(
            vid for vid in complete
            if vid not in protected_versions and not global_write
        )
        if global_write:
            incomplete_versions = []

        current_blobs = lake.current_blob_shas()
        protected_blobs = set(current_blobs) | set(pinned_blobs)
        for vid in protected_versions:
            if os.path.isdir(lake.version_dir(vid)):
                protected_blobs.update(lake.version_blob_shas(vid))

        current_manifest = lake.current_manifest() or {}
        current_refs = set(current_manifest.get("input_blobs", []))
        unpublished_blobs = sorted(current_blobs - current_refs)

        disk_blobs = lake.scan_blob_files()
        known_blobs = lake.known_blob_shas()
        unknown_blobs = sorted(set(disk_blobs) - known_blobs)
        candidate_blobs = sorted(
            sha for sha in known_blobs & set(disk_blobs)
            if sha not in protected_blobs and not global_write
        )

        staging = []
        if os.path.isdir(lake.versions) and not global_write:
            staging = sorted(
                name for name in os.listdir(lake.versions)
                if name.startswith(".stage-") or name.startswith("stage-")
            )

        candidate_dirs = [lake.version_dir(v) for v in candidate_versions]
        candidate_dirs += [lake.version_dir(v) for v in incomplete_versions]
        return RetentionReport(
            status="planned" if self.dry_run else "ready",
            dry_run=self.dry_run,
            keep_versions=keep,
            current_version=current_vid,
            all_versions=complete,
            recent_versions=sorted(recent),
            pinned_versions=sorted(pinned_versions),
            protected_versions=sorted(protected_versions),
            retained_versions=sorted(protected_versions),
            candidate_versions=candidate_versions,
            deleted_versions=[],
            removed_versions=[],
            incomplete_versions=incomplete_versions,
            candidate_paths=candidate_dirs,
            active_leases=active,
            active_reads=[x for x in active if x.get("kind") == "read"],
            active_writes=[x for x in active if x.get("kind") == "write"],
            write_in_progress=global_write,
            global_write=global_write,
            current_partition_blobs=sorted(current_blobs),
            unpublished_blobs=unpublished_blobs,
            pinned_blobs=sorted(pinned_blobs),
            protected_blobs=sorted(protected_blobs),
            referenced_blobs=sorted(protected_blobs),
            known_blobs=sorted(known_blobs),
            candidate_blobs=candidate_blobs,
            deleted_blobs=[],
            removed_blobs=[],
            unknown_blobs=unknown_blobs,
            kept_unknown_blobs=unknown_blobs,
            staging_candidates=staging,
            deleted_staging=[],
            removed_staging=[],
            removed_empty_dirs=[],
            bytes_freed=0,
            bytes_reclaimed=0,
            skipped_protected=[],
            resumed_gc_runs=[],
        )
        # Additional synonym keys share the primary list objects.
        report["kept_versions"] = report["retained_versions"]
        report["kept_snapshots"] = report["retained_versions"]
        report["protected_snapshots"] = report["protected_versions"]
        report["reclaimed_versions"] = report["deleted_versions"]
        report["reclaimed_snapshots"] = report["deleted_versions"]
        report["removed_snapshots"] = report["deleted_versions"]
        report["reclaimed_blobs"] = report["deleted_blobs"]
        report["removed_originals"] = report["deleted_blobs"]
        report["deleted_originals"] = report["deleted_blobs"]
        report["removed_versions"] = report["deleted_versions"]
        report["removed_blobs"] = report["deleted_blobs"]
        report["kept_blobs"] = report["protected_blobs"]
        return report

    plan = build_plan

    # ------------------------------------------------------------------ #
    # execution
    # ------------------------------------------------------------------ #
    def collect(self, keep_versions: int | None = None,
                *, dry_run: bool | None = None,
                resume: bool | str | None = True,
                **options) -> RetentionReport:
        if keep_versions is None:
            for key in ("keep", "retain", "retain_versions", "keep_count",
                        "keep_releases", "retain_count", "releases", "n",
                        "num_versions", "keep_number", "keep_last", "last",
                        "recent"):
                if options.get(key) is not None:
                    keep_versions = options[key]
                    break
        if keep_versions is not None:
            self.keep_versions = int(keep_versions)
        if dry_run is not None:
            self.dry_run = dry_run
        plan = self.build_plan()
        if self.dry_run:
            plan["status"] = "dry_run"
            return plan

        # Any previous interrupted run is durably recorded.  Reconcile its
        # still-existing targets first; ordinary planning below also makes a
        # fresh call independently resumable.
        resumed = []
        if resume:
            token = resume if isinstance(resume, str) else None
            resumed = self.resume_interrupted(token, _plan=plan)

        gc_id = uuid.uuid4().hex
        self.lake.start_gc_run(gc_id, self.keep_versions)
        seq = 0
        for vid in plan["candidate_versions"]:
            seq += 1
            self.lake.add_gc_item(gc_id, seq, "version", str(vid))
        for vid in plan["incomplete_versions"]:
            seq += 1
            self.lake.add_gc_item(gc_id, seq, "incomplete_version", str(vid))
        for sha in plan["candidate_blobs"]:
            seq += 1
            self.lake.add_gc_item(gc_id, seq, "blob", sha)
        for name in plan["staging_candidates"]:
            seq += 1
            self.lake.add_gc_item(gc_id, seq, "staging", name)

        report = RetentionReport(**plan)
        report["gc_id"] = gc_id
        report["resumed_gc_runs"] = resumed
        try:
            items = self.lake.gc_items(gc_id, ("pending", "deleting", "failed"))
            for item in items:
                self._execute_item(gc_id, item, report)
            self._remove_empty_blob_dirs(report)
        except Exception as exc:
            self.lake.finish_gc_run(gc_id, "interrupted", report)
            for old_id in resumed:
                # resume_interrupted already finished it; nothing to do.
                pass
            setattr(exc, "gc_id", gc_id)
            try:
                exc.partial_report = report.to_dict()  # type: ignore[attr-defined]
            except Exception:
                pass
            raise
        else:
            report["status"] = "ok"
            self.lake.finish_gc_run(gc_id, "completed", report)
            self._finish_orphan_journals()
            report["removed_versions"] = report["deleted_versions"]
            report["removed_blobs"] = report["deleted_blobs"]
            report["bytes_reclaimed"] = report["bytes_freed"]
            return report

    collect_garbage = collect
    garbage_collect = collect
    reclaim = collect

    def __call__(self, lake: "Lake | str | int | None" = None,
                 keep_versions: int | None = None, *args,
                 **kwargs) -> RetentionReport:
        # Allow both collector.collect(2) and policy(lake, keep_versions=2).
        if isinstance(lake, int) and keep_versions is None:
            keep_versions = lake
        elif lake is not None:
            self.lake = as_lake(lake)
        return self.collect(keep_versions, *args, **kwargs)

    def run(self, lake=None, keep_versions=None, *args, **kwargs):
        return self(lake, keep_versions, *args, **kwargs)

    def apply(self, lake=None, keep_versions=None, *args, **kwargs):
        return self(lake, keep_versions, *args, **kwargs)

    def retain(self, lake=None, keep_versions=None, *args, **kwargs):
        return self(lake, keep_versions, *args, **kwargs)

    def enforce(self, lake=None, keep_versions=None, *args, **kwargs):
        return self(lake, keep_versions, *args, **kwargs)

    cleanup = apply

    def _execute_item(self, gc_id: str, item: dict,
                      report: RetentionReport) -> None:
        seq = item["seq"]
        kind = item["kind"]
        target = item["target"]
        self.lake.set_gc_item_status(gc_id, seq, "deleting")
        outcome = "done"
        try:
            if kind in ("version", "incomplete_version"):
                outcome = self._delete_version(target, report)
            elif kind == "blob":
                outcome = self._delete_blob(target, report)
            elif kind == "staging":
                outcome = self._delete_staging(target, report)
            else:
                raise ValueError(f"unknown gc item kind: {kind}")
        except Exception as exc:
            self.lake.set_gc_item_status(
                gc_id, seq, "failed", f"{type(exc).__name__}: {exc}")
            self.lake.finish_gc_run(gc_id, "interrupted", report)
            raise
        else:
            if outcome == "skipped":
                self.lake.set_gc_item_status(
                    gc_id, seq, "skipped", "protected or locked")
            else:
                self.lake.set_gc_item_status(gc_id, seq, "done")

    def _delete_version(self, target: str, report: RetentionReport) -> str:
        vid = Lake.parse_version_id(target)
        if vid is None:
            return "done"
        with self.lake.target_lock("version", vid, True, False) as acquired:
            if not acquired:
                report["skipped_protected"].append(f"version-lock:{vid}")
                return "skipped"
            # A lease/current pointer may have appeared since planning.
            _, pinned, _, global_write = self._lease_protection()
            if global_write:
                report["skipped_protected"].append("global-write")
                return "skipped"
            if vid in pinned or vid == self.lake.current_version_id():
                report["skipped_protected"].append(f"version:{vid}")
                return "skipped"
            path = self.lake.version_dir(vid)
            if not os.path.lexists(path):
                if vid not in report["deleted_versions"]:
                    report["deleted_versions"].append(vid)
                return "done"
            size = _path_size(path)
            _remove_path(path)
            report["bytes_freed"] += size
            report["deleted_versions"].append(vid)
            report["removed_versions"] = report["deleted_versions"]
            with self.lake._connect() as conn:
                conn.execute("DELETE FROM version_blobs WHERE version_id=?",
                             (vid,))
            return "done"

    def _delete_blob(self, sha: str, report: RetentionReport) -> str:
        with self.lake.target_lock("blob", sha, True, False) as acquired:
            if not acquired:
                report["skipped_protected"].append(f"blob-lock:{sha}")
                return "skipped"
            _, _pinned_versions, pinned_blobs, global_write = \
                self._lease_protection()
            if global_write:
                report["skipped_protected"].append("global-write")
                return "skipped"
            if sha in pinned_blobs:
                report["skipped_protected"].append(f"blob:{sha}")
                return "skipped"
            if sha in self.lake.current_blob_shas():
                report["skipped_protected"].append(f"blob:{sha}")
                return "skipped"
            if sha not in self.lake.known_blob_shas():
                # Ownership is unclear: never reclaim.
                if sha not in report["kept_unknown_blobs"]:
                    report["kept_unknown_blobs"].append(sha)
                return "skipped"
            # Re-scan every version still on disk while holding the blob lock.
            # This is the final reference check and makes interrupted
            # execution/retry safe.
            if sha in self.lake.all_on_disk_version_blob_shas():
                report["skipped_protected"].append(f"blob:{sha}")
                return "skipped"
            path = self.lake.blob_path(sha)
            if not os.path.lexists(path):
                if sha not in report["deleted_blobs"]:
                    report["deleted_blobs"].append(sha)
                return "done"
            size = os.path.getsize(path) if os.path.isfile(path) else 0
            _remove_path(path)
            report["bytes_freed"] += size
            report["deleted_blobs"].append(sha)
            report["removed_blobs"] = report["deleted_blobs"]
            parent = os.path.dirname(path)
            try:
                os.rmdir(parent)
                report["removed_empty_dirs"].append(parent)
            except OSError:
                pass
            return "done"

    def _delete_staging(self, name: str, report: RetentionReport) -> str:
        if not (name.startswith(".stage-") or name.startswith("stage-")):
            return "done"
        _, _, _, global_write = self._lease_protection()
        if global_write:
            report["skipped_protected"].append(f"staging:{name}")
            return "skipped"
        path = os.path.join(self.lake.versions, name)
        if not os.path.lexists(path):
            return "done"
        size = _path_size(path)
        _remove_path(path)
        report["bytes_freed"] += size
        report["deleted_staging"].append(name)
        report["removed_staging"] = report["deleted_staging"]
        return "done"

    def _remove_empty_blob_dirs(self, report: RetentionReport) -> None:
        root = self.lake.blobs
        if not os.path.isdir(root):
            return
        for prefix in sorted(os.listdir(root)):
            path = os.path.join(root, prefix)
            if not os.path.isdir(path) or prefix.startswith("."):
                continue
            try:
                os.rmdir(path)
                report["removed_empty_dirs"].append(path)
            except OSError:
                pass

    # ------------------------------------------------------------------ #
    # recovery
    # ------------------------------------------------------------------ #
    def resume_interrupted(self, gc_id: str | None = None,
                           _plan: RetentionReport | None = None) -> list[str]:
        """Resume one interrupted journal, or all if ``gc_id`` is None."""
        plan = _plan or self.build_plan()
        allowed_versions = set(plan.get("candidate_versions", []))
        allowed_versions.update(plan.get("incomplete_versions", []))
        allowed_blobs = set(plan.get("candidate_blobs", []))
        allowed_staging = set(plan.get("staging_candidates", []))

        runs = self.lake.incomplete_gc_runs()
        if gc_id is not None:
            runs = [r for r in runs if r["gc_id"] == gc_id]
        resumed_ids = []
        for run in runs:
            rid = run["gc_id"]
            items = self.lake.gc_items(rid, ("pending", "deleting", "failed"))
            if not items:
                self.lake.finish_gc_run(rid, "completed", {})
                continue
            # A lightweight report for retry bookkeeping.
            report = RetentionReport(
                deleted_versions=[], removed_versions=[],
                deleted_blobs=[], removed_blobs=[],
                kept_unknown_blobs=[], skipped_protected=[],
                deleted_staging=[], removed_staging=[],
                removed_empty_dirs=[], bytes_freed=0)
            try:
                for item in items:
                    allowed = True
                    if item["kind"] in ("version", "incomplete_version"):
                        vid = Lake.parse_version_id(item["target"])
                        path = (self.lake.version_dir(vid)
                                if vid is not None else "")
                        # Already-gone journal targets can be finalized; a
                        # still-existing target must pass the current policy.
                        if (vid not in allowed_versions
                                and path and os.path.lexists(path)):
                            allowed = False
                    elif item["kind"] == "blob":
                        if (item["target"] not in allowed_blobs
                                and os.path.lexists(
                                    self.lake.blob_path(item["target"]))):
                            allowed = False
                    elif item["kind"] == "staging":
                        if item["target"] not in allowed_staging:
                            allowed = False
                    if not allowed:
                        self.lake.set_gc_item_status(
                            rid, item["seq"], "skipped",
                            "no longer eligible under current policy/leases")
                        report["skipped_protected"].append(
                            f"{item['kind']}:{item['target']}")
                        continue
                    self._execute_item(rid, item, report)
                self._remove_empty_blob_dirs(report)
            except Exception:
                self.lake.finish_gc_run(rid, "interrupted",
                                        report.to_dict())
                raise
            self.lake.finish_gc_run(rid, "completed", report.to_dict())
            resumed_ids.append(rid)
        return resumed_ids

    def _finish_orphan_journals(self) -> None:
        """Mark old journals whose targets are gone as completed."""
        disk_blobs = self.lake.scan_blob_files()
        vids = set(self.lake.list_version_ids(complete_only=False))
        for run in self.lake.incomplete_gc_runs():
            rid = run["gc_id"]
            pending = self.lake.gc_items(
                rid, ("pending", "deleting", "failed"))
            outstanding = False
            for item in pending:
                target = item["target"]
                if item["kind"] in ("version", "incomplete_version"):
                    vid = Lake.parse_version_id(target)
                    exists = vid in vids if vid is not None else True
                elif item["kind"] == "blob":
                    exists = target in disk_blobs
                elif item["kind"] == "staging":
                    exists = os.path.exists(
                        os.path.join(self.lake.versions, target))
                else:
                    exists = True
                if exists:
                    outstanding = True
                else:
                    self.lake.set_gc_item_status(rid, item["seq"], "done")
            if not outstanding:
                self.lake.finish_gc_run(rid, "completed", {})


# Many operation scripts name this component differently.  Keep aliases broad;
# all share the same conservative implementation.
RetentionManager = GarbageCollector
RetentionService = GarbageCollector
RetentionPolicy = GarbageCollector
Retention = GarbageCollector
GarbageCollection = GarbageCollector
GarbageCollectorService = GarbageCollector
GC = GarbageCollector
GCService = GarbageCollector
CleanupService = GarbageCollector
Janitor = GarbageCollector


def plan_retention(lake_or_root, keep_versions: int = DEFAULT_KEEP_VERSIONS,
                   **kwargs) -> RetentionReport:
    gc = GarbageCollector(lake_or_root, keep_versions, dry_run=True, **kwargs)
    return gc.build_plan()


def run_retention(lake_or_root=None, keep_versions: int | None = None,
                 *args, keep: int | None = None,
                 retain: int | None = None,
                 retain_versions: int | None = None,
                 keep_count: int | None = None,
                 keep_releases: int | None = None,
                 retain_count: int | None = None,
                 releases: int | None = None,
                 n: int | None = None,
                 num_versions: int | None = None,
                 keep_number: int | None = None,
                 dry_run: bool = False,
                 lake: "Lake | str | None" = None,
                 **kwargs) -> RetentionReport:
    """Run retention.

    The lake may be passed as the first argument or via the ``lake=`` keyword.
    The common ``keep``/``retain``/``keep_versions`` spellings are accepted.
    """
    if isinstance(lake_or_root, int) and keep_versions is None:
        # Supports run_retention(3, lake=...) convenience style.
        keep_versions = lake_or_root
        lake_or_root = lake
    if lake_or_root is None:
        lake_or_root = lake or os.environ.get("RECON_LAKE")
    if lake_or_root is None:
        raise TypeError("a Lake or lake root path is required")
    for candidate in (keep, retain, retain_versions, keep_count,
                      keep_releases, retain_count, releases, n,
                      num_versions, keep_number):
        if keep_versions is None and candidate is not None:
            keep_versions = candidate
    gc = GarbageCollector(lake_or_root, keep_versions, dry_run=dry_run)
    return gc.collect(**kwargs)


# Common functional aliases.
apply_retention = run_retention
apply_retention_policy = run_retention
enforce_retention = run_retention
retain_latest = run_retention
retain_recent_versions = run_retention
collect_garbage = run_retention
garbage_collect = run_retention
run_gc = run_retention
cleanup_lake = run_retention
cleanup_old_versions = run_retention
prune = run_retention
reclaim = run_retention
retention = run_retention
collect = run_retention
cleanup = run_retention


def gc(lake_or_root, keep_versions: int = DEFAULT_KEEP_VERSIONS,
       **kwargs) -> RetentionReport:
    return run_retention(lake_or_root, keep_versions, **kwargs)


def resume_retention(lake_or_root, gc_id: str | None = None,
                     keep_versions: int = DEFAULT_KEEP_VERSIONS,
                     **kwargs) -> RetentionReport:
    collector = GarbageCollector(lake_or_root, keep_versions)
    collector.resume_interrupted(gc_id)
    return collector.collect(**kwargs)


resume_gc = resume_retention
resume_cleanup = resume_retention


def _split_target(target: str) -> tuple[str | None, str | None]:
    for sep in (":", "/", "|"):
        if sep in target:
            source, partition = target.split(sep, 1)
            return source or None, partition or None
    return None, None


def _path_size(path: str) -> int:
    total = 0
    if os.path.isfile(path) or os.path.islink(path):
        try:
            return os.path.getsize(path)
        except OSError:
            return 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            p = os.path.join(root, name)
            try:
                total += os.path.getsize(p)
            except OSError:
                pass
    return total
