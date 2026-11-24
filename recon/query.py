"""Read-only query layer over the currently published complete version.

Every read resolves the single ``current`` version snapshot, so a query
never observes a half-published state. Cross-night queries are served from
the same snapshot, giving repeatable reads even while a new version is being
published.
"""
from __future__ import annotations

import os
import weakref

from .storage import Lake


class QueryService:
    """Read queries over one snapshot.

    With no explicit version, every operation resolves ``current`` again,
    preserving the original behavior.  Passing ``version``/``version_dir``
    binds the service to one old complete version and takes a retention lease
    by default, so a paused reader prevents garbage collection from removing
    that snapshot while still allowing unrelated garbage to be reclaimed.
    """

    def __init__(self, lake: Lake, version: object = None, *,
                 version_id: int | None = None,
                 version_dir: str | None = None,
                 lease=None, auto_lease: bool = True) -> None:
        if isinstance(lake, (str, os.PathLike)):
            lake = Lake(os.fspath(lake))
        self.lake = lake
        self.lease = lease
        self._finalizer = None
        if version_dir is not None:
            self.version_dir = version_dir
            self.version_id = (version_id
                               if version_id is not None
                               else lake.version_id_from_dir(version_dir))
        elif version is not None or version_id is not None:
            target = version_id if version_id is not None else version
            self.version_id = lake.resolve_version_id(target)
            self.version_dir = lake.version_dir(self.version_id)
        else:
            self.version_id = None
            self.version_dir = None

        if (self.version_dir is not None and self.lease is None
                and auto_lease):
            self.lease = lake.read_lease(self.version_id)
            self._finalizer = weakref.finalize(self, self.lease.release)

    # ------------------------------------------------------------------ #
    # snapshot binding / lifecycle
    # ------------------------------------------------------------------ #
    def open_snapshot(self, version: object = None,
                      ttl_seconds: float | None = None) -> "Snapshot":
        if version is None and self.version_id is not None:
            version = self.version_id
        vid = self.lake.resolve_version_id(version)
        lease = self.lake.read_lease(vid, ttl_seconds=ttl_seconds)
        return Snapshot(self.lake, version_id=vid, lease=lease)

    # Common synonyms.
    snapshot_session = open_snapshot
    snapshot_context = open_snapshot
    at_version = open_snapshot
    for_version = open_snapshot
    use_version = open_snapshot
    open_version = open_snapshot
    at = open_snapshot

    def close(self) -> None:
        if self._finalizer is not None and self._finalizer.alive:
            self._finalizer()
        elif self.lease is not None:
            self.lease.release()

    def __enter__(self) -> "QueryService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _docs(self) -> list[dict]:
        return self.lake.read_all_group_docs(self.version_dir)

    def snapshot(self) -> dict:
        if self.version_dir is not None:
            manifest = self.lake.manifest_for_dir(self.version_dir)
            if manifest is None:
                return {"published": False, "groups": [],
                        "version_id": self.version_id}
            return {
                "published": True,
                "version_id": self.version_id,
                "snapshot_dir": self.version_dir,
                **manifest,
            }
        manifest = self.lake.current_manifest()
        if manifest is None:
            return {"published": False, "groups": []}
        vid = self.lake.current_version_id()
        return {"published": True, "version_id": vid, **manifest}

    def list_groups(self, status: str | None = None) -> list[dict]:
        out = []
        for d in self._docs():
            if status and d["status"] != status:
                continue
            out.append({
                "group_id": d["group_id"],
                "status": d["status"],
                "nights": d["nights"],
                "pair_count": len(d["pairs"]),
            })
        return out

    def get_group(self, group_id: str) -> dict | None:
        return self.lake.read_group_doc(group_id, self.version_dir)

    def query_pairs(self, night: str | None = None,
                    project: str | None = None,
                    receiver: str | None = None,
                    status: str | None = None) -> list[dict]:
        """Flat list of pair occupancy rows with min/max and determined flag."""
        rows = []
        for d in self._docs():
            if status and d["status"] != status:
                continue
            for entry in d.get("pair_results", []):
                if night is not None and entry["night"] != night:
                    continue
                if project is not None and entry["project"] != project:
                    continue
                if receiver is not None and entry["receiver"] != receiver:
                    continue
                rows.append({
                    "group_id": d["group_id"],
                    "group_status": d["status"],
                    **entry,
                })
        rows.sort(key=lambda r: (r["night"], r["project"], r["receiver"]))
        return rows

    def iter_pairs(self, *args, **kwargs):
        """Streaming counterpart to :meth:`query_pairs`.

        A bound snapshot retains its lease until :meth:`close` or context exit,
        so a consumer may pause between rows while retention runs elsewhere.
        """
        for row in self.query_pairs(*args, **kwargs):
            yield row

    def by_night(self, night: str) -> dict:
        """All pair bounds for one night, grouped by owning group."""
        result = {}
        for d in self._docs():
            block = d.get("by_night", {}).get(night)
            if block:
                result[d["group_id"]] = {
                    "status": d["status"],
                    "pairs": block,
                }
        return result

    def lineage(self, group_id: str) -> dict | None:
        """Input files and raw record locations for a group."""
        d = self.lake.read_group_doc(group_id, self.version_dir)
        if d is None:
            return None
        return {
            "group_id": group_id,
            "files": d.get("lineage", {}).get("files", []),
            "records": d.get("lineage", {}).get("records", []),
        }

    def quarantine(self) -> list[dict]:
        # Durable table is authoritative: it includes rejections even when a
        # voided ingest was not followed by another published version.
        return self.lake.all_quarantine()

    def summary(self) -> dict:
        docs = self._docs()
        totals = {"feasible": 0, "isolated": 0, "pending": 0}
        determined = 0
        bounded = 0
        for d in docs:
            totals[d["status"]] = totals.get(d["status"], 0) + 1
            for e in d.get("pair_results", []):
                if e["determined"]:
                    determined += 1
                else:
                    bounded += 1
        return {
            "group_total": len(docs),
            "groups_by_status": totals,
            "pairs_determined": determined,
            "pairs_bounded": bounded,
            "quarantined_records": len(self.lake.all_quarantine()),
            "version_id": self.version_id or self.lake.current_version_id(),
        }


class Snapshot(QueryService):
    """A lease-protected, immutable old query snapshot."""

    def __init__(self, lake: Lake, version_id: int, lease=None) -> None:
        super().__init__(
            lake,
            version_id=version_id,
            version_dir=lake.version_dir(version_id),
            lease=lease,
            auto_lease=lease is None,
        )

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def open_snapshot(lake: Lake, version: object = None,
                  ttl_seconds: float | None = None) -> Snapshot:
    """Module-level convenience wrapper for ``lake.open_snapshot``."""
    return lake.open_snapshot(version, ttl_seconds=ttl_seconds)
