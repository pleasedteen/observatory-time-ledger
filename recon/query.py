"""Read-only query layer over the currently published complete version.

Every read resolves the single ``current`` version snapshot, so a query
never observes a half-published state. Cross-night queries are served from
the same snapshot, giving repeatable reads even while a new version is being
published.
"""
from __future__ import annotations

from .storage import Lake


class QueryService:
    def __init__(self, lake: Lake) -> None:
        self.lake = lake

    def _docs(self) -> list[dict]:
        return self.lake.read_all_group_docs()

    def snapshot(self) -> dict:
        manifest = self.lake.current_manifest()
        if manifest is None:
            return {"published": False, "groups": []}
        return {"published": True, **manifest}

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
        return self.lake.read_group_doc(group_id)

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
        """Input files and raw record locations used by a group."""
        d = self.lake.read_group_doc(group_id)
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
        }
