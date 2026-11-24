"""Reconciliation engine: pairing, grouping, total checking, feasible bounds.

Glossary
--------
A *pair* is an allowed (project, receiver, night) triple from the schedule;
it is the unit whose occupied milliseconds we bound.

Two independent total sides constrain the pairs:
  * device total  keyed by (receiver, night)        — occupied milliseconds
  * project total keyed by (project, settlement_period) — occupied milliseconds

A pair joins one device total through its (receiver, night) and one project
total through (project, period(night)). Two pairs are *connected* when they
share either side; connected components are the reconciliation *groups*. A
group therefore spans nights whenever a project settlement total (or a
receiver/night device total) is shared across nights.

A group is resolved only when every referenced total exists; otherwise it is
``pending`` (待补). With all totals present but no feasible integer allocation
it is ``isolated`` (整组隔离); otherwise ``feasible`` and every pair receives
an exact [min_ms, max_ms] interval, equal when the value is determined.
"""
from __future__ import annotations

from .maxflow import feasible_bounds
from .storage import canonical_json, sha256_bytes

STATUS_FEASIBLE = "feasible"
STATUS_INFEASIBLE = "isolated"     # 整组隔离
STATUS_PENDING = "pending"         # 待补


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def build_pairs(schedule_records: list[dict]) -> set:
    """Allowed (project, receiver, night) triples from schedule records."""
    pairs = set()
    for rec in schedule_records:
        for night in rec["nights"]:
            for receiver in rec["receivers"]:
                pairs.add((rec["project"], receiver, night))
    return pairs


def build_groups(pairs: set, pair_device: dict, pair_project: dict):
    """Union pairs that share a device total or a project settlement total."""
    uf = UnionFind()
    for p in pairs:
        uf.find(p)

    by_device: dict = {}
    by_project: dict = {}
    for p in pairs:
        by_device.setdefault(pair_device[p], []).append(p)
        by_project.setdefault(pair_project[p], []).append(p)

    for members in by_device.values():
        for m in members[1:]:
            uf.union(members[0], m)
    for members in by_project.values():
        for m in members[1:]:
            uf.union(members[0], m)

    components: dict = {}
    for p in pairs:
        components.setdefault(uf.find(p), set()).add(p)
    return list(components.values())


def reconcile_groups(pairs: set, device_index: dict, project_index: dict,
                     pair_lineage: dict, night_period: dict) -> list[dict]:
    # resolve each pair to its two total keys; pairs whose night has no known
    # period still get a project key placeholder so missing totals surface.
    pair_device = {}
    pair_project = {}
    for (project, receiver, night) in pairs:
        pair_device[(project, receiver, night)] = (receiver, night)
        period = night_period.get(night)
        pair_project[(project, receiver, night)] = (project, period)

    groups = build_groups(pairs, pair_device, pair_project)
    docs = []
    for component in groups:
        docs.append(_reconcile_one(
            component, device_index, project_index, pair_lineage,
            pair_device, pair_project, night_period))
    docs.sort(key=lambda d: d["group_id"])
    return docs


def _group_id(component: set) -> str:
    return "g" + sha256_bytes(canonical_json(sorted(component)))[:16]


def _pair_key(triple) -> str:
    project, receiver, night = triple
    return f"{project}|{receiver}|{night}"


def _reconcile_one(component, device_index, project_index, pair_lineage,
                   pair_device, pair_project, night_period) -> dict:
    gid = _group_id(component)
    nights = sorted({p[2] for p in component})
    pair_keys = sorted(component)
    key_str = [_pair_key(k) for k in pair_keys]

    device_refs = sorted({pair_device[p] for p in component})
    project_refs = sorted({pair_project[p] for p in component})

    missing_device = [k for k in device_refs if k not in device_index]
    # a project ref with period None (night not in any ledger) is also missing
    missing_project = [k for k in project_refs
                       if k[1] is None or k not in project_index]

    lineage = _collect_lineage(component, device_refs, project_refs,
                               device_index, project_index, pair_lineage,
                               night_period)

    base = {
        "group_id": gid,
        "nights": nights,
        "pairs": key_str,
        "lineage": lineage,
    }

    if missing_device or missing_project:
        base["status"] = STATUS_PENDING
        base["missing_totals"] = {
            "device": [list(k) for k in missing_device],
            "project": [list(k) for k in missing_project],
        }
        return base

    supply, demand = [], []
    sup_id, dem_id = {}, {}
    for (r, n) in device_refs:
        sid = f"dev::{r}::{n}"
        sup_id[(r, n)] = sid
        supply.append((sid, device_index[(r, n)]["occupied_ms"]))
    for (p, period) in project_refs:
        did = f"prj::{p}::{period}"
        dem_id[(p, period)] = did
        demand.append((did, project_index[(p, period)]["occupied_ms"]))

    pair_supply, pair_demand = {}, {}
    pair_lower, pair_upper = {}, {}
    for k, ks in zip(pair_keys, key_str):
        project, receiver, night = k
        dkey = (receiver, night)
        pkey = (project, night_period[night])
        pair_supply[ks] = sup_id[dkey]
        pair_demand[ks] = dem_id[pkey]
        cap = min(device_index[dkey]["occupied_ms"],
                  project_index[pkey]["occupied_ms"])
        pair_lower[ks] = 0
        pair_upper[ks] = cap

    status, bounds = feasible_bounds(
        key_str, pair_lower, pair_upper, supply, demand,
        pair_supply, pair_demand)

    base["totals"] = _totals_view(device_refs, project_refs,
                                  device_index, project_index)
    if status == "infeasible":
        base["status"] = STATUS_INFEASIBLE
        base["reason"] = "no integer allocation satisfies all present totals"
        return base

    base["status"] = STATUS_FEASIBLE
    by_night: dict = {}
    pair_results = []
    for k, ks in zip(pair_keys, key_str):
        mn, mx = bounds[ks]
        project, receiver, night = k
        entry = {
            "project": project,
            "receiver": receiver,
            "night": night,
            "pair": ks,
            "min_ms": mn,
            "max_ms": mx,
            "determined": mn == mx,
            "value_ms": mn if mn == mx else None,
        }
        pair_results.append(entry)
        by_night.setdefault(night, []).append(entry)
    base["pair_results"] = pair_results
    base["by_night"] = {n: sorted(entries, key=lambda e: e["pair"])
                        for n, entries in sorted(by_night.items())}
    return base


def _totals_view(device_refs, project_refs, device_index, project_index):
    return {
        "device": [
            {"receiver": r, "night": n,
             "occupied_ms": device_index[(r, n)]["occupied_ms"]}
            for (r, n) in device_refs
        ],
        "project": [
            {"project": p, "settlement_period": period,
             "occupied_ms": project_index[(p, period)]["occupied_ms"]}
            for (p, period) in project_refs
        ],
    }


def _collect_lineage(component, device_refs, project_refs,
                     device_index, project_index, pair_lineage,
                     night_period):
    files = []
    records = []
    seen_files = set()

    def add_file(path):
        if path and path not in seen_files:
            seen_files.add(path)
            files.append(path)

    for triple in sorted(component):
        for loc in pair_lineage.get(triple, []):
            add_file(loc.get("source_path"))
            records.append(loc)

    for ref in device_refs:
        rec = device_index.get(ref)
        if rec:
            add_file(rec.get("_source_path"))
            records.append({
                "source": "device",
                "source_path": rec.get("_source_path"),
                "location": rec.get("_line"),
                "ref": list(ref),
            })

    for ref in project_refs:
        project, period = ref
        rec = project_index.get(ref)
        if rec:
            add_file(rec.get("_source_path"))
            records.append({
                "source": "ledger",
                "source_path": rec.get("_source_path"),
                "location": rec.get("_row"),
                "ref": list(ref),
            })

    return {"files": sorted(seen_files), "records": records}
