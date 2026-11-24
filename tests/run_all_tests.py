#!/usr/bin/env python3 -I
"""Offline test suite: run with  python3 -I tests/run_all_tests.py

Exits non-zero on the first failure. Covers the Parquet codec, max-flow
bounds (including exhaustive random verification), parsers/quarantine,
and the full ingestion/rerun/query/snapshot/idempotency lifecycle.
"""
import os
import random
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from recon import parquet as pq
from recon.fixtures import build_fixture, RECEIVER_MAP, FIXTURE_FILES
from recon.maxflow import Dinic, feasible_bounds, _Network
from recon.parsers import parse_device, parse_schedule, parse_ledger
from recon.pipeline import Pipeline, IngestionReport
from recon.query import QueryService
from recon.storage import Lake

PASS = 0


def check(cond, label):
    global PASS
    if not cond:
        raise AssertionError(label)
    PASS += 1
    print(f"  ok - {label}")


def test_parquet():
    print("[parquet codec]")
    cols = {"project": ["伽马", "beta", "", "γ"],
            "period": ["P1", "P1", "P2", "P2"],
            "occupy_ms": [0, 86_400_000, 9_000_000_000, 2 ** 40]}
    for comp in ("gzip", "none"):
        p = os.path.join(tempfile.mkdtemp(), "t.parquet")
        pq.write_table(p, cols, compression=comp)
        check(pq.read_table(p) == cols, f"round-trip {comp} with unicode/big ints")
    p = os.path.join(tempfile.mkdtemp(), "e.parquet")
    pq.write_table(p, {"a": [], "b": []})
    check(pq.read_table(p) == {"a": [], "b": []}, "empty table round-trip")


def test_maxflow():
    print("[maxflow bounds]")
    d = Dinic(4)
    d.add_edge(0, 1, 3); d.add_edge(0, 2, 2)
    d.add_edge(1, 3, 2); d.add_edge(2, 3, 3)
    check(d.max_flow(0, 3) == 4, "dinic basic max flow")

    st, res = feasible_bounds(
        ["x"], {"x": 0}, {"x": 100}, [("R", 100)], [("P", 100)],
        {"x": "R"}, {"x": "P"})
    check(st == "feasible" and res == {"x": (100, 100)}, "single fixed pair")

    st, _ = feasible_bounds(
        ["x"], {"x": 0}, {"x": 100}, [("R", 100)], [("P", 90)],
        {"x": "R"}, {"x": "P"})
    check(st == "infeasible", "unbalanced totals isolated")

    st, res = feasible_bounds(
        ["x", "y"], {"x": 40, "y": 40}, {"x": 100, "y": 100},
        [("R", 100)], [("P", 100)],
        {"x": "R", "y": "R"}, {"x": "P", "y": "P"})
    check(res == {"x": (40, 60), "y": (40, 60)}, "shared-total interval bounds")

    # exhaustive random verification against brute-force pinned feasibility
    random.seed(7)
    for _ in range(25):
        ns, nd = random.randint(1, 3), random.randint(1, 3)
        sups = [f"R{i}" for i in range(ns)]
        dems = [f"P{j}" for j in range(nd)]
        pairs = [(r, p) for r in sups for p in dems if random.random() < 0.7]
        if not pairs:
            continue
        keys = [f"{r}|{p}" for r, p in pairs]
        ps = {f"{r}|{p}": r for r, p in pairs}
        pd = {f"{r}|{p}": p for r, p in pairs}
        supply = [(r, random.randint(0, 20)) for r in sups]
        demand = [(p, random.randint(0, 20)) for p in dems]
        lo = {k: 0 for k in keys}
        hi = {k: random.randint(0, 18) for k in keys}
        net = _Network(keys, lo, hi, supply, demand, ps, pd)
        st, res = feasible_bounds(keys, lo, hi, supply, demand, ps, pd)
        if not net.feasible():
            check(st == "infeasible", "random infeasible reported")
            continue
        for k in keys:
            feas = {v for v in range(lo[k], hi[k] + 1)
                    if net.feasible({**lo, k: v}, {**hi, k: v})}
            check(res[k] == (min(feas), max(feas)),
                  f"random bounds exact for {k}")
    print("  ok - 25 random networks match exhaustive feasibility")


def test_parsers():
    print("[parsers / quarantine]")
    work = tempfile.mkdtemp()
    p = os.path.join(work, "d.csv")
    open(p, "wb").write(
        "night,receiver_code,occupied_seconds\nN1,DEV-X,100\n".encode("gb18030"))
    r = parse_device(p, "N1", {"DEV-A": "R-A"})
    check(not r.ok and "unmapped receiver_code" in r.quarantine[0].reason,
          "unmapped receiver quarantined")

    p2 = os.path.join(work, "d2.csv")
    open(p2, "wb").write(
        "night,receiver_code,occupied_seconds\n夜间,DEV-A,100\n".encode("gb18030"))
    check(parse_device(p2, "N1", {"DEV-A": "R-A"}).ok,
          "GB18030 unicode decoded")

    lp = os.path.join(work, "l.parquet")
    pq.write_table(lp, {"project": ["X"], "settlement_period": ["P"],
                        "night": ["N"], "occupy_ms": [-1]})
    check(not parse_ledger(lp, "P").ok, "negative ledger quarantined")

    jp = os.path.join(work, "s.jsonl")
    open(jp, "wb").write(b'{"project":"A","receivers":["R"],"nights":["N"]}\nBAD\n')
    check(not parse_schedule(jp, "N").ok, "invalid JSONL quarantined")


def _ingest_fixture(pipe, src, run_id, report):
    for source, items in FIXTURE_FILES.items():
        for partition, rel in items:
            pipe.ingest(source, partition, os.path.join(src, rel),
                        run_id, report)


def test_lifecycle():
    print("[end-to-end lifecycle]")
    work = tempfile.mkdtemp()
    src = build_fixture(os.path.join(work, "inputs"))
    lake = Lake(os.path.join(work, "lake"))
    pipe = Pipeline(lake, RECEIVER_MAP)

    _ingest_fixture(pipe, src, "init", IngestionReport("init"))
    pipe.rerun(note="initial")
    q = QueryService(lake)
    s1 = q.summary()
    check(s1["groups_by_status"].get("pending") == 1, "one pending group initially")
    check(s1["groups_by_status"].get("isolated") == 1, "one isolated group")
    check(s1["pairs_bounded"] == 4 and s1["pairs_determined"] == 1,
          "interval and determined pairs present")
    rows = q.query_pairs()
    check(any(r["determined"] for r in rows)
          and any(not r["determined"] for r in rows),
          "min<max interval and equal min=max both observable")

    # cross-night group spans N1 and N2 in one component
    cross = [g for g in q.list_groups(status="feasible")
             if set(g["nights"]) == {"N1", "N2"}]
    check(len(cross) == 1, "cross-night group connected across nights")

    # malformed partition voided, prior state retained
    rep = IngestionReport("bad")
    parsed = pipe.ingest("device", "N3",
                        os.path.join(src, "device/bad/night=N3/part.csv"),
                        "bad", rep)
    check(parsed is None and rep.voided and rep.quarantine,
          "malformed replacement voided with reason retained")
    pipe.rerun(note="after-void")
    check(QueryService(lake).summary()["groups_by_status"].get("pending") == 1,
          "voided ingest leaves pending group unchanged")

    # corrected partition + incremental rerun resolves only that group
    IngestionReport_fix = IngestionReport("fix")
    pipe.ingest("device", "N3",
                os.path.join(src, "device/fixes/night=N3/part.csv"),
                "fix", IngestionReport_fix)
    rr = pipe.rerun(changed_partitions={"device": {"N3"}}, note="fix")
    check(rr.affected_groups == ["g18f36b45e548dc96"],
          "incremental rerun recomputes only the affected group")
    check(QueryService(lake).summary()["groups_by_status"].get("pending") == 0,
          "pending group resolved after correction")

    # lineage resolves files and raw locations
    gid = q.list_groups(status="feasible")[0]["group_id"]
    lin = q.lineage(gid)
    check(bool(lin["files"]) and all("location" in r for r in lin["records"]),
          "lineage lists input files and raw record locations")

    # idempotency
    before = {g["group_id"]: lake.group_version(g["group_id"])
              for g in q.list_groups()}
    pipe.rerun(note="repeat")
    after = {g["group_id"]: lake.group_version(g["group_id"])
             for g in QueryService(lake).list_groups()}
    check(all(before[g]["result_hash"] == after[g]["result_hash"]
              and before[g]["input_hash"] == after[g]["input_hash"]
              for g in before), "identical reruns keep business result+lineage")

    # snapshot isolation / crash recovery
    snap = q.snapshot()
    check(snap["published"] and os.path.isdir(lake.current_version_dir()),
          "current pointer resolves to one complete version")
    pipe.rerun(note="post-crash")
    check(QueryService(lake).snapshot()["group_count"] == snap["group_count"],
          "rerun after simulated interruption restores a complete version")

    # offline originals: rerun from blobs only
    shutil.rmtree(src)
    pipe.rerun(note="offline")
    check(QueryService(lake).summary()["group_total"] == 3,
          "reconciliation still works after source files go offline")
    shutil.rmtree(work, ignore_errors=True)


def main():
    t0 = time.time()
    test_parquet()
    test_maxflow()
    test_parsers()
    test_lifecycle()
    print(f"\nALL TESTS PASSED in {time.time() - t0:.2f}s ({PASS} assertions)")


if __name__ == "__main__":
    main()
