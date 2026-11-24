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
from recon.retention import run_retention
import recon.retention as retention_mod

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


def test_retention_gc():
    print("[retention / unreferenced-data reclamation]")
    work = tempfile.mkdtemp()
    try:
        lake = Lake(os.path.join(work, "lake"))
        pipe = Pipeline(lake, {"DEV-A": "R-A"})
        src = os.path.join(work, "inputs")
        os.makedirs(src, exist_ok=True)

        schedule_path = os.path.join(src, "schedule.jsonl")
        with open(schedule_path, "w", encoding="utf-8") as f:
            f.write('{"project":"ALPHA","receivers":["R-A"],'
                    '"nights":["N1"]}\n')
        rep = IngestionReport("schedule")
        pipe.ingest("schedule", "N1", schedule_path, "schedule", rep)

        ledger_path = os.path.join(src, "ledger.parquet")
        pq.write_table(ledger_path, {
            "project": ["ALPHA"],
            "settlement_period": ["P1"],
            "night": ["N1"],
            "occupy_ms": [1000],
        })
        rep = IngestionReport("ledger")
        pipe.ingest("ledger", "P1", ledger_path, "ledger", rep)

        device_path = os.path.join(src, "device.csv")
        device_shas = []
        for i in range(5):
            # The extra batch column changes bytes/blob without changing the
            # reconciled business result.
            with open(device_path, "w", encoding="utf-8") as f:
                f.write("night,receiver_code,occupied_seconds,batch\n")
                f.write(f"N1,DEV-A,1,release-{i}\n")
            rep = IngestionReport(f"device-{i}")
            parsed = pipe.ingest("device", "N1", device_path,
                                 f"device-{i}", rep)
            check(parsed is not None, f"retention fixture ingest {i} accepted")
            device_shas.append(rep.accepted[-1][2])
            pipe.rerun(note=f"release-{i + 1}")

        check(lake.list_version_ids() == [1, 2, 3, 4, 5],
              "five published releases exist for retention fixture")

        # Pause a reader on the oldest release and start streaming its rows.
        old = lake.open_snapshot(1)
        old_groups = old.list_groups()
        old_rows = old.query_pairs()
        paused_iter = old.iter_pairs()
        first_row = next(paused_iter)
        check(old.version_id == 1 and len(old_groups) == 1
              and len(old_rows) == 1 and first_row["pair"] == "ALPHA|R-A|N1",
              "reader opened and paused on old v1 snapshot")

        # A newly received replacement has not been rerun/published yet.
        with open(device_path, "w", encoding="utf-8") as f:
            f.write("night,receiver_code,occupied_seconds,batch\n")
            f.write("N1,DEV-A,1,unpublished\n")
        rep = IngestionReport("unpublished")
        pipe.ingest("device", "N1", device_path, "unpublished", rep)
        unpublished_sha = rep.accepted[-1][2]

        # An unregistered/manually placed old original has unknown ownership.
        unknown_sha, unknown_path = lake.store_blob(b"orphan with no attribution")

        plan = run_retention(lake, keep_versions=2, dry_run=True)
        check(plan["candidate_versions"] == [2, 3],
              "dry-run plans only versions outside the newest two")
        check(os.path.isdir(lake.version_dir(2)),
              "dry-run does not delete planned snapshots")

        # Interrupt after physically removing the first planned snapshot.
        real_remove = retention_mod._remove_path
        interrupted = {}

        def remove_after_v2(path):
            real_remove(path)
            if os.path.basename(os.path.normpath(path)) == "v2":
                interrupted["happened"] = True
                raise RuntimeError("simulated cleanup interruption")

        retention_mod._remove_path = remove_after_v2
        try:
            run_retention(lake, keep_versions=2)
        except RuntimeError:
            pass
        finally:
            retention_mod._remove_path = real_remove

        check(interrupted.get("happened") is True,
              "fixture injected a cleanup interruption after v2")
        check(not os.path.exists(lake.version_dir(2))
              and os.path.isdir(lake.version_dir(3))
              and os.path.isdir(lake.version_dir(1)),
              "interruption leaves partial cleanup while pinned v1 survives")

        # A second offline run must reconcile the journal and continue.
        report = run_retention(lake, keep_versions=2)
        check(report["status"] == "ok", "interrupted retention run resumes")
        check(not os.path.exists(lake.version_dir(2))
              and not os.path.exists(lake.version_dir(3)),
              "resumed cleanup removes remaining obsolete snapshot")
        check(os.path.isdir(lake.version_dir(1)),
              "paused old snapshot remains available during cleanup")

        # The paused stream and fresh reads of the old snapshot finish.
        check(list(paused_iter) == [], "paused old-version iterator runs to end")
        after_rows = old.query_pairs()
        check(len(after_rows) == 1
              and after_rows[0]["pair"] == old_rows[0]["pair"],
              "old version query still completes after other data is reclaimed")

        check(not os.path.exists(lake.blob_path(device_shas[1]))
              and not os.path.exists(lake.blob_path(device_shas[2])),
              "confirmed unreferenced originals from deleted releases reclaimed")
        check(os.path.exists(lake.blob_path(device_shas[0])),
              "original used by paused old release protected")
        check(os.path.exists(lake.blob_path(device_shas[3]))
              and os.path.exists(lake.blob_path(device_shas[4])),
              "originals used by retained releases protected")
        check(os.path.exists(lake.blob_path(unpublished_sha)),
              "received but unpublished partition blob protected")
        check(os.path.exists(unknown_path) and unknown_sha in report["unknown_blobs"],
              "unattributed old original retained")

        # Once the old query closes, the extra pin is released and a later
        # retention pass can reclaim that snapshot and its unique blob.
        old.close()
        run_retention(lake, keep_versions=2)
        check(not os.path.exists(lake.version_dir(1))
              and not os.path.exists(lake.blob_path(device_shas[0])),
              "closed old snapshot becomes reclaimable")
        check(os.path.exists(unknown_path),
              "unknown-attribution original remains after reader closes")

        # An active write lease on one known obsolete blob protects it while
        # retention still completes and protects the newest unpublished blob.
        with open(device_path, "w", encoding="utf-8") as f:
            f.write("night,receiver_code,occupied_seconds,batch\n")
            f.write("N1,DEV-A,1,write-lock-1\n")
        rep_w1 = IngestionReport("write-1")
        pipe.ingest("device", "N1", device_path, "write-1", rep_w1)
        write_locked_sha = rep_w1.accepted[-1][2]

        with open(device_path, "w", encoding="utf-8") as f:
            f.write("night,receiver_code,occupied_seconds,batch\n")
            f.write("N1,DEV-A,1,write-lock-2\n")
        rep_w2 = IngestionReport("write-2")
        pipe.ingest("device", "N1", device_path, "write-2", rep_w2)
        write_current_sha = rep_w2.accepted[-1][2]

        with lake.write_lease("blob", write_locked_sha):
            locked_report = run_retention(lake, keep_versions=2)
            check(os.path.exists(lake.blob_path(write_locked_sha)),
                  "blob with an active write lease protected")
            check(write_locked_sha in locked_report["protected_blobs"],
                  "retention plan records write-protected blob")
        check(os.path.exists(lake.blob_path(write_current_sha)),
              "newest received partition remains protected during write test")
        run_retention(lake, keep_versions=2)
        check(not os.path.exists(lake.blob_path(write_locked_sha)),
              "released write-lease blob can then be reclaimed")
        check(os.path.exists(lake.blob_path(write_current_sha)),
              "current received blob remains after later retention")

        check(len(QueryService(lake).query_pairs()) == 1,
              "current published version remains queryable after retention")
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main():
    t0 = time.time()
    test_parquet()
    test_maxflow()
    test_parsers()
    test_lifecycle()
    test_retention_gc()
    print(f"\nALL TESTS PASSED in {time.time() - t0:.2f}s ({PASS} assertions)")


if __name__ == "__main__":
    main()
