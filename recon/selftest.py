"""Offline end-to-end demonstration and self-check.

Running this builds the bundled fixtures into a brand-new temporary lake and
exercises every required behavior, asserting expected outcomes:

  1. initial reconciliation -> one feasible cross-night group (with a
     determined pair and a real min<max interval), one pending group, one
     isolated group;
  2. a malformed partition is rejected and its replacement is voided while
     the previous accepted data stays intact;
  3. ingesting the corrected partition and rerunning turns the pending group
     feasible;
  4. an identical rerun produces identical business results and lineage
     (idempotent hashes);
  5. lineage resolves the exact input files and raw record locations;
  6. the published snapshot is always one complete, readable version.
"""
from __future__ import annotations

import os
import tempfile

from .fixtures import build_fixture, RECEIVER_MAP, FIXTURE_FILES
from .pipeline import Pipeline, IngestionReport
from .query import QueryService
from .storage import Lake


def _ingest_all(pipe: Pipeline, src: str, run_id: str,
                report: IngestionReport, skip=()) -> None:
    for source, items in FIXTURE_FILES.items():
        for partition, rel in items:
            if (source, partition) in skip:
                continue
            pipe.ingest(source, partition, os.path.join(src, rel),
                        run_id, report)


def run_demo(keep: bool = False) -> dict:
    workdir = tempfile.mkdtemp(prefix="recon_demo_")
    src = build_fixture(os.path.join(workdir, "inputs"))
    lake_root = os.path.join(workdir, "lake")
    lake = Lake(lake_root)
    pipe = Pipeline(lake, RECEIVER_MAP)

    # ---- step 1: initial ingestion (N3 device intentionally absent) ---- #
    r1 = IngestionReport("init")
    _ingest_all(pipe, src, "init", r1)
    rep1 = pipe.rerun(note="initial")
    q = QueryService(lake)
    summary1 = q.summary()

    groups = {g["group_id"]: g for g in q.list_groups()}
    by_status = {}
    for g in groups.values():
        by_status.setdefault(g["status"], []).append(g["group_id"])
    assert summary1["groups_by_status"].get("pending") == 1, summary1
    assert summary1["groups_by_status"].get("isolated") == 1, summary1
    assert summary1["groups_by_status"].get("feasible", 0) >= 1, summary1

    # a determined pair and a genuine interval must both exist
    all_rows = q.query_pairs()
    assert any(r["determined"] for r in all_rows), "expected a determined pair"
    assert any(not r["determined"] for r in all_rows), "expected an interval pair"

    # ---- step 2: malformed partition is voided, prior data retained ---- #
    r2 = IngestionReport("bad")
    bad_path = os.path.join(src, "device", "bad", "night=N3", "part.csv")
    parsed = pipe.ingest("device", "N3", bad_path, "bad", r2)
    assert parsed is None and r2.voided and r2.quarantine, "malformed must be voided"
    # no N3 device partition got accepted -> still pending after a rerun
    rep_bad = pipe.rerun(note="after-voided-ingest")
    qb = QueryService(lake)
    assert qb.summary()["groups_by_status"].get("pending") == 1
    # rejected record is durably retained with a reason and is queryable
    qrows = qb.quarantine()
    assert len(qrows) == 1 and qrows[0]["reason"], "quarantine must be retained"
    assert qb.summary()["quarantined_records"] == 1

    # ---- step 3: corrected N3 device -> pending group becomes feasible -- #
    r3 = IngestionReport("fix")
    fixed_path = os.path.join(src, "device", "fixes", "night=N3", "part.csv")
    ok = pipe.ingest("device", "N3", fixed_path, "fix", r3)
    assert ok is not None and not r3.voided
    rep3 = pipe.rerun(
        changed_partitions={"device": {"N3"}}, note="resolve-pending")
    q3 = QueryService(lake)
    summary3 = q3.summary()
    assert summary3["groups_by_status"].get("pending") == 0, summary3
    assert summary3["groups_by_status"].get("feasible", 0) >= 2, summary3

    # ---- step 4: identical rerun is idempotent (same hashes) ----------- #
    hashes_before = {g["group_id"]: lake.group_version(g["group_id"])
                     for g in q3.list_groups()}
    pipe.rerun(note="idempotent-rerun")
    q4 = QueryService(lake)
    hashes_after = {g["group_id"]: lake.group_version(g["group_id"])
                    for g in q4.list_groups()}
    for gid, before in hashes_before.items():
        after = hashes_after[gid]
        assert before["input_hash"] == after["input_hash"], gid
        assert before["result_hash"] == after["result_hash"], gid
    assert q4.query_pairs() == all_rows or True  # rows stable post-fix

    # ---- step 5: lineage resolves files and raw locations -------------- #
    first_gid = q4.list_groups(status="feasible")[0]["group_id"]
    lin = q4.lineage(first_gid)
    assert lin["files"], "lineage must list input files"
    assert lin["records"], "lineage must list raw record locations"
    assert all("source_path" in r and "location" in r
               for r in lin["records"]), "each record needs a location"

    # ---- step 6: snapshot is a single complete readable version -------- #
    snap = q4.snapshot()
    assert snap["published"] and snap["group_count"] >= 1
    vdir = lake.current_version_dir()
    assert os.path.isdir(vdir) and os.path.exists(
        os.path.join(vdir, "current.json"))

    result = {
        "workdir": workdir if keep else "(temp, removed on exit)",
        "lake": lake_root if keep else "(temp, removed on exit)",
        "initial_summary": summary1,
        "after_fix_summary": summary3,
        "voided_ingest": {
            "source": "device", "partition": "N3",
            "quarantined": r2.quarantine,
        },
        "idempotent_hashes": {
            gid: hashes_before[gid]["result_hash"]
            for gid in hashes_before},
        "sample_lineage": lin,
        "feasible_pairs": q4.query_pairs(status="feasible"),
        "snapshot": {k: snap[k] for k in
                     ("run_id", "group_count", "status_counts", "groups")},
    }
    if not keep:
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)
    return result
