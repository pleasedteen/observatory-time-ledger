"""Command-line entry points: ingest, rerun, query, lineage, demo/selftest.

Run ``python3 -I recon_cli.py <command> --help`` for options. The CLI is the
single supported way to drive the lake; all output is UTF-8 JSON or readable
plain text.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .fixtures import RECEIVER_MAP
from .pipeline import Pipeline, IngestionReport
from .query import QueryService
from .storage import Lake


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False))


def _load_receiver_map(path: str | None) -> dict:
    if not path:
        return dict(RECEIVER_MAP)
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, dict):
        return dict(obj)
    # also accept [{"device_code":..,"receiver":..}, ...]
    return {row["device_code"]: row["receiver"] for row in obj}


def cmd_ingest(args) -> int:
    lake = Lake(args.lake)
    pipe = Pipeline(lake, _load_receiver_map(args.receiver_map))
    run_id = "cli-" + os.urandom(6).hex()
    report = IngestionReport(run_id)
    lake.start_run(run_id, f"ingest {args.source} {args.partition}")
    parsed = pipe.ingest(args.source, args.partition, args.file, run_id, report)
    if parsed is None:
        lake.finish_run(run_id, "voided")
        report.status = "voided"
        print(f"REPLACEMENT VOIDED: {args.source}/{args.partition} "
              f"had {len(report.quarantine)} rejected record(s); "
              f"previous partition retained.", file=sys.stderr)
        _print(report.quarantine)
        return 2
    lake.finish_run(run_id, "accepted")
    report.status = "accepted"
    _print(report.to_dict())
    return 0


def cmd_rerun(args) -> int:
    lake = Lake(args.lake)
    pipe = Pipeline(lake, _load_receiver_map(args.receiver_map))
    changed = None
    if args.changed:
        changed = {}
        for token in args.changed:
            source, parts = token.split("=", 1)
            changed[source] = {p for p in parts.split(",") if p}
    report = pipe.rerun(changed_partitions=changed, note="cli rerun")
    _print(report.to_dict())
    return 0 if report.status == "ok" else 1


def cmd_query(args) -> int:
    q = QueryService(Lake(args.lake))
    rows = q.query_pairs(night=args.night, project=args.project,
                         receiver=args.receiver, status=args.status)
    if args.format == "json":
        _print(rows)
    else:
        if not rows:
            print("(no matching pairs)")
            return 0
        hdr = f"{'night':<6}{'project':<10}{'receiver':<10}{'min_ms':>12}{'max_ms':>12}  status"
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            val = (str(r["value_ms"]) if r["determined"]
                   else f"{r['min_ms']}..{r['max_ms']}")
            print(f"{r['night']:<6}{r['project']:<10}{r['receiver']:<10}"
                  f"{r['min_ms']:>12}{r['max_ms']:>12}  {r['group_status']}"
                  + ("" if r["determined"] else "  (interval)"))
    return 0


def cmd_groups(args) -> int:
    q = QueryService(Lake(args.lake))
    _print(q.list_groups(status=args.status))
    return 0


def cmd_group(args) -> int:
    q = QueryService(Lake(args.lake))
    doc = q.get_group(args.group_id)
    if doc is None:
        print(f"group not found: {args.group_id}", file=sys.stderr)
        return 3
    _print(doc)
    return 0


def cmd_lineage(args) -> int:
    q = QueryService(Lake(args.lake))
    info = q.lineage(args.group_id)
    if info is None:
        print(f"group not found: {args.group_id}", file=sys.stderr)
        return 3
    _print(info)
    return 0


def cmd_quarantine(args) -> int:
    q = QueryService(Lake(args.lake))
    _print(q.quarantine())
    return 0


def cmd_summary(args) -> int:
    q = QueryService(Lake(args.lake))
    _print(q.summary())
    return 0


def cmd_snapshot(args) -> int:
    q = QueryService(Lake(args.lake))
    _print(q.snapshot())
    return 0


def cmd_demo(args) -> int:
    """Build offline fixtures into a fresh temp lake and run the full flow."""
    from .selftest import run_demo
    result = run_demo(keep=args.keep)
    _print(result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="recon_cli",
        description="Radio receiver time reconciliation data-lake CLI")
    p.add_argument("--lake", default=os.environ.get("RECON_LAKE", ".lake"),
                   help="lake root directory (default: .lake or $RECON_LAKE)")
    p.add_argument("--receiver-map", default=None,
                   help="JSON file mapping device code -> canonical receiver")
    sub = p.add_subparsers(dest="command", required=True)

    pi = sub.add_parser("ingest", help="ingest one source partition file")
    pi.add_argument("--source", required=True,
                    choices=["device", "schedule", "ledger"])
    pi.add_argument("--partition", required=True)
    pi.add_argument("--file", required=True)
    pi.set_defaults(func=cmd_ingest)

    pr = sub.add_parser("rerun", help="recompute affected groups and publish")
    pr.add_argument("--changed", action="append",
                    help="source=part1,part2 (repeatable); default = all")
    pr.set_defaults(func=cmd_rerun)

    pq_ = sub.add_parser("query", help="query pair min/max occupancy")
    pq_.add_argument("--night")
    pq_.add_argument("--project")
    pq_.add_argument("--receiver")
    pq_.add_argument("--status", choices=["feasible", "isolated", "pending"])
    pq_.add_argument("--format", choices=["table", "json"], default="table")
    pq_.set_defaults(func=cmd_query)

    pg = sub.add_parser("groups", help="list reconciliation groups")
    pg.add_argument("--status", choices=["feasible", "isolated", "pending"])
    pg.set_defaults(func=cmd_groups)

    pgr = sub.add_parser("group", help="show one group document")
    pgr.add_argument("group_id")
    pgr.set_defaults(func=cmd_group)

    pl = sub.add_parser("lineage", help="input files/records for a group")
    pl.add_argument("group_id")
    pl.set_defaults(func=cmd_lineage)

    pqz = sub.add_parser("quarantine", help="list rejected records")
    pqz.set_defaults(func=cmd_quarantine)

    ps = sub.add_parser("summary", help="aggregate counts")
    ps.set_defaults(func=cmd_summary)

    pss = sub.add_parser("snapshot", help="current published version manifest")
    pss.set_defaults(func=cmd_snapshot)

    pd = sub.add_parser("demo", help="offline end-to-end demonstration")
    pd.add_argument("--keep", action="store_true",
                    help="print and keep the temp lake/fixture paths")
    pd.set_defaults(func=cmd_demo)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
