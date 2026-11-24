"""Parsers and validators for the three input sources.

Every parser returns a structured :class:`ParsedPartition`:
  * ``records``  – clean, normalized rows (all times in integer milliseconds)
  * ``quarantine`` – rejected raw rows with a machine-readable reason
  * ``lineage``  – per-record pointers back to the source file and location

A record is rejected (and the whole partition replacement is declared void
by the caller) if an identifier cannot be mapped or a numeric value cannot
be converted. Clean and rejected rows never mix: a partition is either fully
accepted or, if it has any rejection, entirely discarded while the rejected
rows are retained in quarantine for inspection.
"""
from __future__ import annotations

import csv
import io
import json

from . import parquet as pq

SOURCE_DEVICE = "device"
SOURCE_SCHEDULE = "schedule"
SOURCE_LEDGER = "ledger"


class QuarantineRecord:
    def __init__(self, source, partition, source_path, location, raw, reason):
        self.source = source
        self.partition = partition
        self.source_path = source_path
        # location: 1-based line number (csv/jsonl) or row index (parquet)
        self.location = location
        self.raw = raw            # original content as text, best effort
        self.reason = reason

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "partition": self.partition,
            "source_path": self.source_path,
            "location": self.location,
            "raw": self.raw,
            "reason": self.reason,
        }


class ParsedPartition:
    def __init__(self, source, partition, source_path, records, quarantine,
                 columns=None):
        self.source = source
        self.partition = partition
        self.source_path = source_path
        self.records = records            # list[dict] normalized
        self.quarantine = quarantine      # list[QuarantineRecord]
        self.columns = columns or []

    @property
    def ok(self) -> bool:
        return not self.quarantine


# --------------------------------------------------------------------------- #
# Device log: GB18030 CSV, one total occupied-seconds value per receiver/night
# expected header: night, receiver_code, occupied_seconds
# --------------------------------------------------------------------------- #
def parse_device(path: str, partition: str,
                 receiver_map: dict) -> ParsedPartition:
    with open(path, "rb") as f:
        raw_bytes = f.read()
    try:
        text = raw_bytes.decode("gb18030")
    except UnicodeDecodeError as exc:
        q = QuarantineRecord(SOURCE_DEVICE, partition, path, 0,
                             raw_bytes[:200].decode("gb18030", "replace"),
                             f"gb18030 decode failure: {exc}")
        return ParsedPartition(SOURCE_DEVICE, partition, path, [], [q])

    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    records, quarantine = [], []
    if not rows:
        return ParsedPartition(SOURCE_DEVICE, partition, path, [], [])

    header = [h.strip() for h in rows[0]]
    required = ["night", "receiver_code", "occupied_seconds"]
    missing = [c for c in required if c not in header]
    if missing:
        q = QuarantineRecord(SOURCE_DEVICE, partition, path, 1,
                             ",".join(header),
                             f"missing required columns: {missing}")
        return ParsedPartition(SOURCE_DEVICE, partition, path, [], [q])
    idx = {c: header.index(c) for c in required}

    for line_no, row in enumerate(rows[1:], start=2):
        if not row or all(not c.strip() for c in row):
            continue  # blank line
        raw_text = ",".join(row)
        if len(row) <= max(idx.values()):
            quarantine.append(QuarantineRecord(
                SOURCE_DEVICE, partition, path, line_no, raw_text,
                "wrong column count"))
            continue
        night = row[idx["night"]].strip()
        code = row[idx["receiver_code"]].strip()
        sec_text = row[idx["occupied_seconds"]].strip()
        if not night:
            quarantine.append(QuarantineRecord(
                SOURCE_DEVICE, partition, path, line_no, raw_text,
                "empty night"))
            continue
        if code not in receiver_map:
            quarantine.append(QuarantineRecord(
                SOURCE_DEVICE, partition, path, line_no, raw_text,
                f"unmapped receiver_code: {code!r}"))
            continue
        try:
            seconds = int(sec_text)
        except ValueError:
            quarantine.append(QuarantineRecord(
                SOURCE_DEVICE, partition, path, line_no, raw_text,
                f"occupied_seconds not an integer: {sec_text!r}"))
            continue
        if seconds < 0:
            quarantine.append(QuarantineRecord(
                SOURCE_DEVICE, partition, path, line_no, raw_text,
                f"occupied_seconds negative: {seconds}"))
            continue
        records.append({
            "night": night,
            "receiver_code": code,
            "receiver": receiver_map[code],
            "occupied_ms": seconds * 1000,
            "_line": line_no,
        })
    return ParsedPartition(SOURCE_DEVICE, partition, path, records,
                           quarantine, columns=required)


# --------------------------------------------------------------------------- #
# Schedule log: UTF-8 JSON Lines; each line declares candidate receivers and
# the observation nights a project may use.
# --------------------------------------------------------------------------- #
def parse_schedule(path: str, partition: str) -> ParsedPartition:
    records, quarantine = [], []
    with open(path, "rb") as f:
        raw_bytes = f.read()
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        q = QuarantineRecord(SOURCE_SCHEDULE, partition, path, 0,
                             raw_bytes[:200].decode("utf-8", "replace"),
                             f"utf-8 decode failure: {exc}")
        return ParsedPartition(SOURCE_SCHEDULE, partition, path, [], [q])

    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            quarantine.append(QuarantineRecord(
                SOURCE_SCHEDULE, partition, path, line_no, line,
                f"invalid json: {exc.msg}"))
            continue
        if not isinstance(obj, dict):
            quarantine.append(QuarantineRecord(
                SOURCE_SCHEDULE, partition, path, line_no, line,
                "json line is not an object"))
            continue
        project = obj.get("project")
        receivers = obj.get("receivers")
        nights = obj.get("nights")
        if not isinstance(project, str) or not project:
            quarantine.append(QuarantineRecord(
                SOURCE_SCHEDULE, partition, path, line_no, line,
                "missing or invalid 'project'"))
            continue
        if not isinstance(receivers, list) or not receivers or \
                not all(isinstance(r, str) and r for r in receivers):
            quarantine.append(QuarantineRecord(
                SOURCE_SCHEDULE, partition, path, line_no, line,
                "missing or invalid 'receivers' (need non-empty string list)"))
            continue
        if not isinstance(nights, list) or not nights or \
                not all(isinstance(n, str) and n for n in nights):
            quarantine.append(QuarantineRecord(
                SOURCE_SCHEDULE, partition, path, line_no, line,
                "missing or invalid 'nights' (need non-empty string list)"))
            continue
        records.append({
            "project": project,
            "receivers": list(receivers),
            "nights": list(nights),
            "_line": line_no,
        })
    return ParsedPartition(SOURCE_SCHEDULE, partition, path, records,
                           quarantine,
                           columns=["project", "receivers", "nights"])


# --------------------------------------------------------------------------- #
# Settlement ledger: Parquet; actual occupied milliseconds per project for a
# settlement period, optionally tagged with the observation night.
# --------------------------------------------------------------------------- #
LEDGER_REQUIRED = ["project", "settlement_period", "occupy_ms"]


def parse_ledger(path: str, partition: str) -> ParsedPartition:
    try:
        table = pq.read_table(path)
    except Exception as exc:  # malformed parquet: treat whole file as rejected
        q = QuarantineRecord(SOURCE_LEDGER, partition, path, 0, "",
                             f"parquet read failure: {exc}")
        return ParsedPartition(SOURCE_LEDGER, partition, path, [], [q])

    missing = [c for c in LEDGER_REQUIRED if c not in table]
    if missing:
        q = QuarantineRecord(SOURCE_LEDGER, partition, path, 0, "",
                             f"missing required columns: {missing}")
        return ParsedPartition(SOURCE_LEDGER, partition, path, [], [q])

    n = len(table[LEDGER_REQUIRED[0]])
    records, quarantine = [], []
    has_night = "night" in table
    for i in range(n):
        project = table["project"][i]
        period = table["settlement_period"][i]
        ms_val = table["occupy_ms"][i]
        raw = {c: table[c][i] for c in table}
        if not isinstance(project, str) or not project:
            quarantine.append(QuarantineRecord(
                SOURCE_LEDGER, partition, path, i, json.dumps(raw, ensure_ascii=False),
                "missing or invalid 'project'"))
            continue
        if not isinstance(period, str) or not period:
            quarantine.append(QuarantineRecord(
                SOURCE_LEDGER, partition, path, i, json.dumps(raw, ensure_ascii=False),
                "missing or invalid 'settlement_period'"))
            continue
        if not isinstance(ms_val, int) or isinstance(ms_val, bool):
            quarantine.append(QuarantineRecord(
                SOURCE_LEDGER, partition, path, i, json.dumps(raw, ensure_ascii=False),
                f"occupy_ms not an integer: {ms_val!r}"))
            continue
        if ms_val < 0:
            quarantine.append(QuarantineRecord(
                SOURCE_LEDGER, partition, path, i, json.dumps(raw, ensure_ascii=False),
                f"occupy_ms negative: {ms_val}"))
            continue
        rec = {
            "project": project,
            "settlement_period": period,
            "occupy_ms": ms_val,
            "_row": i,
        }
        if has_night:
            night = table["night"][i]
            if not isinstance(night, str) or not night:
                quarantine.append(QuarantineRecord(
                    SOURCE_LEDGER, partition, path, i,
                    json.dumps(raw, ensure_ascii=False),
                    "invalid 'night'"))
                continue
            rec["night"] = night
        records.append(rec)
    return ParsedPartition(SOURCE_LEDGER, partition, path, records,
                           quarantine, columns=list(table.keys()))
