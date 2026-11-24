"""Offline, self-contained input fixtures.

On-disk layout under a generated root (the three real formats, partitioned
exactly as the station provides them):

  device/night=<night>/part.csv      GB18030 CSV, receiver seconds per night
  schedule/night=<night>/part.jsonl  UTF-8 JSON Lines, allowed pairs per night
  ledger/period=<period>/part.parquet Parquet, project occupied-ms per period

Reconciliation scenario
-----------------------
* Cross-night feasible group (settlement period P1 spans nights N1 and N2):
    N1 devices  R-A = 10,000 s, R-B = 4,000 s
    N2 device   R-A =  6,000 s
    project ALPHA period P1 total = 15,000,000 ms (covers N1 and N2)
    project BETA  period P1 total =  5,000,000 ms (N1 only)
    Device side sums to 20,000,000 ms and the project side to 20,000,000 ms,
    so a feasible allocation exists with genuine min<max intervals on the N1
    2x2 pairs and a determined N2 pair.
* Pending group: project GAMMA period P2 has a ledger total for night N3, but
  the N3 device total is missing until a corrected device file is ingested.
* Isolated group: project DELTA night N4 has a project total that disagrees
  with the R-A device total, so no allocation is feasible.

Extras: a corrected N3 device file (resolves the pending group) and a
malformed N3 device file (non-numeric seconds -> quarantine, void replacement).
"""
from __future__ import annotations

import os
import tempfile

from . import parquet as pq

RECEIVER_MAP = {
    "DEV-A": "R-A",
    "DEV-B": "R-B",
    "DEV-C": "R-C",
}


def _write(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def build_fixture(root: str | None = None) -> str:
    root = root or tempfile.mkdtemp(prefix="recon_fixture_")
    dev = os.path.join(root, "device")
    sch = os.path.join(root, "schedule")
    led = os.path.join(root, "ledger")

    # ---------------- device CSV (GB18030), one file per night ----------- #
    n1 = ("night,receiver_code,occupied_seconds\n"
          "N1,DEV-A,10000\n"
          "N1,DEV-B,4000\n")
    _write(os.path.join(dev, "night=N1", "part.csv"), n1.encode("gb18030"))

    n2 = ("night,receiver_code,occupied_seconds\n"
          "N2,DEV-A,6000\n")
    _write(os.path.join(dev, "night=N2", "part.csv"), n2.encode("gb18030"))

    # N4 device total 5,000 s = 5,000,000 ms; DELTA ledger will mismatch.
    n4 = ("night,receiver_code,occupied_seconds\n"
          "N4,DEV-A,5000\n")
    _write(os.path.join(dev, "night=N4", "part.csv"), n4.encode("gb18030"))

    # corrected N3 device file (ingested later to resolve the pending group)
    n3_fixed = ("night,receiver_code,occupied_seconds\n"
                "N3,DEV-C,3000\n")
    _write(os.path.join(dev, "fixes", "night=N3", "part.csv"),
           n3_fixed.encode("gb18030"))

    # malformed N3 device file: non-numeric occupied_seconds -> quarantine
    n3_bad = ("night,receiver_code,occupied_seconds\n"
              "N3,DEV-C,not_a_number\n")
    _write(os.path.join(dev, "bad", "night=N3", "part.csv"),
           n3_bad.encode("gb18030"))

    # ---------------- schedule JSONL (UTF-8), per night ----------------- #
    _write(os.path.join(sch, "night=N1", "part.jsonl"), (
        '{"project": "ALPHA", "receivers": ["R-A", "R-B"], "nights": ["N1"]}\n'
        '{"project": "BETA", "receivers": ["R-A", "R-B"], "nights": ["N1"]}\n'
    ).encode("utf-8"))

    _write(os.path.join(sch, "night=N2", "part.jsonl"), (
        '{"project": "ALPHA", "receivers": ["R-A"], "nights": ["N2"]}\n'
    ).encode("utf-8"))

    _write(os.path.join(sch, "night=N3", "part.jsonl"), (
        '{"project": "GAMMA", "receivers": ["R-C"], "nights": ["N3"]}\n'
    ).encode("utf-8"))

    _write(os.path.join(sch, "night=N4", "part.jsonl"), (
        '{"project": "DELTA", "receivers": ["R-A"], "nights": ["N4"]}\n'
    ).encode("utf-8"))

    # ---------------- ledger Parquet, per settlement period ------------- #
    # P1 spans N1 and N2. Project totals (ms):
    #   ALPHA 15,000,000 (N1 part + fixed 6,000,000 on N2)
    #   BETA   5,000,000 (N1 only)
    p1 = {
        "project": ["ALPHA", "BETA", "ALPHA"],
        "settlement_period": ["P1", "P1", "P1"],
        "night": ["N1", "N1", "N2"],
        "occupy_ms": [9_000_000, 5_000_000, 6_000_000],
    }
    pq.write_table(os.path.join(led, "period=P1", "part.parquet"),
                   p1, compression="gzip")

    # P2: GAMMA N3 total present; N3 device total initially missing -> pending.
    p2 = {
        "project": ["GAMMA"],
        "settlement_period": ["P2"],
        "night": ["N3"],
        "occupy_ms": [3_000_000],
    }
    pq.write_table(os.path.join(led, "period=P2", "part.parquet"),
                   p2, compression="gzip")

    # P3: DELTA N4 project total 9,000,000 vs device 5,000,000 -> isolated.
    p3 = {
        "project": ["DELTA"],
        "settlement_period": ["P3"],
        "night": ["N4"],
        "occupy_ms": [9_000_000],
    }
    pq.write_table(os.path.join(led, "period=P3", "part.parquet"),
                   p3, compression="gzip")

    return root


FIXTURE_FILES = {
    "device": [
        ("N1", "device/night=N1/part.csv"),
        ("N2", "device/night=N2/part.csv"),
        ("N4", "device/night=N4/part.csv"),
    ],
    "schedule": [
        ("N1", "schedule/night=N1/part.jsonl"),
        ("N2", "schedule/night=N2/part.jsonl"),
        ("N3", "schedule/night=N3/part.jsonl"),
        ("N4", "schedule/night=N4/part.jsonl"),
    ],
    "ledger": [
        ("P1", "ledger/period=P1/part.parquet"),
        ("P2", "ledger/period=P2/part.parquet"),
        ("P3", "ledger/period=P3/part.parquet"),
    ],
}
