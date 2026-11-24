#!/usr/bin/env python3
"""Entry point that works under `python3 -I` (isolated mode).

The script's own directory is inserted into sys.path explicitly so the
adjacent `recon` package is importable even when -I suppresses cwd/PYTHONPATH.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from recon.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
