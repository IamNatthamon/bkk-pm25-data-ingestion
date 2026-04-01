#!/usr/bin/env python3
"""
Run Bronze ingestion (weather, AQ, FIRMS).

Execution order:
  1. Weather: backfill_weather_with_wind_uv.py (or existing weather ingestion)
  2. AQ: scripts/ingestion/backfill_5years.py, run_backfill_aq_2023_fast.py, backfill_missing_months.py
  3. FIRMS: download to data/bronze/raw_hotspot (manual or external script)

Run from repo root: python scripts/pipeline/run_ingestion.py

This script invokes the ingestion steps or prints the commands.
Does not overwrite historical data; all backfills are idempotent (skip existing).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bronze layer ingestion")
    parser.add_argument("--weather", action="store_true", help="Run weather backfill (wind U/V)")
    parser.add_argument("--aq", action="store_true", help="Run AQ backfill (2019–2023)")
    parser.add_argument("--dry-run", action="store_true", help="Print commands only")
    args = parser.parse_args()

    if not args.weather and not args.aq:
        parser.print_help()
        print("\n# Recommended: run with --weather and/or --aq")
        print("# FIRMS: download CSVs to data/bronze/raw_hotspot (see docs)")
        sys.exit(0)

    if args.weather:
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "src/past/backfill_weather_with_wind_uv.py"),
        ]
        if args.dry_run:
            print("Would run:", " ".join(cmd))
        else:
            subprocess.run(cmd, check=True, cwd=PROJECT_ROOT)

    if args.aq:
        # Default: fast AQ backfill for 2023
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "scripts/ingestion/run_backfill_aq_2023_fast.py"),
        ]
        if args.dry_run:
            print("Would run:", " ".join(cmd))
        else:
            subprocess.run(cmd, check=True, cwd=PROJECT_ROOT)

    print("Bronze ingestion step(s) completed.")


if __name__ == "__main__":
    main()
