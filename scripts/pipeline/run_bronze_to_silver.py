#!/usr/bin/env python3
"""
Run Bronze → Silver (weather + wind U/V, AQ, FIRMS).

Execution order:
  1. scripts/merge_wind_uv_to_silver.py — add u10_ms, v10_ms to Silver weather
  2. AQ: already produced by backfill scripts (no separate bronze→silver AQ script)
  3. src/bronze_to_silver_firms_hotspot.py — FIRMS CSV/JSON → Silver Parquet
  4. scripts/silver_firms_daily_aggregate.py — daily FIRMS aggregate

Run from repo root: python scripts/pipeline/run_bronze_to_silver.py. All steps are idempotent.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bronze → Silver transforms")
    parser.add_argument("--weather", action="store_true", help="Merge wind U/V into Silver weather")
    parser.add_argument("--firms", action="store_true", help="Run FIRMS Bronze → Silver")
    parser.add_argument("--all", action="store_true", help="Run all (weather + FIRMS)")
    parser.add_argument("--dry-run", action="store_true", help="Print commands only")
    args = parser.parse_args()

    if args.all:
        args.weather = True
        args.firms = True

    if not args.weather and not args.firms:
        parser.print_help()
        print("\n# Recommended: run with --all")
        sys.exit(0)

    if args.weather:
        cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / "merge_wind_uv_to_silver.py")]
        if args.dry_run:
            print("Would run:", " ".join(cmd))
        else:
            subprocess.run(cmd, check=True, cwd=PROJECT_ROOT)

    if args.firms:
        cmd = [sys.executable, str(PROJECT_ROOT / "src" / "bronze_to_silver_firms_hotspot.py")]
        if args.dry_run:
            print("Would run:", " ".join(cmd))
        else:
            subprocess.run(cmd, check=True, cwd=PROJECT_ROOT)
        # Daily aggregate (type==0, confidence, 500km Bangkok → hotspot_count, frp_sum, etc.)
        cmd2 = [sys.executable, str(PROJECT_ROOT / "scripts" / "silver_firms_daily_aggregate.py")]
        if args.dry_run:
            print("Would run:", " ".join(cmd2))
        else:
            subprocess.run(cmd2, cwd=PROJECT_ROOT)

    print("Bronze → Silver step(s) completed.")


if __name__ == "__main__":
    main()
