#!/usr/bin/env python3
"""
Run Silver → Gold (model-ready features).

Uses src.silver_to_gold.pipeline. Optionally runs create_gold_airquality.py
for hourly AQ 2023–2025 to data/gold/airquality_combined.

Execution (from repo root):
  python scripts/pipeline/run_silver_to_gold.py              # daily pipeline → model_ready
  python scripts/pipeline/run_silver_to_gold.py --aq-hourly # also create airquality_combined
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Silver → Gold")
    parser.add_argument("--pipeline", action="store_true", default=True,
                        help="Run daily silver_to_gold pipeline (default: True)")
    parser.add_argument("--no-pipeline", action="store_false", dest="pipeline",
                        help="Skip daily pipeline")
    parser.add_argument("--aq-hourly", action="store_true",
                        help="Also run create_gold_airquality.py (hourly AQ 2023–2025)")
    parser.add_argument("--dry-run", action="store_true", help="Print commands only")
    args = parser.parse_args()

    if args.pipeline:
        run_script = PROJECT_ROOT / "scripts" / "run_silver_to_gold.py"
        if args.dry_run:
            print("Would run:", sys.executable, run_script)
        else:
            subprocess.run([sys.executable, str(run_script)], check=True, cwd=PROJECT_ROOT)

    if args.aq_hourly:
        cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / "create_gold_airquality.py")]
        if args.dry_run:
            print("Would run:", " ".join(cmd))
        else:
            subprocess.run(cmd, check=True, cwd=PROJECT_ROOT)

    print("Silver → Gold step(s) completed.")


if __name__ == "__main__":
    main()
