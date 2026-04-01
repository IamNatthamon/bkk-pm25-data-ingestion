#!/usr/bin/env python3
"""Delegate to scripts/pipeline/run_bronze_to_silver.py (run from repo root)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent


def main() -> None:
    script = _ROOT / "scripts" / "pipeline" / "run_bronze_to_silver.py"
    raise SystemExit(
        subprocess.call([sys.executable, str(script), *sys.argv[1:]], cwd=_ROOT)
    )


if __name__ == "__main__":
    main()
