#!/usr/bin/env python3
"""
Validate Bronze Weather Data (Open-Meteo)

Checks:
- File integrity
- Missing U/V
- Hourly length
- Time continuity
- NaN values
- Extreme values
"""

import gzip
import json
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

BRONZE_ROOT = Path("data/bronze/openmeteo_weather")

EXPECTED_MIN_HOURS = 2000   # ~ 3 months
EXTREME_THRESHOLD = 200     # km/h unrealistic wind threshold

total_files = 0
corrupt_files = []
missing_uv = []
bad_length = []
gap_files = []
nan_files = []
extreme_files = []

print("=" * 70)
print("Validating Bronze Weather Data")
print("=" * 70)

if not BRONZE_ROOT.exists():
    print("Bronze directory not found:", BRONZE_ROOT)
    sys.exit(1)

for fp in BRONZE_ROOT.rglob("*.json.gz"):
    total_files += 1

    try:
        with gzip.open(fp, "rt", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        corrupt_files.append(fp)
        continue

    hourly = data.get("hourly", {})

    # 1️⃣ Check U/V presence
    if "wind_u_component_10m" not in hourly or \
       "wind_v_component_10m" not in hourly:
        missing_uv.append(fp)
        continue

    times = hourly.get("time", [])
    u = hourly.get("wind_u_component_10m", [])
    v = hourly.get("wind_v_component_10m", [])

    # 2️⃣ Check length
    if len(times) < EXPECTED_MIN_HOURS:
        bad_length.append((fp, len(times)))

    # 3️⃣ Time continuity check
    try:
        df = pd.DataFrame({"time": pd.to_datetime(times)})
        df = df.sort_values("time")
        diffs = df["time"].diff().dropna()
        if not all(diffs == pd.Timedelta(hours=1)):
            gap_files.append(fp)
    except Exception:
        gap_files.append(fp)

    # 4️⃣ NaN check
    if np.isnan(u).any() or np.isnan(v).any():
        nan_files.append(fp)

    # 5️⃣ Extreme value check
    if np.max(np.abs(u)) > EXTREME_THRESHOLD or \
       np.max(np.abs(v)) > EXTREME_THRESHOLD:
        extreme_files.append(fp)

print("\n" + "=" * 70)
print("VALIDATION SUMMARY")
print("=" * 70)

print(f"Total files scanned: {total_files}")
print(f"Corrupt JSON files: {len(corrupt_files)}")
print(f"Missing U/V files: {len(missing_uv)}")
print(f"Bad hourly length: {len(bad_length)}")
print(f"Time gap detected: {len(gap_files)}")
print(f"NaN detected: {len(nan_files)}")
print(f"Extreme wind values: {len(extreme_files)}")

print("=" * 70)

if any([
    corrupt_files,
    missing_uv,
    bad_length,
    gap_files,
    nan_files,
    extreme_files
]):
    print("⚠ Issues detected. Review problematic files.")
else:
    print("✅ Bronze validation PASSED. Ready for Silver transform.")
