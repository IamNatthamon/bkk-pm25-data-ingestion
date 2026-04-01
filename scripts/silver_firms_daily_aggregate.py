#!/usr/bin/env python3
"""
Aggregate Silver FIRMS hotspot to daily: filter type==0, confidence nominal/high,
spatial filter 500 km Bangkok, then aggregate by date → hotspot_count, frp_sum, frp_mean, weighted_frp_sum.

Reads: data/silver/firms_hotspot (parquet, must have type, frp, latitude, longitude, timestamp_utc or acq_date).
Writes: data/silver/firms_hotspot_daily (parquet, partition year/month, columns: date, hotspot_count, frp_sum, frp_mean, weighted_frp_sum).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
SILVER_FIRMS = DATA_ROOT / "silver" / "firms_hotspot"
SILVER_FIRMS_DAILY = DATA_ROOT / "silver" / "firms_hotspot_daily"

# Bangkok center (WGS84)
BANGKOK_LAT = 13.7563
BANGKOK_LON = 100.5018
RADIUS_KM = 500.0

# type: 0 = vegetation fire (keep); confidence: n/nominal, h/high
TYPE_FILTER = 0
CONFIDENCE_ALLOW = {"n", "h", "nominal", "high"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)
sys.stdout.reconfigure(line_buffering=True)


def run() -> None:
    if not SILVER_FIRMS.exists():
        log.warning("Silver FIRMS path not found: %s", SILVER_FIRMS)
        return

    files = [f for f in SILVER_FIRMS.rglob("*.parquet") if "batch" not in f.name or f.name.endswith("_merged.parquet")]
    if not files:
        files = list(SILVER_FIRMS.rglob("*.parquet"))
    if not files:
        log.warning("No parquet files in %s", SILVER_FIRMS)
        return

    log.info("Loading %d FIRMS silver files", len(files))
    df = pl.read_parquet(files)

    # Date column
    if "timestamp_utc" in df.columns:
        df = df.with_columns(pl.col("timestamp_utc").dt.date().alias("date"))
    elif "acq_date" in df.columns:
        df = df.with_columns(pl.col("acq_date").str.to_date().alias("date"))
    else:
        log.error("No date column (timestamp_utc or acq_date)")
        return

    # Filter type == 0
    if "type" in df.columns:
        df = df.filter(pl.col("type") == TYPE_FILTER)
    # Filter confidence
    if "confidence" in df.columns:
        df = df.filter(pl.col("confidence").cast(pl.Utf8).str.to_lowercase().is_in(list(CONFIDENCE_ALLOW)))

    # 500 km from Bangkok: bounding box (~4.5 deg at this latitude)
    deg_approx = RADIUS_KM / 111.0
    df = df.filter(
        (pl.col("latitude") >= BANGKOK_LAT - deg_approx) & (pl.col("latitude") <= BANGKOK_LAT + deg_approx)
        & (pl.col("longitude") >= BANGKOK_LON - deg_approx) & (pl.col("longitude") <= BANGKOK_LON + deg_approx)
    )

    # Weight for weighted_frp: confidence high > nominal
    if "confidence" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("confidence").cast(pl.Utf8).str.to_lowercase().is_in(["h", "high"]))
            .then(1.2)
            .otherwise(1.0)
            .alias("_w"),
        )
    else:
        df = df.with_columns(pl.lit(1.0).alias("_w"))
    if "frp" not in df.columns:
        df = df.with_columns(pl.lit(0.0).alias("frp"))

    daily = df.group_by("date").agg(
        pl.len().alias("hotspot_count"),
        pl.col("frp").sum().alias("frp_sum"),
        pl.col("frp").mean().alias("frp_mean"),
        (pl.col("frp") * pl.col("_w")).sum().alias("weighted_frp_sum"),
    )

    daily = daily.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
    )

    SILVER_FIRMS_DAILY.mkdir(parents=True, exist_ok=True)
    for (year, month), group in daily.group_by(["year", "month"]):
        part_dir = SILVER_FIRMS_DAILY / f"year={int(year)}" / f"month={int(month):02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out = group.drop("year", "month")
        out.write_parquet(part_dir / f"part_{int(year)}{int(month):02d}.parquet", compression="snappy")
    log.info("Wrote daily aggregate to %s rows=%d", SILVER_FIRMS_DAILY, len(daily))


if __name__ == "__main__":
    run()
