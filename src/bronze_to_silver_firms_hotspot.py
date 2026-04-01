#!/usr/bin/env python3
"""
Create Silver table for NASA FIRMS hotspot data.

Loads Bronze CSV/JSON from data/bronze/raw_hotspot (e.g. firms.modaps.eosdis.nasa.gov/country/),
keeps required columns, combines acq_date + acq_time → timestamp_utc (UTC), drops duplicates
and invalid rows, writes partitioned Parquet to data/silver/firms_hotspot.

No merge with weather. No spatial features. Clean and standardize only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

from src.utils.logger import get_logger, setup_logging

# ── paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
BRONZE_HOTSPOT = DATA_ROOT / "bronze" / "raw_hotspot"
SILVER_HOTSPOT = DATA_ROOT / "silver" / "firms_hotspot"

# Required output columns (after cleaning); type and frp for filtering/aggregation
REQUIRED_COLUMNS = [
    "latitude",
    "longitude",
    "acq_date",
    "acq_time",
    "brightness",
    "confidence",
    "satellite",
    "type",
    "frp",
]

# Canonical silver parquet columns (data only; year/month are partition dirs). Used to normalize
# older partitions that may have been written without type/frp (8 cols) so concat doesn't fail.
SILVER_DATA_COLUMNS = [
    "latitude",
    "longitude",
    "timestamp_utc",
    "acq_date",
    "acq_time",
    "brightness",
    "confidence",
    "satellite",
    "type",
    "frp",
]

# Valid coordinate ranges (WGS84)
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0

log = get_logger(__name__)


def _load_one_csv(filepath: Path) -> pl.DataFrame | None:
    """Load one Bronze CSV; select/rename columns to required set; return None on empty/error."""
    try:
        df = pl.read_csv(filepath, infer_schema_length=10_000)
    except Exception as e:
        log.warning("bronze.read_failed", path=str(filepath), error=str(e))
        return None

    # VIIRS uses bright_ti4; MODIS uses brightness
    if "bright_ti4" in df.columns and "brightness" not in df.columns:
        df = df.with_columns(pl.col("bright_ti4").alias("brightness"))
    if "brightness" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("brightness"))

    needed = [c for c in REQUIRED_COLUMNS if c in df.columns]
    if not needed or "latitude" not in df.columns or "longitude" not in df.columns:
        log.warning("bronze.missing_columns", path=str(filepath), have=df.columns)
        return None

    # type (int) and frp (float) for FIRMS
    if "type" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("type"))
    if "frp" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("frp"))
    out = df.select([c for c in REQUIRED_COLUMNS if c in df.columns])
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            out = out.with_columns(pl.lit(None).alias(c))
    out = out.select(REQUIRED_COLUMNS)
    out = out.with_columns(
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("brightness").cast(pl.Float64),
        pl.col("frp").cast(pl.Float64),
        pl.col("type").cast(pl.Int64),
        pl.col("acq_date").cast(pl.Utf8),
        pl.col("acq_time").cast(pl.Utf8),
        pl.col("confidence").cast(pl.Utf8),
        pl.col("satellite").cast(pl.Utf8),
    )
    return out


def _load_one_json(filepath: Path) -> pl.DataFrame | None:
    """Load one Bronze JSON (e.g. GeoJSON feature list); extract required columns."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.warning("bronze.read_failed", path=str(filepath), error=str(e))
        return None

    features = data.get("features", data) if isinstance(data, dict) else data
    if not isinstance(features, list) or not features:
        return None

    rows = []
    for feat in features:
        props = feat.get("properties", feat) if isinstance(feat, dict) else feat
        geom = feat.get("geometry", {}) if isinstance(feat, dict) else {}
        coords = geom.get("coordinates", [None, None])
        if len(coords) >= 2:
            lon_c, lat_c = coords[0], coords[1]
        else:
            lat_c = props.get("latitude")
            lon_c = props.get("longitude")
        rows.append({
            "latitude": lat_c,
            "longitude": lon_c,
            "acq_date": props.get("acq_date"),
            "acq_time": props.get("acq_time"),
            "brightness": props.get("brightness", props.get("bright_ti4")),
            "confidence": props.get("confidence"),
            "satellite": props.get("satellite"),
            "type": props.get("type"),
            "frp": props.get("frp"),
        })
    df = pl.DataFrame(rows)
    for c in REQUIRED_COLUMNS:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))
    df = df.select(REQUIRED_COLUMNS)
    df = df.with_columns(
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("brightness").cast(pl.Float64),
        pl.col("frp").cast(pl.Float64),
        pl.col("type").cast(pl.Int64),
        pl.col("acq_date").cast(pl.Utf8),
        pl.col("acq_time").cast(pl.Utf8),
        pl.col("confidence").cast(pl.Utf8),
        pl.col("satellite").cast(pl.Utf8),
    )
    return df


BATCH_SIZE = 100  # files per batch to limit memory


def _iter_bronze_files() -> list[Path]:
    """List all Bronze hotspot CSV and JSON files."""
    if not BRONZE_HOTSPOT.exists():
        return []
    out: list[Path] = []
    out.extend(BRONZE_HOTSPOT.rglob("*.csv"))
    out.extend(BRONZE_HOTSPOT.rglob("*.json"))
    return out


def load_bronze_hotspot_batch(paths: list[Path]) -> pl.DataFrame:
    """Load one batch of Bronze files and return a single DataFrame (unified schema)."""
    if not paths:
        return pl.DataFrame(schema={c: pl.Float64 if c in ("latitude", "longitude", "brightness", "frp") else (pl.Int64 if c == "type" else pl.Utf8) for c in REQUIRED_COLUMNS})
    parts: list[pl.DataFrame] = []
    for path in paths:
        one = _load_one_json(path) if path.suffix.lower() == ".json" else _load_one_csv(path)
        if one is not None and not one.is_empty():
            parts.append(one)
    if not parts:
        return pl.DataFrame(schema={c: pl.Float64 if c in ("latitude", "longitude", "brightness", "frp") else (pl.Int64 if c == "type" else pl.Utf8) for c in REQUIRED_COLUMNS})
    return pl.concat(parts)


def build_timestamp_utc(df: pl.DataFrame) -> pl.DataFrame:
    """Combine acq_date + acq_time → timestamp_utc (UTC). acq_time is HHMM as int or string."""
    # Normalize acq_time to 4-digit string (e.g. 852 → "0852", 1950 → "1950")
    acq_time_str = pl.col("acq_time").cast(pl.Float64).fill_null(0).cast(pl.Int64).cast(pl.Utf8).str.zfill(4)
    hh = acq_time_str.str.slice(0, 2)
    mm = acq_time_str.str.slice(2, 4)
    datetime_str = pl.col("acq_date").cast(pl.Utf8) + " " + hh + ":" + mm + ":00"
    return df.with_columns(
        pl.col("acq_date").cast(pl.Utf8),
        pl.col("acq_time").cast(pl.Utf8),
        datetime_str.alias("_dt_str"),
    ).with_columns(
        pl.col("_dt_str").str.to_datetime(format="%Y-%m-%d %H:%M:%S").dt.replace_time_zone("UTC").alias("timestamp_utc"),
    ).drop("_dt_str")


def clean_hotspot(df: pl.DataFrame) -> pl.DataFrame:
    """Remove duplicates, missing lat/lon, invalid coordinates."""
    if df.is_empty():
        return df
    # Drop null lat/lon
    df = df.filter(pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null())
    # Valid WGS84
    df = df.filter(
        (pl.col("latitude") >= LAT_MIN) & (pl.col("latitude") <= LAT_MAX)
        & (pl.col("longitude") >= LON_MIN) & (pl.col("longitude") <= LON_MAX)
    )
    # Duplicates (all columns)
    df = df.unique()
    return df


def write_silver_partitioned_batch(df: pl.DataFrame, batch_id: int) -> None:
    """Write one batch to data/silver/firms_hotspot with year/month partition (batch part files)."""
    if df.is_empty() or "timestamp_utc" not in df.columns:
        return
    df = df.with_columns(
        pl.col("timestamp_utc").dt.year().alias("year"),
        pl.col("timestamp_utc").dt.month().alias("month"),
    )
    out_cols = ["latitude", "longitude", "timestamp_utc", "acq_date", "acq_time", "brightness", "confidence", "satellite", "type", "frp", "year", "month"]
    df = df.select([c for c in out_cols if c in df.columns])

    SILVER_HOTSPOT.mkdir(parents=True, exist_ok=True)
    for (year, month), group in df.group_by(["year", "month"]):
        part_dir = SILVER_HOTSPOT / f"year={int(year)}" / f"month={int(month):02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out = group.drop("year", "month")
        path = part_dir / f"part_{int(year)}{int(month):02d}_batch_{batch_id:04d}.parquet"
        out.write_parquet(path, compression="snappy")
    log.info("silver.batch_written", batch_id=batch_id, rows=len(df))


def _normalize_silver_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure df has exactly SILVER_DATA_COLUMNS so concat of old (8-col) and new (10-col) partitions succeeds."""
    for c in SILVER_DATA_COLUMNS:
        if c not in df.columns:
            if c == "timestamp_utc":
                df = df.with_columns(pl.lit(None).cast(pl.Datetime("us")).alias(c))
            elif c in ("latitude", "longitude", "brightness", "frp"):
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
            elif c == "type":
                df = df.with_columns(pl.lit(None).cast(pl.Int64).alias(c))
            else:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    return df.select(SILVER_DATA_COLUMNS)


def merge_partition_dedup() -> tuple[int, str | None, str | None]:
    """Read all batch part files per partition, concat, unique(), write single part; return total rows, date_min, date_max."""
    total = 0
    all_min: list[pl.Series] = []
    all_max: list[pl.Series] = []
    for year_dir in sorted(SILVER_HOTSPOT.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.startswith("year="):
            continue
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.startswith("month="):
                continue
            part_files = list(month_dir.glob("*.parquet"))
            if not part_files:
                continue
            dfs = [_normalize_silver_schema(pl.read_parquet(p)) for p in part_files]
            merged = pl.concat(dfs).unique()
            year = int(year_dir.name.split("=")[1])
            month = int(month_dir.name.split("=")[1])
            out_path = month_dir / f"part_{year}{month:02d}.parquet"
            merged.write_parquet(out_path, compression="snappy")
            for p in part_files:
                if p != out_path:
                    p.unlink()
            total += len(merged)
            all_min.append(merged["timestamp_utc"].min())
            all_max.append(merged["timestamp_utc"].max())
            log.info("silver.merged", partition=f"{year_dir.name}/{month_dir.name}", rows=len(merged))
    date_min = str(min(all_min)) if all_min else None
    date_max = str(max(all_max)) if all_max else None
    return total, date_min, date_max


def main() -> None:
    setup_logging()

    if not BRONZE_HOTSPOT.exists():
        log.error("bronze.path_not_found", path=str(BRONZE_HOTSPOT))
        sys.exit(1)

    log.info("pipeline.start", name="firms_hotspot_bronze_to_silver")

    files = _iter_bronze_files()
    total_loaded = 0
    batch_id = 0
    for i in range(0, len(files), BATCH_SIZE):
        batch_paths = files[i : i + BATCH_SIZE]
        df = load_bronze_hotspot_batch(batch_paths)
        total_loaded += len(df)
        if df.is_empty():
            continue
        df = build_timestamp_utc(df)
        df = clean_hotspot(df)
        if df.is_empty():
            continue
        write_silver_partitioned_batch(df, batch_id)
        batch_id += 1

    log.info("bronze.load_complete", total_rows=total_loaded, files=len(files))

    if batch_id == 0:
        log.warning("pipeline.no_rows_after_clean")
        return

    rows_after_cleaning, date_min, date_max = merge_partition_dedup()
    log.info(
        "pipeline.complete",
        rows=rows_after_cleaning,
        date_min=date_min or "N/A",
        date_max=date_max or "N/A",
    )


if __name__ == "__main__":
    main()
