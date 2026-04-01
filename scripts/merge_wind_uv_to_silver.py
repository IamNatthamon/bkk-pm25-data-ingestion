#!/usr/bin/env python3
"""
Merge wind U/V components from Bronze openmeteo_weather into Silver.

Reads Bronze JSON.gz files that contain wind_u_component_10m and wind_v_component_10m,
flattens hourly data, converts to m/s (÷3.6), then LEFT JOINs onto the existing
Silver openmeteo_weather table on (lat, lon, timestamp_utc). Adds u10_ms and v10_ms
(FLOAT32); does not overwrite other columns. Writes back to the same Silver
partitioned location. Incremental-safe and idempotent.
"""

from __future__ import annotations

import gzip
import json
import logging
import sys
from pathlib import Path

import polars as pl

# ── paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
BRONZE_WEATHER = DATA_ROOT / "bronze" / "openmeteo_weather"
SILVER_WEATHER = DATA_ROOT / "silver" / "openmeteo_weather"
STATIONS_PATH = DATA_ROOT / "stations" / "bangkok_stations.parquet"

# km/h → m/s
KMH_TO_MS = 1.0 / 3.6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)
sys.stdout.reconfigure(line_buffering=True)


def _has_wind_uv(filepath: Path) -> bool:
    """True if the Bronze file contains wind_u_component_10m (and thus wind_v)."""
    try:
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return "wind_u_component_10m" in data.get("hourly", {})
    except Exception:
        return False


def _station_idx_from_bronze_path(filepath: Path) -> int | None:
    """Extract station index from path like .../batch_20130401_0000.json.gz -> 0."""
    try:
        # .stem for file.json.gz is "file.json"; strip .json then take last part
        name = filepath.name.removesuffix(".json.gz").removesuffix(".gz")
        return int(name.split("_")[-1])
    except (IndexError, ValueError):
        return None


def _bronze_file_to_uv_df(filepath: Path, stations_df: pl.DataFrame) -> pl.DataFrame | None:
    """
    Read one Bronze weather JSON.gz, flatten hourly, return DataFrame with
    lat, lon, timestamp_utc, u10_ms, v10_ms (FLOAT32). Uses stations_df to emit
    Silver’s (lat, lon) for the same station index so join matches. Returns None if no UV or empty.
    """
    try:
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.warning("bronze.read_failed path=%s error=%s", filepath, e)
        return None

    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    u_kmh = hourly.get("wind_u_component_10m") or []
    v_kmh = hourly.get("wind_v_component_10m") or []

    if not times or "wind_u_component_10m" not in hourly:
        return None

    idx = _station_idx_from_bronze_path(filepath)
    if idx is None or idx >= len(stations_df):
        lat = float(data.get("latitude", 0))
        lon = float(data.get("longitude", 0))
    else:
        lat = float(stations_df["lat"][idx])
        lon = float(stations_df["lon"][idx])

    n = len(times)
    u_kmh = (u_kmh + [None] * n)[:n]
    v_kmh = (v_kmh + [None] * n)[:n]

    df = pl.DataFrame({
        "lat": [lat] * n,
        "lon": [lon] * n,
        "timestamp_utc": times,
        "wind_u_kmh": u_kmh,
        "wind_v_kmh": v_kmh,
    })
    df = df.with_columns(
        pl.col("timestamp_utc").str.to_datetime("%Y-%m-%dT%H:%M").dt.replace_time_zone("UTC").dt.cast_time_unit("ns"),
    )
    df = df.with_columns(
        (pl.col("wind_u_kmh").cast(pl.Float64) * KMH_TO_MS).cast(pl.Float32).alias("u10_ms"),
        (pl.col("wind_v_kmh").cast(pl.Float64) * KMH_TO_MS).cast(pl.Float32).alias("v10_ms"),
    ).select(["lat", "lon", "timestamp_utc", "u10_ms", "v10_ms"])

    return df


def load_uv_from_bronze(stations_df: pl.DataFrame) -> pl.DataFrame:
    """
    Scan Bronze openmeteo_weather for files with wind U/V, flatten hourly,
    return single DataFrame with lat, lon, timestamp_utc, u10_ms, v10_ms (FLOAT32).
    Uses stations_df to emit Silver’s (lat, lon) per station index from path.
    Deduplicated on (lat, lon, timestamp_utc) keep last.
    """
    files = [f for f in BRONZE_WEATHER.rglob("*.json.gz") if _has_wind_uv(f)]
    log.info("bronze.uv_files_found count=%d", len(files))
    if not files:
        return pl.DataFrame(schema={"lat": pl.Float64, "lon": pl.Float64, "timestamp_utc": pl.Datetime("ns", "UTC"), "u10_ms": pl.Float32, "v10_ms": pl.Float32})

    dfs: list[pl.DataFrame] = []
    for fp in files:
        one = _bronze_file_to_uv_df(fp, stations_df)
        if one is not None and not one.is_empty():
            dfs.append(one)

    if not dfs:
        return pl.DataFrame(schema={"lat": pl.Float64, "lon": pl.Float64, "timestamp_utc": pl.Datetime("ns", "UTC"), "u10_ms": pl.Float32, "v10_ms": pl.Float32})

    uv = pl.concat(dfs)
    uv = uv.unique(subset=["lat", "lon", "timestamp_utc"], keep="last")
    log.info("bronze.uv_rows unique_keys=%d", len(uv))
    return uv


# Canonical Silver weather column order (existing + u10_ms, v10_ms before metadata)
SILVER_WEATHER_COLUMNS = [
    "stationID", "lat", "lon", "timestamp_utc", "timestamp_unix_ms",
    "temp_c", "humidity_pct", "pressure_hpa", "precipitation_mm",
    "wind_ms", "wind_dir_deg", "shortwave_radiation_wm2", "cloud_cover_pct",
    "u10_ms", "v10_ms",
    "data_source", "ingestion_timestamp_utc", "load_id", "pipeline_version", "record_hash",
]


def load_silver_weather() -> pl.DataFrame:
    """Load all Silver openmeteo_weather parquets into one DataFrame."""
    parquets = [f for f in SILVER_WEATHER.rglob("*.parquet") if f.suffix == ".parquet" and not f.name.endswith(".md5")]
    if not parquets:
        raise FileNotFoundError(f"No parquet files in {SILVER_WEATHER}")
    log.info("silver.files_loaded count=%d", len(parquets))
    # Read and unify schema: timestamp_utc → ns; add u10_ms/v10_ms if missing; same column order
    parts: list[pl.DataFrame] = []
    for p in parquets:
        part = pl.read_parquet(p)
        if "timestamp_utc" in part.columns:
            part = part.with_columns(pl.col("timestamp_utc").dt.cast_time_unit("ns"))
        for c in SILVER_WEATHER_COLUMNS:
            if c not in part.columns:
                if c in ("u10_ms", "v10_ms"):
                    part = part.with_columns(pl.lit(None).cast(pl.Float32).alias(c))
                else:
                    part = part.with_columns(pl.lit(None).alias(c))
        part = part.select(SILVER_WEATHER_COLUMNS)
        parts.append(part)
    return pl.concat(parts)


def merge_uv_into_silver(silver: pl.DataFrame, uv: pl.DataFrame) -> pl.DataFrame:
    """
    LEFT JOIN uv onto silver on (lat, lon, timestamp_utc). Add u10_ms, v10_ms;
    coalesce with existing columns if present so we do not overwrite with null.
    Deduplicate so no duplicate rows.
    """
    # Join on (lat, lon, timestamp_utc); UV lat/lon aligned with Silver via stations in load_uv_from_bronze
    join_cols = ["lat", "lon", "timestamp_utc"]
    uv_select = uv.select(join_cols + ["u10_ms", "v10_ms"])

    # Left join: silver rows keep; uv adds u10_ms, v10_ms where key matches.
    merged = silver.join(uv_select, on=join_cols, how="left")

    for c in ["u10_ms", "v10_ms"]:
        if f"{c}_right" in merged.columns:
            left_col = pl.col(c) if c in merged.columns else pl.lit(None).cast(pl.Float32)
            merged = merged.with_columns(pl.coalesce(left_col, pl.col(f"{c}_right")).alias(c)).drop(f"{c}_right")
        elif c not in merged.columns:
            merged = merged.with_columns(pl.lit(None).cast(pl.Float32).alias(c))

    merged = merged.unique(subset=join_cols, keep="last")
    return merged


def write_silver_by_partition(merged: pl.DataFrame) -> None:
    """
    Write merged DataFrame back to Silver under year=.../month=... using
    timestamp_utc for partition keys. One file per partition; replace existing
    part files in that partition (incremental-safe).
    """
    if "timestamp_utc" not in merged.columns:
        raise ValueError("merged must have timestamp_utc")
    merged = merged.with_columns(
        pl.col("timestamp_utc").dt.year().alias("_year"),
        pl.col("timestamp_utc").dt.month().alias("_month"),
    )
    for (year, month), group in merged.group_by(["_year", "_month"]):
        part_dir = SILVER_WEATHER / f"year={int(year)}" / f"month={int(month):02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_df = group.drop("_year", "_month")
        out_path = part_dir / f"part_{int(year)}{int(month):02d}_uv_merged.parquet"
        # Remove other part files in this partition to avoid duplicates
        for old in part_dir.glob("*.parquet"):
            if old != out_path:
                old.unlink()
        out_df.write_parquet(out_path, compression="snappy")
        log.info("silver.written partition=year=%s/month=%s path=%s rows=%d", year, month, out_path.name, len(out_df))


def main() -> None:
    if not BRONZE_WEATHER.exists():
        log.error("Bronze path not found: %s", BRONZE_WEATHER)
        sys.exit(1)
    if not SILVER_WEATHER.exists():
        log.error("Silver path not found: %s", SILVER_WEATHER)
        sys.exit(1)

    log.info("=== Merge wind U/V from Bronze into Silver ===")

    if not STATIONS_PATH.exists():
        log.error("Stations file not found: %s", STATIONS_PATH)
        sys.exit(1)
    stations_df = pl.read_parquet(STATIONS_PATH)

    uv = load_uv_from_bronze(stations_df)
    if uv.is_empty():
        log.warning("No Bronze UV data; exiting without changing Silver.")
        return

    silver = load_silver_weather()
    rows_before = len(silver)
    log.info("silver.rows_before merge=%d", rows_before)

    merged = merge_uv_into_silver(silver, uv)
    rows_after = len(merged)
    null_u10 = merged["u10_ms"].null_count()
    null_v10 = merged["v10_ms"].null_count()
    log.info("silver.rows_after merge=%d", rows_after)
    log.info("silver.null_u10=%d null_v10=%d", null_u10, null_v10)

    write_silver_by_partition(merged)

    log.info("=== Merge complete ===")


if __name__ == "__main__":
    main()
