"""Load Silver layer data with schema validation"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ..utils.logger import get_logger
from ..utils.schema import LazyFrame

log = get_logger(__name__)

# Union schema for AQ: some silver files use long names (nitrogen_dioxide_ugm3), others short (no2_ugm3).
# Include both so scan_parquet accepts all files; pipeline coalesces when aggregating.
AQ_SILVER_SCHEMA_UNION: dict[str, pl.DataType] = {
    "stationID": pl.Utf8,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "timestamp_utc": pl.Datetime("us"),  # downcast from ns if needed
    "timestamp_unix_ms": pl.Int64,
    "pm2_5_ugm3": pl.Float64,
    "pm10_ugm3": pl.Float64,
    "nitrogen_dioxide_ugm3": pl.Float64,
    "ozone_ugm3": pl.Float64,
    "sulphur_dioxide_ugm3": pl.Float64,
    "carbon_monoxide_ugm3": pl.Float64,
    "no2_ugm3": pl.Float64,
    "o3_ugm3": pl.Float64,
    "so2_ugm3": pl.Float64,
    "co_ugm3": pl.Float64,
    "data_source": pl.Utf8,
    "ingestion_timestamp_utc": pl.Datetime("us"),
    "load_id": pl.Utf8,
    "pipeline_version": pl.Utf8,
    "record_hash": pl.Utf8,
}


def load_silver_weather(silver_dir: Path) -> LazyFrame:
    """
    Load all weather data from Silver layer (Hive-partitioned Parquet).
    
    Filters out .md5 sidecar files and validates schema.
    """
    if not silver_dir.exists():
        raise FileNotFoundError(f"Silver weather directory not found: {silver_dir}")

    log.info("silver.load.start", source="weather", path=str(silver_dir))

    parquet_files = [
        str(f) for f in silver_dir.rglob("*.parquet") if not f.name.endswith(".md5")
    ]

    if not parquet_files:
        raise ValueError(f"No parquet files found in {silver_dir}")

    log.info(
        "silver.load.files_found",
        source="weather",
        file_count=len(parquet_files),
    )

    # Normalize dtypes across parquet files to avoid SchemaError on collect (ns/μs, Float32/64)
    cast_opts = pl.ScanCastOptions(
        datetime_cast="nanosecond-downcast",
        float_cast="upcast",
    )
    df = pl.scan_parquet(parquet_files, cast_options=cast_opts)

    log.info("silver.load.complete", source="weather", columns=df.collect_schema().names())

    return df


def load_silver_airquality(silver_dir: Path) -> LazyFrame | None:
    """
    Load all air quality data from Silver layer.
    
    Returns None if directory doesn't exist or is empty (expected for current data).
    """
    if not silver_dir.exists():
        log.warning("silver.load.missing", source="airquality", path=str(silver_dir))
        return None

    parquet_files = [
        str(f) for f in silver_dir.rglob("*.parquet") if not f.name.endswith(".md5")
    ]

    if not parquet_files:
        log.warning("silver.load.empty", source="airquality", path=str(silver_dir))
        return None

    log.info(
        "silver.load.start",
        source="airquality",
        file_count=len(parquet_files),
    )

    cast_opts = pl.ScanCastOptions(
        datetime_cast="nanosecond-downcast",
        float_cast="upcast",
    )
    # Union schema + ignore extra so files with either long or short AQ column names all load
    df = pl.scan_parquet(
        parquet_files,
        schema=AQ_SILVER_SCHEMA_UNION,
        missing_columns="insert",
        extra_columns="ignore",
        cast_options=cast_opts,
    )

    log.info("silver.load.complete", source="airquality", columns=df.collect_schema().names())

    return df


def load_stations(stations_path: Path) -> pl.DataFrame:
    """Load station metadata"""
    if not stations_path.exists():
        raise FileNotFoundError(f"Stations file not found: {stations_path}")

    log.info("stations.load", path=str(stations_path))

    df = pl.read_parquet(stations_path)

    log.info("stations.loaded", count=len(df), columns=df.columns)

    return df
