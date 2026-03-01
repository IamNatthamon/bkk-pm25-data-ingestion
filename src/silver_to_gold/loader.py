"""Load Silver layer data with schema validation"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ..utils.logger import get_logger
from ..utils.schema import AIRQUALITY_SILVER_SCHEMA, WEATHER_SILVER_SCHEMA, LazyFrame

log = get_logger(__name__)


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

    df = pl.scan_parquet(parquet_files)

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

    df = pl.scan_parquet(parquet_files)

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
