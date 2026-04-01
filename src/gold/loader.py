"""
Gold Layer Data Loader — DEPRECATED

Use src.silver_to_gold.loader instead.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import polars as pl

from src.utils.logger import get_logger

log = get_logger(__name__)

warnings.warn(
    "src.gold.loader is deprecated. Use src.silver_to_gold.loader instead.",
    DeprecationWarning,
    stacklevel=2,
)

_RENAME_MAP = {
    "no2_ugm3": "nitrogen_dioxide_ugm3",
    "o3_ugm3": "ozone_ugm3",
    "so2_ugm3": "sulphur_dioxide_ugm3",
    "co_ugm3": "carbon_monoxide_ugm3",
}

_NUMERIC_COLS = [
    "pm2_5_ugm3", "pm10_ugm3", "nitrogen_dioxide_ugm3",
    "ozone_ugm3", "sulphur_dioxide_ugm3", "carbon_monoxide_ugm3",
]


def load_silver_airquality(
    silver_path: Path,
    years: list[int],
) -> pl.DataFrame:
    """Load Silver air quality data for specified years, handling schema mismatches."""
    log.info("loader.load_silver_aq", years=years)

    all_dfs: list[pl.DataFrame] = []

    for year in years:
        year_path = silver_path / f"year={year}"
        if not year_path.exists():
            log.warning("loader.year_missing", year=year)
            continue

        year_dfs: list[pl.DataFrame] = []
        months_loaded = 0

        for month in range(1, 13):
            month_path = year_path / f"month={month:02d}"
            if not month_path.exists():
                continue

            parquet_files = [
                f for f in month_path.iterdir()
                if f.suffix == ".parquet" and not f.name.endswith(".md5")
            ]
            if not parquet_files:
                continue

            month_file_dfs: list[pl.DataFrame] = []
            for pf in parquet_files:
                try:
                    df = pl.read_parquet(pf)

                    for old_name, new_name in _RENAME_MAP.items():
                        if old_name in df.columns:
                            df = df.rename({old_name: new_name})

                    if df.schema.get("timestamp_utc") == pl.Datetime("ns", "UTC"):
                        df = df.with_columns(
                            pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
                        )

                    cast_exprs = [
                        pl.col(c).cast(pl.Float64)
                        for c in _NUMERIC_COLS
                        if c in df.columns
                    ]
                    if cast_exprs:
                        df = df.with_columns(cast_exprs)

                    month_file_dfs.append(df)
                except Exception as exc:
                    log.error("loader.file_error", file=pf.name, error=str(exc))

            if month_file_dfs:
                year_dfs.append(pl.concat(month_file_dfs))
                months_loaded += 1

        if year_dfs:
            year_combined = pl.concat(year_dfs)
            all_dfs.append(year_combined)
            log.info(
                "loader.year_loaded",
                year=year,
                rows=len(year_combined),
                stations=year_combined["stationID"].n_unique(),
                months=months_loaded,
            )

    if not all_dfs:
        raise ValueError("No data loaded. Check Silver layer paths.")

    combined = pl.concat(all_dfs).sort(["stationID", "timestamp_utc"])
    log.info(
        "loader.complete",
        total_rows=len(combined),
        stations=combined["stationID"].n_unique(),
        date_min=str(combined["timestamp_utc"].min()),
        date_max=str(combined["timestamp_utc"].max()),
    )
    return combined


def load_stations(stations_path: Path) -> pl.DataFrame:
    """Load station metadata from Parquet."""
    log.info("loader.load_stations", path=str(stations_path))
    stations = pl.read_parquet(stations_path)
    log.info("loader.stations_loaded", count=len(stations))
    return stations


if __name__ == "__main__":
    from src.utils.logger import setup_logging

    setup_logging()
    try:
        from config.gold import config  # type: ignore[import]

        df = load_silver_airquality(config.silver_aq_path, config.target_years)
        log.info(
            "loader.summary",
            shape=list(df.shape),
            memory_mb=round(df.estimated_size("mb"), 1),
        )
    except ImportError:
        log.warning("loader.config_not_found")
