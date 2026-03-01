"""Data quality assessment for Silver layer"""

from __future__ import annotations

import polars as pl

from ..utils.logger import get_logger
from ..utils.schema import DataFrame, LazyFrame

log = get_logger(__name__)


def assess_data_quality(df: LazyFrame) -> dict[str, dict[str, float | int | str]]:
    """
    Comprehensive data quality assessment.
    
    Returns per-column statistics: dtype, null_count, null_pct, min, max, mean
    """
    log.info("quality.assess.start")

    df_collected = df.collect()

    quality_report = {}

    for col in df_collected.columns:
        col_data = df_collected[col]
        dtype = str(col_data.dtype)
        total_count = len(col_data)
        null_count = col_data.null_count()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 0.0

        stats = {
            "dtype": dtype,
            "null_count": null_count,
            "null_pct": round(null_pct, 2),
        }

        if col_data.dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
            stats["min"] = col_data.min()
            stats["max"] = col_data.max()
            stats["mean"] = col_data.mean()

        quality_report[col] = stats

    log.info(
        "quality.assess.complete",
        total_columns=len(quality_report),
        total_rows=total_count,
    )

    return quality_report


def print_quality_report(report: dict[str, dict[str, float | int | str]]) -> None:
    """Pretty print data quality report"""
    print("\n" + "=" * 100)
    print("DATA QUALITY REPORT")
    print("=" * 100)
    print(
        f"{'Column':<30} {'Type':<15} {'Null %':<10} {'Min':<15} {'Max':<15} {'Mean':<15}"
    )
    print("-" * 100)

    for col, stats in report.items():
        null_pct = f"{stats['null_pct']:.1f}%"
        min_val = f"{stats.get('min', 'N/A'):.2f}" if isinstance(stats.get("min"), (int, float)) else "N/A"
        max_val = f"{stats.get('max', 'N/A'):.2f}" if isinstance(stats.get("max"), (int, float)) else "N/A"
        mean_val = f"{stats.get('mean', 'N/A'):.2f}" if isinstance(stats.get("mean"), (int, float)) else "N/A"

        print(
            f"{col:<30} {stats['dtype']:<15} {null_pct:<10} {min_val:<15} {max_val:<15} {mean_val:<15}"
        )

    print("=" * 100 + "\n")


def check_temporal_coverage(df: DataFrame) -> dict[str, str | int]:
    """
    Check temporal coverage and gaps in the dataset.
    
    Returns date range, total days, and gap information.
    """
    log.info("quality.temporal_coverage.start")

    if "date" not in df.columns:
        if "timestamp_utc" in df.columns:
            df = df.with_columns(pl.col("timestamp_utc").dt.date().alias("date"))
        elif "timestamp" in df.columns:
            df = df.with_columns(pl.col("timestamp").dt.date().alias("date"))
        else:
            raise ValueError("No 'date', 'timestamp', or 'timestamp_utc' column found")

    min_date = df["date"].min()
    max_date = df["date"].max()
    unique_dates = df["date"].n_unique()

    if min_date and max_date:
        expected_days = (max_date - min_date).days + 1
        missing_days = expected_days - unique_dates
    else:
        expected_days = 0
        missing_days = 0

    coverage = {
        "min_date": str(min_date),
        "max_date": str(max_date),
        "unique_dates": unique_dates,
        "expected_days": expected_days,
        "missing_days": missing_days,
        "coverage_pct": round(
            (unique_dates / expected_days * 100) if expected_days > 0 else 0.0, 2
        ),
    }

    log.info("quality.temporal_coverage.complete", **coverage)

    return coverage


def check_station_coverage(df: DataFrame) -> dict[str, int]:
    """Check how many stations have data"""
    log.info("quality.station_coverage.start")

    station_col = "stationID" if "stationID" in df.columns else "station_id"
    station_count = df[station_col].n_unique()
    total_records = len(df)

    coverage = {
        "unique_stations": station_count,
        "total_records": total_records,
        "avg_records_per_station": total_records // station_count
        if station_count > 0
        else 0,
    }

    log.info("quality.station_coverage.complete", **coverage)

    return coverage
