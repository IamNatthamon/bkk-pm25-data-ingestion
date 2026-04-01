"""
Hotspot feature generation from NASA FIRMS data for Bangkok PM2.5 forecasting.

Loads bronze hotspot CSVs, assigns source regions by bounding box, aggregates
by date and region, and produces wide-format features with lags for integration
into the preprocessing pipeline.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from src.utils.logger import get_logger

log = get_logger(__name__)

# Region bounding boxes (min_lat, max_lat, min_lon, max_lon)
REGIONS: dict[str, dict[str, float]] = {
    "thailand": {"min_lat": 5.6, "max_lat": 20.5, "min_lon": 97.3, "max_lon": 105.7},
    "myanmar": {"min_lat": 9.6, "max_lat": 28.6, "min_lon": 92.2, "max_lon": 101.2},
    "laos": {"min_lat": 13.9, "max_lat": 22.5, "min_lon": 100.1, "max_lon": 107.7},
    "cambodia": {"min_lat": 10.3, "max_lat": 14.7, "min_lon": 102.3, "max_lon": 107.6},
    "vietnam": {"min_lat": 8.6, "max_lat": 23.4, "min_lon": 102.1, "max_lon": 109.5},
    "malaysia": {"min_lat": 0.8, "max_lat": 7.4, "min_lon": 99.6, "max_lon": 119.3},
    "indonesia": {"min_lat": -11.0, "max_lat": 6.1, "min_lon": 95.0, "max_lon": 141.0},
    "china_yunnan": {"min_lat": 21.1, "max_lat": 29.2, "min_lon": 97.5, "max_lon": 106.2},
}

# FIRMS confidence string → numeric
CONFIDENCE_MAP: dict[str, float] = {"l": 0.25, "n": 0.5, "h": 0.75}

HOTSPOT_REGION_COLS: list[str] = [
    "hotspot_thailand",
    "hotspot_myanmar",
    "hotspot_laos",
    "hotspot_cambodia",
    "hotspot_vietnam",
    "hotspot_malaysia",
    "hotspot_indonesia",
    "hotspot_china_yunnan",
]

LAG_DAYS: list[int] = [1, 2, 3]

REGION_COUNTRY_FILTER: list[str] = [
    "Thailand", "Myanmar", "Laos", "Cambodia", "Vietnam", "Malaysia", "Indonesia", "China",
]

_EMPTY_SCHEMA = {
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "brightness": pl.Float64,
    "confidence": pl.Float64,
    "acq_date": pl.Date,
    "date": pl.Date,
}


def _empty_hotspot_df() -> pl.DataFrame:
    return pl.DataFrame(schema=_EMPTY_SCHEMA)


def load_hotspot_data(
    bronze_dir: Path,
    file_substrings: list[str] | None = None,
) -> pl.DataFrame:
    """
    Load hotspot CSV files from data/bronze/raw_hotspot/.

    Expects VIIRS/MODIS CSV format with: latitude, longitude, bright_ti4 or brightness,
    confidence, acq_date. Returns a Polars DataFrame with standardized columns.

    Args:
        bronze_dir: Directory containing hotspot CSV files.
        file_substrings: If provided, only load files whose path contains one of these strings.

    Returns:
        DataFrame with columns: latitude, longitude, brightness, confidence, acq_date, date.
    """
    bronze_dir = Path(bronze_dir)
    if not bronze_dir.exists():
        log.warning("hotspot.dir_missing", path=str(bronze_dir))
        return _empty_hotspot_df()

    files = sorted(bronze_dir.rglob("*.csv"))
    if file_substrings:
        files = [f for f in files if any(s in str(f) for s in file_substrings)]
        log.info("hotspot.files_filtered", count=len(files), substrings=file_substrings)

    if not files:
        log.warning("hotspot.no_csv_found", path=str(bronze_dir))
        return _empty_hotspot_df()

    log.info("hotspot.loading", num_files=len(files))
    parts: list[pl.DataFrame] = []

    for fp in files:
        try:
            df = pl.read_csv(fp, infer_schema_length=1000, ignore_errors=True)
        except Exception as exc:
            log.warning("hotspot.file_error", file=fp.name, error=str(exc))
            continue

        if df.is_empty() or "latitude" not in df.columns or "longitude" not in df.columns:
            continue

        # Normalize VIIRS brightness column
        if "bright_ti4" in df.columns and "brightness" not in df.columns:
            df = df.rename({"bright_ti4": "brightness"})

        # Ensure required columns exist
        for col_name in ["brightness", "confidence", "acq_date"]:
            if col_name not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col_name))

        df = df.select(["latitude", "longitude", "brightness", "confidence", "acq_date"])

        # Cast numeric columns
        df = df.with_columns([
            pl.col("latitude").cast(pl.Float64, strict=False),
            pl.col("longitude").cast(pl.Float64, strict=False),
            pl.col("brightness").cast(pl.Float64, strict=False),
        ])

        # Parse acq_date
        df = df.with_columns(
            pl.col("acq_date").cast(pl.Utf8).str.to_date(format="%Y-%m-%d", strict=False)
        )

        # Map confidence string → numeric; default 0.5
        df = df.with_columns(
            pl.col("confidence")
            .cast(pl.Utf8)
            .str.to_lowercase()
            .str.strip_chars()
            .replace(CONFIDENCE_MAP, default=None)
            .cast(pl.Float64)
            .fill_null(0.5)
        )

        # Drop rows with nulls in critical columns
        df = df.drop_nulls(subset=["latitude", "longitude", "acq_date"])

        if df.is_empty():
            continue

        df = df.with_columns(pl.col("acq_date").alias("date"))
        parts.append(df)

    if not parts:
        log.warning("hotspot.no_valid_rows")
        return _empty_hotspot_df()

    out = pl.concat(parts)
    log.info(
        "hotspot.loaded",
        rows=len(out),
        date_min=str(out["date"].min()),
        date_max=str(out["date"].max()),
    )
    return out


def assign_region(
    df: pl.DataFrame,
    regions: dict[str, dict[str, float]] | None = None,
) -> pl.DataFrame:
    """Assign source_region to each row using bounding boxes. Rows outside all regions are dropped."""
    if df.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("source_region"))

    regions = regions or REGIONS

    # Build a region column using a series of when/then expressions
    region_expr = pl.lit(None).cast(pl.Utf8)
    # Iterate in reverse so first match wins (overwritten in reverse = last written wins as first)
    # To make first match win, we build from last to first
    for name, box in reversed(list(regions.items())):
        condition = (
            (pl.col("latitude") >= box["min_lat"])
            & (pl.col("latitude") <= box["max_lat"])
            & (pl.col("longitude") >= box["min_lon"])
            & (pl.col("longitude") <= box["max_lon"])
        )
        region_expr = pl.when(condition).then(pl.lit(name)).otherwise(region_expr)

    df = df.with_columns(region_expr.alias("source_region"))

    before = len(df)
    df = df.filter(pl.col("source_region").is_not_null())
    dropped = before - len(df)

    if dropped:
        log.info("hotspot.outside_regions_dropped", count=dropped)

    log.info(
        "hotspot.regions_assigned",
        rows=len(df),
        regions=df["source_region"].n_unique(),
    )
    return df


def aggregate_hotspots(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate by date and source_region: hotspot_count, avg_brightness, avg_confidence."""
    if df.is_empty() or "source_region" not in df.columns:
        return pl.DataFrame(schema={
            "date": pl.Date,
            "source_region": pl.Utf8,
            "hotspot_count": pl.UInt32,
            "avg_brightness": pl.Float64,
            "avg_confidence": pl.Float64,
        })

    # Cast source_region to Utf8 to avoid object-dtype sort failures
    df = df.with_columns(pl.col("source_region").cast(pl.Utf8))

    return (
        df.group_by(["date", "source_region"])
        .agg(
            pl.col("latitude").count().alias("hotspot_count"),
            pl.col("brightness").mean().alias("avg_brightness"),
            pl.col("confidence").mean().alias("avg_confidence"),
        )
        .sort(["date", "source_region"])
    )


def create_feature_table(agg: pl.DataFrame) -> pl.DataFrame:
    """
    Pivot to wide format: one column per region named hotspot_<region>.
    Fills missing (date, region) combinations with 0.
    """
    if agg.is_empty():
        return pl.DataFrame(schema={"date": pl.Date, **{c: pl.Float64 for c in HOTSPOT_REGION_COLS}})

    # Prefix region names with "hotspot_" before pivoting; use string concat (no lambda)
    agg_prefixed = agg.with_columns(
        (pl.lit("hotspot_") + pl.col("source_region").cast(pl.Utf8)).alias("source_region")
    )

    wide = (
        agg_prefixed.pivot(
            index="date",
            on="source_region",
            values="hotspot_count",
            aggregate_function="sum",
        )
        .sort("date")
    )

    # Ensure all region columns exist, fill missing with 0
    for col in HOTSPOT_REGION_COLS:
        if col not in wide.columns:
            wide = wide.with_columns(pl.lit(0.0).alias(col))

    wide = wide.with_columns([
        pl.col(c).cast(pl.Float64).fill_null(0.0) for c in HOTSPOT_REGION_COLS
    ])

    return wide.select(["date"] + HOTSPOT_REGION_COLS)


def create_lag_features(
    feature_df: pl.DataFrame,
    lag_days: list[int] | None = None,
) -> pl.DataFrame:
    """Add lag features for each region column: hotspot_<region>_lag1, _lag2, _lag3."""
    lag_days = lag_days or LAG_DAYS
    if feature_df.is_empty():
        return feature_df

    lag_exprs = [
        pl.col(col).shift(lag).alias(f"{col}_lag{lag}")
        for col in HOTSPOT_REGION_COLS
        if col in feature_df.columns
        for lag in lag_days
    ]
    return feature_df.with_columns(lag_exprs)


def build_hotspot_feature_table(
    bronze_dir: Path,
    regions: dict[str, dict[str, float]] | None = None,
    lag_days: list[int] | None = None,
    file_substrings: list[str] | None = REGION_COUNTRY_FILTER,
) -> pl.DataFrame:
    """
    End-to-end pipeline: load bronze → assign region → aggregate → wide table → lags.

    Returns a DataFrame with columns: date, hotspot_<region>_cols, *_lag1, *_lag2, *_lag3.
    """
    lag_days = lag_days or LAG_DAYS
    lag_cols = [f"{c}_lag{k}" for c in HOTSPOT_REGION_COLS for k in lag_days]
    empty_schema: dict[str, type] = {"date": pl.Date, **{c: pl.Float64 for c in HOTSPOT_REGION_COLS + lag_cols}}

    raw = load_hotspot_data(bronze_dir, file_substrings=file_substrings)
    if raw.is_empty():
        return pl.DataFrame(schema=empty_schema)

    assigned = assign_region(raw, regions)
    if assigned.is_empty():
        return pl.DataFrame(schema=empty_schema)

    agg = aggregate_hotspots(assigned)
    wide = create_feature_table(agg)
    return create_lag_features(wide, lag_days)


def merge_hotspot_features_into_daily(
    daily_df: pl.DataFrame,
    hotspot_feature_df: pl.DataFrame,
    date_col: str = "date",
) -> pl.DataFrame:
    """
    Left-join hotspot feature table into the main daily DataFrame on date.

    Args:
        daily_df: Main daily DataFrame with a date column.
        hotspot_feature_df: Output of build_hotspot_feature_table() with a 'date' column.
        date_col: Name of the date column in daily_df.

    Returns:
        daily_df with hotspot feature columns joined in (nulls where no hotspot data).
    """
    if daily_df.is_empty():
        return daily_df

    if hotspot_feature_df.is_empty():
        lag_days = LAG_DAYS
        all_hotspot_cols = HOTSPOT_REGION_COLS + [
            f"{c}_lag{k}" for c in HOTSPOT_REGION_COLS for k in lag_days
        ]
        for col in all_hotspot_cols:
            if col not in daily_df.columns:
                daily_df = daily_df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
        return daily_df

    hotspot_cols = [c for c in hotspot_feature_df.columns if c != "date"]

    return daily_df.join(
        hotspot_feature_df.select(["date"] + hotspot_cols),
        left_on=date_col,
        right_on="date",
        how="left",
    )
