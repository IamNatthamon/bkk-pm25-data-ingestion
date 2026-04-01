"""Pydantic schemas and DataFrame validation for the Gold layer."""

from __future__ import annotations

from typing import Any

import polars as pl
from pydantic import BaseModel, Field, field_validator

from src.utils.logger import get_logger

log = get_logger(__name__)


class GoldFeatureRow(BaseModel):
    """Schema for a single Gold layer feature row.

    Physical bounds are derived from AQI standards and sensor capabilities.
    """

    date: str
    stationID: str
    pm2_5_ugm3: float = Field(ge=0.0, le=1500.0)

    @field_validator("pm2_5_ugm3")
    @classmethod
    def validate_pm25_not_nan(cls, v: float) -> float:
        import math

        if math.isnan(v):
            raise ValueError("pm2_5_ugm3 cannot be NaN")
        return v

    @field_validator("stationID")
    @classmethod
    def validate_station_id_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("stationID cannot be empty")
        return v


def validate_gold_schema(
    df: pl.DataFrame,
    required_columns: list[str] | None = None,
) -> tuple[bool, list[str]]:
    """Validate Gold DataFrame schema: required columns, dtypes, null counts.

    Args:
        df: Gold layer DataFrame to validate.
        required_columns: Columns that must be present and non-null.

    Returns:
        (is_valid, list_of_error_messages)
    """
    if required_columns is None:
        required_columns = ["date", "stationID", "pm2_5_ugm3"]

    errors: list[str] = []

    # 1. Required columns present
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    # 2. No nulls in critical columns
    for col in required_columns:
        if col in df.columns:
            null_count = df[col].null_count()
            if null_count > 0:
                errors.append(f"Column '{col}' has {null_count} nulls")

    # 3. PM2.5 physical bounds
    if "pm2_5_ugm3" in df.columns:
        out_of_range = df.filter(
            (pl.col("pm2_5_ugm3") < 0) | (pl.col("pm2_5_ugm3") > 1500)
        )
        if len(out_of_range) > 0:
            errors.append(
                f"pm2_5_ugm3 has {len(out_of_range)} values outside [0, 1500]"
            )

    is_valid = len(errors) == 0
    if is_valid:
        log.info("validation.gold_schema_ok", rows=len(df))
    else:
        log.warning("validation.gold_schema_failed", errors=errors)

    return is_valid, errors


def validate_dataframe_completeness(
    df: pl.DataFrame,
    threshold: float = 0.8,
) -> dict[str, Any]:
    """Check overall data completeness (fraction of non-null values per column).

    Args:
        df: DataFrame to check.
        threshold: Minimum completeness fraction to pass.

    Returns:
        Dict with keys: overall_completeness, column_completeness, failed_columns.
    """
    n = len(df)
    if n == 0:
        return {"overall_completeness": 0.0, "column_completeness": {}, "failed_columns": list(df.columns)}

    completeness = {
        col: 1.0 - df[col].null_count() / n
        for col in df.columns
    }
    failed = [col for col, c in completeness.items() if c < threshold]
    overall = sum(completeness.values()) / len(completeness) if completeness else 0.0

    log.info(
        "validation.completeness",
        overall=round(overall, 3),
        threshold=threshold,
        failed_cols=len(failed),
    )

    return {
        "overall_completeness": overall,
        "column_completeness": completeness,
        "failed_columns": failed,
    }
