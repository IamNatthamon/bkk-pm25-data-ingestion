"""Bronze and Silver validation: schema checks, row counts, critical nulls."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ..utils.logger import get_logger

log = get_logger(__name__)


def log_schema_snapshot(name: str, schema: dict[str, Any] | pl.Schema, row_count: int) -> None:
    """Log a schema snapshot and row count for audit."""
    if hasattr(schema, "items"):
        cols = list(schema.keys())
    else:
        cols = list(schema.names()) if hasattr(schema, "names") else []
    log.info("validation.schema_snapshot", layer=name, columns=cols, row_count=row_count)


def validate_bronze_weather_schema(data: dict[str, Any]) -> tuple[bool, str]:
    """
    Validate a single Bronze weather JSON (Open-Meteo style).
    Returns (ok, message). Empty hourly is allowed (caller may skip).
    """
    if not isinstance(data, dict):
        return False, "not a dict"
    if "latitude" not in data or "longitude" not in data:
        return False, "missing latitude/longitude"
    hourly = data.get("hourly") or {}
    if not isinstance(hourly, dict):
        return False, "hourly not a dict"
    times = hourly.get("time") or []
    if not times and not hourly:
        return True, "empty hourly (allowed)"
    for key in ("temperature_2m", "relative_humidity_2m", "time"):
        if key not in hourly:
            return False, f"hourly missing {key}"
    return True, "ok"


def validate_silver_has_required_columns(
    df: pl.DataFrame,
    required: list[str],
    layer_name: str = "silver",
) -> tuple[bool, list[str]]:
    """
    Check that DataFrame has all required columns.
    Returns (ok, missing_columns).
    """
    have = set(df.columns)
    missing = [c for c in required if c not in have]
    if missing:
        log.warning("validation.missing_columns", layer=layer_name, missing=missing)
        return False, missing
    return True, []


def validate_no_null_critical(df: pl.DataFrame, critical_columns: list[str]) -> tuple[bool, dict[str, int]]:
    """Return (all_ok, null_counts for critical columns)."""
    null_counts = {}
    for c in critical_columns:
        if c in df.columns:
            null_counts[c] = df[c].null_count()
        else:
            null_counts[c] = len(df)
    all_ok = all(n == 0 for n in null_counts.values())
    return all_ok, null_counts
