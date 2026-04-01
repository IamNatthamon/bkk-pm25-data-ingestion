"""Unit tests for src.validation.schemas — Gold layer schema validation."""

from __future__ import annotations

import polars as pl
import pytest

from src.validation.schemas import (
    validate_dataframe_completeness,
    validate_gold_schema,
)


class TestValidateGoldSchema:
    def test_valid_dataframe_passes(self):
        # Given
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "stationID": ["st01", "st02"],
            "pm2_5_ugm3": [25.0, 45.0],
        })
        # When
        is_valid, errors = validate_gold_schema(df)
        # Then
        assert is_valid
        assert errors == []

    def test_missing_required_column_fails(self):
        df = pl.DataFrame({
            "date": ["2024-01-01"],
            "stationID": ["st01"],
            # pm2_5_ugm3 missing
        })
        is_valid, errors = validate_gold_schema(df)
        assert not is_valid
        assert any("pm2_5_ugm3" in e for e in errors)

    def test_null_in_required_column_fails(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", None],
            "stationID": ["st01", "st02"],
            "pm2_5_ugm3": [25.0, 45.0],
        })
        is_valid, errors = validate_gold_schema(df)
        assert not is_valid
        assert any("null" in e.lower() for e in errors)

    def test_pm25_out_of_range_fails(self):
        df = pl.DataFrame({
            "date": ["2024-01-01"],
            "stationID": ["st01"],
            "pm2_5_ugm3": [99999.0],
        })
        is_valid, errors = validate_gold_schema(df)
        assert not is_valid
        assert any("pm2_5_ugm3" in e for e in errors)

    def test_pm25_zero_is_valid(self):
        df = pl.DataFrame({
            "date": ["2024-01-01"],
            "stationID": ["st01"],
            "pm2_5_ugm3": [0.0],
        })
        is_valid, errors = validate_gold_schema(df)
        assert is_valid

    def test_custom_required_columns(self):
        df = pl.DataFrame({"a": [1], "b": [2]})
        is_valid, errors = validate_gold_schema(df, required_columns=["a", "c"])
        assert not is_valid
        assert any("c" in e for e in errors)


class TestValidateDataframeCompleteness:
    def test_complete_df_returns_one(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = validate_dataframe_completeness(df, threshold=0.9)
        assert result["overall_completeness"] == pytest.approx(1.0)
        assert result["failed_columns"] == []

    def test_incomplete_column_detected(self):
        df = pl.DataFrame({"a": [1, None, None], "b": [4, 5, 6]})
        result = validate_dataframe_completeness(df, threshold=0.9)
        assert "a" in result["failed_columns"]

    def test_empty_df_returns_zero(self):
        df = pl.DataFrame({"a": pl.Series([], dtype=pl.Float64)})
        result = validate_dataframe_completeness(df, threshold=0.5)
        assert result["overall_completeness"] == 0.0
