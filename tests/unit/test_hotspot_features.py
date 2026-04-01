"""Unit tests for src.hotspot_features — Polars-based hotspot feature pipeline."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.hotspot_features import (
    HOTSPOT_REGION_COLS,
    aggregate_hotspots,
    assign_region,
    create_feature_table,
    create_lag_features,
)


def _sample_hotspot_df() -> pl.DataFrame:
    """Sample data spanning two regions and two dates. Use datetime.date (not pl.date) in lists."""
    return pl.DataFrame({
        "latitude": [13.5, 16.0, 10.0, 45.0],
        "longitude": [100.5, 99.0, 104.0, 50.0],
        "brightness": [350.0, 400.0, 320.0, 300.0],
        "confidence": [0.75, 0.5, 0.5, 0.25],
        "acq_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 1)],
        "date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 1)],
    })


class TestAssignRegion:
    def test_assigns_thailand_for_bangkok_coords(self):
        df = pl.DataFrame({
            "latitude": [13.75],
            "longitude": [100.52],
            "brightness": [350.0],
            "confidence": [0.75],
            "acq_date": [date(2024, 1, 1)],
            "date": [date(2024, 1, 1)],
        })
        result = assign_region(df)
        assert result["source_region"][0] == "thailand"

    def test_drops_rows_outside_all_regions(self):
        df = pl.DataFrame({
            "latitude": [45.0],
            "longitude": [50.0],
            "brightness": [300.0],
            "confidence": [0.5],
            "acq_date": [date(2024, 1, 1)],
            "date": [date(2024, 1, 1)],
        })
        result = assign_region(df)
        assert len(result) == 0

    def test_empty_input_returns_empty(self):
        df = pl.DataFrame(schema={
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "brightness": pl.Float64,
            "confidence": pl.Float64,
            "acq_date": pl.Date,
            "date": pl.Date,
        })
        result = assign_region(df)
        assert result.is_empty()


class TestAggregateHotspots:
    def test_aggregates_by_date_and_region(self):
        df = _sample_hotspot_df()
        assigned = assign_region(df)
        if assigned.is_empty():
            pytest.skip("No rows assigned to regions in sample data")
        agg = aggregate_hotspots(assigned)
        assert "date" in agg.columns
        assert "source_region" in agg.columns
        assert "hotspot_count" in agg.columns

    def test_hotspot_count_positive(self):
        df = pl.DataFrame({
            "latitude": [13.5],
            "longitude": [100.5],
            "brightness": [350.0],
            "confidence": [0.75],
            "acq_date": [date(2024, 1, 1)],
            "date": [date(2024, 1, 1)],
            "source_region": ["thailand"],
        })
        agg = aggregate_hotspots(df)
        assert agg["hotspot_count"][0] > 0


class TestCreateFeatureTable:
    def test_all_region_cols_present(self):
        agg = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "source_region": ["thailand"],
            "hotspot_count": [5],
            "avg_brightness": [350.0],
            "avg_confidence": [0.75],
        })
        wide = create_feature_table(agg)
        for col in HOTSPOT_REGION_COLS:
            assert col in wide.columns

    def test_missing_regions_filled_with_zero(self):
        agg = pl.DataFrame({
            "date": [date(2024, 1, 1)],
            "source_region": ["thailand"],
            "hotspot_count": [3],
            "avg_brightness": [300.0],
            "avg_confidence": [0.5],
        })
        wide = create_feature_table(agg)
        assert wide["hotspot_myanmar"][0] == pytest.approx(0.0)


class TestCreateLagFeatures:
    def test_creates_lag_columns(self):
        base = pl.DataFrame({
            "date": [date(2024, 1, i) for i in range(1, 6)],
            **{col: [float(i) for i in range(1, 6)] for col in HOTSPOT_REGION_COLS},
        })
        result = create_lag_features(base, lag_days=[1, 2])
        assert "hotspot_thailand_lag1" in result.columns
        assert "hotspot_thailand_lag2" in result.columns

    def test_empty_input_returns_empty(self):
        df = pl.DataFrame(schema={"date": pl.Date, **{c: pl.Float64 for c in HOTSPOT_REGION_COLS}})
        result = create_lag_features(df)
        assert result.is_empty()
