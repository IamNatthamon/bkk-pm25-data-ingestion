"""Unit tests for src.training.evaluator — metrics computation."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.training.evaluator import compute_extreme_accuracy, compute_metrics


class TestComputeMetrics:
    def test_perfect_prediction_zero_mae_rmse(self):
        preds = np.array([[10.0, 20.0], [15.0, 25.0]])
        targets = np.array([[10.0, 20.0], [15.0, 25.0]])
        result = compute_metrics(preds, targets, horizons=[1, 3])
        assert result["MAE"][0] == pytest.approx(0.0, abs=1e-6)
        assert result["RMSE"][0] == pytest.approx(0.0, abs=1e-6)

    def test_perfect_prediction_r2_equals_one(self):
        targets = np.array([[10.0], [20.0], [30.0]])
        preds = targets.copy()
        result = compute_metrics(preds, targets, horizons=[1])
        assert result["R2"][0] == pytest.approx(1.0, abs=1e-5)

    def test_mae_always_non_negative(self):
        preds = np.random.randn(50, 2) * 10
        targets = np.random.randn(50, 2) * 10
        result = compute_metrics(preds, targets, horizons=[1, 3])
        assert (result["MAE"] >= 0).all()
        assert (result["RMSE"] >= 0).all()

    def test_output_has_correct_number_of_rows(self):
        preds = np.zeros((10, 3))
        targets = np.zeros((10, 3))
        result = compute_metrics(preds, targets, horizons=[1, 3, 7])
        assert len(result) == 3

    def test_horizon_labels_correct(self):
        preds = np.zeros((5, 2))
        targets = np.ones((5, 2))
        result = compute_metrics(preds, targets, horizons=[1, 7])
        assert result["horizon_days"].to_list() == [1, 7]

    @given(
        error=st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
    )
    @settings(max_examples=50)
    def test_mae_equals_error_for_constant_offset(self, error: float):
        """Property: MAE should equal the constant error magnitude."""
        preds = np.zeros((20, 1))
        targets = np.full((20, 1), error)
        result = compute_metrics(preds, targets, horizons=[1])
        assert result["MAE"][0] == pytest.approx(error, rel=1e-5)


class TestComputeExtremeAccuracy:
    def test_no_extreme_days_returns_zeros(self):
        preds = np.full((10, 1), 10.0)
        targets = np.full((10, 1), 10.0)
        result = compute_extreme_accuracy(preds, targets, threshold=50.0)
        assert result["n_extreme_days"] == 0
        assert result["detection_rate"] == 0.0

    def test_perfect_extreme_detection(self):
        targets = np.array([[60.0], [10.0], [70.0]])
        preds = targets.copy()
        result = compute_extreme_accuracy(preds, targets, threshold=50.0)
        assert result["detection_rate"] == pytest.approx(1.0)
        assert result["false_alarm_rate"] == pytest.approx(0.0)

    def test_detection_rate_in_0_1(self):
        targets = np.array([[60.0], [10.0], [70.0], [30.0]])
        preds = np.array([[55.0], [10.0], [40.0], [80.0]])
        result = compute_extreme_accuracy(preds, targets, threshold=50.0)
        assert 0.0 <= result["detection_rate"] <= 1.0
        assert 0.0 <= result["false_alarm_rate"] <= 1.0
