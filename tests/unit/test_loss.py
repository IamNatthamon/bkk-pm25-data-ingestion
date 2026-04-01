"""Unit tests for CombinedLoss — MAE + RMSE weighted loss function."""
from __future__ import annotations

import pytest
import torch
from hypothesis import given, settings
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Inline implementation — avoids importing the notebook directly
# ---------------------------------------------------------------------------
import torch.nn as nn


class CombinedLoss(nn.Module):
    """Weighted MAE + RMSE loss (replicated from model_training.ipynb)."""

    def __init__(self, mae_weight: float = 0.7, rmse_weight: float = 0.3):
        super().__init__()
        self.mae_weight = mae_weight
        self.rmse_weight = rmse_weight

    def forward(self, pred: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        mae = torch.mean(torch.abs(pred - target))
        rmse = torch.sqrt(torch.mean((pred - target) ** 2) + 1e-8)
        return self.mae_weight * mae + self.rmse_weight * rmse


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCombinedLoss:
    """Given / When / Then tests for CombinedLoss."""

    def test_zero_loss_for_perfect_prediction(self):
        # Given
        loss_fn = CombinedLoss(mae_weight=0.7, rmse_weight=0.3)
        pred   = torch.tensor([[1.0, 2.0, 3.0]])
        target = torch.tensor([[1.0, 2.0, 3.0]])

        # When
        loss = loss_fn(pred, target)

        # Then — loss should be near-zero (rmse term has epsilon 1e-8)
        assert loss.item() < 1e-3, "Loss should be ~0 for perfect prediction"

    def test_positive_loss_for_errors(self):
        # Given
        loss_fn = CombinedLoss(mae_weight=0.7, rmse_weight=0.3)
        pred   = torch.tensor([[1.0, 2.0]])
        target = torch.tensor([[2.0, 3.0]])

        # When
        loss = loss_fn(pred, target)

        # Then
        assert loss.item() > 0, "Loss must be positive for non-zero error"

    def test_larger_error_gives_larger_loss(self):
        # Given
        loss_fn = CombinedLoss()
        small_err = loss_fn(torch.tensor([[1.0]]), torch.tensor([[1.5]]))
        large_err = loss_fn(torch.tensor([[1.0]]), torch.tensor([[10.0]]))

        # Then
        assert large_err.item() > small_err.item()

    def test_loss_is_scalar(self):
        # Given
        loss_fn = CombinedLoss()
        pred   = torch.randn(32, 2)
        target = torch.randn(32, 2)

        # When
        loss = loss_fn(pred, target)

        # Then
        assert loss.ndim == 0, "Loss must be a scalar tensor"

    def test_default_weights_sum_to_one(self):
        loss_fn = CombinedLoss()
        assert abs(loss_fn.mae_weight + loss_fn.rmse_weight - 1.0) < 1e-6

    @pytest.mark.parametrize("mae_w, rmse_w", [
        (0.7, 0.3),
        (0.5, 0.5),
        (1.0, 0.0),
        (0.0, 1.0),
    ])
    def test_custom_weights_accepted(self, mae_w: float, rmse_w: float):
        loss_fn = CombinedLoss(mae_weight=mae_w, rmse_weight=rmse_w)
        pred   = torch.tensor([[2.0]])
        target = torch.tensor([[3.0]])
        loss = loss_fn(pred, target)
        assert loss.item() >= 0

    @given(
        error=st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    )
    @settings(max_examples=50)
    def test_loss_non_negative_for_any_error(self, error: float):
        """Property: loss is always ≥ 0 regardless of error magnitude."""
        loss_fn = CombinedLoss()
        pred   = torch.tensor([[0.0]])
        target = torch.tensor([[error]])
        loss = loss_fn(pred, target)
        assert loss.item() >= 0
