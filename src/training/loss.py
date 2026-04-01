"""Loss functions for PM2.5 forecasting."""

from __future__ import annotations

import torch
import torch.nn as nn


class CombinedLoss(nn.Module):
    """Weighted MAE + RMSE loss.

    MAE provides robust gradients on noisy PM2.5 data.
    RMSE penalizes large spikes (burning season) more heavily.
    """

    def __init__(self, mae_weight: float = 0.7, rmse_weight: float = 0.3) -> None:
        super().__init__()
        if abs(mae_weight + rmse_weight - 1.0) > 1e-6:
            raise ValueError(
                f"mae_weight + rmse_weight must sum to 1.0, got {mae_weight + rmse_weight:.3f}"
            )
        self.mae_weight = mae_weight
        self.rmse_weight = rmse_weight

    def forward(self, pred: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        mae = torch.mean(torch.abs(pred - target))
        rmse = torch.sqrt(torch.mean((pred - target) ** 2) + 1e-8)
        return self.mae_weight * mae + self.rmse_weight * rmse


class HuberLoss(nn.Module):
    """Huber loss — smooth L1, robust to outliers. Good alternative to CombinedLoss."""

    def __init__(self, delta: float = 1.0) -> None:
        super().__init__()
        self.delta = delta
        self._huber = nn.HuberLoss(delta=delta, reduction="mean")

    def forward(self, pred: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        return self._huber(pred, target)
