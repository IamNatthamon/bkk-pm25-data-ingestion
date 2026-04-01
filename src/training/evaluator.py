"""Model evaluation: metrics, seasonal breakdown, and extreme pollution detection."""

from __future__ import annotations

import numpy as np
import polars as pl

from src.utils.logger import get_logger

log = get_logger(__name__)


def compute_metrics(
    preds: np.ndarray,
    targets: np.ndarray,
    horizons: list[int],
) -> pl.DataFrame:
    """Compute MAE, RMSE, R² per forecast horizon.

    Args:
        preds: Shape (N, len(horizons)).
        targets: Shape (N, len(horizons)).
        horizons: Forecast horizon labels (days).

    Returns:
        Polars DataFrame with columns: horizon_days, MAE, RMSE, R2.
    """
    rows = []
    for i, h in enumerate(horizons):
        p = preds[:, i]
        t = targets[:, i]
        mae = float(np.mean(np.abs(p - t)))
        rmse = float(np.sqrt(np.mean((p - t) ** 2)))
        ss_res = np.sum((t - p) ** 2)
        ss_tot = np.sum((t - np.mean(t)) ** 2)
        r2 = float(1.0 - ss_res / (ss_tot + 1e-8))
        rows.append({"horizon_days": h, "MAE": mae, "RMSE": rmse, "R2": r2})

    return pl.DataFrame(rows)


def compute_seasonal_metrics(
    preds: np.ndarray,
    targets: np.ndarray,
    dates: pl.Series,
    horizons: list[int],
) -> pl.DataFrame:
    """Breakdown metrics by Thai pollution seasons.

    Seasons:
      - Burning: Feb–Apr (peak PM2.5)
      - Monsoon: May–Sep (low PM2.5, rain washout)
      - Cool/Dry: Oct–Jan (moderate PM2.5)
    """
    months = dates.dt.month().to_numpy()[: len(preds)]

    season_map = {
        "Burning (Feb-Apr)": np.isin(months, [2, 3, 4]),
        "Monsoon (May-Sep)": np.isin(months, [5, 6, 7, 8, 9]),
        "Cool (Oct-Jan)": np.isin(months, [10, 11, 12, 1]),
    }

    rows = []
    for season, mask in season_map.items():
        if mask.sum() == 0:
            continue
        for i, h in enumerate(horizons):
            p = preds[mask, i]
            t = targets[mask, i]
            rows.append(
                {
                    "season": season,
                    "horizon_days": h,
                    "MAE": float(np.mean(np.abs(p - t))),
                    "RMSE": float(np.sqrt(np.mean((p - t) ** 2))),
                    "n_samples": int(mask.sum()),
                }
            )

    return pl.DataFrame(rows)


def compute_extreme_accuracy(
    preds: np.ndarray,
    targets: np.ndarray,
    threshold: float = 50.0,
) -> dict[str, float]:
    """Accuracy metrics for extreme pollution events (PM2.5 > threshold μg/m³).

    Uses horizon-0 (day-1 forecast) for detection.

    Returns dict with: n_extreme_days, detection_rate (recall), false_alarm_rate (FPR).
    """
    actual_extreme = targets[:, 0] > threshold
    pred_extreme = preds[:, 0] > threshold
    n_extreme = int(actual_extreme.sum())

    if n_extreme == 0:
        return {"n_extreme_days": 0, "detection_rate": 0.0, "false_alarm_rate": 0.0}

    tp = int((actual_extreme & pred_extreme).sum())
    fp = int((~actual_extreme & pred_extreme).sum())
    n_non_extreme = int((~actual_extreme).sum())

    return {
        "n_extreme_days": n_extreme,
        "detection_rate": tp / n_extreme,
        "false_alarm_rate": fp / max(n_non_extreme, 1),
    }


def log_metrics(
    model_name: str,
    metrics: pl.DataFrame,
    seasonal: pl.DataFrame | None = None,
) -> None:
    """Log evaluation metrics via structlog."""
    for row in metrics.iter_rows(named=True):
        log.info(
            "eval.metrics",
            model=model_name,
            horizon=row["horizon_days"],
            mae=round(row["MAE"], 3),
            rmse=round(row["RMSE"], 3),
            r2=round(row["R2"], 3),
        )
    if seasonal is not None:
        for row in seasonal.iter_rows(named=True):
            log.info(
                "eval.seasonal",
                model=model_name,
                season=row["season"],
                horizon=row["horizon_days"],
                mae=round(row["MAE"], 3),
            )
