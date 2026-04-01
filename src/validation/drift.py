"""Data drift detection using statistical tests and optional Evidently AI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from src.utils.logger import get_logger

log = get_logger(__name__)


def detect_drift_statistical(
    reference_df: pl.DataFrame,
    current_df: pl.DataFrame,
    feature_cols: list[str],
    ks_threshold: float = 0.05,
) -> dict[str, Any]:
    """Detect distribution drift using Kolmogorov-Smirnov test (no external deps).

    Args:
        reference_df: Reference (training) data.
        current_df: Current (new) data.
        feature_cols: Columns to test for drift.
        ks_threshold: p-value threshold below which drift is declared.

    Returns:
        Dict with: drift_detected, drifted_features, drift_share, feature_stats.
    """
    try:
        from scipy import stats as scipy_stats
    except ImportError as exc:
        raise ImportError(
            "scipy is required for drift detection. Install with: uv add scipy"
        ) from exc

    drifted: list[str] = []
    feature_stats: dict[str, dict[str, float]] = {}

    for col in feature_cols:
        if col not in reference_df.columns or col not in current_df.columns:
            continue

        ref_vals = reference_df[col].drop_nulls().to_numpy()
        cur_vals = current_df[col].drop_nulls().to_numpy()

        if len(ref_vals) < 10 or len(cur_vals) < 10:
            continue

        ks_stat, p_value = scipy_stats.ks_2samp(ref_vals, cur_vals)
        feature_stats[col] = {"ks_stat": float(ks_stat), "p_value": float(p_value)}

        if p_value < ks_threshold:
            drifted.append(col)

    drift_share = len(drifted) / max(len(feature_cols), 1)
    drift_detected = len(drifted) > 0

    log.info(
        "drift.statistical_check",
        drift_detected=drift_detected,
        drifted_count=len(drifted),
        drift_share=round(drift_share, 3),
    )

    return {
        "drift_detected": drift_detected,
        "drifted_features": drifted,
        "drift_share": drift_share,
        "feature_stats": feature_stats,
    }


def detect_drift_evidently(
    reference_df: pl.DataFrame,
    current_df: pl.DataFrame,
    output_path: Path | None = None,
    stattest: str = "ks",
    stattest_threshold: float = 0.05,
) -> dict[str, Any]:
    """Detect drift using Evidently AI (generates HTML report if output_path given).

    Falls back to detect_drift_statistical if Evidently is not installed.

    Args:
        reference_df: Reference (training) data.
        current_df: Current (new) data.
        output_path: If provided, save HTML drift report to this path.
        stattest: Statistical test to use ('ks', 'psi', 'wasserstein').
        stattest_threshold: p-value / threshold for drift detection.

    Returns:
        Dict with: drift_detected, drifted_features, drift_share.
    """
    try:
        from evidently.metric_preset import DataDriftPreset, DataQualityPreset
        from evidently.report import Report
    except ImportError:
        log.warning("drift.evidently_not_installed_fallback_to_ks")
        feature_cols = [c for c in reference_df.columns if reference_df[c].dtype.is_numeric()]
        return detect_drift_statistical(reference_df, current_df, feature_cols)

    ref_pd = reference_df.to_pandas()
    cur_pd = current_df.to_pandas()

    report = Report(metrics=[
        DataDriftPreset(stattest=stattest, stattest_threshold=stattest_threshold),
        DataQualityPreset(),
    ])
    report.run(reference_data=ref_pd, current_data=cur_pd)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        report.save_html(str(output_path))
        log.info("drift.report_saved", path=str(output_path))

    report_dict = report.as_dict()
    try:
        drift_cols = report_dict["metrics"][0]["result"]["drift_by_columns"]
        drifted = [
            col
            for col, info in drift_cols.items()
            if info.get("drift_detected", False)
        ]
    except (KeyError, IndexError):
        drifted = []

    drift_share = len(drifted) / max(len(reference_df.columns), 1)
    drift_detected = len(drifted) > 0

    log.info(
        "drift.evidently_check",
        drift_detected=drift_detected,
        drifted_count=len(drifted),
        drift_share=round(drift_share, 3),
    )

    return {
        "drift_detected": drift_detected,
        "drifted_features": drifted,
        "drift_share": drift_share,
    }
