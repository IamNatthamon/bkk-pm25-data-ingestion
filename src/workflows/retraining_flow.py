"""
Prefect retraining flow for PM2.5 forecasting.

Orchestrates the full retraining loop as a Prefect flow with:
- Parallel-safe task execution
- Automatic retries on transient failures
- CronSchedule for daily 02:00 UTC execution

Usage:
    # Register and run locally
    uv run python src/workflows/retraining_flow.py

    # Deploy to Prefect server (after `prefect server start`)
    uv run prefect deploy src/workflows/retraining_flow.py:retraining_flow \
        --name daily-pm25-retrain \
        --cron "0 2 * * *"
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from src.utils.logger import get_logger, setup_logging

log = get_logger(__name__)

# ── Project paths ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "model_ready"
REPORTS_DIR = PROJECT_ROOT / "reports"
MODELS_DIR = PROJECT_ROOT / "models"

TARGET_COL = "pm25_mean"
SEQ_LEN = 30
HORIZONS = [1, 3]
DRIFT_THRESHOLD = 0.25
QUALITY_THRESHOLD = 0.80
PRODUCTION_MAE_THRESHOLD = 8.0


def _try_import_prefect() -> bool:
    """Return True if Prefect is installed."""
    try:
        import prefect  # noqa: F401
        return True
    except ImportError:
        return False


# ── Task definitions ──────────────────────────────────────────────────────────

def _task_validate_data(train_df: pl.DataFrame) -> dict[str, Any]:
    """Validate Gold data quality."""
    from src.validation.schemas import validate_dataframe_completeness, validate_gold_schema
    from src.validation.profiling import quick_quality_summary

    is_valid, errors = validate_gold_schema(train_df)
    completeness = validate_dataframe_completeness(train_df, threshold=QUALITY_THRESHOLD)
    quality = quick_quality_summary(train_df)

    result = {
        "is_valid": is_valid,
        "errors": errors,
        "completeness": completeness["overall_completeness"],
        "quality": quality,
    }
    log.info("task.validate_data.complete", **{k: v for k, v in result.items() if k != "errors"})
    return result


def _task_detect_drift(
    reference_df: pl.DataFrame,
    current_df: pl.DataFrame,
) -> dict[str, Any]:
    """Detect distribution drift between reference and current data."""
    from src.validation.drift import detect_drift_evidently

    numeric_cols = [
        c for c in reference_df.columns
        if reference_df[c].dtype.is_numeric() and c in current_df.columns
    ][:30]

    drift_report_path = REPORTS_DIR / "drift_report_latest.html"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        result = detect_drift_evidently(
            reference_df=reference_df.select(numeric_cols),
            current_df=current_df.select(numeric_cols),
            output_path=drift_report_path,
        )
    except Exception as exc:
        log.warning("task.detect_drift.failed", error=str(exc))
        result = {"drift_detected": False, "drifted_features": [], "drift_share": 0.0}

    log.info("task.detect_drift.complete", drift_share=result.get("drift_share"))
    return result


def _task_run_silver_to_gold() -> bool:
    """Re-run Silver→Gold ETL pipeline."""
    try:
        from src.silver_to_gold.pipeline import run_silver_to_gold_pipeline
        from src.utils.config import PipelineConfig

        run_silver_to_gold_pipeline(PipelineConfig())
        log.info("task.silver_to_gold.complete")
        return True
    except Exception as exc:
        log.error("task.silver_to_gold.failed", error=str(exc))
        return False


def _task_train_model(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    feature_cols: list[str],
    use_optuna: bool = True,
    n_trials: int = 30,
) -> dict[str, Any]:
    """Train the STUNN model (with optional Optuna tuning)."""
    from src.training.config import TrainConfig
    from src.training.dataset import build_dataloaders
    from src.training.models import STUNN
    from src.training.trainer import Trainer

    base_cfg = TrainConfig(output_dir=MODELS_DIR)

    if use_optuna:
        try:
            from src.training.tuner import run_optuna_study

            best_params = run_optuna_study(
                train_df=train_df, val_df=val_df,
                feature_cols=feature_cols, target_col=TARGET_COL,
                seq_len=SEQ_LEN, horizons=HORIZONS,
                base_cfg=base_cfg, n_trials=n_trials, timeout_s=1800,
            )
            cfg = TrainConfig(
                output_dir=MODELS_DIR,
                hidden_size=best_params.get("hidden_size", 128),
                num_layers=best_params.get("num_layers", 2),
                dropout=best_params.get("dropout", 0.2),
                learning_rate=best_params.get("learning_rate", 1e-3),
                attention_heads=best_params.get("attention_heads", 4),
                batch_size=best_params.get("batch_size", 64),
                epochs=100, patience=15,
            )
        except Exception as exc:
            log.warning("task.train_model.optuna_failed", error=str(exc))
            cfg = base_cfg
    else:
        cfg = base_cfg

    model = STUNN(
        num_features=len(feature_cols),
        hidden_size=cfg.hidden_size,
        num_layers=cfg.num_layers,
        num_horizons=len(HORIZONS),
        attention_heads=cfg.attention_heads,
        dropout=cfg.dropout,
    )
    train_loader, val_loader, _ = build_dataloaders(
        train_df, val_df, val_df, feature_cols, TARGET_COL, SEQ_LEN, HORIZONS, cfg
    )
    trainer = Trainer(model, cfg, model_name="stunn")
    history = trainer.train(train_loader, val_loader)
    best_val = min(history["val_loss"])

    log.info("task.train_model.complete", best_val_loss=round(best_val, 4))
    return {"best_val_loss": best_val, "model_name": "stunn"}


def _task_evaluate_model(
    test_df: pl.DataFrame,
    feature_cols: list[str],
) -> dict[str, Any]:
    """Evaluate the best trained model on the test set."""
    import torch
    from torch.utils.data import DataLoader

    from src.training.config import TrainConfig
    from src.training.dataset import PM25SequenceDataset
    from src.training.evaluator import compute_metrics, compute_extreme_accuracy
    from src.training.models import STUNN
    from src.training.trainer import Trainer

    cfg = TrainConfig(output_dir=MODELS_DIR)
    model = STUNN(
        num_features=len(feature_cols),
        hidden_size=cfg.hidden_size,
        num_layers=cfg.num_layers,
        num_horizons=len(HORIZONS),
    )
    trainer = Trainer(model, cfg, model_name="stunn")
    try:
        trainer.load_best()
    except FileNotFoundError:
        log.warning("task.evaluate.no_checkpoint")
        return {"error": "no_checkpoint"}

    test_ds = PM25SequenceDataset(test_df, feature_cols, TARGET_COL, SEQ_LEN, HORIZONS)
    test_loader = DataLoader(test_ds, batch_size=128, shuffle=False, num_workers=0)
    _, preds, targets = trainer.evaluate(test_loader)

    metrics_df = compute_metrics(preds, targets, HORIZONS)
    extreme = compute_extreme_accuracy(preds, targets)

    result = {
        "test_mae_h1": float(metrics_df.filter(pl.col("horizon_days") == HORIZONS[0])["MAE"][0]),
        "extreme_detection_rate": extreme.get("detection_rate", 0),
    }
    log.info("task.evaluate.complete", **result)
    return result


# ── Flow (Prefect or plain Python fallback) ────────────────────────────────────

def retraining_flow(
    force_retrain: bool = False,
    use_optuna: bool = True,
    n_optuna_trials: int = 30,
) -> dict[str, Any]:
    """
    PM2.5 retraining flow.

    If Prefect is installed, this function is decorated as a @flow.
    Otherwise it runs as plain Python — same logic, no Prefect dependency.
    """
    setup_logging()
    log.info("flow.start", timestamp=datetime.now(tz=timezone.utc).isoformat())
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # Load Gold data
    for split in ("train.parquet", "val.parquet", "test.parquet"):
        if not (GOLD_DIR / split).exists():
            log.info("flow.gold_missing_running_etl")
            _task_run_silver_to_gold()
            break

    train_df = pl.read_parquet(GOLD_DIR / "train.parquet")
    val_df = pl.read_parquet(GOLD_DIR / "val.parquet")
    test_df = pl.read_parquet(GOLD_DIR / "test.parquet")

    exclude = {"date", "stationID", "load_id", "pipeline_version", "data_source", "record_hash"}
    feature_cols = [
        c for c in train_df.columns
        if c not in exclude and train_df[c].dtype.is_numeric()
    ]

    # Validate
    validation = _task_validate_data(train_df)
    if not validation["is_valid"] or validation["completeness"] < QUALITY_THRESHOLD:
        log.error("flow.data_invalid_abort", errors=validation["errors"][:5])
        return {"status": "aborted", "reason": "data_quality"}

    # Drift detection
    drift = _task_detect_drift(train_df, val_df)
    should_retrain = force_retrain or drift["drift_share"] > DRIFT_THRESHOLD

    if not should_retrain:
        log.info("flow.no_retrain_trigger", drift_share=drift["drift_share"])
        return {"status": "skipped", "drift": drift}

    # Train
    train_result = _task_train_model(
        train_df, val_df, feature_cols,
        use_optuna=use_optuna, n_trials=n_optuna_trials,
    )

    # Evaluate
    eval_result = _task_evaluate_model(test_df, feature_cols)
    test_mae = eval_result.get("test_mae_h1", float("inf"))

    # Deployment decision
    deployed = test_mae < PRODUCTION_MAE_THRESHOLD
    status = "deployed" if deployed else "rejected"

    log.info(
        "flow.complete",
        status=status,
        test_mae=round(test_mae, 3),
        threshold=PRODUCTION_MAE_THRESHOLD,
    )

    return {
        "status": status,
        "test_mae_h1": test_mae,
        "drift_share": drift["drift_share"],
        "train_result": train_result,
    }


# ── Apply Prefect decorators if available ─────────────────────────────────────

if _try_import_prefect():
    from prefect import flow as prefect_flow, task as prefect_task

    _task_validate_data = prefect_task(retries=1)(_task_validate_data)  # type: ignore[assignment]
    _task_detect_drift = prefect_task(retries=1)(_task_detect_drift)  # type: ignore[assignment]
    _task_run_silver_to_gold = prefect_task(retries=2)(_task_run_silver_to_gold)  # type: ignore[assignment]
    _task_train_model = prefect_task()(_task_train_model)  # type: ignore[assignment]
    _task_evaluate_model = prefect_task()(_task_evaluate_model)  # type: ignore[assignment]
    retraining_flow = prefect_flow(name="pm25_retraining")(retraining_flow)  # type: ignore[assignment]

    log.info("workflow.prefect_decorators_applied")
else:
    log.info("workflow.running_without_prefect")


if __name__ == "__main__":
    result = retraining_flow(force_retrain=False, use_optuna=False)
    print(json.dumps(result, indent=2, default=str))
