"""
Automated PM2.5 Retraining Pipeline

Runs in a loop:
  1. Load latest Silver data → run Silver→Gold pipeline
  2. Validate data quality (Pydantic schema + completeness)
  3. Detect distribution drift vs. previous training data
  4. If data issues found: scan Silver data for problems and log diagnostics
  5. Decide: retrain (drift | scheduled | degraded performance) or skip
  6. Optuna hyperparameter search → train final model
  7. Evaluate on held-out test set
  8. Deploy if test MAE < production threshold

Usage:
    # One-shot run
    uv run python scripts/automated_retraining.py

    # Run in continuous loop (e.g. daily)
    uv run python scripts/automated_retraining.py --loop --interval-hours 24

    # Force retrain regardless of drift
    uv run python scripts/automated_retraining.py --force
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

# ── Project root resolution ───────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.training.config import TrainConfig
from src.training.dataset import build_dataloaders
from src.training.evaluator import compute_metrics, compute_extreme_accuracy, log_metrics
from src.training.models import STUNN
from src.training.trainer import Trainer
from src.utils.logger import get_logger, setup_logging
from src.validation.drift import detect_drift_evidently, detect_drift_statistical
from src.validation.profiling import generate_profile_report, quick_quality_summary
from src.validation.schemas import validate_gold_schema, validate_dataframe_completeness

log = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "model_ready"
REPORTS_DIR = PROJECT_ROOT / "reports"
MODELS_DIR = PROJECT_ROOT / "models"

# Production deployment threshold: if new model MAE < this, deploy
PRODUCTION_MAE_THRESHOLD = 8.0

# Drift detection: if drift_share > this fraction, trigger retraining
DRIFT_SHARE_THRESHOLD = 0.25

# Data quality: if overall completeness < this, abort and scan data issues
QUALITY_THRESHOLD = 0.80

# Columns critical for training
FEATURE_COLS_CORE = [
    "pm2_5_mean", "temp_2m_mean", "temp_2m_min", "temp_2m_max",
    "rh_2m_mean", "precip_sum", "wind_speed_mean", "wind_u10_mean",
    "wind_v10_mean", "pressure_mean", "radiation_mean",
]
TARGET_COL = "pm2_5_mean"
SEQ_LEN = 30
HORIZONS = [1, 3]


# ── Data scanning helpers ─────────────────────────────────────────────────────

def scan_data_issues(df: pl.DataFrame) -> dict:
    """Scan a Gold DataFrame for data quality problems.

    Returns a diagnostic dict that can guide data re-ingestion.
    """
    log.info("scan.start", rows=len(df))
    issues: list[str] = []

    # 1. Null audit per column
    null_report = {
        col: int(df[col].null_count())
        for col in df.columns
        if df[col].null_count() > 0
    }
    if null_report:
        issues.append(f"Columns with nulls: {list(null_report.keys())}")

    # 2. Temporal gaps (missing dates per station)
    if "date" in df.columns and "stationID" in df.columns:
        date_gaps: list[str] = []
        for station in df["stationID"].unique().to_list():
            sdf = df.filter(pl.col("stationID") == station).sort("date")
            if sdf.is_empty():
                continue
            dates = sdf["date"].to_list()
            expected_range = set(
                pl.date_range(dates[0], dates[-1], interval="1d", eager=True).to_list()
            )
            actual = set(dates)
            missing = expected_range - actual
            if missing:
                date_gaps.append(f"{station}: {len(missing)} missing dates")
        if date_gaps:
            issues.append(f"Temporal gaps detected: {date_gaps[:5]}")

    # 3. PM2.5 outlier check
    if "pm2_5_mean" in df.columns:
        extreme = df.filter(pl.col("pm2_5_mean") > 500)
        if len(extreme) > 0:
            issues.append(f"pm2_5_mean has {len(extreme)} values > 500 μg/m³")

    # 4. Station coverage
    n_stations = df["stationID"].n_unique() if "stationID" in df.columns else 0
    if n_stations < 10:
        issues.append(f"Only {n_stations} stations found (expected ≥10)")

    result = {
        "issues_found": len(issues) > 0,
        "issues": issues,
        "null_report": null_report,
        "n_stations": n_stations,
        "date_min": str(df["date"].min()) if "date" in df.columns else None,
        "date_max": str(df["date"].max()) if "date" in df.columns else None,
    }

    if issues:
        log.warning("scan.issues_found", count=len(issues), details=issues[:3])
    else:
        log.info("scan.clean", n_stations=n_stations)

    return result


def scan_silver_layer() -> dict:
    """Scan the Silver layer for data completeness and coverage issues."""
    silver_aq = PROJECT_ROOT / "data" / "silver" / "openmeteo_airquality"
    silver_wx = PROJECT_ROOT / "data" / "silver" / "openmeteo_weather"

    result = {}

    for name, path in [("airquality", silver_aq), ("weather", silver_wx)]:
        if not path.exists():
            result[name] = {"status": "missing", "path": str(path)}
            continue

        year_dirs = sorted([d for d in path.iterdir() if d.is_dir() and d.name.startswith("year=")])
        parquet_count = sum(1 for _ in path.rglob("*.parquet"))
        result[name] = {
            "status": "found",
            "years": [d.name for d in year_dirs],
            "parquet_files": parquet_count,
        }

    log.info("scan.silver_layer", **{k: v.get("status", "unknown") for k, v in result.items()})
    return result


# ── Retraining helpers ────────────────────────────────────────────────────────

def load_gold_splits() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load train/val/test Gold parquet files."""
    for split in ("train.parquet", "val.parquet", "test.parquet"):
        if not (GOLD_DIR / split).exists():
            raise FileNotFoundError(
                f"Gold split not found: {GOLD_DIR / split}. "
                "Run the Silver→Gold pipeline first."
            )

    train_df = pl.read_parquet(GOLD_DIR / "train.parquet")
    val_df = pl.read_parquet(GOLD_DIR / "val.parquet")
    test_df = pl.read_parquet(GOLD_DIR / "test.parquet")
    log.info(
        "data.loaded",
        train=len(train_df), val=len(val_df), test=len(test_df),
    )
    return train_df, val_df, test_df


def get_feature_cols(df: pl.DataFrame) -> list[str]:
    """Infer numeric feature columns (exclude metadata and target)."""
    exclude = {
        "date", "stationID", "load_id", "pipeline_version",
        "data_source", "record_hash",
    }
    return [
        c for c in df.columns
        if c not in exclude and df[c].dtype.is_numeric()
    ]


def run_silver_to_gold() -> bool:
    """Re-run Silver→Gold pipeline. Returns True on success."""
    try:
        from src.silver_to_gold.pipeline import run_silver_to_gold_pipeline
        from src.utils.config import PipelineConfig

        config = PipelineConfig()
        run_silver_to_gold_pipeline(config)
        return True
    except Exception as exc:
        log.error("pipeline.silver_to_gold_failed", error=str(exc))
        return False


def load_performance_baseline() -> float | None:
    """Load previous production MAE from the run manifest if available."""
    manifest_files = sorted(MODELS_DIR.glob("stunn_run_manifest.json"))
    if not manifest_files:
        return None
    try:
        with open(manifest_files[-1]) as f:
            manifest = json.load(f)
        return manifest.get("metrics", {}).get("test_mae_h1")
    except Exception:
        return None


def train_best_model(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    feature_cols: list[str],
    use_optuna: bool = True,
    n_trials: int = 30,
) -> tuple[STUNN, Trainer, TrainConfig]:
    """Run Optuna search (optional) then train the final model."""
    base_cfg = TrainConfig(output_dir=MODELS_DIR)

    if use_optuna:
        try:
            from src.training.tuner import run_optuna_study

            best_params = run_optuna_study(
                train_df=train_df,
                val_df=val_df,
                feature_cols=feature_cols,
                target_col=TARGET_COL,
                seq_len=SEQ_LEN,
                horizons=HORIZONS,
                base_cfg=base_cfg,
                n_trials=n_trials,
                timeout_s=1800,
            )
            cfg = TrainConfig(
                output_dir=MODELS_DIR,
                hidden_size=best_params.get("hidden_size", 128),
                num_layers=best_params.get("num_layers", 2),
                dropout=best_params.get("dropout", 0.2),
                learning_rate=best_params.get("learning_rate", 1e-3),
                attention_heads=best_params.get("attention_heads", 4),
                batch_size=best_params.get("batch_size", 64),
                accumulation_steps=best_params.get("accumulation_steps", 1),
                epochs=100,
                patience=15,
            )
            log.info("training.best_params_from_optuna", params=best_params)
        except Exception as exc:
            log.warning("training.optuna_failed_using_defaults", error=str(exc))
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
        train_df, val_df, val_df,
        feature_cols, TARGET_COL, SEQ_LEN, HORIZONS, cfg,
    )

    trainer = Trainer(model, cfg, model_name="stunn")
    trainer.train(train_loader, val_loader)
    return model, trainer, cfg


# ── Main retraining loop ──────────────────────────────────────────────────────

def automated_retraining_loop(
    force: bool = False,
    skip_optuna: bool = False,
    n_optuna_trials: int = 30,
) -> dict:
    """
    Single iteration of the automated retraining loop.

    Steps:
      1. Load Gold data
      2. Validate quality
      3. Detect drift
      4. If issues detected → scan Silver layer for root cause
      5. Decide whether to retrain
      6. Train (with optional Optuna tuning)
      7. Evaluate and conditionally deploy

    Returns:
      dict with run summary (status, metrics, decision).
    """
    run_ts = datetime.now(tz=timezone.utc).isoformat()
    log.info("retraining.loop_start", timestamp=run_ts, force=force)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    summary: dict = {"timestamp": run_ts, "status": "unknown"}

    # ── 1. Load Gold data ────────────────────────────────────────────────────
    try:
        train_df, val_df, test_df = load_gold_splits()
    except FileNotFoundError:
        log.info("retraining.gold_missing_running_pipeline")
        if not run_silver_to_gold():
            summary.update({"status": "failed", "reason": "silver_to_gold_pipeline_failed"})
            return summary
        train_df, val_df, test_df = load_gold_splits()

    feature_cols = get_feature_cols(train_df)

    # ── 2. Validate data quality ─────────────────────────────────────────────
    is_valid, schema_errors = validate_gold_schema(train_df)
    completeness = validate_dataframe_completeness(train_df, threshold=QUALITY_THRESHOLD)

    if not is_valid or completeness["overall_completeness"] < QUALITY_THRESHOLD:
        log.warning(
            "retraining.data_quality_failed",
            schema_valid=is_valid,
            completeness=completeness["overall_completeness"],
            errors=schema_errors[:5],
        )
        # Scan for root cause
        gold_scan = scan_data_issues(train_df)
        silver_scan = scan_silver_layer()

        summary.update({
            "status": "aborted",
            "reason": "data_quality_below_threshold",
            "gold_scan": gold_scan,
            "silver_scan": silver_scan,
        })

        # Save diagnostic report
        diag_path = REPORTS_DIR / f"data_issues_{datetime.now():%Y%m%d_%H%M%S}.json"
        diag_path.write_text(json.dumps(summary, indent=2, default=str))
        log.info("retraining.diagnostics_saved", path=str(diag_path))
        return summary

    # ── 3. Data profiling ────────────────────────────────────────────────────
    profile_path = REPORTS_DIR / "data_profile_latest.html"
    try:
        generate_profile_report(train_df, profile_path, title="Gold Train — Data Profile")
    except Exception as exc:
        log.warning("retraining.profile_failed", error=str(exc))

    quality_summary = quick_quality_summary(train_df)
    log.info("retraining.quality_ok", **quality_summary)

    # ── 4. Drift detection ───────────────────────────────────────────────────
    numeric_cols = [c for c in feature_cols if c in train_df.columns and c in val_df.columns]
    drift_report_path = REPORTS_DIR / "drift_report_latest.html"

    try:
        drift_result = detect_drift_evidently(
            reference_df=train_df.select(numeric_cols[:30]),
            current_df=val_df.select(numeric_cols[:30]),
            output_path=drift_report_path,
        )
    except Exception as exc:
        log.warning("retraining.drift_detection_failed", error=str(exc))
        drift_result = {"drift_detected": False, "drifted_features": [], "drift_share": 0.0}

    log.info("retraining.drift_result", **{k: v for k, v in drift_result.items() if k != "feature_stats"})

    if drift_result["drift_detected"]:
        log.warning(
            "retraining.drift_detected",
            features=drift_result["drifted_features"][:10],
            drift_share=drift_result["drift_share"],
        )
        # If drift is very high, scan for data source issues
        if drift_result["drift_share"] > 0.5:
            silver_scan = scan_silver_layer()
            gold_scan = scan_data_issues(val_df)
            log.warning("retraining.high_drift_scan", silver=silver_scan, gold_issues=gold_scan)

    # ── 5. Retraining decision ────────────────────────────────────────────────
    prev_mae = load_performance_baseline()
    should_retrain = (
        force
        or drift_result["drift_share"] > DRIFT_SHARE_THRESHOLD
        or prev_mae is None
    )

    if not should_retrain:
        log.info(
            "retraining.skipped",
            reason="no_trigger",
            drift_share=round(drift_result["drift_share"], 3),
            prev_mae=prev_mae,
        )
        summary.update({
            "status": "skipped",
            "drift_share": drift_result["drift_share"],
            "prev_mae": prev_mae,
        })
        return summary

    log.info("retraining.triggered", force=force, drift_share=drift_result["drift_share"])

    # ── 6. Train model ────────────────────────────────────────────────────────
    try:
        model, trainer, cfg = train_best_model(
            train_df, val_df, feature_cols,
            use_optuna=not skip_optuna,
            n_trials=n_optuna_trials,
        )
    except Exception as exc:
        log.error("retraining.training_failed", error=str(exc))
        summary.update({"status": "failed", "reason": f"training_error: {exc}"})
        return summary

    # ── 7. Evaluate on test set ───────────────────────────────────────────────
    from src.training.dataset import PM25SequenceDataset
    from torch.utils.data import DataLoader

    test_ds = PM25SequenceDataset(
        test_df, feature_cols, TARGET_COL, SEQ_LEN, HORIZONS
    )
    test_loader = DataLoader(test_ds, batch_size=128, shuffle=False, num_workers=0)
    _, preds, targets = trainer.evaluate(test_loader)

    metrics_df = compute_metrics(preds, targets, HORIZONS)
    log_metrics("stunn", metrics_df)

    test_mae_h1 = float(metrics_df.filter(pl.col("horizon_days") == HORIZONS[0])["MAE"][0])
    extreme_acc = compute_extreme_accuracy(preds, targets, threshold=50.0)

    log.info(
        "retraining.evaluation",
        test_mae_h1=round(test_mae_h1, 3),
        prev_mae=prev_mae,
        threshold=PRODUCTION_MAE_THRESHOLD,
        extreme_detection_rate=extreme_acc.get("detection_rate", 0),
    )

    # ── 8. Save manifest and deploy ───────────────────────────────────────────
    from src.training.registry import save_run_manifest

    run_metrics = {
        "test_mae_h1": test_mae_h1,
        **{f"test_{m.lower()}_h{h}": float(metrics_df.filter(pl.col("horizon_days") == h)[m][0])
           for h in HORIZONS for m in ["MAE", "RMSE", "R2"]},
        "extreme_detection_rate": extreme_acc.get("detection_rate", 0),
    }
    save_run_manifest("stunn", cfg, run_metrics)

    # Deployment decision
    if test_mae_h1 < PRODUCTION_MAE_THRESHOLD:
        log.info(
            "retraining.deployed",
            test_mae=round(test_mae_h1, 3),
            threshold=PRODUCTION_MAE_THRESHOLD,
        )
        summary.update({"status": "deployed", "metrics": run_metrics})
    else:
        log.warning(
            "retraining.rejected",
            test_mae=round(test_mae_h1, 3),
            threshold=PRODUCTION_MAE_THRESHOLD,
        )
        summary.update({"status": "rejected", "metrics": run_metrics})

    return summary


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Automated PM2.5 retraining pipeline")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument(
        "--interval-hours", type=float, default=24.0,
        help="Hours between runs in loop mode (default: 24)",
    )
    parser.add_argument("--force", action="store_true", help="Force retrain regardless of drift")
    parser.add_argument("--skip-optuna", action="store_true", help="Skip Optuna tuning")
    parser.add_argument(
        "--n-trials", type=int, default=30,
        help="Number of Optuna trials (default: 30)",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    if args.loop:
        log.info("retraining.loop_mode", interval_hours=args.interval_hours)
        while True:
            summary = automated_retraining_loop(
                force=args.force,
                skip_optuna=args.skip_optuna,
                n_optuna_trials=args.n_trials,
            )
            log.info("retraining.loop_iteration_done", status=summary.get("status"))
            sleep_s = args.interval_hours * 3600
            log.info("retraining.sleeping", hours=args.interval_hours)
            time.sleep(sleep_s)
    else:
        summary = automated_retraining_loop(
            force=args.force,
            skip_optuna=args.skip_optuna,
            n_optuna_trials=args.n_trials,
        )
        log.info("retraining.done", status=summary.get("status"))


if __name__ == "__main__":
    main()
