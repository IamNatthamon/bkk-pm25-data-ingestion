"""
PM2.5 Forecasting — Monitoring Dashboard

A Streamlit app that visualises:
  1. Model performance across MLflow experiment runs
  2. Data drift detection (current vs. reference data)
  3. Data quality summary (completeness, nulls, outliers)
  4. Silver layer coverage (available years / months per station)

Run with:
    streamlit run dashboards/monitoring.py

Environment variables:
    MLFLOW_TRACKING_URI  — MLflow tracking server URI (default: local ./mlruns)
    GOLD_DIR             — path to Gold model_ready directory
    SILVER_DIR           — path to Silver layer root
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

# ── Project root resolution ───────────────────────────────────────────────────
_DASHBOARD_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _DASHBOARD_DIR.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st

st.set_page_config(
    page_title="PM2.5 Forecasting Monitor",
    page_icon="🌫",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Configuration ─────────────────────────────────────────────────────────────
GOLD_DIR = Path(st.sidebar.text_input(
    "Gold data directory",
    value=str(_PROJECT_ROOT / "data" / "gold" / "model_ready"),
))
MODELS_DIR = _PROJECT_ROOT / "models"
REPORTS_DIR = _PROJECT_ROOT / "reports"


# ── Helper loaders ────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_gold_split(split: str) -> pl.DataFrame | None:
    path = GOLD_DIR / f"{split}.parquet"
    if not path.exists():
        return None
    return pl.read_parquet(path)


@st.cache_data(ttl=60)
def load_mlflow_runs(experiment: str = "pm25_forecasting") -> "pd.DataFrame | None":
    try:
        import mlflow
        import pandas as pd

        mlflow_uri = str(_PROJECT_ROOT / "mlruns")
        mlflow.set_tracking_uri(mlflow_uri)
        runs = mlflow.search_runs(
            experiment_names=[experiment],
            order_by=["start_time DESC"],
        )
        return runs if not runs.empty else None
    except Exception:
        return None


@st.cache_data(ttl=60)
def load_run_manifest() -> dict | None:
    manifests = sorted(MODELS_DIR.glob("stunn_run_manifest.json"))
    if not manifests:
        return None
    try:
        with open(manifests[-1]) as f:
            return json.load(f)
    except Exception:
        return None


def _drift_report_exists() -> bool:
    return (REPORTS_DIR / "drift_report_latest.html").exists()


def _profile_report_exists() -> bool:
    return (REPORTS_DIR / "data_profile_latest.html").exists()


# ── Page sections ─────────────────────────────────────────────────────────────

def render_header() -> None:
    st.title("🌫 Bangkok PM2.5 — Forecasting Monitor")
    st.caption(
        "Real-time model performance, data drift detection, and pipeline health. "
        "Refresh the page to reload cached data."
    )
    st.divider()


def render_model_performance() -> None:
    st.header("📊 Model Performance")

    manifest = load_run_manifest()
    runs = load_mlflow_runs()

    if manifest:
        metrics = manifest.get("metrics", {})
        cfg = manifest.get("config", {})
        ts = manifest.get("created_at", "N/A")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Test MAE (Day 1)", f"{metrics.get('test_mae_h1', 'N/A'):.2f} μg/m³")
        with col2:
            st.metric("Test MAE (Day 3)", f"{metrics.get('test_MAE_h3', 'N/A'):.2f} μg/m³" if 'test_MAE_h3' in metrics else "N/A")
        with col3:
            st.metric("Extreme Detection Rate", f"{metrics.get('extreme_detection_rate', 0):.1%}")
        with col4:
            st.metric("Last Retrained", ts[:10] if ts != "N/A" else "N/A")

        with st.expander("Run config"):
            st.json(cfg)
    else:
        st.info("No run manifest found. Run `scripts/automated_retraining.py` to train a model.")

    if runs is not None:
        st.subheader("Training history (MLflow)")
        metric_cols = [c for c in runs.columns if c.startswith("metrics.")]
        if metric_cols:
            plot_data = runs[["start_time"] + metric_cols[:6]].set_index("start_time")
            st.line_chart(plot_data)
        else:
            st.dataframe(runs[["run_id", "status", "start_time"]].head(10))
    elif not manifest:
        st.warning("MLflow tracking URI not configured or no runs found.")


def render_drift_detection() -> None:
    st.header("📉 Data Drift Detection")

    train_df = load_gold_split("train")
    val_df = load_gold_split("val")

    if train_df is None or val_df is None:
        st.error("Gold data not found. Please run the Silver→Gold pipeline first.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Train rows", f"{len(train_df):,}")
        st.metric("Train date range",
                  f"{train_df['date'].min()} → {train_df['date'].max()}" if "date" in train_df.columns else "N/A")
    with col2:
        st.metric("Val rows", f"{len(val_df):,}")
        st.metric("Val date range",
                  f"{val_df['date'].min()} → {val_df['date'].max()}" if "date" in val_df.columns else "N/A")

    if st.button("Run drift check (KS test)"):
        with st.spinner("Running statistical drift detection..."):
            try:
                from src.validation.drift import detect_drift_statistical

                numeric_cols = [
                    c for c in train_df.columns
                    if train_df[c].dtype.is_numeric() and c in val_df.columns
                ][:20]
                result = detect_drift_statistical(train_df, val_df, numeric_cols)

                if result["drift_detected"]:
                    st.error(
                        f"Drift detected in {len(result['drifted_features'])} features "
                        f"({result['drift_share']:.0%} of checked columns)"
                    )
                    st.write("Drifted features:", result["drifted_features"])
                else:
                    st.success("No significant drift detected.")

                if result.get("feature_stats"):
                    stats_df = pl.DataFrame([
                        {"feature": k, "ks_stat": v["ks_stat"], "p_value": v["p_value"]}
                        for k, v in result["feature_stats"].items()
                    ]).sort("ks_stat", descending=True)
                    st.dataframe(stats_df.to_pandas(), use_container_width=True)
            except ImportError:
                st.error("scipy not installed. Run: uv add scipy")

    if _drift_report_exists():
        with open(REPORTS_DIR / "drift_report_latest.html") as f:
            html_content = f.read()
        with st.expander("Evidently Drift Report"):
            st.components.v1.html(html_content, height=600, scrolling=True)


def render_data_quality() -> None:
    st.header("✅ Data Quality")

    train_df = load_gold_split("train")
    if train_df is None:
        st.warning("No Gold training data found.")
        return

    from src.validation.profiling import quick_quality_summary
    from src.validation.schemas import validate_gold_schema, validate_dataframe_completeness

    quality = quick_quality_summary(train_df)
    completeness = validate_dataframe_completeness(train_df)
    is_valid, errors = validate_gold_schema(train_df)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Rows", f"{quality['rows']:,}")
    with col2:
        st.metric("Null rate", f"{quality['null_rate']:.1%}")
    with col3:
        st.metric("Completeness", f"{completeness['overall_completeness']:.1%}")
    with col4:
        st.metric("Schema valid", "✅" if is_valid else "❌")

    if errors:
        st.warning(f"Validation errors ({len(errors)}):")
        for err in errors[:5]:
            st.text(f"  • {err}")

    # Null counts per column
    null_data = {
        col: train_df[col].null_count()
        for col in train_df.columns
        if train_df[col].null_count() > 0
    }
    if null_data:
        st.subheader("Columns with nulls")
        null_df = pl.DataFrame({"column": list(null_data.keys()), "null_count": list(null_data.values())})
        st.bar_chart(null_df.to_pandas().set_index("column"))
    else:
        st.success("No null values in Gold training data.")

    if _profile_report_exists():
        with open(REPORTS_DIR / "data_profile_latest.html") as f:
            html_content = f.read()
        with st.expander("Full Data Profile Report (ydata-profiling)"):
            st.components.v1.html(html_content, height=800, scrolling=True)
    else:
        if st.button("Generate data profile"):
            with st.spinner("Generating profile report..."):
                from src.validation.profiling import generate_profile_report

                REPORTS_DIR.mkdir(parents=True, exist_ok=True)
                path = generate_profile_report(
                    train_df,
                    REPORTS_DIR / "data_profile_latest.html",
                    title="Gold Train — Data Profile",
                )
                st.success(f"Report saved to {path}")
                st.rerun()


def render_silver_coverage() -> None:
    st.header("🗄 Silver Layer Coverage")

    silver_root = _PROJECT_ROOT / "data" / "silver"
    if not silver_root.exists():
        st.warning("Silver layer not found.")
        return

    layers = [d for d in silver_root.iterdir() if d.is_dir()]
    if not layers:
        st.warning("No Silver layers found.")
        return

    for layer in sorted(layers):
        with st.expander(f"📂 {layer.name}"):
            year_dirs = sorted([d for d in layer.iterdir() if d.is_dir() and "year=" in d.name])
            coverage_rows = []
            for year_dir in year_dirs:
                year = year_dir.name.replace("year=", "")
                month_dirs = sorted([d for d in year_dir.iterdir() if d.is_dir() and "month=" in d.name])
                parquet_count = sum(1 for _ in year_dir.rglob("*.parquet"))
                size_mb = sum(p.stat().st_size for p in year_dir.rglob("*.parquet")) / 1e6
                coverage_rows.append({
                    "year": year,
                    "months": len(month_dirs),
                    "parquet_files": parquet_count,
                    "size_mb": round(size_mb, 1),
                })
            if coverage_rows:
                cov_df = pl.DataFrame(coverage_rows)
                st.dataframe(cov_df.to_pandas(), use_container_width=True)
                st.caption(f"Total files: {cov_df['parquet_files'].sum():,} | "
                           f"Total size: {cov_df['size_mb'].sum():.1f} MB")


# ── Main app ──────────────────────────────────────────────────────────────────

def main() -> None:
    render_header()

    tab_model, tab_drift, tab_quality, tab_silver = st.tabs([
        "Model Performance",
        "Data Drift",
        "Data Quality",
        "Silver Coverage",
    ])

    with tab_model:
        render_model_performance()

    with tab_drift:
        render_drift_detection()

    with tab_quality:
        render_data_quality()

    with tab_silver:
        render_silver_coverage()

    st.sidebar.divider()
    st.sidebar.caption(
        "**Quick start**\n\n"
        "```bash\n"
        "# Train model\n"
        "uv run python scripts/automated_retraining.py --force\n\n"
        "# Daily loop\n"
        "uv run python scripts/automated_retraining.py --loop\n\n"
        "# Run tests\n"
        "uv run pytest tests/unit/ -v\n"
        "```"
    )


if __name__ == "__main__":
    main()
