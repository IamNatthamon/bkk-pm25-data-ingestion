"""Automated data profiling for the Gold layer."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from src.utils.logger import get_logger

log = get_logger(__name__)


def generate_profile_report(
    df: pl.DataFrame,
    output_path: Path,
    title: str = "Gold Layer Data Profile",
    explorative: bool = True,
) -> Path:
    """Generate a comprehensive HTML data profile using ydata-profiling.

    Falls back to a simple Polars describe() text report if ydata-profiling
    is not installed.

    Args:
        df: DataFrame to profile.
        output_path: Path to write the HTML (or text) report.
        title: Report title.
        explorative: If True, include correlation matrices and interactions.

    Returns:
        Path to the generated report.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from ydata_profiling import ProfileReport

        profile = ProfileReport(
            df.to_pandas(),
            title=title,
            explorative=explorative,
            correlations={
                "pearson": {"calculate": True},
                "spearman": {"calculate": True},
            },
        )
        profile.to_file(output_path)
        log.info("profiling.html_report_saved", path=str(output_path), rows=len(df))

    except ImportError:
        log.warning("profiling.ydata_not_installed_fallback_to_text")

        text_path = output_path.with_suffix(".txt")
        describe = df.describe()
        text_path.write_text(describe.__str__())
        log.info("profiling.text_report_saved", path=str(text_path))
        output_path = text_path

    return output_path


def quick_quality_summary(df: pl.DataFrame) -> dict:
    """Return a lightweight quality summary dict (no external deps).

    Returns dict with: rows, columns, null_rate, duplicate_rate, numeric_cols.
    """
    n = len(df)
    total_cells = n * len(df.columns)
    total_nulls = sum(df[c].null_count() for c in df.columns)

    numeric_cols = [c for c in df.columns if df[c].dtype.is_numeric()]

    duplicate_count = n - df.unique().shape[0]

    summary = {
        "rows": n,
        "columns": len(df.columns),
        "null_rate": round(total_nulls / max(total_cells, 1), 4),
        "duplicate_rate": round(duplicate_count / max(n, 1), 4),
        "numeric_cols": len(numeric_cols),
    }
    log.info("profiling.quick_summary", **summary)
    return summary
