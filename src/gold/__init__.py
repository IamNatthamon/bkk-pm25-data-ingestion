"""
src.gold — DEPRECATED

All functionality has been superseded by src.silver_to_gold.
Use src.silver_to_gold.pipeline.run_silver_to_gold_pipeline() for the canonical ETL pipeline.
"""

import warnings

warnings.warn(
    "The src.gold package is deprecated and will be removed in a future release. "
    "Use src.silver_to_gold instead.",
    DeprecationWarning,
    stacklevel=2,
)
