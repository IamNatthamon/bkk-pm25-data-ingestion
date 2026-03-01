#!/usr/bin/env python3
"""
Execute Silver → Gold transformation pipeline

Usage:
    python scripts/run_silver_to_gold.py
    
Environment variables:
    BKK_PM25_PROJECT_ROOT: Override project root path
    BKK_PM25_RANDOM_SEED: Set random seed (default: 42)
"""

from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.silver_to_gold.pipeline import run_silver_to_gold_pipeline
from src.silver_to_gold.quality import (
    assess_data_quality,
    check_station_coverage,
    check_temporal_coverage,
    print_quality_report,
)
from src.utils.config import PipelineConfig
from src.utils.logger import get_logger, setup_logging


def main() -> None:
    """Main execution function"""
    config = PipelineConfig()

    setup_logging(
        log_file=config.logs_dir / "silver_to_gold.log",
        level="INFO",
    )

    log = get_logger(__name__)

    log.info(
        "pipeline.init",
        silver_weather=str(config.silver_weather_dir),
        silver_aq=str(config.silver_airquality_dir),
        gold_output=str(config.gold_dir),
    )

    try:
        output_paths = run_silver_to_gold_pipeline(config)

        log.info("pipeline.success", outputs=output_paths)

        print("\n" + "=" * 100)
        print("GOLD LAYER CREATED SUCCESSFULLY")
        print("=" * 100)
        for name, path in output_paths.items():
            print(f"{name:>12}: {path}")
        print("=" * 100 + "\n")

    except Exception as e:
        log.error("pipeline.failed", error=str(e), error_type=type(e).__name__)
        raise


if __name__ == "__main__":
    main()
