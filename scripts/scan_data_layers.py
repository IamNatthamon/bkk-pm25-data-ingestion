#!/usr/bin/env python3
"""
Scan and report on all data layers (Bronze/Silver/Gold)

Usage:
    python scripts/scan_data_layers.py
"""

from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import PipelineConfig


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def scan_directory(path: Path, file_pattern: str = "*") -> dict[str, int | str]:
    """Scan directory and return statistics"""
    if not path.exists():
        return {
            "exists": False,
            "file_count": 0,
            "total_size": "N/A",
            "subdirs": 0,
        }

    files = list(path.rglob(file_pattern))
    total_size = sum(f.stat().st_size for f in files if f.is_file())
    subdirs = len([d for d in path.rglob("*") if d.is_dir()])

    return {
        "exists": True,
        "file_count": len(files),
        "total_size": format_size(total_size),
        "subdirs": subdirs,
    }


def main() -> None:
    """Main execution"""
    config = PipelineConfig()

    print("\n" + "=" * 100)
    print("DATA LAYERS SCAN REPORT")
    print("=" * 100)
    print(f"Project root: {config.project_root}")
    print("=" * 100 + "\n")

    layers = [
        ("BRONZE - Weather", config.bronze_weather_dir, "*.json.gz"),
        ("BRONZE - Air Quality", config.bronze_airquality_dir, "*.json.gz"),
        ("SILVER - Weather", config.silver_weather_dir, "*.parquet"),
        ("SILVER - Air Quality", config.silver_airquality_dir, "*.parquet"),
        ("GOLD - Model Ready", config.gold_dir, "*.parquet"),
        ("STATIONS - Metadata", config.stations_path.parent, "*.parquet"),
    ]

    for layer_name, layer_path, pattern in layers:
        stats = scan_directory(layer_path, pattern)

        print(f"📁 {layer_name}")
        print(f"   Path: {layer_path}")
        print(f"   Exists: {'✅ Yes' if stats['exists'] else '❌ No'}")

        if stats["exists"]:
            print(f"   Files: {stats['file_count']:,}")
            print(f"   Size: {stats['total_size']}")
            print(f"   Subdirectories: {stats['subdirs']}")

        print()

    print("=" * 100)

    bronze_weather = scan_directory(config.bronze_weather_dir, "*.json.gz")
    silver_weather = scan_directory(config.silver_weather_dir, "*.parquet")
    gold = scan_directory(config.gold_dir, "*.parquet")

    print("\nSUMMARY:")
    print(
        f"  Bronze → Silver: {bronze_weather['file_count']} JSON.gz → {silver_weather['file_count']} Parquet"
    )
    print(f"  Silver → Gold: {silver_weather['file_count']} Parquet → {gold['file_count']} splits")
    print(f"  Total storage: Bronze ({bronze_weather['total_size']}) + Silver ({silver_weather['total_size']}) + Gold ({gold['total_size']})")
    print("\n" + "=" * 100 + "\n")


if __name__ == "__main__":
    main()
