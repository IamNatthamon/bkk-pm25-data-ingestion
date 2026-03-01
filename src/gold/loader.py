"""
Gold Layer Data Loader

Loads Silver layer data (2023-2025) with schema handling for different datetime precisions.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.stdout.reconfigure(line_buffering=True)


def load_silver_airquality(
    silver_path: Path,
    years: list[int],
) -> pl.DataFrame:
    """
    Load Silver air quality data for specified years.
    
    Handles schema mismatches between years (datetime precision).
    """
    print(f"\n📂 Loading Silver AQ data for years: {years}", flush=True)
    
    all_dfs = []
    
    for year in years:
        year_path = silver_path / f"year={year}"
        
        if not year_path.exists():
            print(f"  ⚠️  Year {year}: No data found", flush=True)
            continue
        
        year_dfs = []
        months_loaded = 0
        
        for month in range(1, 13):
            month_path = year_path / f"month={month:02d}"
            
            if not month_path.exists():
                continue
            
            parquet_files = [
                f for f in month_path.iterdir() 
                if f.suffix == ".parquet" and not f.name.endswith(".md5")
            ]
            
            if not parquet_files:
                continue
            
            # Read each file separately to handle schema differences
            month_file_dfs = []
            for pf in parquet_files:
                try:
                    df = pl.read_parquet(pf)
                    
                    # Standardize column names (handle old naming)
                    rename_map = {
                        "no2_ugm3": "nitrogen_dioxide_ugm3",
                        "o3_ugm3": "ozone_ugm3",
                        "so2_ugm3": "sulphur_dioxide_ugm3",
                        "co_ugm3": "carbon_monoxide_ugm3",
                    }
                    
                    for old_name, new_name in rename_map.items():
                        if old_name in df.columns:
                            df = df.rename({old_name: new_name})
                    
                    # Standardize datetime to microseconds
                    if df.schema["timestamp_utc"] == pl.Datetime("ns", "UTC"):
                        df = df.with_columns(
                            pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
                        )
                    
                    # Ensure Float64 for all numeric columns
                    numeric_cols = [
                        "pm2_5_ugm3", "pm10_ugm3", "nitrogen_dioxide_ugm3",
                        "ozone_ugm3", "sulphur_dioxide_ugm3", "carbon_monoxide_ugm3"
                    ]
                    
                    for col in numeric_cols:
                        if col in df.columns:
                            df = df.with_columns(pl.col(col).cast(pl.Float64))
                    
                    month_file_dfs.append(df)
                    
                except Exception as e:
                    print(f"    ⚠️  Error reading {pf.name}: {e}", flush=True)
            
            if month_file_dfs:
                month_df = pl.concat(month_file_dfs)
                year_dfs.append(month_df)
                months_loaded += 1
        
        if year_dfs:
            year_combined = pl.concat(year_dfs)
            all_dfs.append(year_combined)
            
            print(
                f"  ✅ Year {year}: {len(year_combined):,} rows | "
                f"{year_combined['stationID'].n_unique()} stations | "
                f"{months_loaded}/12 months",
                flush=True
            )
    
    if not all_dfs:
        raise ValueError("No data loaded! Check Silver layer paths.")
    
    combined = pl.concat(all_dfs)
    
    # Sort by station and time
    combined = combined.sort(["stationID", "timestamp_utc"])
    
    print(
        f"\n✅ Total loaded: {len(combined):,} rows | "
        f"{combined['stationID'].n_unique()} stations",
        flush=True
    )
    print(
        f"   Date range: {combined['timestamp_utc'].min()} → {combined['timestamp_utc'].max()}",
        flush=True
    )
    
    return combined


def load_stations(stations_path: Path) -> pl.DataFrame:
    """Load station metadata"""
    print(f"\n📍 Loading stations from {stations_path}", flush=True)
    
    stations = pl.read_parquet(stations_path)
    
    print(f"✅ Loaded {len(stations)} stations", flush=True)
    
    return stations


if __name__ == "__main__":
    from config.gold import config
    
    # Test loader
    df = load_silver_airquality(
        config.silver_aq_path,
        config.target_years
    )
    
    print(f"\n📊 Data Summary:")
    print(f"   Shape: {df.shape}")
    print(f"   Columns: {df.columns}")
    print(f"   Memory: {df.estimated_size('mb'):.1f} MB")
    
    # Check data quality
    null_counts = df.null_count()
    print(f"\n🔍 Null counts:")
    for col in ["pm2_5_ugm3", "pm10_ugm3", "nitrogen_dioxide_ugm3"]:
        if col in null_counts.columns:
            nulls = null_counts[col][0]
            pct = (nulls / len(df)) * 100
            print(f"   {col}: {nulls:,} ({pct:.2f}%)")
