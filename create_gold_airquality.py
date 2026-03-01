#!/usr/bin/env python3
"""
Create Gold Layer for Air Quality Data (2023-2025)

This script:
1. Loads Silver AQ data from 2023-2025
2. Handles schema mismatches (datetime precision)
3. Combines all data into unified dataset
4. Performs feature engineering
5. Saves to Gold layer for ML
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import polars as pl

sys.stdout.reconfigure(line_buffering=True)

PROJECT_ROOT = Path.cwd()
SILVER_AQ = PROJECT_ROOT / "data" / "silver" / "openmeteo_airquality"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_AQ = GOLD_DIR / "airquality_combined"

GOLD_AQ.mkdir(parents=True, exist_ok=True)

TARGET_YEARS = [2023, 2024, 2025]


def load_year_data(year: int) -> pl.DataFrame:
    """Load all data for a specific year, handling schema mismatches"""
    print(f"\n📅 Loading {year} data...", flush=True)
    
    year_dfs = []
    
    for month in range(1, 13):
        month_path = SILVER_AQ / f"year={year}" / f"month={month:02d}"
        
        if not month_path.exists():
            continue
        
        parquet_files = [f for f in month_path.glob("*.parquet") if not f.name.endswith(".md5")]
        
        if not parquet_files:
            continue
        
        # Read each file individually to handle schema issues
        for pf in parquet_files:
            try:
                df = pl.read_parquet(pf)
                
                # Standardize datetime to microseconds
                if df.schema["timestamp_utc"] == pl.Datetime("ns", "UTC"):
                    df = df.with_columns(
                        pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
                    )
                
                # Ensure ingestion_timestamp_utc is datetime with UTC timezone
                if "ingestion_timestamp_utc" in df.columns:
                    if df.schema["ingestion_timestamp_utc"] == pl.String:
                        df = df.with_columns(
                            pl.col("ingestion_timestamp_utc").str.to_datetime().dt.replace_time_zone("UTC")
                        )
                    elif df.schema["ingestion_timestamp_utc"] == pl.Datetime("us"):
                        df = df.with_columns(
                            pl.col("ingestion_timestamp_utc").dt.replace_time_zone("UTC")
                        )
                
                # Standardize column names (some files use short names)
                column_mapping = {
                    "no2_ugm3": "nitrogen_dioxide_ugm3",
                    "o3_ugm3": "ozone_ugm3",
                    "so2_ugm3": "sulphur_dioxide_ugm3",
                    "co_ugm3": "carbon_monoxide_ugm3",
                }
                
                for old_name, new_name in column_mapping.items():
                    if old_name in df.columns:
                        df = df.rename({old_name: new_name})
                
                # Standardize all float columns to Float64
                float_cols = ["pm2_5_ugm3", "pm10_ugm3", "nitrogen_dioxide_ugm3", 
                             "ozone_ugm3", "sulphur_dioxide_ugm3", "carbon_monoxide_ugm3"]
                
                for col in float_cols:
                    if col in df.columns:
                        if df.schema[col] in [pl.Float32, pl.Float64]:
                            df = df.with_columns(pl.col(col).cast(pl.Float64))
                
                year_dfs.append(df)
                
            except Exception as e:
                print(f"  ⚠️  Skipping {pf.name}: {e}", flush=True)
    
    if not year_dfs:
        return pl.DataFrame()
    
    df_year = pl.concat(year_dfs)
    print(f"  ✅ {year}: {len(df_year):,} rows | {df_year['stationID'].n_unique()} stations", flush=True)
    
    return df_year


def create_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """Add temporal features for ML"""
    print("\n🔧 Creating temporal features...", flush=True)
    
    df = df.with_columns([
        # Date components
        pl.col("timestamp_utc").dt.year().alias("year"),
        pl.col("timestamp_utc").dt.month().alias("month"),
        pl.col("timestamp_utc").dt.day().alias("day"),
        pl.col("timestamp_utc").dt.hour().alias("hour"),
        pl.col("timestamp_utc").dt.weekday().alias("weekday"),
        pl.col("timestamp_utc").dt.ordinal_day().alias("day_of_year"),
        
        # Cyclical encoding
        (pl.col("timestamp_utc").dt.hour() * 2 * 3.14159 / 24).sin().alias("hour_sin"),
        (pl.col("timestamp_utc").dt.hour() * 2 * 3.14159 / 24).cos().alias("hour_cos"),
        (pl.col("timestamp_utc").dt.month() * 2 * 3.14159 / 12).sin().alias("month_sin"),
        (pl.col("timestamp_utc").dt.month() * 2 * 3.14159 / 12).cos().alias("month_cos"),
        
        # Time flags
        (pl.col("timestamp_utc").dt.hour().is_between(6, 18)).alias("is_daytime"),
        (pl.col("timestamp_utc").dt.weekday() < 5).alias("is_weekday"),
    ])
    
    print(f"  ✅ Added temporal features", flush=True)
    return df


def create_lag_features(df: pl.DataFrame, target_col: str = "pm2_5_ugm3") -> pl.DataFrame:
    """Create lag features for time series"""
    print(f"\n🔧 Creating lag features for {target_col}...", flush=True)
    
    # Sort by station and time
    df = df.sort(["stationID", "timestamp_utc"])
    
    # Create lag features (1h, 3h, 6h, 12h, 24h)
    lag_hours = [1, 3, 6, 12, 24]
    
    for lag in lag_hours:
        df = df.with_columns(
            pl.col(target_col)
            .shift(lag)
            .over("stationID")
            .alias(f"{target_col}_lag_{lag}h")
        )
    
    print(f"  ✅ Added {len(lag_hours)} lag features", flush=True)
    return df


def create_rolling_features(df: pl.DataFrame, target_col: str = "pm2_5_ugm3") -> pl.DataFrame:
    """Create rolling window statistics"""
    print(f"\n🔧 Creating rolling features for {target_col}...", flush=True)
    
    # Sort by station and time
    df = df.sort(["stationID", "timestamp_utc"])
    
    # Rolling windows (3h, 6h, 12h, 24h)
    windows = [3, 6, 12, 24]
    
    for window in windows:
        df = df.with_columns([
            pl.col(target_col)
            .rolling_mean(window)
            .over("stationID")
            .alias(f"{target_col}_rolling_mean_{window}h"),
            
            pl.col(target_col)
            .rolling_std(window)
            .over("stationID")
            .alias(f"{target_col}_rolling_std_{window}h"),
            
            pl.col(target_col)
            .rolling_max(window)
            .over("stationID")
            .alias(f"{target_col}_rolling_max_{window}h"),
            
            pl.col(target_col)
            .rolling_min(window)
            .over("stationID")
            .alias(f"{target_col}_rolling_min_{window}h"),
        ])
    
    print(f"  ✅ Added rolling features for {len(windows)} windows", flush=True)
    return df


def create_station_aggregates(df: pl.DataFrame) -> pl.DataFrame:
    """Create station-level aggregate features"""
    print("\n🔧 Creating station aggregate features...", flush=True)
    
    # Calculate station statistics
    station_stats = df.group_by("stationID").agg([
        pl.col("pm2_5_ugm3").mean().alias("station_pm25_mean"),
        pl.col("pm2_5_ugm3").std().alias("station_pm25_std"),
        pl.col("pm10_ugm3").mean().alias("station_pm10_mean"),
    ])
    
    # Join back to main dataframe
    df = df.join(station_stats, on="stationID", how="left")
    
    print(f"  ✅ Added station aggregate features", flush=True)
    return df


def main() -> None:
    """Main execution"""
    print("\n" + "=" * 100, flush=True)
    print(" " * 25 + "🏗️  CREATING GOLD LAYER: AIR QUALITY (2023-2025)", flush=True)
    print("=" * 100 + "\n", flush=True)
    
    # Load data from all years
    all_years_data = []
    
    for year in TARGET_YEARS:
        df_year = load_year_data(year)
        if not df_year.is_empty():
            all_years_data.append(df_year)
    
    if not all_years_data:
        print("❌ No data loaded!", flush=True)
        return
    
    # Combine all years
    print("\n" + "=" * 100, flush=True)
    print("🔗 Combining all years...", flush=True)
    df_combined = pl.concat(all_years_data)
    
    print(f"\n📊 Combined Dataset:", flush=True)
    print(f"   Total rows: {len(df_combined):,}", flush=True)
    print(f"   Date range: {df_combined['timestamp_utc'].min()} → {df_combined['timestamp_utc'].max()}", flush=True)
    print(f"   Stations: {df_combined['stationID'].n_unique()}", flush=True)
    print(f"   Years: {sorted(df_combined['timestamp_utc'].dt.year().unique().to_list())}", flush=True)
    
    # Sort by station and time
    df_combined = df_combined.sort(["stationID", "timestamp_utc"])
    
    # Feature engineering
    print("\n" + "=" * 100, flush=True)
    print("⚙️  FEATURE ENGINEERING", flush=True)
    print("=" * 100, flush=True)
    
    df_gold = create_temporal_features(df_combined)
    df_gold = create_lag_features(df_gold, "pm2_5_ugm3")
    df_gold = create_rolling_features(df_gold, "pm2_5_ugm3")
    df_gold = create_station_aggregates(df_gold)
    
    # Remove rows with null target (first few rows per station)
    initial_rows = len(df_gold)
    df_gold = df_gold.filter(pl.col("pm2_5_ugm3").is_not_null())
    removed_rows = initial_rows - len(df_gold)
    
    print(f"\n🧹 Removed {removed_rows:,} rows with null target", flush=True)
    
    # Save to Gold layer
    print("\n" + "=" * 100, flush=True)
    print("💾 SAVING TO GOLD LAYER", flush=True)
    print("=" * 100, flush=True)
    
    # Save as single parquet file
    gold_file = GOLD_AQ / "airquality_2023_2025.parquet"
    df_gold.write_parquet(gold_file, compression="snappy")
    
    file_size_mb = gold_file.stat().st_size / (1024 * 1024)
    
    print(f"\n✅ Saved Gold layer:", flush=True)
    print(f"   File: {gold_file}", flush=True)
    print(f"   Size: {file_size_mb:.1f} MB", flush=True)
    print(f"   Rows: {len(df_gold):,}", flush=True)
    print(f"   Columns: {len(df_gold.columns)}", flush=True)
    
    # Save column list
    columns_file = GOLD_AQ / "columns.txt"
    with open(columns_file, "w") as f:
        f.write("# Gold Layer Columns\n\n")
        f.write("## Original Columns\n")
        for col in ["stationID", "lat", "lon", "timestamp_utc", "pm2_5_ugm3", "pm10_ugm3", 
                    "nitrogen_dioxide_ugm3", "ozone_ugm3", "sulphur_dioxide_ugm3", "carbon_monoxide_ugm3"]:
            if col in df_gold.columns:
                f.write(f"- {col}\n")
        
        f.write("\n## Temporal Features\n")
        for col in df_gold.columns:
            if any(x in col for x in ["year", "month", "day", "hour", "weekday", "sin", "cos", "is_"]):
                f.write(f"- {col}\n")
        
        f.write("\n## Lag Features\n")
        for col in df_gold.columns:
            if "lag" in col:
                f.write(f"- {col}\n")
        
        f.write("\n## Rolling Features\n")
        for col in df_gold.columns:
            if "rolling" in col:
                f.write(f"- {col}\n")
        
        f.write("\n## Station Aggregates\n")
        for col in df_gold.columns:
            if "station_" in col:
                f.write(f"- {col}\n")
    
    print(f"   Columns list: {columns_file}", flush=True)
    
    # Summary statistics
    print("\n" + "=" * 100, flush=True)
    print("📈 SUMMARY STATISTICS", flush=True)
    print("=" * 100, flush=True)
    
    print(f"\nPM2.5 Statistics:", flush=True)
    print(f"   Mean: {df_gold['pm2_5_ugm3'].mean():.2f} µg/m³", flush=True)
    print(f"   Std:  {df_gold['pm2_5_ugm3'].std():.2f} µg/m³", flush=True)
    print(f"   Min:  {df_gold['pm2_5_ugm3'].min():.2f} µg/m³", flush=True)
    print(f"   Max:  {df_gold['pm2_5_ugm3'].max():.2f} µg/m³", flush=True)
    print(f"   Null: {df_gold['pm2_5_ugm3'].null_count():,} ({df_gold['pm2_5_ugm3'].null_count() / len(df_gold) * 100:.2f}%)", flush=True)
    
    print(f"\nData Coverage:", flush=True)
    yearly_counts = df_gold.group_by(pl.col("timestamp_utc").dt.year()).agg(pl.len().alias("count"))
    for row in yearly_counts.sort("timestamp_utc").iter_rows(named=True):
        print(f"   {row['timestamp_utc']}: {row['count']:,} rows", flush=True)
    
    print("\n" + "=" * 100, flush=True)
    print("✅ GOLD LAYER CREATION COMPLETE!", flush=True)
    print("=" * 100 + "\n", flush=True)
    
    print("📁 Output files:", flush=True)
    print(f"   • {gold_file}", flush=True)
    print(f"   • {columns_file}", flush=True)
    print(f"\n🎯 Ready for ML model training!\n", flush=True)


if __name__ == "__main__":
    main()
