# 🏆 Gold Layer: Air Quality Database (2023-2025)

## 📊 Overview

**Gold Layer** คือ database กลางที่รวมข้อมูล Air Quality จาก **3 ปีเต็ม** (2023-2025) พร้อม feature engineering สำหรับ Machine Learning

### ✅ Data Coverage

| Year | Rows | Months | Status |
|------|------|--------|--------|
| 2023 | 643,752 | 12/12 | ✅ Complete |
| 2024 | 1,387,872 | 12/12 | ✅ Complete |
| 2025 | 1,333,296 | 12/12 | ✅ Complete |
| **Total** | **3,364,920** | **36 months** | 🎉 |

---

## 📁 File Location

```
data/gold/airquality_combined/
├── airquality_2023_2025.parquet    # 375 MB - Main dataset
└── columns.txt                      # Column documentation
```

---

## 🔢 Dataset Specifications

### Basic Info
- **Total rows**: 3,364,920
- **Total columns**: 52
- **Date range**: 2023-01-01 00:00:00 → 2025-12-31 23:00:00
- **Stations**: 79 Bangkok monitoring stations
- **File size**: 375 MB (compressed Parquet)
- **Time resolution**: Hourly

### PM2.5 Statistics
- **Mean**: 26.28 µg/m³
- **Median**: 19.40 µg/m³
- **Std Dev**: 24.41 µg/m³
- **Min**: 0.80 µg/m³
- **Max**: 549.50 µg/m³
- **95th percentile**: 70.30 µg/m³
- **Null values**: 0 (0.00%)

---

## 📋 Features (52 columns)

### 1. Original Columns (16)
Core data from Silver layer:
- `stationID` - Station identifier
- `lat`, `lon` - GPS coordinates
- `timestamp_utc` - Timestamp in UTC
- `timestamp_unix_ms` - Unix timestamp (milliseconds)
- `pm2_5_ugm3` - **Target variable** (PM2.5 concentration)
- `pm10_ugm3` - PM10 concentration
- `nitrogen_dioxide_ugm3` - NO₂ concentration
- `ozone_ugm3` - O₃ concentration
- `sulphur_dioxide_ugm3` - SO₂ concentration
- `carbon_monoxide_ugm3` - CO concentration
- `data_source` - Data source identifier
- `ingestion_timestamp_utc` - When data was ingested
- `load_id` - Batch load identifier
- `pipeline_version` - Pipeline version
- `record_hash` - Record hash for deduplication

### 2. Temporal Features (12)
Time-based features for ML:
- `year`, `month`, `day`, `hour` - Date components
- `weekday` - Day of week (0=Monday, 6=Sunday)
- `day_of_year` - Day number in year (1-365/366)
- `hour_sin`, `hour_cos` - Cyclical hour encoding
- `month_sin`, `month_cos` - Cyclical month encoding
- `is_daytime` - Boolean: 06:00-18:00
- `is_weekday` - Boolean: Monday-Friday

### 3. Lag Features (5)
Historical PM2.5 values:
- `pm2_5_ugm3_lag_1h` - PM2.5 from 1 hour ago
- `pm2_5_ugm3_lag_3h` - PM2.5 from 3 hours ago
- `pm2_5_ugm3_lag_6h` - PM2.5 from 6 hours ago
- `pm2_5_ugm3_lag_12h` - PM2.5 from 12 hours ago
- `pm2_5_ugm3_lag_24h` - PM2.5 from 24 hours ago

### 4. Rolling Window Features (16)
Statistical aggregations over time windows (3h, 6h, 12h, 24h):

**For each window:**
- `pm2_5_ugm3_rolling_mean_{window}h` - Rolling average
- `pm2_5_ugm3_rolling_std_{window}h` - Rolling standard deviation
- `pm2_5_ugm3_rolling_max_{window}h` - Rolling maximum
- `pm2_5_ugm3_rolling_min_{window}h` - Rolling minimum

### 5. Station Aggregate Features (3)
Station-level statistics:
- `station_pm25_mean` - Average PM2.5 for this station (all time)
- `station_pm25_std` - PM2.5 standard deviation for this station
- `station_pm10_mean` - Average PM10 for this station

---

## 🎯 Usage Examples

### Load Data

```python
import polars as pl

# Load full dataset
df = pl.read_parquet('data/gold/airquality_combined/airquality_2023_2025.parquet')

# Load specific columns only
df_subset = pl.read_parquet(
    'data/gold/airquality_combined/airquality_2023_2025.parquet',
    columns=['stationID', 'timestamp_utc', 'pm2_5_ugm3', 'hour', 'weekday']
)
```

### Filter by Date

```python
# Get 2024 data only
df_2024 = df.filter(pl.col('year') == 2024)

# Get specific month
df_jan_2024 = df.filter(
    (pl.col('year') == 2024) & (pl.col('month') == 1)
)

# Get date range
df_range = df.filter(
    pl.col('timestamp_utc').is_between('2024-01-01', '2024-03-31')
)
```

### Filter by Station

```python
# Single station
df_station = df.filter(pl.col('stationID') == 'bkp01t')

# Multiple stations
df_stations = df.filter(pl.col('stationID').is_in(['bkp01t', 'bkp02t', 'bkp03t']))
```

### Aggregate Statistics

```python
# Monthly averages
monthly_avg = df.group_by(['year', 'month']).agg([
    pl.col('pm2_5_ugm3').mean().alias('pm25_mean'),
    pl.col('pm2_5_ugm3').std().alias('pm25_std'),
    pl.col('stationID').n_unique().alias('stations')
])

# Station rankings
station_avg = df.group_by('stationID').agg([
    pl.col('pm2_5_ugm3').mean().alias('avg_pm25'),
    pl.col('pm2_5_ugm3').max().alias('max_pm25')
]).sort('avg_pm25', descending=True)
```

---

## 🤖 ML Model Training

### Train/Val/Test Split

```python
# Chronological split (important for time series!)
train_end = '2024-06-30'
val_end = '2024-12-31'

df_train = df.filter(pl.col('timestamp_utc') <= train_end)
df_val = df.filter(
    (pl.col('timestamp_utc') > train_end) & 
    (pl.col('timestamp_utc') <= val_end)
)
df_test = df.filter(pl.col('timestamp_utc') > val_end)

print(f"Train: {len(df_train):,} rows")
print(f"Val:   {len(df_val):,} rows")
print(f"Test:  {len(df_test):,} rows")
```

### Feature Selection

```python
# Select features for modeling
feature_cols = [
    # Temporal
    'hour', 'weekday', 'day_of_year', 'month',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    'is_daytime', 'is_weekday',
    
    # Lag features
    'pm2_5_ugm3_lag_1h', 'pm2_5_ugm3_lag_3h', 
    'pm2_5_ugm3_lag_6h', 'pm2_5_ugm3_lag_12h', 'pm2_5_ugm3_lag_24h',
    
    # Rolling features
    'pm2_5_ugm3_rolling_mean_24h', 'pm2_5_ugm3_rolling_std_24h',
    
    # Other pollutants
    'pm10_ugm3', 'nitrogen_dioxide_ugm3', 'ozone_ugm3',
    
    # Station context
    'station_pm25_mean', 'station_pm25_std',
]

target_col = 'pm2_5_ugm3'

X_train = df_train.select(feature_cols)
y_train = df_train.select(target_col)
```

---

## 📈 Data Quality Notes

### ✅ Strengths
- **Complete coverage**: 3 full years of hourly data
- **No missing targets**: PM2.5 has 0% null values
- **Rich features**: 52 columns including temporal, lag, and rolling features
- **Consistent schema**: All data standardized to Float64
- **79 stations**: Good spatial coverage across Bangkok

### ⚠️ Considerations
- **Lag features**: First 24 hours per station have null lag features (expected)
- **Rolling features**: First few hours per station have null rolling stats (expected)
- **Data imbalance**: Some months may have slightly different row counts
- **Outliers**: Max PM2.5 of 549.5 µg/m³ (extreme pollution event) - consider clipping for training

---

## 🔧 Scripts Created

1. **`create_gold_airquality.py`** - Main pipeline script
   - Loads Silver data from 2023-2025
   - Handles schema mismatches
   - Performs feature engineering
   - Saves to Gold layer

2. **`explore_gold_airquality.ipynb`** - Jupyter notebook
   - Data exploration
   - Quality checks
   - Visualizations
   - Usage examples

3. **`backfill_airquality_2023.ipynb`** - Backfill notebook
   - Used to fetch 2023 data from Open-Meteo API

---

## 🎯 Ready for ML!

Gold layer พร้อมใช้งานสำหรับ:
- ✅ PM2.5 forecasting models
- ✅ Time series analysis
- ✅ Anomaly detection
- ✅ Spatial analysis
- ✅ Feature importance studies

**Next step**: Train your ML model using this Gold layer dataset! 🚀
