# ETL Pipeline - Bronze/Silver/Gold Architecture

Complete ETL implementation based on the project specification document.

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Kafka Streams  │────▶│  S3 Landing     │────▶│  ETL Pipeline   │────▶│  Gold Layer     │
│  (Producers)    │     │  (Raw JSON)     │     │  (Spark)        │     │  (Aggregated)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                       │
                              ▼                       ▼
                        Bronze Layer           Silver Layer
                        (Raw + Metadata)       (Cleaned + Features)
```

## Data Flow

### 1. Kafka → S3 Landing (Every 30 seconds)
- `kafka_to_local_uploader.py` consumes from Kafka
- Batches data by topic (vitals/movement)
- Saves as JSONL with timestamp partitioning

### 2. Landing → Bronze (Every 30 seconds)
- Reads raw JSON from landing zone
- Adds lineage metadata (`etl_processed_time`, `source_file`)
- Routes to vitals/movement bronze tables
- Saves as Parquet

### 3. Bronze → Silver (Cleaning)
- **Vitals**: Filters nulls, validates HR (30-220), SpO2 (90-100%)
- **Movement**: Filters nulls, calculates SVM (Signal Vector Magnitude)
- Converts timestamps
- Saves as Parquet

### 4. Silver → Gold (Aggregation)
- **Vitals Gold**: 10-minute windows, MEWS score calculation
- **Movement Gold**: 10-minute windows, fall detection count
- Optimized for BI dashboard queries

## File Structure

```
scripts/
├── etl_pipeline.py          # Main ETL (Bronze/Silver/Gold)
├── etl_scheduler.py         # Runs ETL every 30 seconds
├── kafka_to_local_uploader.py  # Kafka → Local (simulates S3)
└── ...

data/
├── s3_mock/                 # Kafka uploader output
└── etl_output/
    ├── landing/             # Raw JSON input
    ├── bronze/              # Raw + metadata
    ├── silver/              # Cleaned data
    └── gold/                # Aggregated data
```

## Quick Start

### Option 1: Run All Components (Recommended)

```bash
# Double-click or run:
start_pipeline.bat
```

This opens 5 terminal windows:
1. Vitals Producer
2. Movement Producer
3. Alert Engine (real-time)
4. S3/Local Uploader (Kafka → Landing)
5. **ETL Scheduler** (Landing → Bronze → Silver → Gold)

### Option 2: Run ETL Manually

```bash
# Activate venv
.\.venv\Scripts\Activate.ps1

# Run ETL once
python scripts/etl_pipeline.py

# Run ETL scheduler (every 30 seconds)
python scripts/etl_scheduler.py --interval 30 --auto-copy
```

## ETL Layers Explained

### Bronze Layer (Raw Ingestion)

**Input:** Raw JSON from landing zone
**Output:** `bronze/vitals/`, `bronze/movement/` (Parquet)

**Transformations:**
- Add `etl_processed_time` timestamp
- Add `source_file` lineage
- Add `etl_batch_date` for partitioning
- Router pattern: Split by `device_type`

```python
# Sample Bronze Record (Vitals)
{
  "patient_id": "P101",
  "device_type": "vital_monitor",
  "timestamp": 1710183600,
  "heart_rate": 85,
  "spo2": 98,
  "etl_processed_time": "2026-03-15 16:30:45",
  "source_file": "s3://.../vitals_20260315_163045.jsonl",
  "etl_batch_date": "2026-03-15"
}
```

### Silver Layer (Cleaning & Features)

**Input:** Bronze Parquet files
**Output:** `silver/vitals/`, `silver/movement/` (Parquet)

**Vitals Cleaning:**
- Remove null `heart_rate` and `spo2`
- Filter impossible values:
  - HR: 30-220 BPM
  - SpO2: 90-100%
- Convert timestamp to datetime

**Movement Cleaning:**
- Remove null x/y/z values
- Calculate SVM: `sqrt(x² + y² + z²)`
- Convert timestamp to datetime

```python
# Sample Silver Record (Movement)
{
  "patient_id": "P101",
  "timestamp": "2026-03-15 16:30:00",
  "x": 0.52,
  "y": 9.81,
  "z": 0.12,
  "svm": 9.83,  # Calculated feature
  "event_timestamp": "2026-03-15T16:30:00"
}
```

### Gold Layer (Aggregation)

**Input:** Silver Parquet files
**Output:** `gold/vitals_summary/`, `gold/movement_summary/` (Parquet)

**Vitals Aggregation (10-minute windows):**
- `avg_hr`: Average heart rate
- `peak_hr`: Maximum heart rate
- `avg_spo2`: Average SpO2
- `peak_spo2`: Maximum SpO2
- `mews_score`: Modified Early Warning Score (0-3)

**MEWS Score Logic:**
```
avg_hr > 130    → 3 (Critical Tachycardia)
avg_hr >= 111   → 2 (High HR)
avg_hr <= 40    → 2 (Critical Bradycardia)
avg_spo2 < 90   → 3 (Hypoxia)
otherwise       → 0 (Normal)
```

**Movement Aggregation (10-minute windows):**
- `avg_activity`: Average SVM
- `peak_impact`: Maximum SVM
- `fall_events_detected`: Count of SVM > 25.0

```python
# Sample Gold Record (Vitals Summary)
{
  "patient_id": "P101",
  "window_start": "2026-03-15 16:30:00",
  "window_end": "2026-03-15 16:40:00",
  "avg_hr": 85.5,
  "peak_hr": 135,
  "avg_spo2": 97.2,
  "mews_score": 2  # Alert!
}
```

## Scheduler Configuration

### Testing Mode (30 seconds)
```bash
python scripts/etl_scheduler.py --interval 30 --auto-copy
```

- Runs every 30 seconds
- Auto-copies data from `s3_mock` to `landing`
- Cleans up processed files after each run

### Production Mode (10 minutes)
```bash
python scripts/etl_scheduler.py --interval 600
```

- Runs every 10 minutes
- Expects data to be in landing zone from Kafka consumers
- Use with S3 paths (configure `.env`)

## Monitoring ETL Progress

### Check Output Directories
```bash
# Bronze
dir data\etl_output\bronze\vitals
dir data\etl_output\bronze\movement

# Silver
dir data\etl_output\silver\vitals
dir data\etl_output\silver\movement

# Gold
dir data\etl_output\gold\vitals_summary
dir data\etl_output\gold\movement_summary
```

### Read Parquet Files (Python)
```python
import pandas as pd

# Read Gold layer data
vitals_gold = pd.read_parquet("data/etl_output/gold/vitals_summary/")
print(vitals_gold.head())

# Check for alerts
alerts = vitals_gold[vitals_gold['mews_score'] > 0]
print(f"Alerts: {len(alerts)}")
```

### View in Spark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL-Monitor").getOrCreate()

# Load Gold data
gold_df = spark.read.parquet("data/etl_output/gold/vitals_summary/")
gold_df.show()
gold_df.printSchema()
```

## Troubleshooting

### No Data in Landing Zone
- Check Kafka producers are running
- Verify `kafka_to_local_uploader.py` is consuming
- Check `data/s3_mock/` for upstream data

### Spark Memory Issues
```python
# Reduce partitions in etl_pipeline.py
.config("spark.sql.shuffle.partitions", "2")  # From 4 to 2
```

### ETL Fails on Timestamps
- Ensure timestamps are Unix epoch (seconds since 1970)
- Check for null values in timestamp column

### Schema Mismatch
- Bronze expects: `patient_id`, `device_type`, `timestamp`, `heart_rate`, `spo2` (vitals)
- Bronze expects: `patient_id`, `device_type`, `timestamp`, `x`, `y`, `z` (movement)

## Production Deployment (S3)

1. **Configure `.env`:**
   ```env
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_SESSION_TOKEN=...
   S3_BUCKET_NAME=health-project
   ```

2. **Update paths in `etl_pipeline.py`:**
   - Set `use_local=False` in `run_etl_pipeline()`

3. **Schedule with Airflow/Cron:**
   ```bash
   # Cron every 10 minutes
   */10 * * * * cd /path && python scripts/etl_scheduler.py --interval 600
   ```

4. **Monitor with CloudWatch:**
   - Track S3 bucket sizes
   - Set up Lambda triggers on new Gold data
   - Alert on ETL failures

## Next Steps

After ETL completes, you can:

1. **Connect BI Tools:**
   - AWS Athena: Query Gold Parquet files
   - Power BI: Connect to S3/Gold layer
   - Tableau: Import aggregated data

2. **Set Up Alerts:**
   - Lambda function on `gold/vitals_summary/` for MEWS > 2
   - SNS notifications for critical patients

3. **Data Retention:**
   - Archive Gold data to Glacier after 30 days
   - Keep Bronze for 7 days (reprocessing)
   - Delete Landing after successful ETL
