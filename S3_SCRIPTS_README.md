# S3 Production Scripts

Production-ready scripts for Kafka → S3 ingestion and ETL pipeline.
Airflow now schedules the Spark ETL job; this file focuses on the scripts.

## Quick Start

### Local Testing (Default)
```bash
# Opens the producer and alerting windows for local file storage
start_pipeline.bat
```

### S3 Production Mode
```bash
# Opens the producer and alerting windows with S3 storage
start_pipeline_s3.bat
```

## Scripts Overview

| Script | Purpose | Mode |
|--------|---------|------|
| `kafka_to_s3_stream.py` | Kafka → S3 ingestion | Local or S3 |
| `etl_s3_pipeline.py` | Standalone ETL for S3-style runs | S3 only |
| `kafka_to_local_uploader.py` | Kafka → Local (testing) | Local only |
| `etl_pipeline.py` | Spark ETL invoked by Airflow or manually | Local or S3 |
| `etl_scheduler.py` | Fallback scheduler wrapper | Local or S3 |

## Configuration

### 1. Local Mode (Testing)

No configuration needed. Data saves to:
- `data/s3_mock/` - Kafka uploader output
- `data/etl_output/` - ETL pipeline output

To run the ETL on schedule in local mode, set:

```env
AIRFLOW_ETL_MODE=local
ETL_LOCAL_BASE_DIR=/opt/airflow/project/data/etl_output
```

### 2. S3 Mode (Production)

Edit `.env` file:

```env
# AWS Credentials
AWS_ACCESS_KEY_ID=ASIA...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_REGION=us-west-2

# S3 Bucket
S3_BUCKET_NAME=your-health-data-bucket
```

## Usage

### Kafka to S3 Stream

**Test S3 Connection:**
```bash
python scripts/kafka_to_s3_stream.py --test
```

**Run in Local Mode:**
```bash
python scripts/kafka_to_s3_stream.py --local
```

**Run in S3 Production Mode:**
```bash
python scripts/kafka_to_s3_stream.py
```

**Features:**
- Consumes from Kafka topics (`raw_vitals`, `raw_movement`)
- Batches data (5000 messages or 10 minutes)
- Uploads to S3 with time-based partitioning
- Format: `raw/{data_type}/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/data_HHMMSS.jsonl`

### ETL S3 Pipeline

**Test S3 Connection:**
```bash
python scripts/etl_s3_pipeline.py --test
```

**Run Once:**
```bash
python scripts/etl_s3_pipeline.py --once
```

**Run on Schedule (every 10 minutes):**
```bash
python scripts/etl_s3_pipeline.py --schedule
```

**Custom Interval:**
```bash
python scripts/etl_s3_pipeline.py --schedule --interval 300
```

**Features:**
- **Bronze**: Raw ingestion with lineage metadata
- **Silver**: Data cleaning and feature engineering
- **Gold**: Clinical aggregation (MEWS scores, fall counts)

### Airflow Orchestration

Primary ETL scheduling is now handled by Airflow.

- DAG: `dags/health_etl_dag.py`
- DAG id: `health_etl_pipeline`
- UI: `http://localhost:8081`

See `AIRFLOW_ETL_ORCHESTRATION.md` for the startup flow.

## S3 Directory Structure

```
s3://your-bucket/
├── landing/                    # Raw JSON from Kafka consumers
│   ├── vitals_20260315_163045.jsonl
│   └── movement_20260315_163045.jsonl
│
├── raw/                        # Kafka to S3 Stream output
│   ├── vitals/
│   │   └── year=2026/
│   │       └── month=03/
│   │           └── day=15/
│   │               └── hour=15/
│   │                   └── minute=30/
│   │                       └── data_153045_123456.jsonl
│   └── movement/
│       └── ...
│
├── bronze/                     # ETL Bronze Layer
│   ├── vitals/
│   │   └── *.parquet (with metadata)
│   └── movement/
│       └── *.parquet (with metadata)
│
├── silver/                     # ETL Silver Layer
│   ├── vitals/
│   │   └── *.parquet (cleaned)
│   └── movement/
│       └── *.parquet (with SVM)
│
└── gold/                       # ETL Gold Layer
    ├── vitals_summary/
    │   └── *.parquet (MEWS scores)
    └── movement_summary/
        └── *.parquet (fall counts)
```

## Data Flow

```
┌──────────────────┐
│  Kafka Topics    │
│  - raw_vitals    │
│  - raw_movement  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  kafka_to_s3_    │
│  stream.py       │
│  (Every 10 min)  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  S3 landing/     │
│  (Raw JSONL)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  etl_s3_         │
│  pipeline.py     │
│  (Every 10 min)  │
└────────┬─────────┘
         │
         ├──────┬──────────┬────────┐
         ▼      ▼          ▼        ▼
     Bronze  Silver    Gold    Gold
     (Raw)  (Clean)  (Vitals) (Movement)
```

## Monitoring

### Check S3 Files
```bash
# AWS CLI
aws s3 ls s3://your-bucket/landing/
aws s3 ls s3://your-bucket/bronze/vitals/
aws s3 ls s3://your-bucket/gold/vitals_summary/
```

### Query with Athena
```sql
-- Vitals Gold (MEWS Scores)
SELECT 
    patient_id,
    window_start,
    avg_hr,
    peak_hr,
    mews_score
FROM vitals_summary
WHERE mews_score > 0
ORDER BY window_start DESC
LIMIT 100;

-- Movement Gold (Fall Detection)
SELECT 
    patient_id,
    window_start,
    fall_events_detected,
    peak_impact
FROM movement_summary
WHERE fall_events_detected > 0
ORDER BY window_start DESC;
```

### Read Parquet in Python
```python
import boto3
import pandas as pd
from io import BytesIO

# Read from S3
s3 = boto3.client('s3')
obj = s3.get_object(
    Bucket='your-bucket',
    Key='gold/vitals_summary/year=2026/month=03/day=15/data.parquet'
)

# Read parquet
df = pd.read_parquet(BytesIO(obj['Body'].read()))
print(df.head())
```

## Troubleshooting

### S3 Connection Failed
```bash
# Test credentials
python scripts/kafka_to_s3_stream.py --test

# Check .env file
cat .env | grep AWS_
```

### No Data in S3
1. Check Kafka producers are running
2. Verify `kafka_to_s3_stream.py` is consuming
3. Check upload interval (10 minutes by default)

### ETL Not Processing
1. Check landing zone has files: `aws s3 ls s3://bucket/landing/`
2. Verify Spark can access S3 (IAM permissions)
3. Check ETL logs for errors

### Permission Denied
Ensure IAM role/user has:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket",
                "arn:aws:s3:::your-bucket/*"
            ]
        }
    ]
}
```

## Production Deployment

### 1. AWS Lambda (Event-Driven)
Trigger ETL when new files arrive in landing zone:
- S3 Event Notification → Lambda → Start Glue Job

### 2. AWS Glue (Scheduled)
Schedule ETL as Glue job:
```python
# glue_etl_job.py
import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Similar to etl_s3_pipeline.py but with Glue context
```

### 3. Airflow (Orchestration)
```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

with DAG('health-etl', schedule_interval='*/10 * * * *') as dag:
    run_etl = GlueJobOperator(
        job_name='health-etl-job',
        script_location='s3://your-bucket/scripts/etl.py',
        task_id='run_etl'
    )
```

## Cost Optimization

### S3 Lifecycle Policies
```json
{
    "Rules": [
        {
            "ID": "Archive old data",
            "Status": "Enabled",
            "Prefix": "bronze/",
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "GLACIER"
                }
            ]
        }
    ]
}
```

### Partition Pruning
Always filter by date in queries:
```sql
-- Good (uses partitions)
SELECT * FROM vitals 
WHERE year=2026 AND month=03 AND day=15;

-- Bad (scans everything)
SELECT * FROM vitals;
```

## Security Best Practices

1. **Encrypt data at rest**: Enable S3 SSE-S3 or SSE-KMS
2. **Encrypt data in transit**: Use HTTPS for all S3 operations
3. **Restrict bucket policies**: Least privilege access
4. **Rotate credentials**: Update `.env` regularly
5. **Enable CloudTrail**: Audit S3 access
6. **Use VPC Endpoints**: Keep traffic within AWS network
