# Phase 1 Complete - Quick Reference

## 🚀 Quick Start

### Local Testing (Recommended for Development)
```bash
# Double-click this file for producers and alerting; start Airflow separately for ETL
start_pipeline.bat
```

### S3 Production Mode
```bash
# Double-click this file (configure .env first); start Airflow separately for ETL
start_pipeline_s3.bat
```

## 📁 Scripts Created

### Kafka Producers
| Script | Description |
|--------|-------------|
| `iot_vitals_producer.py` | Streams health data (HR, SpO2, stress) to Kafka |
| `iot_movement_producer.py` | Streams accelerometer data to Kafka |

### Kafka Consumers
| Script | Description |
|--------|-------------|
| `simple_alert_engine.py` | Real-time alert detection (Python) |
| `kafka_to_local_uploader.py` | Kafka → Local filesystem (testing) |
| `kafka_to_s3_stream.py` | Kafka → S3 (production) |

### ETL Pipelines
| Script | Description |
|--------|-------------|
| `etl_pipeline.py` | Spark ETL for local testing or Airflow runs (Bronze/Silver/Gold) |
| `etl_scheduler.py` | Fallback wrapper around the Spark ETL |
| `etl_s3_pipeline.py` | Standalone ETL for S3 production |
| `dags/health_etl_dag.py` | Airflow DAG for scheduled ETL |

### Launcher Scripts
| Script | Mode | Description |
|--------|------|-------------|
| `start_pipeline.bat` | Local | Opens 5 terminals for local testing |
| `start_pipeline_s3.bat` | S3 | Opens 5 terminals for production |
| `stop_pipeline.bat` | Both | Stops all components |

## 📊 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    KAFKA PRODUCERS                            │
├──────────────────────────────────────────────────────────────┤
│  iot_vitals_producer.py    →  raw_vitals topic               │
│  iot_movement_producer.py  →  raw_movement topic             │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                   REAL-TIME PATH (Hot)                        │
├──────────────────────────────────────────────────────────────┤
│  simple_alert_engine.py                                      │
│  - Detects: TACHYCARDIA, BRADYCARDIA, HYPOXIA, HIGH_STRESS  │
│  - Fall detection from movement data                         │
│  - Outputs to: critical_alerts topic                         │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│              BATCH INGESTION (Warm Path)                      │
├──────────────────────────────────────────────────────────────┤
│  Local:  kafka_to_local_uploader.py                          │
│  S3:     kafka_to_s3_stream.py                               │
│  - Batches every 10 minutes                                  │
│  - Time-based partitioning                                   │
│  - JSON Lines format                                         │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                 ETL PIPELINE (Spark)                          │
├──────────────────────────────────────────────────────────────┤
│  Bronze Layer: Raw + lineage metadata                        │
│  Silver Layer: Cleaned + features (SVM)                      │
│  Gold Layer:   Aggregated (MEWS scores, fall counts)         │
│  Orchestrated by Airflow: health_etl_pipeline                │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                   ANALYTICS / BI                              │
├──────────────────────────────────────────────────────────────┤
│  - AWS Athena queries                                        │
│  - Power BI / Tableau dashboards                             │
│  - ML model training                                         │
└──────────────────────────────────────────────────────────────┘
```

## 🎯 Alert Rules

| Alert | Condition | Threshold |
|-------|-----------|-----------|
| TACHYCARDIA | Heart Rate | > 130 BPM |
| BRADYCARDIA | Heart Rate | < 40 BPM |
| HYPOXIA | SpO2 | < 90% |
| HIGH_STRESS | Stress Level | >= 8 |
| FALL_DETECTED | SVM (impact) | > 25.0 |

### MEWS Score (Gold Layer)
```
avg_hr > 130    → 3 (Critical)
avg_hr >= 111   → 2 (High)
avg_hr <= 40    → 2 (Critical)
avg_spo2 < 90   → 3 (Hypoxia)
otherwise       → 0 (Normal)
```

## 📂 Data Flow

### Local Mode Paths
```
data/
├── s3_mock/
│   └── raw/
│       ├── vitals/year=2026/month=03/day=15/hour=15/
│       └── movement/year=2026/month=03/day=15/hour=15/
└── etl_output/
    ├── landing/
    ├── bronze/vitals/, bronze/movement/
    ├── silver/vitals/, silver/movement/
    └── gold/vitals_summary/, gold/movement_summary/
```

### S3 Mode Paths
```
s3://your-bucket/
├── landing/
├── raw/vitals/, raw/movement/
├── bronze/vitals/, bronze/movement/
├── silver/vitals/, silver/movement/
└── gold/vitals_summary/, gold/movement_summary/
```

## 🔧 Configuration

### .env File (S3 Mode)
```env
AWS_ACCESS_KEY_ID=ASIA...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_REGION=us-west-2
S3_BUCKET_NAME=your-bucket-name
AIRFLOW_ETL_MODE=auto
AIRFLOW_ETL_SCHEDULE=*/10 * * * *
```

### Docker Compose (Kafka)
```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# View logs
docker-compose logs -f
```

### Airflow UI
- URL: http://localhost:8081
- DAG: `health_etl_pipeline`

## 📈 Monitoring

### Kafka UI
- URL: http://localhost:8080
- Topics: raw_vitals, raw_movement, critical_alerts

### Command Line
```bash
# Check topics
docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092

# View alerts
docker exec -it kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic critical_alerts \
  --max-messages 10 \
  --timeout-ms 5000

# Check consumer lag
docker exec -it kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group alert-engine-group
```

### S3 (Production)
```bash
# List files
aws s3 ls s3://your-bucket/landing/
aws s3 ls s3://your-bucket/gold/vitals_summary/

# Query with Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM vitals_summary WHERE mews_score > 0" \
  --query-execution-context "Database=health_db" \
  --result-configuration "OutputLocation=s3://your-bucket/athena-results/"
```

## 🛠️ Troubleshooting

### Kafka won't start
```bash
docker-compose down -v
docker-compose up -d
docker-compose logs -f kafka
```

### Producers not sending
```bash
# Test connection
python scripts/iot_vitals_producer.py --dry-run
```

### No alerts appearing
1. Check producers are running
2. Verify Kafka topics exist
3. Check alert engine logs

### ETL failing
```bash
# Test locally first
python scripts/etl_pipeline.py

# Check S3 connection
python scripts/kafka_to_s3_stream.py --test
```

## 📝 Documentation Files

| File | Description |
|------|-------------|
| `PHASE1_QUICKSTART.md` | Quick start guide |
| `PHASE1_README.md` | Full Phase 1 documentation |
| `ETL_PIPELINE_README.md` | ETL pipeline details |
| `S3_SCRIPTS_README.md` | S3 production scripts |
| `SCRIPTS_README.md` | Launcher scripts guide |

## ⏭️ Next Steps (Phase 2)

1. **Data Lake Architecture**
   - Implement Delta Lake format
   - Add schema evolution
   - Set up data versioning

2. **Advanced Analytics**
   - ML model for fall prediction
   - Anomaly detection for vitals
   - Patient risk scoring

3. **Dashboard**
   - Real-time patient monitoring
   - Historical trend analysis
   - Alert management

4. **Production Hardening**
   - Error handling and retries
   - Dead letter queues
   - Monitoring and alerting (CloudWatch)

5. **Security**
   - Data encryption (at rest & in transit)
   - IAM roles and policies
   - Audit logging
