# S3 ETL Pipeline - Kafka to Cloud Storage

This pipeline consumes data from Kafka brokers and uploads it to S3 (or local storage for testing) in batched intervals with time-based partitioning.

## Architecture

```
┌─────────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│  Kafka Topics   │────▶│  Batch Uploader     │────▶│  S3 Bucket       │
│  - raw_vitals   │     │  (10 min intervals) │     │  (partitioned)   │
│  - raw_movement │     │                     │     │                  │
└─────────────────┘     └─────────────────────┘     └──────────────────┘
                                                        └── raw/
                                                            ├── vitals/
                                                            │   └── year=2026/
                                                            │       └── month=03/
                                                            │           └── day=14/
                                                            │               └── hour=03/
                                                            │                   └── data_153045.jsonl
                                                            └── movement/
                                                                └── year=2026/
                                                                    └── month=03/
                                                                        └── ...
```

## Features

- **Batched Uploads**: Data is buffered and uploaded every 10 minutes
- **Time-based Partitioning**: S3 paths organized by year/month/day/hour
- **Separate Streams**: Vitals and movement data saved to separate paths
- **JSON Lines Format**: Efficient for big data processing (Spark, Athena)
- **Offset Tracking**: Kafka consumer groups track processed offsets
- **Local Testing**: Test mode saves to local filesystem

## Configuration

### 1. AWS Credentials (`.env` file)

```env
AWS_ACCESS_KEY_ID=ASIA...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_REGION=us-west-2
S3_BUCKET_NAME=your-bucket-name
```

### 2. Upload Settings

In `scripts/kafka_to_s3_uploader.py`:

```python
UPLOAD_INTERVAL_SECONDS = 600  # Upload every 10 minutes
BATCH_SIZE = 1000              # Or when batch reaches 1000 messages
```

## Usage

### Option A: Upload to S3 (Production)

```bash
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Run S3 uploader
python scripts/kafka_to_s3_uploader.py
```

### Option B: Local Testing (Development)

```bash
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Run local uploader (saves to data/s3_mock/)
python scripts/kafka_to_local_uploader.py
```

## S3 Directory Structure

Data is partitioned for efficient querying:

```
s3://your-bucket/
└── raw/
    ├── vitals/
    │   └── year=2026/
    │       └── month=03/
    │           └── day=14/
    │               └── hour=03/
    │                   ├── data_153045_123456.jsonl
    │                   └── data_154045_654321.jsonl
    └── movement/
        └── year=2026/
            └── month=03/
                └── ...
```

## Data Format (JSON Lines)

Each line in the `.jsonl` file is a complete JSON object:

```json
{"offset": 1234, "partition": 0, "timestamp": 1773445799646, "key": "4174", "value": {"patient_id": "4174", "heart_rate": 93, "spo2": 100, ...}}
{"offset": 1235, "partition": 0, "timestamp": 1773445799847, "key": "3385", "value": {"patient_id": "3385", "heart_rate": 78, "spo2": 95, ...}}
```

## Next Steps (Downstream ETL)

Once data is in S3, you can:

1. **AWS Athena**: Query data directly with SQL
   ```sql
   SELECT patient_id, AVG(heart_rate) 
   FROM vitals 
   WHERE year=2026 AND month=03 AND day=14
   GROUP BY patient_id
   ```

2. **Spark on EMR**: Process with PySpark
   ```python
   df = spark.read.json("s3://your-bucket/raw/vitals/")
   ```

3. **Lambda Triggers**: Process on upload with AWS Lambda

4. **Glue Crawler**: Catalog data for Athena/Redshift

## Monitoring

The uploader prints statistics:

```
============================================================
FINAL STATISTICS
============================================================
  Vitals Records:
    Consumed:  5,432
    Uploaded:  5,432
  Movement Records:
    Consumed:  12,890
    Uploaded:  12,890
  Total Uploads: 24

  Data Location: C:\...\data\s3_mock
============================================================
```

## Troubleshooting

### S3 Connection Failed
- Check AWS credentials in `.env`
- Verify session token hasn't expired
- Ensure S3 bucket exists in specified region

### No Data Being Consumed
- Verify Kafka is running: `docker-compose ps`
- Check topics exist: `docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092`
- Ensure producers are running

### Upload Fails
- Check S3 bucket permissions
- Verify IAM user has `s3:PutObject` permission
- Check network connectivity to AWS

## Security Best Practices

1. **Never commit `.env`** - Already in `.gitignore`
2. **Rotate credentials regularly** - Especially if exposed
3. **Use IAM roles in production** - Instead of access keys
4. **Enable S3 encryption** - Server-side encryption (SSE-S3 or SSE-KMS)
5. **Restrict bucket policies** - Least privilege access
