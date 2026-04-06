# Airflow ETL Orchestration

Airflow is the primary scheduler for the Spark Bronze/Silver/Gold pipeline.
The Spark job stays in `scripts/etl_pipeline.py`; Airflow only triggers it.
`scripts/etl_scheduler.py` remains as a fallback wrapper for manual runs.

## Services

- Kafka broker: `localhost:9092`
- Kafka UI: `http://localhost:8080`
- Airflow UI: `http://localhost:8081`

## Required Environment

Copy `.env.example` to `.env` and set the values you need.

For local runs:

- `AIRFLOW_ETL_MODE=local`
- `ETL_LOCAL_BASE_DIR=/opt/airflow/project/data/etl_output`

For S3 runs:

- `AIRFLOW_ETL_MODE=s3`
- `AWS_ACCESS_KEY_ID=...`
- `AWS_SECRET_ACCESS_KEY=...`
- `AWS_SESSION_TOKEN=...`
- `AWS_REGION=...`
- `S3_BUCKET_NAME=...`

If `AIRFLOW_ETL_MODE=auto`, the ETL script uses S3 only when the AWS
credentials and bucket are present. Otherwise it falls back to local mode.

## Start Order

1. Start the stack:

   ```bash
   docker-compose up -d
   ```

2. Wait for Kafka and Airflow to become healthy.

3. Start the producers and alert engine from your host if you are doing a
   full local demo.

4. Open the Airflow UI and confirm the `health_etl_pipeline` DAG is present.

5. Let Airflow trigger the ETL on schedule, or run the DAG manually from the
   UI for a one-off execution.

## Local Development Flow

Use this flow when you want the DAG to write into the repo-mounted local
output tree:

1. Set `AIRFLOW_ETL_MODE=local`.
2. Start Docker Compose.
3. Confirm the DAG has access to `data/etl_output`.
4. Trigger `health_etl_pipeline` in Airflow.
5. Inspect:

   - `data/etl_output/bronze/vitals`
   - `data/etl_output/bronze/movement`
   - `data/etl_output/silver/vitals`
   - `data/etl_output/silver/movement`
   - `data/etl_output/gold/vitals_summary`
   - `data/etl_output/gold/movement_summary`

## S3 Production Flow

Use this flow when the ETL should read and write to S3:

1. Set `AIRFLOW_ETL_MODE=s3`.
2. Provide the AWS and bucket variables in `.env`.
3. Start Docker Compose.
4. Trigger `health_etl_pipeline` in Airflow.
5. Validate the S3 paths:

   - `s3://<bucket>/landing/`
   - `s3://<bucket>/bronze/vitals/`
   - `s3://<bucket>/bronze/movement/`
   - `s3://<bucket>/silver/vitals/`
   - `s3://<bucket>/silver/movement/`
   - `s3://<bucket>/gold/vitals_summary/`
   - `s3://<bucket>/gold/movement_summary/`

## Fallback Scheduler

If you need a manual run outside Airflow:

```bash
python scripts/etl_scheduler.py --once --mode local
python scripts/etl_scheduler.py --once --mode s3
python scripts/etl_scheduler.py --interval 600 --mode local
```

The fallback scheduler reuses the same ETL code path, so it should produce the
same Bronze/Silver/Gold layout as the DAG.

## Troubleshooting

- If the DAG does not appear in Airflow, confirm `./dags` is mounted into the
  container and that `AIRFLOW_PROJECT_DIR` points to `/opt/airflow/project`.
- If local writes fail, confirm the `data/etl_output` mount exists and is
  writable.
- If S3 writes fail, confirm the AWS variables are set and the bucket exists.
