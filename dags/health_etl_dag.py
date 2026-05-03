"""
health_etl_dag.py - Airflow DAG for IoT Patient Monitoring ETL Pipeline
Orchestrates Bronze → Silver → Gold ETL using etl_s3_pipeline.py
Schedule: Every 10 minutes (configurable via ETL_INTERVAL_MINUTES env var)
"""

from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.environ.get('AIRFLOW_PROJECT_DIR', '/opt/airflow/project'), 'scripts'))
import etl_s3_pipeline as mod

import importlib.util
import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — all overridable via Airflow Variables or env vars
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(os.getenv("AIRFLOW_PROJECT_DIR", "/opt/airflow/project"))
SCRIPTS_DIR = PROJECT_DIR / "scripts"
ETL_INTERVAL_MINUTES = int(os.getenv("ETL_INTERVAL_MINUTES", "10"))

# AWS — loaded from Airflow env / .env mounted into container
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME")

# Pipeline knobs
FALLS_SVM_THRESHOLD   = float(os.getenv("FALLS_SVM_THRESHOLD", "25.0"))
WINDOW_MINUTES        = int(os.getenv("WINDOW_MINUTES", "10"))
MIN_RECORD_THRESHOLD  = int(os.getenv("MIN_RECORD_THRESHOLD", "1"))

# ---------------------------------------------------------------------------
# Dynamically import etl_s3_pipeline so the DAG works even if the scripts
# folder is not on PYTHONPATH at Airflow startup time.
# ---------------------------------------------------------------------------
def _load_etl_module():
    etl_path = SCRIPTS_DIR / "etl_s3_pipeline.py"
    if not etl_path.exists():
        raise FileNotFoundError(
            f"etl_s3_pipeline.py not found at {etl_path}. "
            "Check AIRFLOW_PROJECT_DIR env var."
        )
    spec = importlib.util.spec_from_file_location("etl_s3_pipeline", etl_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["etl_s3_pipeline"] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Task functions — each maps to one DAG task
# ---------------------------------------------------------------------------
def task_validate_s3(**context):
    """Validate AWS credentials and S3 bucket access before running ETL."""
    missing = [v for v in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET_NAME"]
               if not os.getenv(v)]
    if missing:
        raise ValueError(f"Missing required env vars: {missing}")

    mod = _load_etl_module()
    if not mod.test_s3_connection():
        raise ConnectionError("S3 connection/permission check failed — aborting DAG run.")
    log.info("S3 validation passed. Bucket: s3://%s", S3_BUCKET_NAME)


def task_bronze(**context):
    """Ingest JSONL files from S3 landing zone into Bronze Parquet layer."""
    mod = _load_etl_module()
    run_id    = context["run_id"].replace(":", "_").replace("+", "_")[:32]
    batch_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    s3 = mod._make_s3_client()
    success, ingested_keys = mod.process_bronze(s3, run_id, batch_date)

    if not success:
        log.warning("Bronze: no new data found in landing zone.")
        # Push flag so downstream tasks can skip gracefully
        context["ti"].xcom_push(key="has_data", value=False)
        context["ti"].xcom_push(key="batch_date", value=batch_date)
        context["ti"].xcom_push(key="run_id", value=run_id)
        return

    log.info("Bronze complete. Archiving %d landing files…", len(ingested_keys))
    mod.archive_landing_files(s3, ingested_keys)

    context["ti"].xcom_push(key="has_data", value=True)
    context["ti"].xcom_push(key="batch_date", value=batch_date)
    context["ti"].xcom_push(key="run_id", value=run_id)


def task_silver(**context):
    """Clean Bronze data and compute derived features → Silver Parquet."""
    ti = context["ti"]
    has_data   = ti.xcom_pull(task_ids="bronze", key="has_data")
    batch_date = ti.xcom_pull(task_ids="bronze", key="batch_date")
    run_id     = ti.xcom_pull(task_ids="bronze", key="run_id")

    if not has_data:
        log.info("Silver: skipping — no Bronze data this run.")
        return

    mod = _load_etl_module()
    s3  = mod._make_s3_client()
    mod.process_silver(s3, run_id, batch_date)
    log.info("Silver complete for batch %s", batch_date)


def task_gold(**context):
    """Aggregate Silver data → Gold Parquet (MEWS scores, fall events)."""
    ti = context["ti"]
    has_data   = ti.xcom_pull(task_ids="bronze", key="has_data")
    batch_date = ti.xcom_pull(task_ids="bronze", key="batch_date")
    run_id     = ti.xcom_pull(task_ids="bronze", key="run_id")

    if not has_data:
        log.info("Gold: skipping — no Silver data this run.")
        return

    mod = _load_etl_module()
    s3  = mod._make_s3_client()
    mod.process_gold(s3, run_id, batch_date)
    log.info("Gold complete for batch %s", batch_date)


def task_summary(**context):
    """Log a run summary with S3 paths for each layer."""
    ti = context["ti"]
    has_data   = ti.xcom_pull(task_ids="bronze", key="has_data")
    batch_date = ti.xcom_pull(task_ids="bronze", key="batch_date")
    run_id     = ti.xcom_pull(task_ids="bronze", key="run_id")

    if not has_data:
        log.info("Run summary: no data processed this cycle.")
        return

    bucket = S3_BUCKET_NAME
    log.info("=" * 60)
    log.info("ETL RUN COMPLETE  run_id=%s  batch=%s", run_id, batch_date)
    log.info("S3 paths written:")
    for layer in ["bronze/vitals", "bronze/movement",
                  "silver/vitals", "silver/movement",
                  "gold/vitals-summary", "gold/movement-summary"]:
        log.info("  s3://%s/%s/etl-batch-date=%s/", bucket, layer, batch_date)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "pritesh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="health_etl_pipeline",
    description="IoT Patient Monitoring — Bronze → Silver → Gold ETL (S3)",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=f"*/{ETL_INTERVAL_MINUTES} * * * *",   # every N minutes
    catchup=False,
    max_active_runs=1,          # prevent overlapping runs
    tags=["iot", "healthcare", "etl", "kafka", "s3"],
) as dag:

    validate = PythonOperator(
        task_id="validate_s3",
        python_callable=task_validate_s3,
        doc_md="Validate AWS credentials and S3 bucket write access.",
    )

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=task_bronze,
        doc_md="Ingest JSONL from S3 landing zone → Bronze Parquet.",
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=task_silver,
        doc_md="Clean Bronze data, validate ranges, add features → Silver Parquet.",
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=task_gold,
        doc_md="Aggregate Silver → Gold Parquet with MEWS scores and fall counts.",
    )

    summary = PythonOperator(
        task_id="summary",
        python_callable=task_summary,
        doc_md="Log run summary with S3 output paths.",
    )

    # Task dependency chain
    validate >> bronze >> silver >> gold >> summary
