from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = Path(os.getenv("AIRFLOW_PROJECT_DIR", "/opt/airflow/project"))
ETL_SPARK_SCRIPT = PROJECT_DIR / "scripts" / "etl_pipeline.py"
ETL_S3_SCRIPT = PROJECT_DIR / "scripts" / "etl_s3_pipeline.py"
ETL_MODE = os.getenv("AIRFLOW_ETL_MODE", "auto").strip().lower()
ETL_SCHEDULE = os.getenv("AIRFLOW_ETL_SCHEDULE", "*/10 * * * *")
ETL_LOCAL_BASE_DIR = Path(
    os.getenv("ETL_LOCAL_BASE_DIR", str(PROJECT_DIR / "data" / "etl_output"))
)

if ETL_MODE not in {"auto", "local", "s3"}:
    raise ValueError(f"Unsupported AIRFLOW_ETL_MODE: {ETL_MODE}")


def _has_s3_runtime() -> bool:
    """Return True when the Airflow container has the S3 runtime configured."""
    return all(
        os.getenv(name)
        for name in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "S3_BUCKET_NAME",
        )
    )


def _use_s3_runtime() -> bool:
    """Return True when the DAG should run the S3-native ETL pipeline."""
    return ETL_MODE == "s3" or (ETL_MODE == "auto" and _has_s3_runtime())


def _build_etl_command() -> list[str]:
    """Build the ETL command line from environment-driven config."""
    if _use_s3_runtime():
        if not _has_s3_runtime():
            missing = [
                name
                for name in (
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "S3_BUCKET_NAME",
                )
                if not os.getenv(name)
            ]
            raise ValueError(
                "AIRFLOW_ETL_MODE requested S3, but the following variables are missing: "
                + ", ".join(missing)
            )

        return [
            sys.executable,
            str(ETL_S3_SCRIPT),
            "--once",
        ]

    return [
        sys.executable,
        str(ETL_SPARK_SCRIPT),
        "--mode",
        "local",
        "--local-base",
        str(ETL_LOCAL_BASE_DIR),
    ]


@dag(
    dag_id="health_etl_pipeline",
    description="Run the Bronze/Silver/Gold ETL on a schedule.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=ETL_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["health", "etl", "s3"],
)
def health_etl_pipeline():
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="run_spark_etl")
    def run_spark_etl() -> None:
        etl_script = ETL_S3_SCRIPT if _use_s3_runtime() else ETL_SPARK_SCRIPT
        if not etl_script.exists():
            raise FileNotFoundError(f"ETL script not found: {etl_script}")

        ETL_LOCAL_BASE_DIR.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["ETL_LOCAL_BASE_DIR"] = str(ETL_LOCAL_BASE_DIR)
        env["AIRFLOW_ETL_MODE"] = ETL_MODE

        command = _build_etl_command()
        print("Running ETL command:", " ".join(command))
        subprocess.run(
            command,
            cwd=str(PROJECT_DIR),
            env=env,
            check=True,
        )

    start >> run_spark_etl() >> finish


dag = health_etl_pipeline()
