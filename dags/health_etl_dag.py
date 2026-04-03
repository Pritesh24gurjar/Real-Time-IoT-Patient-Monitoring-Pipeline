from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = Path(os.getenv("AIRFLOW_PROJECT_DIR", "/opt/airflow/project"))
ETL_SCRIPT = PROJECT_DIR / "scripts" / "etl_pipeline.py"
ETL_MODE = os.getenv("AIRFLOW_ETL_MODE", "auto").strip().lower()
ETL_SCHEDULE = os.getenv("AIRFLOW_ETL_SCHEDULE", "*/10 * * * *")
ETL_LOCAL_BASE_DIR = Path(
    os.getenv("ETL_LOCAL_BASE_DIR", str(PROJECT_DIR / "data" / "etl_output"))
)

if ETL_MODE not in {"auto", "local", "s3"}:
    raise ValueError(f"Unsupported AIRFLOW_ETL_MODE: {ETL_MODE}")


def _build_etl_command() -> list[str]:
    """Build the ETL command line from environment-driven config."""
    command = [
        sys.executable,
        str(ETL_SCRIPT),
        "--mode",
        ETL_MODE,
        "--local-base",
        str(ETL_LOCAL_BASE_DIR),
    ]
    return command


@dag(
    dag_id="health_etl_pipeline",
    description="Run the Spark Bronze/Silver/Gold ETL on a schedule.",
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
    tags=["health", "spark", "etl"],
)
def health_etl_pipeline():
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="run_spark_etl")
    def run_spark_etl() -> None:
        if not ETL_SCRIPT.exists():
            raise FileNotFoundError(f"ETL script not found: {ETL_SCRIPT}")

        ETL_LOCAL_BASE_DIR.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["ETL_LOCAL_BASE_DIR"] = str(ETL_LOCAL_BASE_DIR)

        subprocess.run(
            _build_etl_command(),
            cwd=str(PROJECT_DIR),
            env=env,
            check=True,
        )

    start >> run_spark_etl() >> finish


dag = health_etl_pipeline()
