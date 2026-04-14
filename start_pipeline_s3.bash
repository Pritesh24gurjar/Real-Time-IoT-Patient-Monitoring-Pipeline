#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs/startup"
PIDS_FILE="$LOG_DIR/pids.txt"

mkdir -p "$LOG_DIR"

echo "============================================================"
echo "Phase 1 - Kafka Real-Time Pipeline (S3 Production Mode)"
echo "============================================================"
echo "Project Directory: $PROJECT_DIR"
echo

if [[ ! -d "$PROJECT_DIR/.venv" ]]; then
  echo "ERROR: Virtual environment not found at .venv"
  echo "Please create it first: python3 -m venv .venv"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker is not running"
  echo "Please start Docker Desktop first"
  exit 1
fi

if [[ -f "$PROJECT_DIR/.env" ]]; then
  if ! grep -q '^AWS_ACCESS_KEY_ID=' "$PROJECT_DIR/.env"; then
    echo "WARNING: AWS_ACCESS_KEY_ID not found in .env"
  fi
else
  echo "WARNING: .env file not found"
fi

source "$PROJECT_DIR/.venv/bin/activate"

cleanup() {
  if [[ -f "$PIDS_FILE" ]]; then
    while read -r pid name; do
      [[ -z "${pid:-}" ]] && continue
      if kill -0 "$pid" >/dev/null 2>&1; then
        echo "Stopping $name (pid $pid)..."
        kill "$pid" >/dev/null 2>&1 || true
      fi
    done < "$PIDS_FILE"
  fi
}

trap cleanup EXIT INT TERM

echo "Starting Docker services..."
(
  cd "$PROJECT_DIR"
  docker-compose up -d
)

echo "Waiting for Kafka and Airflow to initialize..."
sleep 30

> "$PIDS_FILE"

start_bg() {
  local name="$1"
  shift
  local log_file="$LOG_DIR/${name}.log"

  echo "Starting $name..."
  (
    cd "$PROJECT_DIR"
    "$@"
  ) >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid $name" >> "$PIDS_FILE"
  echo "  log: $log_file"
}

start_bg "vitals_producer" python3 scripts/iot_vitals_producer.py --delay 0.5 --loop
start_bg "movement_producer" python3 scripts/iot_movement_producer.py --delay 0.05 --loop
start_bg "alert_engine" python3 scripts/simple_alert_engine.py
start_bg "kafka_to_s3_stream" python3 scripts/kafka_to_s3_stream.py

echo
echo "============================================================"
echo "All components started"
echo "============================================================"
echo
echo "Running processes:"
echo "  - Kafka broker, Kafka UI, Airflow: docker-compose"
echo "  - Vitals producer"
echo "  - Movement producer"
echo "  - Alert engine"
echo "  - Kafka to S3 stream uploader"
echo
echo "Logs:"
echo "  - $LOG_DIR/vitals_producer.log"
echo "  - $LOG_DIR/movement_producer.log"
echo "  - $LOG_DIR/alert_engine.log"
echo "  - $LOG_DIR/kafka_to_s3_stream.log"
echo
echo "UI endpoints:"
echo "  - Kafka UI: http://localhost:8080"
echo "  - Airflow:  http://localhost:8081"
echo
echo "S3 ETL:"
echo "  - Airflow DAG: health_etl_pipeline"
echo "  - Manual run: python3 scripts/etl_s3_pipeline.py --once"
echo
echo "To stop:"
echo "  - Press Ctrl+C in this terminal"
echo "  - Or run: docker-compose down"
