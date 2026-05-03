#!/usr/bin/env bash
# ============================================================================
# Phase 1 - Kafka Real-Time Pipeline Launcher (S3 Production Mode)
# EC2 / Linux Version - Uses tmux for multiple terminal sessions
# ============================================================================
# Components started:
#   - Kafka Broker + Airflow (Docker Compose)
#   - Vitals Producer
#   - Movement Producer
#   - Alert Engine (Real-time consumer)
#   - Kafka to S3 Stream Uploader
#
# NOTE: ETL S3 Pipeline (Bronze/Silver/Gold) is orchestrated by Airflow.
#       Do NOT run etl_s3_pipeline.py manually - Airflow handles scheduling.
#
# Prerequisites:
#   - Configure .env file with AWS credentials:
#     AWS_ACCESS_KEY_ID=...
#     AWS_SECRET_ACCESS_KEY=...
#     AWS_SESSION_TOKEN=...
#     S3_BUCKET_NAME=your-bucket
#
# Usage:
#   chmod +x start_pipeline_ec2.sh
#   ./start_pipeline_ec2.sh
#
# To stop all components:
#   ./stop_pipeline_ec2.sh
#   OR: tmux kill-session -t pipeline && docker-compose down
# ============================================================================

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs/startup"
SESSION="pipeline"

mkdir -p "$LOG_DIR"

echo "============================================================"
echo " Phase 1 - Kafka Real-Time Pipeline (S3 PRODUCTION MODE)"
echo "============================================================"
echo ""
echo "Project Directory: $PROJECT_DIR"
echo ""

# ----------------------------------------------------------------------------
# Check: Docker is running
# ----------------------------------------------------------------------------
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running"
    echo "Please start Docker: sudo service docker start"
    exit 1
fi

# Fix Docker socket permissions if needed
if ! docker ps > /dev/null 2>&1; then
    echo "Fixing Docker socket permissions..."
    sudo chmod 666 /var/run/docker.sock
fi

# ----------------------------------------------------------------------------
# Check: tmux is installed
# ----------------------------------------------------------------------------
if ! command -v tmux &> /dev/null; then
    echo "Installing tmux..."
    sudo yum install -y tmux
fi

# ----------------------------------------------------------------------------
# Check: .env file and AWS credentials
# ----------------------------------------------------------------------------
if [ -f "$PROJECT_DIR/.env" ]; then
    if ! grep -q "AWS_ACCESS_KEY_ID=" "$PROJECT_DIR/.env"; then
        echo "WARNING: AWS_ACCESS_KEY_ID not found in .env file"
        echo "Please configure .env with:"
        echo "  AWS_ACCESS_KEY_ID=..."
        echo "  AWS_SECRET_ACCESS_KEY=..."
        echo "  AWS_SESSION_TOKEN=..."
        echo "  S3_BUCKET_NAME=..."
        echo ""
        read -rp "Continue anyway? (y/n): " CONTINUE
        [[ "$CONTINUE" =~ ^[Yy]$ ]] || exit 1
    fi
else
    echo "WARNING: .env file not found at $PROJECT_DIR/.env"
    echo "S3 uploader and ETL pipeline may fail without AWS credentials."
    echo ""
    read -rp "Continue anyway? (y/n): " CONTINUE
    [[ "$CONTINUE" =~ ^[Yy]$ ]] || exit 1
fi

# ----------------------------------------------------------------------------
# Start Kafka + Airflow via Docker Compose
# ----------------------------------------------------------------------------
echo "Starting Kafka Broker + Airflow (Docker Compose)..."
cd "$PROJECT_DIR"
docker-compose up -d
echo ""

# ----------------------------------------------------------------------------
# Wait for Kafka to be truly ready (health poll, not blind sleep)
# ----------------------------------------------------------------------------
echo "Waiting for Kafka to be ready..."
MAX_WAIT=60
WAITED=0
until docker exec kafka-broker kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "ERROR: Kafka did not become ready within ${MAX_WAIT}s"
        echo "Check logs: docker-compose logs kafka"
        exit 1
    fi
    echo "  Kafka not ready yet... (${WAITED}s elapsed)"
    sleep 5
    WAITED=$((WAITED + 5))
done
echo "Kafka is ready."
echo ""

# Verify containers are running
if ! docker ps | grep -q "kafka-broker"; then
    echo "ERROR: kafka-broker container is not running"
    echo "Check logs: docker-compose logs kafka"
    exit 1
fi

if ! docker ps | grep -q "airflow"; then
    echo "WARNING: Airflow container is not running yet (may still be initializing)"
    echo "Check logs: docker-compose logs airflow"
fi

echo "All Docker services are up."
echo ""

# ----------------------------------------------------------------------------
# Kill existing tmux session if any
# ----------------------------------------------------------------------------
tmux kill-session -t "$SESSION" 2>/dev/null || true

# ----------------------------------------------------------------------------
# Helper: start a background process in a named tmux window
# ----------------------------------------------------------------------------
start_window() {
    local window_name="$1"
    local command="$2"
    local log_file="$LOG_DIR/${window_name}.log"

    tmux new-window -t "$SESSION" -n "$window_name"
    tmux send-keys -t "$SESSION:$window_name" \
        "cd $PROJECT_DIR && pip3 install -q -r requirements-phase1.txt > /dev/null 2>&1; $command 2>&1 | tee $log_file" Enter
}

# ----------------------------------------------------------------------------
# Create tmux session and launch all components
# ----------------------------------------------------------------------------
echo "Launching tmux session: $SESSION"
echo ""

# Window 0: Vitals Producer
tmux new-session -d -s "$SESSION" -n "vitals-producer"
tmux send-keys -t "$SESSION:vitals-producer" \
    "cd $PROJECT_DIR && echo '[1/4] Starting Vitals Producer...' && python3 scripts/iot_vitals_producer.py --delay 0.5 --loop 2>&1 | tee $LOG_DIR/vitals-producer.log" Enter
echo "  [1/4] Vitals Producer started"

# Window 1: Movement Producer
start_window "movement-producer" "echo '[2/4] Starting Movement Producer...' && python3 scripts/iot_movement_producer.py --delay 0.1 --max-records 10000"
echo "  [2/4] Movement Producer started"

# Window 2: Alert Engine
start_window "alert-engine" "echo '[3/4] Starting Alert Engine...' && python3 scripts/simple_alert_engine.py"
echo "  [3/4] Alert Engine started"

# Window 3: Kafka to S3 Stream
start_window "s3-stream" "echo '[4/4] Starting Kafka to S3 Stream...' && python3 scripts/kafka_to_s3_stream.py"
echo "  [4/4] Kafka to S3 Stream started"

echo ""
echo "============================================================"
echo " All components started! (S3 PRODUCTION MODE)"
echo "============================================================"
echo ""
echo " tmux Session   : $SESSION"
echo " tmux Windows   :"
echo "   0. vitals-producer    - Streaming health data (HR, SpO2)"
echo "   1. movement-producer  - Streaming accelerometer data"
echo "   2. alert-engine       - Detecting critical events"
echo "   3. s3-stream          - Uploading to S3 every 10 min"
echo ""
echo " ETL Pipeline   : Managed by Airflow (not a tmux window)"
echo "   DAG           : health_etl_pipeline"
echo "   Schedule      : \${AIRFLOW_ETL_SCHEDULE} from .env"
echo ""
echo " Logs            : $LOG_DIR"
echo ""
echo " Monitoring:"
echo "   - Kafka UI  : http://<EC2-PUBLIC-IP>:8080"
echo "   - Airflow   : http://<EC2-PUBLIC-IP>:8081"
echo ""
echo " tmux Commands:"
echo "   Attach        : tmux attach -t $SESSION"
echo "   Switch window : Ctrl+B then 0-3"
echo "   Detach        : Ctrl+B then D"
echo "   Kill all      : tmux kill-session -t $SESSION"
echo ""
echo " To stop all:"
echo "   ./stop_pipeline_ec2.sh"
echo "   OR: tmux kill-session -t pipeline && docker-compose down"
echo "============================================================"
echo ""

# Attach to the session so user can monitor
tmux attach -t "$SESSION"