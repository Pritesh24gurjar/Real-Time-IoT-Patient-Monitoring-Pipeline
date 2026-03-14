# Phase 1: Kafka & Real-Time Hot Path

Real-time IoT health monitoring system that streams wearable device data through Kafka and detects critical events using Spark Structured Streaming.

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────┐
│  IoT Simulators │────▶│  Kafka Cluster  │────▶│  Alert Engine       │
│  (Producers)    │     │  (Broker)       │     │  (Spark Streaming)  │
└─────────────────┘     └─────────────────┘     └─────────────────────┘
        │                       │                         │
        │                       │                         ▼
        │                       │                ┌─────────────────────┐
        │                       │                │  critical_alerts    │
        │                       │                │  (Kafka Topic)      │
        ▼                       ▼                └─────────────────────┘
┌─────────────────┐     ┌─────────────────┐
│ raw_vitals      │     │ raw_movement    │
│ (1Hz health)    │     │ (20Hz IMU)      │
└─────────────────┘     └─────────────────┘
```

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|-------|------------|-----------|---------|
| `raw_vitals` | 4 | 24 hours | 1Hz smartwatch health data (HR, SpO2) |
| `raw_movement` | 8 | 6 hours | 20Hz WISDM accelerometer data |
| `critical_alerts` | 2 | 7 days | Real-time alert output |

## Alert Rules

| Alert Type | Condition | Threshold |
|------------|-----------|-----------|
| `FALL_DETECTED` | SVM (impact force) | > 25.0 |
| `TACHYCARDIA` | Heart Rate | > 130 BPM |
| `BRADYCARDIA` | Heart Rate | < 40 BPM |
| `HYPOXIA` | SpO2 Level | < 90% |
| `HIGH_STRESS` | Stress Level | >= 8 |

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Java 11+ (for PySpark)
- Apache Spark 3.4+ (or install via pip)

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements-phase1.txt
pip install pyspark  # Separate install for Spark
```

### 2. Start Kafka Cluster

```bash
docker-compose up -d
```

Wait ~30 seconds for Kafka to initialize. Verify with:

```bash
docker-compose ps
docker-compose logs -f kafka
```

Access Kafka UI at: http://localhost:8080

### 3. Create Kafka Topics (Optional - auto-created if not exists)

```bash
# Inside Kafka container
docker exec -it kafka-broker kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw_vitals --partitions 4 --replication-factor 1

docker exec -it kafka-broker kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw_movement --partitions 8 --replication-factor 1

docker exec -it kafka-broker kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic critical_alerts --partitions 2 --replication-factor 1
```

### 4. Run IoT Simulators (Producers)

**Terminal 1 - Vitals Stream:**
```bash
python scripts/iot_vitals_producer.py --delay 1.0
```

**Terminal 2 - Movement Stream:**
```bash
python scripts/iot_movement_producer.py --delay 0.05 --max-records 10000
```

### 5. Run Alert Engine (Consumer)

```bash
# Using spark-submit
spark-submit scripts/realtime_alert_engine.py

# OR using pyspark
pyspark scripts/realtime_alert_engine.py
```

### 6. Monitor Alerts

**Option A: Kafka Console Consumer**
```bash
docker exec -it kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic critical_alerts \
  --from-beginning
```

**Option B: Kafka UI**
Navigate to http://localhost:8080 → Topics → critical_alerts

**Option C: Console Output**
The alert engine prints detected alerts to stdout in real-time.

## Testing the Hot Path

### Test Fall Detection
```bash
# Run movement producer with sample data
python scripts/iot_movement_producer.py --dry-run
```

Look for SVM values > 25.0 in the output - these trigger `FALL_DETECTED` alerts.

### Test Vital Alerts
```bash
# Run vitals producer with sample data
python scripts/iot_vitals_producer.py --dry-run
```

Look for heart_rate > 130 or SpO2 < 90 to trigger alerts.

## Command Reference

### Producer Options

```bash
# Vitals producer
python scripts/iot_vitals_producer.py --loop --delay 0.5
python scripts/iot_vitals_producer.py --dry-run

# Movement producer
python scripts/iot_movement_producer.py --max-records 5000 --delay 0.1
python scripts/iot_movement_producer.py --loop
```

### Kafka Operations

```bash
# List topics
docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec -it kafka-broker kafka-topics --describe --topic raw_vitals --bootstrap-server localhost:9092

# Check consumer lag
docker exec -it kafka-broker kafka-consumer-groups --describe --group <group-name> --bootstrap-server localhost:9092
```

### Cleanup

```bash
# Stop Kafka
docker-compose down

# Remove volumes (delete all data)
docker-compose down -v

# Reset checkpoints (for Spark re-runs)
rm -rf /tmp/checkpoints/*
```

## Troubleshooting

**Kafka won't start:**
- Check Docker logs: `docker-compose logs kafka`
- Ensure port 9092 is not in use
- Wait 30+ seconds for KRaft initialization

**Spark can't connect to Kafka:**
- Verify Kafka is running: `docker-compose ps`
- Check bootstrap servers: `localhost:9092`
- Ensure `kafka-python` and `pyspark` versions are compatible

**No alerts appearing:**
- Check producer is sending data (monitor console output)
- Verify topics exist: `kafka-topics --list`
- Check Spark checkpoint directory is writable
- Increase data delay to ensure streaming can keep up

## File Structure

```
DE project/
├── docker-compose.yml           # Kafka cluster setup
├── requirements-phase1.txt      # Python dependencies
├── PHASE1_README.md            # This file
└── scripts/
    ├── iot_vitals_producer.py   # Vitals simulator
    ├── iot_movement_producer.py # Movement simulator
    └── realtime_alert_engine.py # Spark streaming alerts
```

## Next Steps (Phase 2)

- Implement Warm Path (batch processing with Spark)
- Add data persistence (Delta Lake / Data Warehouse)
- Create dashboard for alert visualization
- Implement anomaly detection with ML
