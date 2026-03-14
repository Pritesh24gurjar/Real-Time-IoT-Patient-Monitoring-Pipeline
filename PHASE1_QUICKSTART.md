# Phase 1 - Quick Start Guide

## Prerequisites
- Docker Desktop installed and running
- Python 3.9+ with virtual environment (`.venv`)

---

## Step 1: Activate Virtual Environment

**PowerShell:**
```powershell
cd "C:\Users\prite\Documents\CWRU courses\data eng\DE project"
.\.venv\Scripts\Activate.ps1
```

**Command Prompt:**
```cmd
cd "C:\Users\prite\Documents\CWRU courses\data eng\DE project"
.venv\Scripts\activate.bat
```

---

## Step 2: Install Dependencies (first time only)

```bash
pip install -r requirements-phase1.txt
pip install pyspark
```

---

## Step 3: Start Kafka Cluster

```bash
docker-compose up -d
```

Wait 20-30 seconds for Kafka to initialize.

**Verify Kafka is running:**
```bash
docker-compose ps
```

You should see `kafka-broker` with status "Up (healthy)".

---

## Step 4: Start IoT Producers

**Terminal 1 - Vitals Producer:**
```bash
python scripts/iot_vitals_producer.py --delay 0.2
```

**Terminal 2 - Movement Producer:**
```bash
python scripts/iot_movement_producer.py --delay 0.05 --max-records 5000
```

---

## Step 5: Start Alert Engine

**Terminal 3 - Alert Consumer:**
```bash
python scripts/simple_alert_engine.py
```

---

## Step 6: Monitor Alerts

**Option A: Docker Console Consumer**
```bash
docker exec -it kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic critical_alerts --max-messages 20 --timeout-ms 10000
```

**Option B: Kafka UI (Web Browser)**
1. Open http://localhost:8080
2. Click "Topics" → "critical_alerts"
3. View messages in real-time

---

## Producer Options

### Vitals Producer
```bash
# Normal speed (1 reading per 0.2 seconds)
python scripts/iot_vitals_producer.py --delay 0.2

# Loop continuously
python scripts/iot_vitals_producer.py --loop --delay 0.5

# Dry run (see sample data without sending)
python scripts/iot_vitals_producer.py --dry-run
```

### Movement Producer
```bash
# Send 5000 records at 20Hz
python scripts/iot_movement_producer.py --delay 0.05 --max-records 5000

# Loop continuously
python scripts/iot_movement_producer.py --loop --delay 0.1

# Dry run
python scripts/iot_movement_producer.py --dry-run
```

---

## Stop Everything

**Stop Python processes:**
```bash
taskkill /F /IM python.exe
```

**Stop Kafka:**
```bash
docker-compose down
```

**Stop Kafka and delete data:**
```bash
docker-compose down -v
```

---

## Troubleshooting

### Kafka won't start
```bash
# Check logs
docker-compose logs kafka

# Restart
docker-compose down
docker-compose up -d
```

### No alerts appearing
1. Verify producers are running (check terminal output)
2. Check topics exist:
   ```bash
   docker exec -it kafka-broker kafka-topics --list --bootstrap-server localhost:9092
   ```
3. Verify data in topics:
   ```bash
   docker exec -it kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_vitals --max-messages 5 --timeout-ms 5000
   ```

### Port 9092 already in use
```bash
# Find process using port 9092
netstat -ano | findstr :9092

# Kill the process (replace PID)
taskkill /F /PID <PID>
```

---

## Alert Rules

| Alert Type | Condition | Threshold | Expected Frequency |
|------------|-----------|-----------|-------------------|
| `FALL_DETECTED` | SVM (impact force) | > 25.0 | Rare (normal activities) |
| `TACHYCARDIA` | Heart Rate | > 130 BPM | None (max HR in data: 127.6) |
| `BRADYCARDIA` | Heart Rate | < 40 BPM | None (min HR in data: 40.0) |
| `HYPOXIA` | SpO2 | < 90% | None (min SpO2: 90.79%) |
| `HIGH_STRESS` | Stress Level | >= 8 | **~29% of messages** (2,320 records) |

### Alert Distribution in Dataset

```
HIGH_STRESS      2,320 (28.9%) ████████████████████████████████████████████████████████
TACHYCARDIA          0 ( 0.0%)
BRADYCARDIA          0 ( 0.0%)
HYPOXIA              0 ( 0.0%)
```

**Why only HIGH_STRESS alerts?**
- The clean dataset has healthy vitals (HR: 40-127.6 BPM, SpO2: 90.79-100%)
- Stress levels are uniformly distributed (1-10), so ~30% have stress ≥ 8

**To see more alert types**, lower thresholds in `scripts/simple_alert_engine.py`:
```python
TACHYCARDIA_HR_THRESHOLD = 100  # From 130
HYPOXIA_SPO2_THRESHOLD = 95     # From 90
HIGH_STRESS_THRESHOLD = 7       # From 8
```

### Analyze Your Data

```bash
# Run alert distribution analysis
python scripts/analyze_alerts.py
```

---

## File Structure

```
DE project/
├── docker-compose.yml           # Kafka cluster
├── requirements-phase1.txt      # Dependencies
├── PHASE1_QUICKSTART.md        # This file
└── scripts/
    ├── iot_vitals_producer.py   # Health data stream
    ├── iot_movement_producer.py # Accelerometer stream
    ├── simple_alert_engine.py   # Real-time alerts (Python)
    ├── realtime_alert_engine.py # Real-time alerts (Spark)
    └── analyze_alerts.py        # Alert distribution analysis
```
