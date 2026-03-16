# Pipeline Launcher Scripts

Quick-start scripts to run all Phase 1 components in separate terminal windows.

## Files

| File | Description |
|------|-------------|
| `start_pipeline.bat` | Windows Batch script to start all components |
| `stop_pipeline.bat` | Windows Batch script to stop all components |
| `start_pipeline.ps1` | PowerShell script to start all components |
| `stop_pipeline.ps1` | PowerShell script to stop all components |

## Quick Start

### Option 1: Double-click (Easiest)
1. Double-click `start_pipeline.bat`
2. Wait for 4 terminal windows to open
3. Each window shows a different component

### Option 2: Command Line

**Batch (Command Prompt):**
```cmd
start_pipeline.bat
```

**PowerShell:**
```powershell
# May need to allow script execution first:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Then run:
.\start_pipeline.ps1
```

## What Gets Started

| Window | Component | Description |
|--------|-----------|-------------|
| 1 | Vitals Producer | Streams health data (HR, SpO2, stress) to Kafka |
| 2 | Movement Producer | Streams accelerometer data to Kafka |
| 3 | Alert Engine | Detects critical events in real-time |
| 4 | S3/Local Uploader | Batches and saves data every 30 seconds |

## Timeline

```
0:00  - Kafka broker starts (Docker)
0:30  - All 4 terminal windows open
0:35  - First data flows through pipeline
1:05  - First batch uploaded to data/s3_mock/
```

## Monitoring

**Kafka UI (Web):**
- Open http://localhost:8080
- View topics: raw_vitals, raw_movement, critical_alerts

**Local Data Files:**
- Location: `data/s3_mock/raw/`
- Structure: `vitals/` and `movement/` partitioned by date

**Alerts:**
- View in "Alert Engine" terminal window
- Or check Kafka topic: `critical_alerts`

## Stopping

### Option 1: Run Stop Script
```cmd
stop_pipeline.bat
```

### Option 2: Manual
1. Close all 4 terminal windows
2. Run: `docker-compose down`

## Troubleshooting

### "Virtual environment not found"
```powershell
# Create virtual environment
python -m venv .venv

# Install dependencies
.\.venv\Scripts\activate
pip install -r requirements-phase1.txt
```

### "Docker is not running"
- Start Docker Desktop
- Wait for whale icon to be steady
- Run script again

### "Access denied" (PowerShell)
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Port already in use
```powershell
# Find process using port 9092
netstat -ano | findstr :9092

# Kill the process (replace PID)
taskkill /F /PID <PID>
```

## Configuration

Edit component settings in respective scripts:

**Vitals Producer Speed:**
```cmd
python scripts/iot_vitals_producer.py --delay 0.5  # Change delay
```

**Movement Producer Speed:**
```cmd
python scripts/iot_movement_producer.py --delay 0.1 --max-records 10000
```

**Upload Interval:**
Edit `scripts/kafka_to_local_uploader.py`:
```python
test_interval = 30  # Change to desired seconds
```

## Production Mode

For production with S3 uploads:

1. Update `.env` with AWS credentials
2. Edit `start_pipeline.bat` / `.ps1`:
   - Change `kafka_to_local_uploader.py` to `kafka_to_s3_uploader.py`
3. Run the script
