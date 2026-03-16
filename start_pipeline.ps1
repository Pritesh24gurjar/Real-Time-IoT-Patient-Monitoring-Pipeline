# ============================================================================
# Phase 1 - Kafka Real-Time Pipeline Launcher (PowerShell)
# ============================================================================
# This script opens multiple terminal windows to run all components:
#   - Kafka Broker (Docker)
#   - Vitals Producer
#   - Movement Producer
#   - Alert Engine (Real-time consumer)
#   - S3/Local Uploader (ETL pipeline)
#
# Usage:
#   .\start_pipeline.ps1
#
# To stop all components:
#   .\stop_pipeline.ps1
# ============================================================================

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPython = Join-Path $ProjectDir ".venv\Scripts\python.exe"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Phase 1 - Kafka Real-Time Pipeline" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Project Directory: $ProjectDir"
Write-Host ""

# Check if .venv exists
if (-not (Test-Path "$ProjectDir\.venv")) {
    Write-Host "ERROR: Virtual environment not found at .venv" -ForegroundColor Red
    Write-Host "Please create it first: python -m venv .venv" -ForegroundColor Yellow
    exit 1
}

# Check if Docker is running
try {
    $dockerInfo = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Docker not running"
    }
} catch {
    Write-Host "ERROR: Docker is not running" -ForegroundColor Red
    Write-Host "Please start Docker Desktop first" -ForegroundColor Yellow
    exit 1
}

Write-Host "Starting Kafka Broker..." -ForegroundColor Green
Set-Location $ProjectDir
docker-compose up -d

Write-Host "Waiting for Kafka to initialize (30 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30
Write-Host ""

Write-Host "Opening terminal windows for each component..." -ForegroundColor Green
Write-Host ""

# Terminal 1: Vitals Producer
Write-Host "  [1/4] Starting Vitals Producer..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
cd '$ProjectDir';
.\.venv\Scripts\activate;
Write-Host '=== Vitals Producer ===' -ForegroundColor Green;
python scripts/iot_vitals_producer.py --delay 0.5
"@

# Terminal 2: Movement Producer
Write-Host "  [2/4] Starting Movement Producer..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
cd '$ProjectDir';
.\.venv\Scripts\activate;
Write-Host '=== Movement Producer ===' -ForegroundColor Green;
python scripts/iot_movement_producer.py --delay 0.1 --max-records 10000
"@

# Terminal 3: Alert Engine
Write-Host "  [3/4] Starting Alert Engine..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
cd '$ProjectDir';
.\.venv\Scripts\activate;
Write-Host '=== Alert Engine ===' -ForegroundColor Green;
python scripts/simple_alert_engine.py
"@

# Terminal 4: S3/Local Uploader
Write-Host "  [4/4] Starting S3/Local Uploader..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", @"
cd '$ProjectDir';
.\.venv\Scripts\activate;
Write-Host '=== S3/Local Uploader ===' -ForegroundColor Green;
python scripts/kafka_to_local_uploader.py
"@

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "All components started!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Terminal Windows:" -ForegroundColor Yellow
Write-Host "  1. Vitals Producer     - Streaming health data (HR, SpO2)"
Write-Host "  2. Movement Producer   - Streaming accelerometer data"
Write-Host "  3. Alert Engine        - Detecting critical events"
Write-Host "  4. S3/Local Uploader   - Batching data every 30 seconds"
Write-Host ""
Write-Host "Monitoring:" -ForegroundColor Yellow
Write-Host "  - Kafka UI: http://localhost:8080"
Write-Host "  - Local Data: $ProjectDir\data\s3_mock\"
Write-Host ""
Write-Host "To stop all components:" -ForegroundColor Yellow
Write-Host "  Run: .\stop_pipeline.ps1"
Write-Host "  OR close all terminal windows and run: docker-compose down"
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
