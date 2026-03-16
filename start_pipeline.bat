@echo off
REM ============================================================================
REM Phase 1 - Kafka Real-Time Pipeline Launcher (Windows Batch)
REM ============================================================================
REM This script opens multiple terminal windows to run all components:
REM   - Kafka Broker (Docker)
REM   - Vitals Producer
REM   - Movement Producer
REM   - Alert Engine (Real-time consumer)
REM   - S3/Local Uploader (ETL pipeline)
REM
REM Usage:
REM   Double-click start_pipeline.bat         (Local mode)
REM   Double-click start_pipeline_s3.bat    (S3 production mode)
REM   OR
REM   Run from command prompt: start_pipeline.bat
REM
REM To stop all components:
REM   Run: stop_pipeline.bat
REM ============================================================================

set PROJECT_DIR=%~dp0
set VENV_PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe

echo ============================================================
echo Phase 1 - Kafka Real-Time Pipeline
echo ============================================================
echo.
echo Project Directory: %PROJECT_DIR%
echo.

REM Check if .venv exists
if not exist "%PROJECT_DIR%.venv" (
    echo ERROR: Virtual environment not found at .venv
    echo Please create it first: python -m venv .venv
    pause
    exit /b 1
)

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running
    echo Please start Docker Desktop first
    pause
    exit /b 1
)

echo Starting Kafka Broker...
cd /d %PROJECT_DIR%
docker-compose up -d
echo.
echo Waiting for Kafka to initialize (30 seconds)...
timeout /t 30 /nobreak
echo.

echo Opening terminal windows for each component...
echo.

REM Terminal 1: Vitals Producer
echo   [1/4] Starting Vitals Producer...
start "Kafka - Vitals Producer" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/iot_vitals_producer.py --delay 0.5"

REM Terminal 2: Movement Producer
echo   [2/4] Starting Movement Producer...
start "Kafka - Movement Producer" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/iot_movement_producer.py --delay 0.1 --max-records 10000"

REM Terminal 3: Alert Engine
echo   [3/4] Starting Alert Engine...
start "Kafka - Alert Engine" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/simple_alert_engine.py"

REM Terminal 4: S3/Local Uploader
echo   [4/5] Starting S3/Local Uploader...
start "Kafka - S3 Uploader" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/kafka_to_local_uploader.py"

REM Terminal 5: ETL Scheduler
echo   [5/5] Starting ETL Scheduler...
start "Kafka - ETL Pipeline" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/etl_scheduler.py --interval 30 --auto-copy"

echo.
echo ============================================================
echo All components started!
echo ============================================================
echo.
echo Terminal Windows:
echo   1. Vitals Producer     - Streaming health data (HR, SpO2)
echo   2. Movement Producer   - Streaming accelerometer data
echo   3. Alert Engine        - Detecting critical events
echo   4. S3/Local Uploader   - Batching data every 30 seconds
echo   5. ETL Pipeline        - Processing Bronze/Silver/Gold every 30s
echo.
echo Monitoring:
echo   - Kafka UI: http://localhost:8080
echo   - Local Data: %PROJECT_DIR%data\s3_mock\
echo   - ETL Output: %PROJECT_DIR%data\etl_output\
echo.
echo To stop all components:
echo   - Run: stop_pipeline.bat
echo   - OR close all terminal windows and run: docker-compose down
echo ============================================================
echo.
pause
