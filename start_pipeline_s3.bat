@echo off
REM ============================================================================
REM Phase 1 - Kafka Real-Time Pipeline Launcher (S3 Production Mode)
REM ============================================================================
REM This script opens multiple terminal windows to run all components with S3:
REM   - Kafka Broker (Docker)
REM   - Vitals Producer
REM   - Movement Producer
REM   - Alert Engine (Real-time consumer)
REM   - Kafka to S3 Stream Uploader (Production)
REM   - ETL S3 Pipeline (Bronze/Silver/Gold)
REM
REM Prerequisites:
REM   - Configure .env file with AWS credentials:
REM     AWS_ACCESS_KEY_ID=...
REM     AWS_SECRET_ACCESS_KEY=...
REM     AWS_SESSION_TOKEN=...
REM     S3_BUCKET_NAME=your-bucket
REM
REM Usage:
REM   Double-click start_pipeline_s3.bat
REM   OR
REM   Run from command prompt: start_pipeline_s3.bat
REM
REM To stop all components:
REM   Run: stop_pipeline.bat
REM ============================================================================

set PROJECT_DIR=%~dp0
set VENV_PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe

echo ============================================================
echo Phase 1 - Kafka Real-Time Pipeline (S3 PRODUCTION MODE)
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

REM Check AWS credentials
findstr /C:"AWS_ACCESS_KEY_ID=" "%PROJECT_DIR%.env" >nul 2>&1
if errorlevel 1 (
    echo WARNING: AWS credentials not found in .env file
    echo Please configure .env file with:
    echo   AWS_ACCESS_KEY_ID=...
    echo   AWS_SECRET_ACCESS_KEY=...
    echo   AWS_SESSION_TOKEN=...
    echo   S3_BUCKET_NAME=...
    echo.
    set /p continue="Continue anyway? (y/n): "
    if /i not "%continue%"=="y" exit /b 1
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
echo   [1/6] Starting Vitals Producer...
start "Kafka - Vitals Producer" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/iot_vitals_producer.py --delay 0.5"

REM Terminal 2: Movement Producer
echo   [2/6] Starting Movement Producer...
start "Kafka - Movement Producer" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/iot_movement_producer.py --delay 0.1 --max-records 10000"

REM Terminal 3: Alert Engine
echo   [3/6] Starting Alert Engine...
start "Kafka - Alert Engine" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/simple_alert_engine.py"

REM Terminal 4: Kafka to S3 Stream
echo   [4/6] Starting Kafka to S3 Stream...
start "Kafka - S3 Stream" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/kafka_to_s3_stream.py"

REM Terminal 5: ETL S3 Pipeline
echo   [5/6] Starting ETL S3 Pipeline...
start "Kafka - ETL S3" cmd.exe /k "cd /d %PROJECT_DIR% && .venv\Scripts\activate && python scripts/etl_s3_pipeline.py --schedule --interval 30"

echo.
echo ============================================================
echo All components started! (S3 PRODUCTION MODE)
echo ============================================================
echo.
echo Terminal Windows:
echo   1. Vitals Producer      - Streaming health data (HR, SpO2)
echo   2. Movement Producer    - Streaming accelerometer data
echo   3. Alert Engine         - Detecting critical events
echo   4. Kafka to S3 Stream   - Uploading to S3 every 10 min
echo   5. ETL S3 Pipeline      - Bronze/Silver/Gold processing
echo.
echo Monitoring:
echo   - Kafka UI: http://localhost:8080
echo   - S3 Bucket: s3://%S3_BUCKET_NAME%
echo     - Landing:  s3://%S3_BUCKET_NAME%/landing/
echo     - Bronze:   s3://%S3_BUCKET_NAME%/bronze/
echo     - Silver:   s3://%S3_BUCKET_NAME%/silver/
echo     - Gold:     s3://%S3_BUCKET_NAME%/gold/
echo.
echo To stop all components:
echo   - Run: stop_pipeline.bat
echo   - OR close all terminal windows and run: docker-compose down
echo ============================================================
echo.
pause
