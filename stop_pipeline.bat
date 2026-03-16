@echo off
REM ============================================================================
REM Phase 1 - Kafka Real-Time Pipeline Stopper (Windows Batch)
REM ============================================================================
REM This script stops all running components:
REM   - All Python processes (producers, consumers, uploaders)
REM   - Kafka Broker (Docker containers)
REM
REM Usage:
REM   Double-click stop_pipeline.bat
REM   OR
REM   Run from command prompt: stop_pipeline.bat
REM ============================================================================

echo ============================================================
echo Stopping Phase 1 Pipeline Components
echo ============================================================
echo.

REM Stop all Python processes
echo [1/2] Stopping Python processes (producers, consumers, uploaders)...
taskkill /F /IM python.exe 2>nul
if errorlevel 1 (
    echo   No Python processes found running
) else (
    echo   Python processes stopped
)
echo.

REM Stop Docker containers
echo [2/2] Stopping Kafka Docker containers...
docker-compose down 2>nul
if errorlevel 1 (
    echo   No Docker containers found running
) else (
    echo   Docker containers stopped
)
echo.

echo ============================================================
echo All components stopped!
echo ============================================================
echo.
echo To start again:
echo   Run: start_pipeline.bat
echo.
pause
