# ============================================================================
# Phase 1 - Kafka Real-Time Pipeline Stopper (PowerShell)
# ============================================================================
# This script stops all running components:
#   - All Python processes (producers, consumers, uploaders)
#   - Kafka Broker (Docker containers)
#
# Usage:
#   .\stop_pipeline.ps1
# ============================================================================

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Stopping Phase 1 Pipeline Components" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Stop all Python processes
Write-Host "[1/2] Stopping Python processes (producers, consumers, uploaders)..." -ForegroundColor Yellow
$processes = Get-Process python -ErrorAction SilentlyContinue
if ($processes) {
    $processes | Stop-Process -Force
    Write-Host "  Python processes stopped" -ForegroundColor Green
} else {
    Write-Host "  No Python processes found running" -ForegroundColor Gray
}
Write-Host ""

# Stop Docker containers
Write-Host "[2/2] Stopping Kafka Docker containers..." -ForegroundColor Yellow
$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectDir
docker-compose down 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Docker containers stopped" -ForegroundColor Green
} else {
    Write-Host "  No Docker containers found running" -ForegroundColor Gray
}
Write-Host ""

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "All components stopped!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "To start again:" -ForegroundColor Yellow
Write-Host "  Run: .\start_pipeline.ps1"
Write-Host ""
