# PowerShell Script: Reset topic ve trang thai sach 0 message
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location "$ScriptDir\.."

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "[1/3] Dung container Kafka..." -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
docker compose down

Write-Host "`n[2/3] Xoa du lieu topic cu tren host (./data)..." -ForegroundColor Cyan
if (Test-Path ".\data") {
    Get-ChildItem -Path ".\data" -Recurse | Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
}

Write-Host "`n[3/3] Khoi dong lai Kafka va tao topic sach tu dau..." -ForegroundColor Cyan
& "$ScriptDir\start-cluster.ps1"

Write-Host "`n>>> DA RESET THANH CONG! Topic 'clickstream' va 'product_events' hien co 0 message. <<<" -ForegroundColor Green
