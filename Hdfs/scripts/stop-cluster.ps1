# Stop the HDFS lab cluster but KEEP volumes (metadata/blocks persist).
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

docker compose down
Write-Host "[stop-cluster] containers stopped, volumes kept. Use reset-lab.ps1 to wipe data."
