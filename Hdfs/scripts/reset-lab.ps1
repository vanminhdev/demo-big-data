# Full reset: stop the cluster AND remove HDFS data volumes (NameNode metadata +
# DataNode blocks). Does NOT touch anything outside this project's docker-compose
# scope (only volumes declared in Hdfs/docker-compose.yml are removed).
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "[reset-lab] this will DELETE all data stored inside the HDFS lab cluster."
docker compose down -v
Write-Host "[reset-lab] volumes removed. Run start-cluster.ps1 to start from a clean state."
