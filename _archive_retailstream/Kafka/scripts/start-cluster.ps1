# PowerShell Script: Khoi dong cum Spark va Kafka KRaft
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location "$ScriptDir\.."

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "[1/3] Kiem tra va khoi dong cum Spark (tao spark_spark-net)..." -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Push-Location "..\Spark"
docker compose up -d
Pop-Location

Write-Host "`n[2/3] Khoi dong Kafka broker (che do KRaft)..." -ForegroundColor Cyan
docker compose up -d

Write-Host "Dang cho Kafka dat trang thai healthy (toi da 60s)..."
$healthy = $false
for ($i = 1; $i -le 15; $i++) {
    $status = (docker inspect --format='{{.State.Health.Status}}' kafka 2>$null)
    if ($status -eq "healthy") {
        Write-Host "    -> Kafka da SAN SANG (healthy)!" -ForegroundColor Green
        $healthy = $true
        break
    }
    Start-Sleep -Seconds 4
}

if (-not $healthy) {
    Write-Host "    [CANH BAO] Kafka chua dat healthy, tiep tuc kiem tra..." -ForegroundColor Yellow
}

Write-Host "`n[3/3] Tao topic 'clickstream' (4 partitions) va 'product_events' (2 partitions)..." -ForegroundColor Cyan
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
  --topic clickstream --bootstrap-server localhost:29092 --partitions 4 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
  --topic product_events --bootstrap-server localhost:29092 --partitions 2 --replication-factor 1

Write-Host "`nThong tin Topic 'clickstream':" -ForegroundColor Green
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic clickstream --bootstrap-server localhost:29092

Write-Host "`n>>> KAFKA SAN SANG! Bootstrap host: localhost:9092 (Host Windows) | kafka:29092 (Docker network) <<<" -ForegroundColor Green
