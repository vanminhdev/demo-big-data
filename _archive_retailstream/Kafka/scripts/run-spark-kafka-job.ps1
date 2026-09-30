# PowerShell Script: Chay Spark Structured Streaming doc tu Kafka source
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location "$ScriptDir\.."

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "[1/3] Copy job va chuan bi thu muc mount cua Spark..." -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
if (-not (Test-Path "..\Spark\jobs")) { New-Item -ItemType Directory -Path "..\Spark\jobs" | Out-Null }
Copy-Item "spark_kafka_job.py" "..\Spark\jobs\spark_kafka_job.py" -Force
docker exec spark-master mkdir -p /opt/spark-data/ivy2cache

Write-Host "`n[2/3] Xoa checkpoint cu de doc lai tu dau (KAFKA_STARTING_OFFSETS=earliest)..." -ForegroundColor Cyan
$checkpointDir = "..\Spark\data\session13_kafka\checkpoint"
if (Test-Path $checkpointDir) {
    Remove-Item -Path $checkpointDir -Recurse -Force -ErrorAction SilentlyContinue
}
New-Item -ItemType Directory -Path $checkpointDir -Force | Out-Null

Write-Host "`n[3/3] Nop job len cum Spark Standalone that qua spark-submit..." -ForegroundColor Cyan
docker exec `
  -e SPARK_MASTER_URL="spark://spark-master:7077" `
  -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" `
  -e KAFKA_TOPIC="clickstream" `
  -e KAFKA_STARTING_OFFSETS="earliest" `
  -e STREAM_CHECKPOINT_DIR="/opt/spark-data/session13_kafka/checkpoint" `
  -e STREAM_OUTPUT_MODE="update" `
  -e STREAM_WINDOW_DURATION="1 day" `
  -e STREAM_WATERMARK_DELAY="1 day" `
  spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/opt/spark-data/ivy2cache `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 `
  --driver-memory 512m --executor-memory 512m `
  /opt/spark-apps/spark_kafka_job.py

Write-Host "`n>>> HOAN TAT JOB SPARK STRUCTURED STREAMING! <<<" -ForegroundColor Green
