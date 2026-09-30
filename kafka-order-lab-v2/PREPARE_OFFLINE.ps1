$ErrorActionPreference = "Stop"

Write-Host "=== Kafka Lab: prepare environment before class ===" -ForegroundColor Cyan

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker was not found. Install Docker Desktop first."
}

docker version
docker compose version

Write-Host "Pulling fixed images..." -ForegroundColor Yellow
docker pull apache/kafka:4.3.1
docker pull provectuslabs/kafka-ui:v0.7.2

Write-Host "Building the lab application image..." -ForegroundColor Yellow
docker compose build

Write-Host "Starting the lab once to verify everything works..." -ForegroundColor Yellow
docker compose up -d

Write-Host "Waiting for services..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

docker compose ps

Write-Host "Stopping containers but keeping images/volumes..." -ForegroundColor Yellow
docker compose down

Write-Host "Preparation finished. The lab images are now cached locally." -ForegroundColor Green
Write-Host "Do NOT run 'docker compose pull' at school when there is no Internet." -ForegroundColor Green
