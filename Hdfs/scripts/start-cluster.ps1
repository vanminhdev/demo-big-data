# Start the HDFS lab cluster (1 NameNode + 2 DataNode) and wait for health.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not (Test-Path ".env")) {
    Write-Host "[start-cluster] .env not found, copying from .env.example"
    Copy-Item ".env.example" ".env"
}

docker compose up -d

Write-Host "[start-cluster] waiting for containers to report healthy..."
$ok = $false
for ($i = 0; $i -lt 30; $i++) {
    $statuses = docker compose ps --format '{{.Name}} {{.Health}}'
    $unhealthy = $statuses | Where-Object { $_ -notmatch "healthy" }
    if (-not $unhealthy) {
        Write-Host "[start-cluster] all containers healthy."
        docker compose ps
        $port = if ($env:NAMENODE_HTTP_PORT) { $env:NAMENODE_HTTP_PORT } else { "9870" }
        Write-Host ""
        Write-Host "NameNode Web UI: http://localhost:$port"
        $ok = $true
        break
    }
    Start-Sleep -Seconds 5
}

if (-not $ok) {
    Write-Warning "[start-cluster] some containers did not become healthy in time."
    docker compose ps
    exit 1
}
