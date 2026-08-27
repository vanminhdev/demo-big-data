# Copy data/web_logs_sample.jsonl into the NameNode container and upload it to HDFS.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$src = "data/web_logs_sample.jsonl"
$hdfsDir = "/retailstream/web_logs"

if (-not (Test-Path $src)) {
    Write-Error "[load-sample-data] $src not found."
    exit 1
}

docker cp $src hdfs-namenode:/tmp/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -mkdir -p $hdfsDir
docker exec hdfs-namenode hdfs dfs -put -f /tmp/web_logs_sample.jsonl "$hdfsDir/web_logs_sample.jsonl"

Write-Host "[load-sample-data] uploaded. Listing:"
docker exec hdfs-namenode hdfs dfs -ls $hdfsDir
