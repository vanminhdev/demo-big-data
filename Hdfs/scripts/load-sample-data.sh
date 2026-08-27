#!/usr/bin/env bash
# Copy data/web_logs_sample.jsonl into the NameNode container and upload it to HDFS.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # avoid Git Bash rewriting /tmp/... paths on Windows
cd "$(dirname "$0")/.."

SRC="data/web_logs_sample.jsonl"
HDFS_DIR="/retailstream/web_logs"

if [ ! -f "$SRC" ]; then
  echo "[load-sample-data] $SRC not found." >&2
  exit 1
fi

docker cp "$SRC" hdfs-namenode:/tmp/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -mkdir -p "$HDFS_DIR"
docker exec hdfs-namenode hdfs dfs -put -f /tmp/web_logs_sample.jsonl "$HDFS_DIR/web_logs_sample.jsonl"

echo "[load-sample-data] uploaded. Listing:"
docker exec hdfs-namenode hdfs dfs -ls "$HDFS_DIR"
