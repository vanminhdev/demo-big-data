#!/usr/bin/env bash
# Kich ban demo: xoa sach data_source_v2 + checkpoint, rot toan bo 19 file
# batch vao 1 luot, chay streaming_job.py (Trigger.availableNow: xu ly het
# roi tu dung - phu hop demo tren lop, khong can cho trigger interval that).
#
# Tham so: $1 = output mode (update|complete|append), mac dinh "update".
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

MODE="${1:-update}"
SPARK_DATA_DIR="../Spark/data/session11_streaming"

if [ ! -f batches_staging_v2/win_01_0900.jsonl ]; then
  echo "Chua co du lieu batch, chay scripts/prepare-data.sh truoc..."
  bash scripts/prepare-data.sh
fi

echo "[1/4] Dam bao cum Spark da chay..."
(cd ../Spark && docker compose up -d)

echo "[2/4] Reset data_source_v2 + checkpoint cho mode=$MODE..."
mkdir -p "$SPARK_DATA_DIR/data_source_v2" "$SPARK_DATA_DIR/checkpoint_v2"
rm -rf "${SPARK_DATA_DIR:?}/data_source_v2"/*.jsonl
# rm -rf tren Windows bind-mount doi khi bao "Directory not empty" do do tre
# dong bo filesystem - thu lai 1 lan la du (khong phai loi thuc su).
rm -rf "$SPARK_DATA_DIR/checkpoint_v2/${MODE}_mode" 2>/dev/null || \
  { sleep 2; rm -rf "$SPARK_DATA_DIR/checkpoint_v2/${MODE}_mode"; }
cp batches_staging_v2/win_*.jsonl "$SPARK_DATA_DIR/data_source_v2/"

echo "[3/4] Copy streaming_job.py vao Spark/jobs..."
mkdir -p ../Spark/jobs
cp streaming_job.py ../Spark/jobs/streaming_job.py

echo "[4/4] Chay streaming_job.py (output mode=$MODE, window=5 minutes)..."
docker exec \
  -e STREAM_SOURCE_DIR="/opt/spark-data/session11_streaming/data_source_v2" \
  -e STREAM_CHECKPOINT_DIR="/opt/spark-data/session11_streaming/checkpoint_v2/${MODE}_mode" \
  -e STREAM_OUTPUT_MODE="$MODE" \
  spark-master /opt/spark/bin/spark-submit /opt/spark-apps/streaming_job.py
