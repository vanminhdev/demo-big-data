#!/usr/bin/env bash
# Kich ban demo checkpoint restart (V04): rot 10 file dau -> chay -> dung ->
# rot 9 file con lai -> chay lai VOI CUNG checkpoint -> chung minh batch tiep
# tuc tu giua chung (khong xu ly lai tu dau).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

SPARK_DATA_DIR="../Spark/data/session11_streaming"
MODE="update"

if [ ! -f batches_staging_v2/win_01_0900.jsonl ]; then
  bash scripts/prepare-data.sh
fi

echo "[1/5] Dam bao cum Spark da chay..."
(cd ../Spark && docker compose up -d)

echo "[2/5] Reset sach data_source_v2 + checkpoint..."
mkdir -p "$SPARK_DATA_DIR/data_source_v2" "$SPARK_DATA_DIR/checkpoint_v2"
rm -f "$SPARK_DATA_DIR/data_source_v2"/*.jsonl
# rm -rf tren Windows bind-mount doi khi bao "Directory not empty" do do tre
# dong bo filesystem - thu lai 1 lan la du (khong phai loi thuc su).
rm -rf "$SPARK_DATA_DIR/checkpoint_v2/${MODE}_mode" 2>/dev/null || \
  { sleep 2; rm -rf "$SPARK_DATA_DIR/checkpoint_v2/${MODE}_mode"; }
mkdir -p ../Spark/jobs
cp streaming_job.py ../Spark/jobs/streaming_job.py

echo "[3/5] Rot 10 file dau tien (win_01..win_10), chay lan 1..."
cp batches_staging_v2/win_0[1-9]_*.jsonl batches_staging_v2/win_10_*.jsonl "$SPARK_DATA_DIR/data_source_v2/"
docker exec \
  -e STREAM_SOURCE_DIR="/opt/spark-data/session11_streaming/data_source_v2" \
  -e STREAM_CHECKPOINT_DIR="/opt/spark-data/session11_streaming/checkpoint_v2/${MODE}_mode" \
  -e STREAM_OUTPUT_MODE="$MODE" \
  spark-master /opt/spark/bin/spark-submit /opt/spark-apps/streaming_job.py \
  | tail -20

echo ""
echo "[4/5] Rot 9 file con lai (win_11..win_19)..."
cp batches_staging_v2/win_1[1-9]_*.jsonl "$SPARK_DATA_DIR/data_source_v2/"

echo "[5/5] Chay lan 2, DUNG CHECKPOINT CU - quan sat batchId tiep tuc tu 11, khong quay lai 0..."
docker exec \
  -e STREAM_SOURCE_DIR="/opt/spark-data/session11_streaming/data_source_v2" \
  -e STREAM_CHECKPOINT_DIR="/opt/spark-data/session11_streaming/checkpoint_v2/${MODE}_mode" \
  -e STREAM_OUTPUT_MODE="$MODE" \
  spark-master /opt/spark/bin/spark-submit /opt/spark-apps/streaming_job.py \
  | grep -E "batchId=|Batch:|state for version"
