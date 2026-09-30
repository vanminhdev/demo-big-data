#!/usr/bin/env bash
# Buổi 15: chạy 4 thí nghiệm tối ưu trên cụm Spark (1 master + 2 worker).
# AQE tắt để thấy rõ tác động của từng thay đổi (AQE tự sửa một phần các lỗi này).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
mkdir -p ../Spark/data/cityride ../Spark/jobs
cp -r ../00_shared_data/cityride/lab ../Spark/data/cityride/
cp b15_optimization.py ../Spark/jobs/
(cd ../Spark && docker compose up -d >/dev/null)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --executor-cores 1 --total-executor-cores 2 --driver-memory 768m \
  --conf spark.sql.adaptive.enabled=false \
  /opt/spark-apps/b15_optimization.py
