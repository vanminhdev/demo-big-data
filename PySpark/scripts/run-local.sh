#!/usr/bin/env bash
# Buổi 10: chạy cùng pipeline ở chế độ local[*] (một tiến trình, không dùng worker).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
mkdir -p ../Spark/data/cityride ../Spark/jobs
cp -r ../00_shared_data/cityride/lab ../Spark/data/cityride/
cp b10_cityride_pipeline.py ../Spark/jobs/
(cd ../Spark && docker compose up -d)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master "local[*]" --driver-memory 1g \
  --conf spark.sql.shuffle.partitions=4 \
  /opt/spark-apps/b10_cityride_pipeline.py
