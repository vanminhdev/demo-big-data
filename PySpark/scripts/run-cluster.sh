#!/usr/bin/env bash
# Buổi 10: chạy pipeline CityRide trên cụm Spark Standalone (2 worker).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
mkdir -p ../Spark/data/cityride ../Spark/jobs
cp -r ../00_shared_data/cityride/lab ../Spark/data/cityride/
cp b10_cityride_pipeline.py ../Spark/jobs/
(cd ../Spark && docker compose up -d)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --executor-cores 1 --total-executor-cores 2 \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=4 \
  /opt/spark-apps/b10_cityride_pipeline.py
