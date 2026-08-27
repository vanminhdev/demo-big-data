#!/usr/bin/env bash
# Chay process_retailstream.py tren Spark Standalone cluster that (2 worker).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

echo "Dam bao cum Spark da chay..."
(cd ../Spark && docker compose up -d)

echo "Copy job vao Spark/jobs..."
mkdir -p ../Spark/jobs
cp process_retailstream.py ../Spark/jobs/process_retailstream.py

docker exec -e SPARK_MASTER_URL="spark://spark-master:7077" spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=4 \
  --executor-memory 512m --executor-cores 1 --total-executor-cores 2 \
  --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py
