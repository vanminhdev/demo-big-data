#!/usr/bin/env bash
# Chay process_retailstream.py o che do local[*] (ben trong container spark-master).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

echo "Copy job vao Spark/jobs (container chi mount duoc Spark/jobs)..."
mkdir -p ../Spark/jobs
cp process_retailstream.py ../Spark/jobs/process_retailstream.py

docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.sql.shuffle.partitions=4 \
  --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py
