#!/usr/bin/env bash
# Huan luyen pipeline MLlib tren Spark Standalone cluster that (2 worker).
# QUAN TRONG: PHAI chi dinh --executor-memory <= 640m (kich thuoc worker),
# neu khong job se ket vinh vien o "Initial job has not accepted any
# resources" - da gap loi nay khi kiem thu (xem sessions/12_mllib/LOCAL_REPORT.md
# ISSUE-04).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

bash scripts/prepare-data.sh

RUN_TAG="${1:-cluster_mode_v2}"
echo "Chay tren Spark Standalone cluster, RUN_TAG=$RUN_TAG..."
docker exec \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e MLLIB_RUN_TAG="$RUN_TAG" \
  -e PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --executor-cores 1 --total-executor-cores 2 \
  /opt/spark-data/mllib_session12/train_pipeline.py

echo ""
echo "Ket qua: Spark/data/mllib_session12/output/$RUN_TAG/metrics.json"
docker exec spark-master cat "/opt/spark-data/mllib_session12/output/$RUN_TAG/metrics.json"
