#!/usr/bin/env bash
# Chay demo_job.py (Buoi 9) tren cum Spark Standalone that.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

# QUAN TRONG: PHAI chi dinh --executor-memory <= 640m - moi Spark worker chi
# co 640MB (xem docker-compose.yml), trong khi mac dinh Spark xin 1024MB/executor
# se khien job ket vinh vien o trang thai "WAITING" khong bao gio chay (da
# kiem thu that va gap loi nay khi viet script).
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=6 \
  --executor-memory 512m --executor-cores 1 --total-executor-cores 2 \
  /opt/spark-apps/demo_job.py
