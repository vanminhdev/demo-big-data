#!/usr/bin/env bash
# Buổi 9: chạy demo CityRide trên cụm Spark Standalone (1 master + 2 worker).
# Tham số 1 (tùy chọn): số giây giữ Spark UI (http://localhost:4040) sau khi chạy xong.
#
# Hai cấu hình giúp Spark UI dễ đọc khi học:
#   spark.sql.adaptive.enabled=false   : mỗi action ứng với đúng một job
#   spark.sql.files.maxPartitionBytes=4m: tệp 25 MB được chia sẵn thành nhiều partition
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

# Đưa dữ liệu CityRide vào thư mục mount /opt/spark-data của mọi container
mkdir -p data/cityride
cp -r ../00_shared_data/cityride/lab data/cityride/

docker compose up -d
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --executor-cores 1 --total-executor-cores 2 \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.files.maxPartitionBytes=4m \
  /opt/spark-apps/b09_cityride_spark.py "${1:-0}"
