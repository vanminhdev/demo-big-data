#!/usr/bin/env bash
# Buổi 12: huấn luyện mô hình dự đoán hủy chuyến. Tham số: cluster (mặc định) | local
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
mkdir -p ../Spark/data/cityride ../Spark/jobs
cp -r ../00_shared_data/cityride/lab ../Spark/data/cityride/
cp b12_cancel_model.py ../Spark/jobs/
(cd ../Spark && docker compose up -d >/dev/null)

# Image apache/spark không có numpy (MLlib cần). Cài một lần vào thư mục dùng
# chung /opt/spark-data/pylibs (mọi container đều thấy), rồi trỏ PYTHONPATH tới đó
# cho cả Driver và Executor.
PYLIBS=/opt/spark-data/pylibs
if [ ! -d ../Spark/data/pylibs/numpy ]; then
  docker exec spark-master pip install --quiet --target "$PYLIBS" numpy
fi

if [ "${1:-cluster}" = local ]; then
  docker exec -e PYTHONPATH="$PYLIBS" spark-master /opt/spark/bin/spark-submit \
    --master "local[*]" --driver-memory 1g \
    --conf spark.sql.shuffle.partitions=4 \
    /opt/spark-apps/b12_cancel_model.py
else
  docker exec -e PYTHONPATH="$PYLIBS" spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 512m --executor-cores 1 --total-executor-cores 2 --driver-memory 768m \
    --conf spark.executorEnv.PYTHONPATH="$PYLIBS" \
    --conf spark.sql.shuffle.partitions=4 \
    /opt/spark-apps/b12_cancel_model.py
fi
