#!/usr/bin/env bash
# Copy du lieu lab (co tuong quan gia lap, xem 00_shared_data/README.md) +
# job vao thu muc mount cua Spark, cai numpy dung chung neu chua co.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

echo "Dam bao cum Spark da chay..."
(cd ../Spark && docker compose up -d)

echo "Copy du lieu lab + train_pipeline.py..."
mkdir -p ../Spark/data/mllib_session12
cp ../00_shared_data/lab/orders.csv ../Spark/data/mllib_session12/orders.csv
cp ../00_shared_data/lab/customers.csv ../Spark/data/mllib_session12/customers.csv
cp train_pipeline.py ../Spark/data/mllib_session12/train_pipeline.py

if ! docker exec spark-master test -d /opt/spark-data/mllib_session12/pylibs/numpy 2>/dev/null; then
  echo "Cai numpy vao thu muc dung chung (mot lan)..."
  docker exec spark-master pip3 install --no-cache-dir \
    --target=/opt/spark-data/mllib_session12/pylibs numpy
else
  echo "numpy da co san, bo qua cai dat."
fi

echo "Du lieu san sang tai Spark/data/mllib_session12/"
