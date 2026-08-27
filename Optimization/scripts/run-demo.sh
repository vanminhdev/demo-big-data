#!/usr/bin/env bash
# Chay ca 3 tinh huong toi uu hoa (data skew, small files, shuffle) tren cum
# Spark Standalone that, tu dong doc lai bang chung event log cuoi cung.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

echo "[1/5] Dam bao cum Spark da chay..."
(cd ../Spark && docker compose up -d)

echo "[2/5] Chuan bi du lieu (copy vao Spark/data, tao du lieu lech neu chua co)..."
mkdir -p ../Spark/data/session15_optimization ../Spark/jobs
if [ ! -f data/clickstream_skewed.jsonl ]; then
  python make_skewed_data.py
fi
cp ../00_shared_data/lab/orders.csv ../Spark/data/session15_optimization/orders_lab.csv
cp ../00_shared_data/lab/order_items.csv ../Spark/data/session15_optimization/order_items_lab.csv
cp data/clickstream_skewed.jsonl ../Spark/data/session15_optimization/clickstream_skewed.jsonl
cp data_skew_demo.py small_files_demo.py shuffle_demo.py ../Spark/jobs/
docker exec spark-master mkdir -p /opt/spark-data/session15_optimization/event_logs

echo "[3/5] V01 - Data skew (AQE tat de thay ro do lech)..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark-data/session15_optimization/event_logs \
  --executor-memory 512m --driver-memory 512m --total-executor-cores 2 \
  /opt/spark-apps/data_skew_demo.py

echo "[4/5] V02 - Small files..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark-data/session15_optimization/event_logs \
  --executor-memory 512m --driver-memory 512m --total-executor-cores 2 \
  /opt/spark-apps/small_files_demo.py

echo "[5/5] V03 - Shuffle lon (AQE tat)..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark-data/session15_optimization/event_logs \
  --executor-memory 512m --driver-memory 512m --total-executor-cores 2 \
  /opt/spark-apps/shuffle_demo.py

echo ""
echo "Hoan tat 3 kich ban. Doc lai bang chung tung app bang:"
echo "  python parse_event_log.py ../Spark/data/session15_optimization/event_logs/<app-id>"
docker exec spark-master ls -t /opt/spark-data/session15_optimization/event_logs/
