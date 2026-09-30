#!/usr/bin/env bash
# Chay Spark Structured Streaming doc tu Kafka topic "clickstream" that,
# tren cum Spark Standalone that (khong phai local[*]). Yeu cau: Spark +
# Kafka da chay (bash scripts/start-cluster.sh), va topic co du lieu (bash
# scripts/demo-produce-consume.sh hoac tu chay producer.py truoc).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

echo "[1/3] Copy job + du lieu vao thu muc mount cua Spark..."
mkdir -p ../Spark/jobs ../Spark/data/session13_kafka/checkpoint
cp spark_kafka_job.py ../Spark/jobs/spark_kafka_job.py
docker exec spark-master mkdir -p /opt/spark-data/ivy2cache

echo "[2/3] Xoa checkpoint cu de doc lai tu dau (KAFKA_STARTING_OFFSETS=earliest)..."
echo "  (neu khong xoa, lan chay sau se tiep tuc tu offset cu, so lieu demo se khac lan truoc)"
# LUU Y: "rm -rf dir/*" KHONG xoa duoc file an (vi du .metadata.crc) vi glob
# "*" khong khop dotfile - phai xoa het ca thu muc roi tao lai (da gap loi
# that: "FileAlreadyExistsException: .metadata.crc already exists" khi chi
# dung "rm -rf .../*").
rm -rf ../Spark/data/session13_kafka/checkpoint
mkdir -p ../Spark/data/session13_kafka/checkpoint

echo "[3/3] Chay spark-submit tren cluster that (--packages tai connector Kafka qua Internet)..."
docker exec \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
  -e KAFKA_TOPIC="clickstream" \
  -e KAFKA_STARTING_OFFSETS="earliest" \
  -e STREAM_CHECKPOINT_DIR="/opt/spark-data/session13_kafka/checkpoint" \
  -e STREAM_OUTPUT_MODE="update" \
  -e STREAM_WINDOW_DURATION="1 day" \
  -e STREAM_WATERMARK_DELAY="1 day" \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark-data/ivy2cache \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  --driver-memory 512m --executor-memory 512m \
  /opt/spark-apps/spark_kafka_job.py
