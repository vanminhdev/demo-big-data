#!/usr/bin/env bash
# Khoi dong Kafka (KRaft, 1 broker) + tao san 2 topic clickstream/product_events.
# Kafka gan vao network cua Spark (spark_spark-net) nen Spark PHAI chay truoc.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

echo "[1/3] Dam bao cum Spark (spark_spark-net) da chay..."
(cd ../Spark && docker compose up -d)

echo "[2/3] Khoi dong Kafka..."
docker compose up -d

for i in $(seq 1 20); do
  status=$(docker inspect --format='{{.State.Health.Status}}' kafka 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then
    echo "    Kafka healthy."
    break
  fi
  sleep 5
  if [ "$i" -eq 20 ]; then
    echo "    LOI: Kafka khong healthy sau 100s." >&2
    exit 1
  fi
done

echo "[3/3] Tao topic (bo qua neu da co)..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic clickstream --bootstrap-server localhost:29092 --partitions 4 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic product_events --bootstrap-server localhost:29092 --partitions 2 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic clickstream --bootstrap-server localhost:29092
echo "Kafka san sang. bootstrap host: localhost:9092 (host) / kafka:29092 (trong Docker network)."
