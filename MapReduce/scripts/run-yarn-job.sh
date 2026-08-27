#!/usr/bin/env bash
# Chay job MapReduce (dem luot xem theo product_id) that tren YARN
# (ResourceManager + NodeManager), dung cum Hdfs/docker-compose.yml.
# Kich ban da kiem thu that, xem sessions/07_mapreduce/LOCAL_REPORT.md V06/V07.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

NAMENODE_IMAGE="retailstream-hadoop-namenode-py3:2.0.0-hadoop3.2.1-java8"
NODEMANAGER_IMAGE="retailstream-hadoop-nodemanager-py3:2.0.0-hadoop3.2.1-java8"

echo "[1/6] Build image NameNode/NodeManager co san python3 (bo qua neu da co)..."
docker build -t "$NAMENODE_IMAGE" -f Dockerfile.namenode-with-python3 . >/dev/null
docker build -t "$NODEMANAGER_IMAGE" -f Dockerfile.nodemanager-with-python3 . >/dev/null
echo "    OK."

echo "[2/6] Khoi dong cum HDFS+YARN (Hdfs/docker-compose.yml)..."
(cd ../Hdfs && docker compose up -d)

echo "[3/6] Cho NodeManager healthy..."
for i in $(seq 1 30); do
  status=$(docker inspect --format='{{.State.Health.Status}}' hdfs-nodemanager1 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then
    echo "    NodeManager healthy."
    break
  fi
  sleep 5
  if [ "$i" -eq 30 ]; then
    echo "    LOI: NodeManager khong healthy sau 150s." >&2
    exit 1
  fi
done

echo "[4/6] Nap du lieu web_logs len HDFS neu chua co, copy mapper/reducer..."
docker exec hdfs-namenode hdfs dfs -mkdir -p /retailstream/web_logs 2>/dev/null || true
if ! docker exec hdfs-namenode hdfs dfs -test -e /retailstream/web_logs/web_logs_sample.jsonl 2>/dev/null; then
  docker cp ../00_shared_data/sample/web_logs_sample.jsonl hdfs-namenode:/tmp/web_logs_sample.jsonl
  docker exec hdfs-namenode hdfs dfs -put -f /tmp/web_logs_sample.jsonl /retailstream/web_logs/web_logs_sample.jsonl
fi
docker cp mapper.py hdfs-namenode:/tmp/mapper.py
docker cp reducer.py hdfs-namenode:/tmp/reducer.py

echo "[5/6] Chay job Hadoop Streaming tren YARN that..."
docker exec hdfs-namenode hdfs dfs -test -e /retailstream/output_yarn 2>/dev/null && \
  docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/output_yarn
docker exec hdfs-namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D mapreduce.job.name=retailstream-product-view-count-yarn \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' \
  -input /retailstream/web_logs/web_logs_sample.jsonl \
  -output /retailstream/output_yarn

echo "[6/6] Ket qua (dem luot xem theo product_id):"
docker exec hdfs-namenode hdfs dfs -cat /retailstream/output_yarn/part-00000

echo ""
echo "Xem trang YARN ResourceManager UI: http://localhost:${RESOURCEMANAGER_HTTP_PORT:-8088}/cluster"
