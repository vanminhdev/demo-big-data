#!/usr/bin/env bash
# Start the HDFS lab cluster (1 NameNode + 2 DataNode) and wait for health.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "[start-cluster] .env not found, copying from .env.example"
  cp .env.example .env
fi

docker compose up -d

echo "[start-cluster] waiting for containers to report healthy..."
for i in $(seq 1 30); do
  unhealthy=$(docker compose ps --format '{{.Name}} {{.Health}}' | grep -v healthy || true)
  if [ -z "$unhealthy" ]; then
    echo "[start-cluster] all containers healthy."
    docker compose ps
    echo ""
    echo "NameNode Web UI: http://localhost:${NAMENODE_HTTP_PORT:-9870}"
    exit 0
  fi
  sleep 5
done

echo "[start-cluster] WARNING: some containers did not become healthy in time." >&2
docker compose ps
exit 1
