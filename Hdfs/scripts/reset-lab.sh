#!/usr/bin/env bash
# Full reset: stop the cluster AND remove HDFS data volumes (NameNode metadata +
# DataNode blocks). Does NOT touch anything outside this project's docker-compose
# scope (only volumes declared in Hdfs/docker-compose.yml are removed).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[reset-lab] this will DELETE all data stored inside the HDFS lab cluster."
docker compose down -v
echo "[reset-lab] volumes removed. Run start-cluster.sh to start from a clean state."
