#!/usr/bin/env bash
# Stop the HDFS lab cluster but KEEP volumes (metadata/blocks persist).
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose down
echo "[stop-cluster] containers stopped, volumes kept. Use reset-lab.sh to wipe data."
