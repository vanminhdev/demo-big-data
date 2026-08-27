#!/usr/bin/env bash
# Khoi dong cum Spark Standalone (1 master + 2 worker), dung chung cho
# Buoi 9/10/11/12/13/15.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

docker compose up -d

echo "Cho spark-master healthy..."
for i in $(seq 1 20); do
  status=$(docker inspect --format='{{.State.Health.Status}}' spark-master 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then
    echo "  spark-master healthy."
    break
  fi
  sleep 5
  if [ "$i" -eq 20 ]; then
    echo "  LOI: spark-master khong healthy sau 100s." >&2
    exit 1
  fi
done

docker compose ps
echo ""
echo "Spark Master UI: http://localhost:8080"
