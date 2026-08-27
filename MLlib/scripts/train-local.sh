#!/usr/bin/env bash
# Huan luyen pipeline MLlib o che do local[*], ngay trong container spark-master.
# LUU Y: mem_limit cua spark-master da tang len 1536m (Spark/docker-compose.yml,
# 2026-08-22) - neu doi lai ve 512m se bi OOMKilled voi du lieu 50.000 dong.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

bash scripts/prepare-data.sh

RUN_TAG="${1:-local_mode_v2}"
echo "Chay local[*], RUN_TAG=$RUN_TAG..."
docker exec \
  -e MLLIB_RUN_TAG="$RUN_TAG" \
  -e PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
  spark-master /opt/spark/bin/spark-submit \
  /opt/spark-data/mllib_session12/train_pipeline.py

echo ""
echo "Ket qua: Spark/data/mllib_session12/output/$RUN_TAG/metrics.json"
docker exec spark-master cat "/opt/spark-data/mllib_session12/output/$RUN_TAG/metrics.json"
