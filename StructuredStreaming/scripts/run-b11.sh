#!/usr/bin/env bash
# Buổi 11: chạy demo đếm yêu cầu đặt xe theo cửa sổ 5 phút.
#   bash scripts/run-b11.sh update     # hoặc append / complete
#   bash scripts/run-b11.sh restart    # checkpoint: chạy 3 đợt, dừng, chạy tiếp 3 đợt
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
MODE="${1:-update}"
DATA=../Spark/data/b11
mkdir -p ../Spark/jobs && cp b11_booking_stream.py ../Spark/jobs/
(cd ../Spark && docker compose up -d >/dev/null)
submit() { docker exec spark-master /opt/spark/bin/spark-submit --master "local[2]" \
             /opt/spark-apps/b11_booking_stream.py "$@"; }
rm -rf "$DATA"; mkdir -p "$DATA/incoming"
if [ "$MODE" = "restart" ]; then
  cp booking_batches/batch_0[1-3].jsonl "$DATA/incoming/"
  echo "######## LAN CHAY 1: 3 dot dau ########"; submit update /opt/spark-data/b11/ckpt_restart
  cp booking_batches/batch_0[4-6].jsonl "$DATA/incoming/"
  echo "######## LAN CHAY 2: cung checkpoint, them 3 dot ########"; submit update /opt/spark-data/b11/ckpt_restart
else
  cp booking_batches/*.jsonl "$DATA/incoming/"
  submit "$MODE"
fi
