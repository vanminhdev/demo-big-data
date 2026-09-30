#!/usr/bin/env bash
# Tao du lieu nen thoi gian (90 phut) + chia 19 file batch 5 phut. Chi can
# chay 1 lan (hoac lai khi muon tao lai voi anchor thoi gian moi).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[1/2] Nen truc thoi gian clickstream_sample.jsonl -> 90 phut..."
python compress_timeline.py

echo "[2/2] Chia thanh 19 file cua so 5 phut (batches_staging_v2/)..."
python prepare_batches_v2.py

echo "Da xong. Xem batches_staging_v2/manifest.json de biet chi tiet (ban ghi muon o dau)."
