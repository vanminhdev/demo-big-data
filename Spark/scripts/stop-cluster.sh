#!/usr/bin/env bash
# Dung cum Spark. CANH BAO: Kafka (Buoi 13) gan vao network cua cum nay
# (spark_spark-net) - dung cum Kafka truoc neu dang chay, hoac dung Spark se
# lam Kafka mat ket noi network (khong tu dong dung Kafka o day de tranh mat
# du lieu ngoai y muon).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

if docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
  echo "CANH BAO: container 'kafka' dang chay va dung network cua Spark." >&2
  echo "Nen 'cd ../Kafka && docker compose down' truoc khi dung Spark." >&2
  read -r -p "Van tiep tuc dung Spark? (y/N) " ans
  [ "$ans" = "y" ] || { echo "Huy."; exit 1; }
fi

docker compose down
