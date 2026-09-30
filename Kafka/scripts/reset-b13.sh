#!/usr/bin/env bash
# Buổi 13: đưa Kafka về trạng thái trống rồi chạy demo CityRide.
# Không xóa topic bằng lệnh khi Kafka đang chạy: trên Windows, thư mục dữ liệu
# mount vào container không đổi tên được nên broker sẽ tự tắt. Thay vào đó
# dừng broker, làm trống Kafka/data (dữ liệu runtime, đã .gitignore), khởi động lại.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
(cd ../Spark && docker compose up -d >/dev/null)     # Kafka dùng chung mạng với Spark
docker compose down
rm -rf data && mkdir data
docker compose up -d
for i in $(seq 1 40); do
  [ "$(docker inspect --format='{{.State.Health.Status}}' kafka)" = healthy ] && break; sleep 3
done
python b13_kafka_demo.py
