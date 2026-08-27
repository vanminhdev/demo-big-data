#!/usr/bin/env bash
# Reset topic clickstream/product_events ve trang thai sach (0 message).
#
# QUAN TRONG: KHONG dung "kafka-topics.sh --delete" - da kiem thu that va
# phat hien lam CRASH ca broker tren Windows bind-mount
# (java.nio.file.AccessDeniedException khi Kafka doi ten thu muc log de xoa,
# filesystem NTFS qua Docker Desktop khong ho tro atomic rename ma Kafka can).
# Cach an toan da kiem thu: dung han container, xoa thu muc du lieu tren HOST
# (./data), khoi dong lai container tu dau (log trong sach hoan toan).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

echo "[1/3] Dung Kafka..."
docker compose down

echo "[2/3] Xoa du lieu cu tren host (./data)..."
rm -rf ./data/*

echo "[3/3] Khoi dong lai Kafka tu dau va tao lai topic..."
bash scripts/start-cluster.sh

echo "Da reset. Topic 'clickstream' va 'product_events' hien co 0 message."
