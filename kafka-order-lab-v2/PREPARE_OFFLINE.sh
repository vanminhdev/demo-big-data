#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka Lab: prepare environment before class ==="
command -v docker >/dev/null || { echo "Docker not found."; exit 1; }

docker version
docker compose version

echo "Pulling fixed images..."
docker pull apache/kafka:4.3.1
docker pull provectuslabs/kafka-ui:v0.7.2

echo "Building the lab application image..."
docker compose build

echo "Starting once to verify the lab..."
docker compose up -d
sleep 10
docker compose ps

echo "Stopping containers..."
docker compose down

echo "Preparation finished. Images are cached locally."
echo "Do not run 'docker compose pull' at school without Internet."
