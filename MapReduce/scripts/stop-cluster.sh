#!/usr/bin/env bash
# Dung cum HDFS+YARN dung chung boi Buoi 6 (HDFS) va Buoi 7 (MapReduce).
set -euo pipefail
cd "$(dirname "$0")/../../Hdfs"
export MSYS_NO_PATHCONV=1
docker compose down
