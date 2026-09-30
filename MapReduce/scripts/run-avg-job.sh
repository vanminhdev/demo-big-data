#!/usr/bin/env bash
# Chay job MapReduce tinh thoi gian phan hoi trung binh theo status_code tren YARN.
# Yeu cau: cum Hdfs da chay va web_logs da nam tren HDFS (scripts/run-yarn-job.sh lam san viec nay).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

docker cp mapper_avg.py hdfs-namenode:/tmp/mapper_avg.py
docker cp reducer_avg.py hdfs-namenode:/tmp/reducer_avg.py
docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/output_avg >/dev/null 2>&1 || true
docker exec hdfs-namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D mapreduce.job.name=retailstream-avg-response-time \
  -D mapreduce.job.reduces=2 \
  -files /tmp/mapper_avg.py,/tmp/reducer_avg.py \
  -mapper 'python3 mapper_avg.py' -reducer 'python3 reducer_avg.py' \
  -input /retailstream/web_logs/web_logs_sample.jsonl \
  -output /retailstream/output_avg 2>&1 | grep -E "map [0-9]+%|completed|Output" || true
echo "--- ket qua (status_code, so luong, tong ms, trung binh ms) ---"
docker exec hdfs-namenode hdfs dfs -cat '/retailstream/output_avg/part-*' | sort
