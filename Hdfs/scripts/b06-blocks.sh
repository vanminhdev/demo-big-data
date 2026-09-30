#!/usr/bin/env bash
# Buổi 6: quan sát một tệp được chia thành nhiều block và bản sao trên 2 DataNode.
# Dùng web_logs bản lab (~20 MB) với block size giáo dục 4 MB (mặc định HDFS là 128 MB;
# với 128 MB tệp này chỉ có 1 block). Block size đặt riêng cho tệp khi upload, không đổi cấu hình cụm.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
SRC=../00_shared_data/lab/web_logs.jsonl
DST=/retailstream/web_logs_lab/web_logs.jsonl

docker compose up -d namenode datanode1 datanode2 >/dev/null
for i in $(seq 1 40); do
  n=$(docker exec hdfs-namenode hdfs dfsadmin -report 2>/dev/null | grep -c "^Name:" || true)
  [ "$n" -ge 2 ] && break; sleep 3
done
docker exec hdfs-namenode hdfs dfsadmin -safemode wait >/dev/null   # chờ NameNode rời safe mode

echo "== 1. Upload $(du -h "$SRC" | cut -f1) với block size 4 MB, replication 2"
docker cp "$SRC" hdfs-namenode:/tmp/web_logs.jsonl
docker exec hdfs-namenode hdfs dfs -mkdir -p /retailstream/web_logs_lab
docker exec hdfs-namenode hdfs dfs -D dfs.blocksize=4194304 -put -f /tmp/web_logs.jsonl "$DST" 2>&1 | grep -v SaslDataTransfer || true
docker exec hdfs-namenode hdfs dfs -ls /retailstream/web_logs_lab

echo "== 2. fsck: danh sách block và vị trí bản sao"
docker exec hdfs-namenode hdfs fsck "$DST" -files -blocks -locations 2>/dev/null \
  | grep -E "blk_|Total blocks|Average block replication|Status|Number of data-nodes" \
  | sed -E 's/DatanodeInfoWithStorage\[([0-9.]+):[0-9]+,[^]]*\]/\1/g'

echo "== 3. Dung lượng mỗi DataNode đang dùng"
docker exec hdfs-namenode hdfs dfsadmin -report 2>/dev/null | grep -E "^Name:|^DFS Used:"
