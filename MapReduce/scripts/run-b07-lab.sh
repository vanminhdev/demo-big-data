#!/usr/bin/env bash
# Buổi 7: chạy job đếm lượt xem theo product_id trên web_logs bản lab (5 block HDFS),
# YARN có 2 NodeManager -> 5 map task chạy song song trên 2 node, 2 reducer.
# Cần chạy trước: bash ../Hdfs/scripts/b06-blocks.sh (đưa tệp 5 block lên HDFS).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
IN=/retailstream/web_logs_lab/web_logs.jsonl
OUT=/retailstream/output_b07_lab

(cd ../Hdfs && docker compose up -d >/dev/null)
for nm in hdfs-nodemanager1 hdfs-nodemanager2; do
  for i in $(seq 1 40); do
    [ "$(docker inspect --format='{{.State.Health.Status}}' $nm 2>/dev/null)" = healthy ] && break; sleep 5
  done
done
docker exec hdfs-namenode hdfs dfsadmin -safemode wait >/dev/null
echo "== NodeManager đang hoạt động:"
docker exec hdfs-resourcemanager yarn node -list 2>/dev/null | grep -E "RUNNING|Total"

docker cp mapper.py hdfs-namenode:/tmp/mapper.py
docker cp reducer.py hdfs-namenode:/tmp/reducer.py
docker exec hdfs-namenode hdfs dfs -rm -r -f "$OUT" >/dev/null 2>&1 || true

echo "== Chạy job (2 reducer)"
docker exec hdfs-namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D mapreduce.job.name=b07-product-views-lab -D mapreduce.job.reduces=2 \
  -D yarn.app.mapreduce.am.resource.mb=512 \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' \
  -input "$IN" -output "$OUT" 2>&1 \
  | grep -E "Running job|number of splits|map [0-9]+% reduce|completed successfully|Launched map|Launched reduce|Data-local|Rack-local|Map input records|Map output records|Reduce input groups|Reduce output records|Shuffle"

echo "== Tệp kết quả"
docker exec hdfs-namenode hdfs dfs -ls "$OUT" | grep part
echo "== 5 sản phẩm nhiều lượt xem nhất"
docker exec hdfs-namenode hdfs dfs -cat "$OUT/part-*" | sort -t$'\t' -k2 -nr | head -5

echo "== Mỗi task chạy trên node nào (đọc tệp lịch sử job .jhist trên HDFS)"
sleep 3
JH=$(docker exec hdfs-namenode hdfs dfs -ls /tmp/hadoop-yarn/staging/history/done_intermediate/root 2>/dev/null      | grep "b07%2Dproduct%2Dviews%2Dlab.*jhist" | sort -k6,7 | tail -1 | awk '{print $NF}')
docker exec hdfs-namenode mapred job -history all "$JH" 2>/dev/null | grep -E "^attempt_"   | awk '{n=split($1,p,"_"); host=($7 ~ /^nodemanager/)?$7:$(NF-1); print p[n-2]"_"p[n-1], $6, host}'   | sed 's/^m_/map    /; s/^r_/reduce /'
