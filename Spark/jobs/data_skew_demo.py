"""
Buoi 15 - Optimization: DATA SKEW DEMO
Dung lai cluster Spark Standalone co san (Spark/docker-compose.yml), khong dung ha tang moi.

Muc dich (demo giao duc, KHONG phai production benchmark):
So sanh df.repartition(8, "product_id") tren 2 bien the cua CUNG mot tap clickstream (lab,
100.000 dong, lay tu 00_shared_data/lab/clickstream.jsonl, KHONG sua noi dung goc):
  - UNIFORM: file clickstream_uniform.jsonl = ban sao nguyen ban 00_shared_data/lab/clickstream.jsonl
             (phan phoi product_id gan nhu deu, xac nhan truoc: max 1 product_id chiem ~0.04%).
  - SKEWED : file clickstream_skewed.jsonl = ban bien doi CO CHU DICH rieng cho buoi nay
             (Optimization/data/clickstream_skewed.jsonl, sinh boi script Python rieng, seed=42):
             ~90% ban ghi bi gan lai product_id = "PROD00001", 10% con lai giu nguyen phan phoi goc.

LUU Y KY THUAT quan trong: ban dau demo dung groupBy("product_id").count() truc tiep, nhung
Spark tu dong lam "map-side partial aggregate" (combiner) truoc khi shuffle cho phep tinh
count, nen du lieu THAT SU di qua shuffle chi la (key, partial_count) rat nho va GAN NHU DEU
theo so luong key phan biet moi partition dau vao - khong con the hien duoc do lech ban ghi
tho. Vi vay demo nay dung df.repartition(8, "product_id") - hash-partition lai TOAN BO ban ghi
tho theo product_id (KHONG co combiner) - moi ban ghi deu di qua shuffle nguyen ven, nen 1
product_id chiem 90% se khien 1 trong 8 partition dau ra nhan ~90% tong so ban ghi, quan sat
duoc truc tiep qua shuffle_read_bytes/duration cua tung task trong stage shuffle-read.

QUAN TRONG: phai spark-submit voi --conf spark.sql.adaptive.enabled=false. Spark 3.5 mac dinh
bat AQE (Adaptive Query Execution), tinh nang CoalescePartitions cua AQE se tu dong gop cac
partition nho lai (o quy mo demo nay du lieu qua nho) khien stage phia sau shuffle chi con
1 task duy nhat - lam MAT DAU HIEU skew can quan sat (khong con nhieu task de so sanh). Tat AQE
la lua chon co chu dich cho demo nay de giu du 8 partition/task nhu yeu cau, KHONG phai cau
hinh khuyen nghi cho production (production thuong NEN bat AQE vi no giup skew join tu dong
trong nhieu truong hop, kha nang nay khac voi repartition() thu cong dung trong demo nay).

Job chay voi spark.eventLog.enabled=true (truyen qua spark-submit --conf), ghi event log JSON
vao /opt/spark-data/session15_optimization/event_logs/ - script rieng (parse_event_log.py,
chay tren host) doc lai file nay SAU KHI job da ket thuc de lay so lieu task that su, khong
phu thuoc vao viec REST API :4040 con song hay khong (khong co Spark History Server trong du
an nay, va REST API chi song trong luc driver dang chay nen de bi race-condition khi scrape
tu ben ngoai).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.appName("RetailStream-Buoi15-DataSkewDemo").getOrCreate()
sc = spark.sparkContext
print("APP_ID:", sc.applicationId)

schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", StringType()),
        StructField("customer_id", StringType()),
        StructField("session_id", StringType()),
        StructField("product_id", StringType()),
        StructField("event_type", StringType()),
    ]
)


def run(label, path):
    print(f"--- RUN {label}: {path} ---")
    df = spark.read.schema(schema).json(path)
    total = df.count()
    print(f"[{label}] total_rows={total}")

    top5 = df.groupBy("product_id").count().orderBy(F.desc("count")).limit(5).collect()
    print(f"[{label}] top5 product_id theo so ban ghi:")
    for r in top5:
        print(f"   {r['product_id']}: {r['count']}")

    # Hash-repartition THAT SU theo product_id, khong co map-side combiner -> moi ban ghi
    # tho deu di qua shuffle nguyen ven, boc lo do lech ro rang o phia reduce.
    repartitioned = df.repartition(8, "product_id")
    per_partition = (
        repartitioned.withColumn("pid", F.spark_partition_id())
        .groupBy("pid")
        .count()
        .orderBy("pid")
        .collect()
    )
    counts = [r["count"] for r in per_partition]
    print(f"[{label}] so ban ghi tren tung partition sau repartition(8, product_id): {counts}")
    if counts:
        print(f"[{label}] max/min partition ratio: {max(counts) / max(min(counts), 1):.2f}x")


run("UNIFORM", "/opt/spark-data/session15_optimization/clickstream_uniform.jsonl")
run("SKEWED", "/opt/spark-data/session15_optimization/clickstream_skewed.jsonl")

spark.stop()
print("DONE")
