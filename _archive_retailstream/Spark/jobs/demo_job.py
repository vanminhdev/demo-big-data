"""
Buổi 9 - Apache Spark - demo job chạy trên Spark Standalone cluster (1 master + 2 worker).

Mục tiêu minh họa:
  - driver / executor: job chạy phân tán trên 2 worker (không phải local[*]).
  - partition: repartition dữ liệu orders/order_items để có nhiều partition quan sát được.
  - transformation & action: filter/select/groupBy (wide) so với join (wide, có shuffle).
  - job/stage/task: mỗi action tạo 1 Spark job, groupBy/join tạo ranh giới stage (shuffle boundary).

Dữ liệu: RetailStream mức sample (00_shared_data/sample), mount vào container tại /opt/spark-data.
  - orders_sample.csv       (151 dòng kể cả header -> 150 bản ghi)
  - order_items_sample.csv
  - web_logs_sample.jsonl   (200 dòng)

Cách chạy (từ trong container spark-master, xem README.md):

  /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.sql.shuffle.partitions=6 \
      /opt/spark-apps/demo_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/opt/spark-data"
OUTPUT_DIR = "/opt/spark-data/output"


def main():
    spark = (
        SparkSession.builder
        .appName("RetailStream-Buoi9-DemoJob")
        .getOrCreate()
    )
    sc = spark.sparkContext

    print("=" * 70)
    print(f"Spark master (deploy) : {sc.master}")
    print(f"Application ID        : {sc.applicationId}")
    print(f"Default parallelism   : {sc.defaultParallelism}")
    print("=" * 70)

    # ---- 1. Đọc dữ liệu RetailStream (orders + order_items) ----
    orders = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{DATA_DIR}/orders_sample.csv")
    )
    order_items = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{DATA_DIR}/order_items_sample.csv")
    )

    # Ép số partition ban đầu để dễ quan sát trên Spark UI (narrow transformation: repartition
    # theo round-robin không theo key -> thực chất vẫn là 1 shuffle vì repartition không có cột
    # là round-robin partitioning, dùng để minh họa số partition điều khiển được).
    orders = orders.repartition(4)
    order_items = order_items.repartition(4)

    print(f"orders partitions      : {orders.rdd.getNumPartitions()}")
    print(f"order_items partitions : {order_items.rdd.getNumPartitions()}")

    # ---- 2. Wide transformation: join theo order_id (có shuffle) ----
    joined = orders.join(order_items, on="order_id", how="inner")

    # ---- 3. Wide transformation: groupBy status để tính doanh thu (có shuffle) ----
    revenue_by_status = (
        joined
        .withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
        .groupBy("status")
        .agg(
            F.sum("line_amount").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.count("*").alias("line_item_count"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    print("---- Doanh thu theo status (action: collect) ----")
    rows = revenue_by_status.collect()  # ACTION 1 -> tạo Spark job
    for r in rows:
        print(r)

    # Lưu kết quả xuống thư mục output (mount ra ngoài host) để làm evidence.
    (
        revenue_by_status
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{OUTPUT_DIR}/revenue_by_status_csv")
    )

    # ---- 4. Xử lý web_logs (JSON Lines) - đếm request theo status_code ----
    web_logs = spark.read.json(f"{DATA_DIR}/web_logs_sample.jsonl")
    web_logs = web_logs.repartition(4)

    status_code_counts = (
        web_logs.groupBy("status_code")
        .count()
        .orderBy(F.desc("count"))
    )

    print("---- Số request theo status_code (action: show) ----")
    status_code_counts.show(20, truncate=False)  # ACTION 2 -> tạo Spark job

    (
        status_code_counts
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{OUTPUT_DIR}/web_logs_status_code_csv")
    )

    # In physical plan để minh họa DAG / shuffle boundary (dùng cho slide/lab).
    print("---- Physical plan: revenue_by_status (join + groupBy) ----")
    revenue_by_status.explain(mode="formatted")

    print("Job hoàn tất. Application ID:", sc.applicationId)
    print("Xem chi tiết stage/task tại Spark UI: http://localhost:8080 (master)"
          " va http://localhost:4040 (application, khi job dang chay).")

    spark.stop()


if __name__ == "__main__":
    main()
