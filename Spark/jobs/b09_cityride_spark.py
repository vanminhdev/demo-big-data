"""
Buổi 9 – Apache Spark: demo trên cụm Standalone (1 master + 2 worker).

Bối cảnh: CityRide, ứng dụng gọi xe tại Hà Nội. Bảng trips.csv có 300.000
chuyến (07–09/2026).

Mỗi mục in ra một hiện tượng cần quan sát:
  [1] Dữ liệu được chia thành partition; mỗi partition là một task.
  [2] Transformation chỉ ghi vào kế hoạch; action mới chạy job.
  [3] Bước tại chỗ (lọc, thêm cột) và bước cần trao đổi dữ liệu (gom nhóm):
      gom nhóm tách job thành 2 stage.
  [4] Cache: dùng lại dữ liệu đã đọc cho nhiều câu hỏi.
  [5] Cùng một bài toán viết bằng RDD và bằng DataFrame.

Chạy: bash scripts/run-b09.sh   (xem README.md)
"""

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA = "/opt/spark-data/cityride/lab"
HOLD_UI_SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 0


def timed(label, fn):
    t0 = time.time()
    result = fn()
    print(f"    {label}: {time.time() - t0:.2f} s")
    return result


def main():
    spark = SparkSession.builder.appName("CityRide-B09").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print("=" * 64)
    print(f"master = {sc.master} | app = {sc.applicationId}")
    print(f"tong so loi CPU duoc cap = {sc.defaultParallelism}")
    print("=" * 64)

    # [1] Partition
    # Tệp 25 MB, mỗi partition tối đa 4 MB -> Spark tự chia khi đọc
    trips = spark.read.csv(f"{DATA}/trips.csv", header=True, inferSchema=False)
    print("[1] So partition cua bang trips:", trips.rdd.getNumPartitions())

    # [2] Lazy evaluation
    print("[2] Khai bao 3 transformation (chua chay):")
    completed = timed("filter + withColumn + select",
                      lambda: trips.filter(F.col("status") == "completed")
                      .withColumn("fare", F.col("fare_vnd").cast("long"))
                      .select("trip_id", "pickup_zone", "fare"))
    sc.setJobDescription("B09 - dem chuyen hoan thanh (count)")
    n = timed("action count()", lambda: completed.count())
    print(f"    so chuyen hoan thanh = {n}")

    # [3] Bước tại chỗ và bước trao đổi dữ liệu
    sc.setJobDescription("B09 - doanh thu theo quan (groupBy)")
    by_zone = (completed.groupBy("pickup_zone")
               .agg(F.count("*").alias("so_chuyen"), F.sum("fare").alias("doanh_thu")))
    rows = sorted(by_zone.collect(), key=lambda r: -r["doanh_thu"])  # 12 dòng: sắp xếp ở Driver
    print("[3] Doanh thu theo quan don (gom nhom -> can trao doi du lieu):")
    for r in rows:
        print(f"    {r['pickup_zone']}  {r['so_chuyen']:>6} chuyen  {r['doanh_thu']:>14,} d")

    # [4] Cache
    sc.setJobDescription("B09 - khong cache")
    print("[4] Hai cau hoi tren cung du lieu, KHONG cache:")
    timed("cau hoi 1 (dem chuyen xe may)",
          lambda: trips.filter(F.col("vehicle_type") == "bike").count())
    timed("cau hoi 2 (dem chuyen o to)",
          lambda: trips.filter(F.col("vehicle_type") == "car").count())
    trips.cache()
    sc.setJobDescription("B09 - co cache")
    timed("nap cache (lan dau van phai doc tep)", lambda: trips.count())
    print("    Sau khi cache:")
    timed("cau hoi 1 (dem chuyen xe may)",
          lambda: trips.filter(F.col("vehicle_type") == "bike").count())
    timed("cau hoi 2 (dem chuyen o to)",
          lambda: trips.filter(F.col("vehicle_type") == "car").count())

    # [5] RDD và DataFrame cho cùng bài toán: số chuyến theo quận đón
    sc.setJobDescription("B09 - RDD reduceByKey")
    rdd_result = (trips.rdd
                  .map(lambda row: (row["pickup_zone"], 1))
                  .reduceByKey(lambda a, b: a + b)
                  .collect())
    sc.setJobDescription("B09 - DataFrame groupBy")
    df_result = trips.groupBy("pickup_zone").count().collect()
    same = sorted(rdd_result) == sorted((r["pickup_zone"], r["count"]) for r in df_result)
    print("[5] RDD va DataFrame cho cung ket qua:", same)
    print("    3 quan nhieu chuyen nhat:", sorted(rdd_result, key=lambda x: -x[1])[:3])

    if HOLD_UI_SECONDS:
        print(f"Giu Spark UI tai http://localhost:4040 trong {HOLD_UI_SECONDS} s ...")
        time.sleep(HOLD_UI_SECONDS)
    spark.stop()


if __name__ == "__main__":
    main()
