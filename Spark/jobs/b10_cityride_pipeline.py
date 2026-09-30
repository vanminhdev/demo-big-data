"""
Buổi 10 – PySpark DataFrame: pipeline phân tích CityRide.

  đọc (schema) → làm sạch → ghép bảng → tổng hợp → ghi Parquet theo tháng

Mỗi mục [n] in ra một điều cần quan sát trên lớp. Chạy được ở hai chế độ,
logic không đổi, chỉ khác địa chỉ master:
  bash scripts/run-local.sh     (local[*] trong container)
  bash scripts/run-cluster.sh   (spark://spark-master:7077, 2 worker)
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DATA = os.environ.get("CITYRIDE_DATA", "/opt/spark-data/cityride/lab")
OUT = os.environ.get("CITYRIDE_OUT", "/opt/spark-data/output/b10")

TRIP_SCHEMA = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("request_time", T.TimestampType()),
    T.StructField("pickup_zone", T.StringType()),
    T.StructField("dropoff_zone", T.StringType()),
    T.StructField("driver_id", T.StringType()),
    T.StructField("vehicle_type", T.StringType()),
    T.StructField("distance_km", T.DoubleType()),
    T.StructField("duration_min", T.IntegerType()),
    T.StructField("eta_min", T.IntegerType()),
    T.StructField("surge", T.DoubleType()),
    T.StructField("is_raining", T.IntegerType()),
    T.StructField("fare_vnd", T.LongType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("status", T.StringType()),
])


def section(title):
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


def dir_size_mb(path):
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total / 1e6


def main():
    spark = SparkSession.builder.appName("CityRide-B10").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("master =", spark.sparkContext.master)

    # [1] Schema: tự đoán và khai báo tường minh
    section("[1] Doc CSV: tu doan kieu va khai bao schema")
    t0 = time.time()
    guessed = spark.read.csv(f"{DATA}/trips.csv", header=True, inferSchema=True)
    t_infer = time.time() - t0
    t0 = time.time()
    trips = spark.read.csv(f"{DATA}/trips.csv", header=True, schema=TRIP_SCHEMA)
    t_schema = time.time() - t0
    print(f"inferSchema=True : {t_infer:.2f} s (phai quet du lieu de doan kieu)")
    print(f"schema khai bao  : {t_schema:.2f} s (chua doc du lieu)")
    print("kieu tu doan     :", [(f.name, f.dataType.simpleString()) for f in guessed.schema.fields][:6])
    trips.printSchema()

    drivers = spark.read.csv(f"{DATA}/drivers.csv", header=True, inferSchema=True)
    zones = spark.read.csv(f"{DATA}/zones.csv", header=True, inferSchema=True)
    payments = spark.read.csv(f"{DATA}/trip_payments.csv", header=True, inferSchema=True)

    # [2] Làm sạch
    section("[2] Kiem tra chat luong du lieu")
    completed = trips.filter(F.col("status") == "completed")
    print("tong so chuyen            :", trips.count())
    print("chuyen hoan thanh         :", completed.count())
    print("hoan thanh, fare_vnd = 0  :", completed.filter(F.col("fare_vnd") == 0).count())
    print("hoan thanh, payment rong  :", completed.filter(F.col("payment_method").isNull()).count())
    clean = (completed
             .filter(F.col("fare_vnd") > 0)
             .withColumn("payment_method", F.coalesce("payment_method", F.lit("unknown")))
             .withColumn("month", F.date_format("request_time", "yyyy-MM")))
    print("sau lam sach              :", clean.count())

    # [3] Ghép bảng: inner join làm mất dòng
    section("[3] Ghep voi bang tai xe: inner va left")
    n_clean = clean.count()
    n_inner = clean.join(drivers, "driver_id", "inner").count()
    n_left = clean.join(drivers, "driver_id", "left").count()
    print(f"truoc khi ghep : {n_clean}")
    print(f"inner join     : {n_inner}  (mat {n_clean - n_inner} chuyen co driver_id la)")
    print(f"left join      : {n_left}")

    # [4] Ghép bảng: một chuyến nhiều dòng thanh toán làm nhân dòng
    section("[4] Ghep voi bang thanh toan: nhan dong")
    right_total = clean.agg(F.sum("fare_vnd")).first()[0]
    joined = clean.join(payments, "trip_id", "inner")
    wrong_total = joined.agg(F.sum("fare_vnd")).first()[0]
    correct_total = joined.agg(F.sum("amount_vnd")).first()[0]
    print(f"so dong sau ghep            : {joined.count()} (truoc: {n_clean})")
    print(f"tong fare_vnd truoc ghep    : {right_total:,}")
    print(f"tong fare_vnd SAU ghep (sai): {wrong_total:,}")
    print(f"tong amount_vnd sau ghep    : {correct_total:,}")
    ex = joined.filter(F.col("payment_method") == "split").select(
        "trip_id", "fare_vnd", "method", "amount_vnd").limit(2)
    ex.show(truncate=False)

    # [5] Tổng hợp + ghép bảng nhỏ (zones): Spark gửi bảng nhỏ tới mọi máy
    section("[5] Doanh thu theo thang va quan (ghep bang zones 12 dong)")
    report = (clean.groupBy("month", "pickup_zone")
              .agg(F.count("*").alias("so_chuyen"),
                   F.sum("fare_vnd").alias("doanh_thu"))
              .join(zones, clean.pickup_zone == zones.zone_id)
              .select("month", "zone_name", "so_chuyen", "doanh_thu")
              .orderBy("month", F.desc("doanh_thu")))
    report.show(6, truncate=False)
    report.explain()

    # [6] Spark SQL: cùng câu hỏi, cùng kế hoạch
    section("[6] Spark SQL")
    clean.createOrReplaceTempView("trips_clean")
    zones.createOrReplaceTempView("zones")
    sql_report = spark.sql("""
        SELECT t.month, z.zone_name, COUNT(*) AS so_chuyen, SUM(t.fare_vnd) AS doanh_thu
        FROM trips_clean t JOIN zones z ON t.pickup_zone = z.zone_id
        GROUP BY t.month, z.zone_name
        ORDER BY t.month, doanh_thu DESC
    """)
    sql_report.show(3, truncate=False)
    print("SQL va DataFrame cung ket qua:",
          sorted(sql_report.collect()) == sorted(report.collect()))

    # [7] Parquet chia thư mục theo tháng
    section("[7] Ghi Parquet chia thu muc theo thang")
    (clean.write.mode("overwrite").partitionBy("month").parquet(f"{OUT}/trips_parquet"))
    print("thu muc:", sorted(os.listdir(f"{OUT}/trips_parquet")))
    print(f"CSV goc (moi trang thai) : {os.path.getsize(f'{DATA}/trips.csv') / 1e6:.1f} MB")
    print(f"Parquet (da lam sach)    : {dir_size_mb(f'{OUT}/trips_parquet'):.1f} MB")
    clean.write.mode("overwrite").csv(f"{OUT}/trips_clean_csv", header=True)
    print(f"CSV (cung du lieu sach)  : {dir_size_mb(f'{OUT}/trips_clean_csv'):.1f} MB")

    pq = spark.read.parquet(f"{OUT}/trips_parquet")
    aug = pq.filter(F.col("month") == "2026-08").select("pickup_zone", "fare_vnd")
    print("chuyen thang 8:", aug.count())
    aug.explain()

    # [8] Python UDF và hàm có sẵn
    section("[8] Python UDF va ham co san")

    @F.udf(T.StringType())
    def fare_band_udf(v):
        return "cao" if v >= 100000 else ("vua" if v >= 40000 else "thap")

    builtin = F.when(F.col("fare_vnd") >= 100000, "cao").when(F.col("fare_vnd") >= 40000, "vua").otherwise("thap")
    for name, expr in (("Python UDF", fare_band_udf("fare_vnd")), ("ham co san", builtin)):
        t0 = time.time()
        res = pq.withColumn("band", expr).groupBy("band").count().collect()
        print(f"{name:12s}: {time.time() - t0:.2f} s  {sorted((r['band'], r['count']) for r in res)}")

    spark.stop()


if __name__ == "__main__":
    main()
