"""
Buổi 15 – Tối ưu pipeline Spark: đo trước, sửa một chỗ, đo lại.

Bốn thí nghiệm trên dữ liệu CityRide (chạy trên cụm 2 worker):
  [1] Dữ liệu lệch: 2 triệu chuyến, 80% ở Q01, ghép với bảng quận bằng shuffle
      → một task gánh gần hết dữ liệu. Sửa: gửi bảng nhỏ tới mọi máy (broadcast).
  [2] Nhiều tệp nhỏ: cùng dữ liệu ghi thành 400 tệp và 4 tệp; so thời gian đọc.
  [3] CSV và Parquet: đọc 2 cột để tính tổng doanh thu.
  [4] Số partition sau shuffle: 200 (mặc định) và 4 cho dữ liệu nhỏ.

Số liệu từng task lấy từ REST API của Spark UI (http://localhost:4040/api/v1).
"""

import json
import os
import statistics
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA = "/opt/spark-data/cityride/lab"
OUT = "/opt/spark-data/output/b15"
UI = "http://localhost:4040/api/v1"


def section(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70, flush=True)


def api(path):
    with urllib.request.urlopen(f"{UI}{path}") as r:
        return json.loads(r.read())


def job_stats(sc, group):
    """Tóm tắt các stage của những job có job group = group."""
    app = api("/applications")[0]["id"]
    jobs = [j for j in api(f"/applications/{app}/jobs") if j.get("jobGroup") == group]
    out = []
    for j in sorted(jobs, key=lambda j: j["jobId"]):
        for sid in sorted(j["stageIds"]):
            st = api(f"/applications/{app}/stages/{sid}")
            if not st or st[0]["status"] != "COMPLETE":
                continue
            st = st[0]
            tasks = api(f"/applications/{app}/stages/{sid}/0/taskList?length=10000")
            durs = [t["duration"] / 1000 for t in tasks if "duration" in t]
            recs = [t.get("taskMetrics", {}).get("shuffleReadMetrics", {}).get("recordsRead", 0) for t in tasks]
            out.append({"stage": sid, "tasks": len(tasks),
                        "max_s": max(durs) if durs else 0, "median_s": statistics.median(durs) if durs else 0,
                        "shuffle_write_mb": st["shuffleWriteBytes"] / 1e6,
                        "shuffle_read_mb": st["shuffleReadBytes"] / 1e6,
                        "max_records": max(recs) if recs else 0, "sum_records": sum(recs), "records": recs})
    return out


def run(sc, group, fn):
    sc.setJobGroup(group, group)
    t0 = time.time()
    res = fn()
    wall = time.time() - t0
    time.sleep(1)
    stats = job_stats(sc, group)
    print(f"  [{group}] thoi gian: {wall:.1f} s")
    for s in stats:
        print(f"    stage {s['stage']:>3}: {s['tasks']:>3} task | task lau nhat {s['max_s']:6.2f} s, "
              f"trung vi {s['median_s']:5.2f} s | shuffle ghi {s['shuffle_write_mb']:6.1f} MB, "
              f"doc {s['shuffle_read_mb']:6.1f} MB | task nhan nhieu dong nhat {s['max_records']}/{s['sum_records']}")
        if s["sum_records"] > 100000:
            print(f"      so dong moi task: {sorted(s['records'], reverse=True)}")
    return res, wall, stats


def dir_files(path):
    return sum(1 for f in os.listdir(path) if f.endswith(".parquet"))


def main():
    spark = SparkSession.builder.appName("CityRide-B15").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print("master =", sc.master)

    zones = spark.read.csv(f"{DATA}/zones.csv", header=True, inferSchema=True)

    # 2 triệu chuyến, 80% đón ở Q01 (ví dụ: đêm nhạc hội ở Hoàn Kiếm)
    n = 2_000_000
    skewed = (spark.range(n)
              .withColumn("r", F.rand(seed=15))
              .withColumn("pickup_zone",
                          F.when(F.col("r") < 0.8, F.lit("Q01"))
                           .otherwise(F.format_string("Q%02d", (F.floor(F.rand(seed=7) * 11) + 2).cast("int"))))
              .withColumn("fare_vnd", (F.rand(seed=3) * 100000 + 15000).cast("long"))
              .drop("r"))
    skewed.write.mode("overwrite").parquet(f"{OUT}/skewed_trips")
    skewed = spark.read.parquet(f"{OUT}/skewed_trips")

    section("[1] Du lieu lech: ghep 2 trieu chuyen (80% Q01) voi bang 12 quan")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)      # buộc ghép bằng shuffle
    spark.conf.set("spark.sql.shuffle.partitions", 12)
    run(sc, "skew-shuffle-join", lambda: skewed.join(zones, skewed.pickup_zone == zones.zone_id)
        .groupBy("zone_name").agg(F.sum("fare_vnd")).collect())
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    run(sc, "skew-broadcast-join", lambda: skewed.join(F.broadcast(zones), skewed.pickup_zone == zones.zone_id)
        .groupBy("zone_name").agg(F.sum("fare_vnd")).collect())

    section("[2] Nhieu tep nho")
    trips = spark.read.csv(f"{DATA}/trips.csv", header=True, inferSchema=True)
    trips.repartition(400).write.mode("overwrite").parquet(f"{OUT}/trips_400_files")
    trips.coalesce(4).write.mode("overwrite").parquet(f"{OUT}/trips_4_files")
    for name in ("trips_400_files", "trips_4_files"):
        path = f"{OUT}/{name}"
        print(f"  {name}: {dir_files(path)} tep")
        run(sc, name, lambda p=path: spark.read.parquet(p).groupBy("pickup_zone").count().collect())

    section("[3] CSV va Parquet: tong doanh thu theo quan (2 cot)")
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    run(sc, "read-csv", lambda: spark.read.csv(f"{DATA}/trips.csv", header=True, inferSchema=False)
        .groupBy("pickup_zone").agg(F.sum(F.col("fare_vnd").cast("long"))).collect())
    run(sc, "read-parquet", lambda: spark.read.parquet(f"{OUT}/trips_4_files")
        .groupBy("pickup_zone").agg(F.sum("fare_vnd")).collect())

    section("[4] So partition sau shuffle cho du lieu nho")
    for p in (200, 4):
        spark.conf.set("spark.sql.shuffle.partitions", p)
        run(sc, f"shuffle-{p}", lambda: spark.read.parquet(f"{OUT}/trips_4_files")
            .groupBy("driver_id").agg(F.sum("fare_vnd")).count())
    spark.stop()


if __name__ == "__main__":
    main()
