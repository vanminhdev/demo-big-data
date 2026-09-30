"""
Buổi 12 – Spark MLlib: dự đoán khách hủy chuyến CityRide.

Nhãn: label = 1 nếu status = rider_cancelled, ngược lại 0.
Đặc trưng lúc khách vừa đặt xe (chưa biết kết quả chuyến):
  eta_min, surge, is_raining, distance_km, giờ đặt xe, vehicle_type, pickup_zone

Các mục in ra:
  [1] tỷ lệ nhãn: dữ liệu mất cân bằng
  [2] mô hình "rò rỉ": dùng fare_vnd (chỉ có khi chuyến hoàn thành)
  [3] pipeline đúng: StringIndexer → OneHotEncoder → VectorAssembler → LogisticRegression
  [4] đánh giá: AUC, ma trận nhầm lẫn, precision, recall ở 2 ngưỡng
  [5] hệ số của mô hình
  [6] lưu PipelineModel, nạp lại, dự đoán 3 yêu cầu mới
"""

import os
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# xác suất lớp 1 (hủy) trong vector probability
p_huy = F.udf(lambda v: float(v[1]), T.DoubleType())

DATA = os.environ.get("CITYRIDE_DATA", "/opt/spark-data/cityride/lab")
OUT = os.environ.get("CITYRIDE_OUT", "/opt/spark-data/output/b12")


def section(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70, flush=True)


def confusion(pred, threshold):
    p = pred.withColumn("p1", p_huy("probability")) \
            .withColumn("du_doan", (F.col("p1") >= threshold).cast("int"))
    c = {(r["label"], r["du_doan"]): r["count"]
         for r in p.groupBy("label", "du_doan").count().collect()}
    tp, fn = c.get((1, 1), 0), c.get((1, 0), 0)
    fp, tn = c.get((0, 1), 0), c.get((0, 0), 0)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    acc = (tp + tn) / (tp + tn + fp + fn)
    print(f"  nguong {threshold}: TP={tp} FN={fn} FP={fp} TN={tn} | "
          f"accuracy={acc:.3f} precision={prec:.3f} recall={rec:.3f}")


def main():
    spark = SparkSession.builder.appName("CityRide-B12-MLlib").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("master =", spark.sparkContext.master)

    trips = spark.read.csv(f"{DATA}/trips.csv", header=True, inferSchema=True)
    df = (trips
          .withColumn("label", (F.col("status") == "rider_cancelled").cast("int"))
          .withColumn("hour", F.hour("request_time"))
          .select("trip_id", "label", "eta_min", "surge", "is_raining", "distance_km",
                  "hour", "vehicle_type", "pickup_zone", "fare_vnd"))

    section("[1] Ty le nhan")
    total = df.count()
    for r in df.groupBy("label").count().orderBy("label").collect():
        print(f"  label={r['label']}: {r['count']} ({r['count'] / total:.1%})")

    train, test = df.randomSplit([0.8, 0.2], seed=42)
    train.cache()
    print(f"  train={train.count()}  test={test.count()}")

    auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

    section("[2] Mo hinh 'ro ri': dung fare_vnd lam dac trung")
    leak = (train.withColumn("co_cuoc", F.col("fare_vnd").isNotNull().cast("int")))
    leak_model = Pipeline(stages=[
        VectorAssembler(inputCols=["co_cuoc", "eta_min"], outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label")]).fit(leak)
    leak_pred = leak_model.transform(test.withColumn("co_cuoc", F.col("fare_vnd").isNotNull().cast("int")))
    print(f"  AUC tren test = {auc_eval.evaluate(leak_pred):.4f}  (qua dep: fare_vnd chi co khi chuyen da hoan thanh)")

    section("[3] Pipeline dung: chi dung thong tin luc khach vua dat xe")
    cats = ["vehicle_type", "pickup_zone"]
    nums = ["eta_min", "surge", "is_raining", "distance_km", "hour"]
    pipeline = Pipeline(stages=[
        StringIndexer(inputCols=cats, outputCols=[c + "_idx" for c in cats]),
        OneHotEncoder(inputCols=[c + "_idx" for c in cats], outputCols=[c + "_vec" for c in cats]),
        VectorAssembler(inputCols=nums + [c + "_vec" for c in cats], outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=50),
    ])
    t0 = time.time()
    model = pipeline.fit(train)
    print(f"  fit: {time.time() - t0:.1f} s")
    pred = model.transform(test)
    pred.select("trip_id", "eta_min", "surge", "features", "probability", "prediction").show(3, truncate=60)

    section("[4] Danh gia tren tap test")
    print(f"  AUC = {auc_eval.evaluate(pred):.4f}")
    print(f"  mo hinh 'luon doan khong huy': accuracy = {1 - pred.agg(F.avg('label')).first()[0]:.3f}")
    for th in (0.5, 0.3):
        confusion(pred, th)

    section("[5] He so cua mo hinh (dac trung so)")
    lr = model.stages[-1]
    for name, w in zip(nums, lr.coefficients.toArray()[:len(nums)]):
        print(f"  {name:12s} {w:+.3f}")
    print(f"  intercept    {lr.intercept:+.3f}")

    section("[6] Luu, nap lai va du doan yeu cau moi")
    model.write().overwrite().save(f"{OUT}/cancel_model")
    loaded = PipelineModel.load(f"{OUT}/cancel_model")
    new = spark.createDataFrame([
        ("N1", 3, 1.0, 0, 4.0, 10, "bike", "Q09"),
        ("N2", 9, 1.5, 1, 6.0, 18, "car", "Q01"),
        ("N3", 14, 1.5, 1, 8.0, 18, "bike", "Q01"),
    ], ["trip_id", "eta_min", "surge", "is_raining", "distance_km", "hour", "vehicle_type", "pickup_zone"])
    (loaded.transform(new)
     .select("trip_id", "eta_min", "surge", "is_raining",
             F.round(p_huy("probability"), 3).alias("xac_suat_huy"))
     .show())
    spark.stop()


if __name__ == "__main__":
    main()
