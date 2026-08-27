"""
Buoi 15 - Optimization: SMALL FILES PROBLEM DEMO
Dung lai cluster Spark Standalone co san, khong dung ha tang moi.

Muc dich (demo giao duc, KHONG phai production benchmark):
Ghi CUNG mot tap du lieu orders (lab, 50.000 dong, tu 00_shared_data/lab/orders.csv, khong sua
noi dung) thanh 2 bien the:
  - MANY : df.repartition(50).write.csv(...)  -> ky vong ~50 file nho.
  - FEW  : df.coalesce(2).write.csv(...)      -> ky vong 2 file lon.
Sau do doc lai CA HAI (buoc count() ep Spark quet toan bo) va so sanh so luong task cua stage
doc + thoi gian stage qua Spark Application REST API (:4040) - nhieu file nho thuong tao nhieu
task nho hon (1 task/file dang co ban voi CSV), tang overhead lap lich so voi it file lon.

Job chay voi spark.eventLog.enabled=true, so lieu that duoc lay lai tu event log SAU KHI job
ket thuc bang parse_event_log.py (khong scrape REST API :4040 dang song vi de race-condition).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.appName("RetailStream-Buoi15-SmallFilesDemo").getOrCreate()
sc = spark.sparkContext
print("APP_ID:", sc.applicationId)

schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_time", StringType()),
        StructField("status", StringType()),
        StructField("payment_method", StringType()),
        StructField("total_amount", StringType()),
    ]
)

src = "/opt/spark-data/session15_optimization/orders_lab.csv"
many_path = "/opt/spark-data/session15_optimization/output/orders_many_files"
few_path = "/opt/spark-data/session15_optimization/output/orders_few_files"

df = spark.read.schema(schema).option("header", "true").csv(src)
input_rows = df.count()
print("INPUT_ROWS:", input_rows)

print("--- WRITE MANY (repartition 50) ---")
df.repartition(50).write.mode("overwrite").option("header", "true").csv(many_path)

print("--- WRITE FEW (coalesce 2) ---")
df.coalesce(2).write.mode("overwrite").option("header", "true").csv(few_path)

print("--- READ MANY ---")
df_many = spark.read.schema(schema).option("header", "true").csv(many_path)
c1 = df_many.count()
print("MANY_FILES read count:", c1)

print("--- READ FEW ---")
df_few = spark.read.schema(schema).option("header", "true").csv(few_path)
c2 = df_few.count()
print("FEW_FILES read count:", c2)

assert c1 == input_rows == c2, "row count mismatch giua cac bien the!"

spark.stop()
print("DONE")
