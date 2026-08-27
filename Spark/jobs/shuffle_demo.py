"""
Buoi 15 - Optimization: SHUFFLE LON DEMO
Dung lai cluster Spark Standalone co san, khong dung ha tang moi.

Muc dich (demo giao duc, KHONG phai production benchmark):
Join orders (lab, 50.000 dong) voi order_items (lab, 124.863 dong) theo order_id, 2 cach:
  - UNOPT: join toan bo 2 bang, TAT auto-broadcast (spark.sql.autoBroadcastJoinThreshold=-1) de
           ep Spark dung sort-merge join that su co shuffle ca 2 phia (o quy mo lab nay, mac
           dinh Spark se tu dong broadcast bang orders vi no du nho hon nguong 10MB - phai tat
           di de minh hoa dung "shuffle lon" nhu se xay ra voi bang lon hon trong thuc te).
  - OPT  : loc orders chi giu status="PAID" TRUOC khi join (giam khoi luong 1 phia), giu nguyen
           auto-broadcast mac dinh (10MB) => Spark tu dong broadcast bang orders_paid (da nho di
           sau khi loc) thay vi shuffle ca 2 phia.
QUAN TRONG: phai spark-submit voi --conf spark.sql.adaptive.enabled=false de AQE khong tu dong
gop nho stage/partition lai o quy mo demo nay, giup so sanh so stage/task de doc hon. Day la
lua chon co chu dich cho demo, KHONG phai khuyen nghi production.

So sanh shuffle read/write bytes that cua 2 stage tuong ung, lay tu Spark event log (chay voi
spark.eventLog.enabled=true) bang parse_event_log.py SAU KHI job ket thuc - khong scrape REST
API :4040 dang song vi de race-condition (khong co Spark History Server trong du an nay).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("RetailStream-Buoi15-ShuffleDemo").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")
sc = spark.sparkContext
print("APP_ID:", sc.applicationId)

orders_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_time", StringType()),
        StructField("status", StringType()),
        StructField("payment_method", StringType()),
        StructField("total_amount", DoubleType()),
    ]
)
items_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
    ]
)

orders = spark.read.schema(orders_schema).option("header", "true").csv(
    "/opt/spark-data/session15_optimization/orders_lab.csv"
)
items = spark.read.schema(items_schema).option("header", "true").csv(
    "/opt/spark-data/session15_optimization/order_items_lab.csv"
)
print("orders rows:", orders.count(), "order_items rows:", items.count())

print("--- UNOPT: full join, auto-broadcast TAT ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
unopt = orders.join(items, "order_id")
unopt_count = unopt.count()
print("UNOPT join result rows:", unopt_count)

print("--- OPT: loc orders status=PAID truoc, auto-broadcast BAT (mac dinh 10MB) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
orders_paid = orders.filter(F.col("status") == "PAID")
paid_count = orders_paid.count()
print("orders_paid rows:", paid_count)
opt = items.join(orders_paid, "order_id")
opt_count = opt.count()
print("OPT join result rows:", opt_count)

spark.stop()
print("DONE")
