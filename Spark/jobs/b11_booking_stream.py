"""
Buổi 11 – Structured Streaming: đếm yêu cầu đặt xe theo quận, cửa sổ 5 phút.

  nguồn: thư mục JSON (mỗi tệp mới là một đợt dữ liệu)
  truy vấn: watermark 10 phút + groupBy(window 5 phút, zone_id).count()
  đích: console

Tham số:
  argv[1] output mode: update | append | complete   (mặc định update)
  argv[2] thư mục checkpoint (mặc định theo output mode)

Trigger availableNow + maxFilesPerTrigger=1: mỗi tệp đang có là một
micro-batch, xử lý hết thì dừng. Chạy lại với cùng checkpoint sẽ tiếp tục
từ tệp chưa xử lý.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BASE = "/opt/spark-data/b11"
MODE = sys.argv[1] if len(sys.argv) > 1 else "update"
CHECKPOINT = sys.argv[2] if len(sys.argv) > 2 else f"{BASE}/checkpoint_{MODE}"

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("zone_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
])

spark = (SparkSession.builder.appName(f"CityRide-B11-{MODE}")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

events = (spark.readStream.schema(SCHEMA)
          .option("maxFilesPerTrigger", 1)
          .json(f"{BASE}/incoming"))

counts = (events
          .withWatermark("event_time", "10 minutes")
          .groupBy(F.window("event_time", "5 minutes"), "zone_id")
          .count()
          .select(F.date_format("window.start", "HH:mm").alias("tu"),
                  F.date_format("window.end", "HH:mm").alias("den"),
                  "zone_id", F.col("count").alias("so_yeu_cau")))

query = (counts.writeStream
         .outputMode(MODE)
         .format("console")
         .option("truncate", False).option("numRows", 50)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)
         .start())
query.awaitTermination()

print(f"\n==== Tom tat cac micro-batch (output mode = {MODE}) ====")
print("batch | dong vao | watermark sau batch | so dong trang thai | bi bo vi muon")
for p in query.recentProgress:
    st = p["stateOperators"][0] if p["stateOperators"] else {}
    print(f"{p['batchId']:>5} | {p['numInputRows']:>8} | {p['eventTime'].get('watermark', '-'):>24} | "
          f"{st.get('numRowsTotal', '-'):>18} | {st.get('numRowsDroppedByWatermark', '-'):>13}")
spark.stop()
