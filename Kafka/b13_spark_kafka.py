"""
Buổi 13: Spark Structured Streaming đọc topic ride-events từ Kafka.

Đếm số sự kiện theo loại (ride_requested / driver_accepted / trip_completed).
Kafka giữ vị trí đọc (offset) trong checkpoint của Spark, nên chạy lại chỉ
đọc phần mới.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.appName("CityRide-B13-Kafka").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
schema = T.StructType([T.StructField("trip_id", T.StringType()),
                       T.StructField("zone_id", T.StringType()),
                       T.StructField("event_type", T.StringType())])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "ride-events")
       .option("startingOffsets", "earliest")
       .load())                                   # cột key, value (nhị phân), partition, offset, timestamp

events = (raw.select(F.col("partition"), F.col("offset"),
                     F.from_json(F.col("value").cast("string"), schema).alias("e"))
             .select("partition", "offset", "e.*"))

counts = events.groupBy("event_type").count()

q = (counts.writeStream.outputMode("complete").format("console")
     .option("checkpointLocation", "/opt/spark-data/b13/checkpoint")
     .trigger(availableNow=True).start())
q.awaitTermination()
spark.stop()
