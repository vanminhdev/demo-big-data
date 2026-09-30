"""
streaming_job.py
Buoi 11 - Spark Structured Streaming (RetailStream / clickstream)

Doc su kien clickstream qua FILE SOURCE cua Structured Streaming (khong dung
Kafka - Kafka duoc ghep o Buoi 13), voi EXPLICIT SCHEMA (khong inferSchema),
tong hop so su kien theo event_type trong tumbling window 1 ngay tren
event_time, co watermark 1 ngay de xu ly du lieu den muon, ghi ket qua ra
console sink, va bat checkpoint de co the dung/khoi dong lai ma khong xu ly
lai tu dau.

Toan bo cau hinh nam o khoi CONFIG duoi day (bien moi truong), KHONG sua
logic xu ly khi doi tham so.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# =====================================================================
# CONFIG TAP TRUNG
# =====================================================================
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "local[*]")
APP_NAME = "retailstream-session11-structured-streaming"

# Duong dan BEN TRONG container Spark (mount tu Spark/data qua /opt/spark-data)
SOURCE_DIR = os.environ.get(
    "STREAM_SOURCE_DIR", "/opt/spark-data/session11_streaming/data_source"
)
CHECKPOINT_DIR = os.environ.get(
    "STREAM_CHECKPOINT_DIR",
    "/opt/spark-data/session11_streaming/checkpoint/update_mode",
)
OUTPUT_MODE = os.environ.get("STREAM_OUTPUT_MODE", "update")  # complete|append|update
# Da doi tu "1 day" xuong "5 minutes" theo yeu cau giang vien (BRIEF Buoi 11
# goi y window "vai phut" - dung nguyen ban); phai dung dong thoi voi du lieu
# NEN THOI GIAN o data_source_v2/ (xem compress_timeline.py) vi du lieu goc
# clickstream_sample.jsonl trai 15 ngay, khong hop voi window phut.
WINDOW_DURATION = os.environ.get("STREAM_WINDOW_DURATION", "5 minutes")
WATERMARK_DELAY = os.environ.get("STREAM_WATERMARK_DELAY", "3 minutes")
MAX_FILES_PER_TRIGGER = os.environ.get("STREAM_MAX_FILES_PER_TRIGGER", "1")
# =====================================================================
# HET CONFIG
# =====================================================================

# Schema tuong minh theo 00_DATA_CONTRACT.md muc "clickstream" - KHONG dung
# spark.readStream...option("inferSchema", true).
CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), True),
        StructField("customer_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_type", StringType(), True),
    ]
)


def log(title):
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def main():
    log("V01 - CONFIG + SCHEMA TUONG MINH (khong inferSchema)")
    print("SPARK_MASTER_URL      =", SPARK_MASTER_URL)
    print("SOURCE_DIR            =", SOURCE_DIR)
    print("CHECKPOINT_DIR        =", CHECKPOINT_DIR)
    print("OUTPUT_MODE           =", OUTPUT_MODE)
    print("WINDOW_DURATION       =", WINDOW_DURATION)
    print("WATERMARK_DELAY       =", WATERMARK_DELAY)
    print("MAX_FILES_PER_TRIGGER =", MAX_FILES_PER_TRIGGER)

    spark = (
        SparkSession.builder.appName(APP_NAME).master(SPARK_MASTER_URL).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("spark.master (thuc te) =", spark.sparkContext.master)
    print("applicationId          =", spark.sparkContext.applicationId)

    CLICKSTREAM_SCHEMA.jsonValue()  # buoc su dung, tranh nham voi schema suy dien
    print("CLICKSTREAM_SCHEMA (tuong minh, dinh nghia trong code):")
    print(CLICKSTREAM_SCHEMA.simpleString())

    stream_df = (
        spark.readStream.format("json")
        .schema(CLICKSTREAM_SCHEMA)  # explicit schema - KHONG inferSchema
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(SOURCE_DIR)
    )

    log("V02 - WINDOW AGGREGATION: dem so su kien theo event_type / tumbling window")
    log("V03 - WATERMARK: cho phep du lieu den muon toi da " + WATERMARK_DELAY)
    grouped = (
        stream_df.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION).alias("window"),
            F.col("event_type"),
        )
        .count()
    )

    if OUTPUT_MODE == "complete":
        # orderBy chi duoc phep khi outputMode = complete (can toan bo state
        # de sap xep - khong dung duoc voi append/update).
        grouped = grouped.orderBy("window", "event_type")

    log("V04 - CHECKPOINT: writeStream voi checkpointLocation = " + CHECKPOINT_DIR)
    log("Trigger.availableNow=True: xu ly het du lieu dang co trong SOURCE_DIR roi TU DUNG")
    query = (
        grouped.writeStream.format("console")
        .outputMode(OUTPUT_MODE)
        .option("truncate", "false")
        .option("numRows", 300)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    log("Query da dung (Trigger.availableNow het viec). Progress cac micro-batch:")
    for p in query.recentProgress:
        batch_id = p.get("batchId")
        num_input = p.get("numInputRows")
        sources = p.get("sources", [])
        state_ops = p.get("stateOperators", [])
        dropped = None
        if state_ops:
            dropped = state_ops[0].get("numRowsDroppedByWatermark")
        print(
            f"batchId={batch_id} numInputRows={num_input} "
            f"numRowsDroppedByWatermark={dropped} "
            f"sourceDesc={[s.get('description') for s in sources]}"
        )

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
