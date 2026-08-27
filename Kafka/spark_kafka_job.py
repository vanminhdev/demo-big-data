"""
spark_kafka_job.py
Buoi 13 - Spark Structured Streaming doc truc tiep tu Kafka source (RetailStream / clickstream)

Khac voi Buoi 11 (File Source), job nay doc su kien clickstream TRUC TIEP tu
topic Kafka "clickstream" (Kafka/docker-compose.yml, 4 partition), giu
nguyen ky thuat window/watermark da dung o Buoi 11: tumbling window 1 ngay
tren event_time, watermark 1 ngay, dem so su kien theo event_type.

Chay tren cum Spark Standalone co san (Spark/docker-compose.yml), ket noi
Kafka qua hostname noi bo "kafka:29092" (Kafka duoc gan them vao network
spark_spark-net - xem Kafka/docker-compose.yml).

Toan bo cau hinh nam o khoi CONFIG duoi day (bien moi truong).
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# =====================================================================
# CONFIG TAP TRUNG
# =====================================================================
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
APP_NAME = "retailstream-session13-kafka-structured-streaming"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "clickstream")
STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")

CHECKPOINT_DIR = os.environ.get(
    "STREAM_CHECKPOINT_DIR",
    "/opt/spark-data/session13_kafka/checkpoint",
)
OUTPUT_MODE = os.environ.get("STREAM_OUTPUT_MODE", "update")  # complete|append|update
WINDOW_DURATION = os.environ.get("STREAM_WINDOW_DURATION", "1 day")
WATERMARK_DELAY = os.environ.get("STREAM_WATERMARK_DELAY", "1 day")
# Trigger.availableNow: xu ly het du lieu dang co trong topic roi TU DUNG -
# phu hop de doi chieu tong so message da doc voi so message producer da gui.
# =====================================================================
# HET CONFIG
# =====================================================================

# Schema tuong minh theo 00_DATA_CONTRACT.md muc "clickstream" - gia tri
# JSON nam trong truong "value" (bytes) cua Kafka record, phai tu parse.
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
    log("V05 - CONFIG (Spark Structured Streaming + Kafka source)")
    print("SPARK_MASTER_URL        =", SPARK_MASTER_URL)
    print("KAFKA_BOOTSTRAP_SERVERS =", KAFKA_BOOTSTRAP_SERVERS)
    print("KAFKA_TOPIC             =", KAFKA_TOPIC)
    print("STARTING_OFFSETS        =", STARTING_OFFSETS)
    print("CHECKPOINT_DIR          =", CHECKPOINT_DIR)
    print("OUTPUT_MODE             =", OUTPUT_MODE)
    print("WINDOW_DURATION         =", WINDOW_DURATION)
    print("WATERMARK_DELAY         =", WATERMARK_DELAY)

    spark = (
        SparkSession.builder.appName(APP_NAME).master(SPARK_MASTER_URL).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("spark.master (thuc te) =", spark.sparkContext.master)
    print("applicationId          =", spark.sparkContext.applicationId)

    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    # Cot goc cua Kafka source: key, value, topic, partition, offset,
    # timestamp, timestampType. Parse "value" (bytes JSON) theo schema
    # tuong minh clickstream, giu them partition/offset de doi chieu.
    parsed = kafka_raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), CLICKSTREAM_SCHEMA).alias("data"),
    ).select("kafka_key", "kafka_partition", "kafka_offset", "data.*")

    log("V05a - WINDOW AGGREGATION tu du lieu Kafka: dem so su kien theo event_type / tumbling window")
    grouped = (
        parsed.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION).alias("window"),
            F.col("event_type"),
        )
        .count()
    )

    if OUTPUT_MODE == "complete":
        grouped = grouped.orderBy("window", "event_type")

    log("V05b - CHECKPOINT: writeStream voi checkpointLocation = " + CHECKPOINT_DIR)
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
    total_input_rows = 0
    for p in query.recentProgress:
        batch_id = p.get("batchId")
        num_input = p.get("numInputRows") or 0
        total_input_rows += num_input
        sources = p.get("sources", [])
        state_ops = p.get("stateOperators", [])
        dropped = None
        if state_ops:
            dropped = state_ops[0].get("numRowsDroppedByWatermark")
        start_offset = sources[0].get("startOffset") if sources else None
        end_offset = sources[0].get("endOffset") if sources else None
        print(
            f"batchId={batch_id} numInputRows={num_input} "
            f"numRowsDroppedByWatermark={dropped} "
            f"startOffset={start_offset} endOffset={end_offset}"
        )

    log("V05c - DOI CHIEU TONG SO MESSAGE DA DOC TU KAFKA")
    print("TONG SO MESSAGE SPARK DA DOC TU KAFKA (tong numInputRows tat ca batch) =", total_input_rows)

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
