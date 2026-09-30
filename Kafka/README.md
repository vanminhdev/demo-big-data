# Buổi 13 – Apache Kafka: sự kiện chuyến xe CityRide

Kafka 3.7.1 (KRaft, 1 broker) trong Docker, gắn vào mạng của cụm Spark. Producer và consumer Python chạy trên máy host (`pip install kafka-python`), kết nối `localhost:9092`.

```bash
bash scripts/reset-b13.sh     # dừng Kafka, làm trống Kafka/data, khởi động lại, chạy b13_kafka_demo.py
```

**Không xóa topic bằng lệnh khi Kafka đang chạy.** Trên Windows, thư mục dữ liệu mount vào container không đổi tên được, nên broker sẽ tự tắt ("log dir is offline"). `reset-b13.sh` làm trống `Kafka/data` (dữ liệu runtime, đã có trong `.gitignore`) khi broker đã dừng.

## `b13_kafka_demo.py`: 5 thí nghiệm (`evidence/b13_run.log`)

| Thí nghiệm | Quan sát | Kết quả |
|---|---|---|
| [1] key = `trip_id`, topic 4 partition | 3 sự kiện của một chuyến cùng partition, offset tăng dần | T0001: P0 offset 3, 11, 24 |
| [2] group `pricing`, 2 consumer | chia partition | A: P2, P3 (30 sự kiện); B: P0, P1 (60 sự kiện) |
| [3] group `analytics` | đọc độc lập, đủ 90; thứ tự từng chuyến đúng | `True` |
| [4] commit offset rồi chạy lại (group `notify`) | lag trước lần 2: 80; lần 2 đọc đúng 80; lag sau: 0 | |
| [5] key = `zone_id` | partition nóng | P0 0, P1 59, P2 100, P3 241; key = `trip_id`: 105, 93, 116, 86 |

## Kafka → Spark Structured Streaming

`b13_spark_kafka.py` đọc topic `ride-events` bằng `readStream.format("kafka")`, đếm theo `event_type` (40 / 40 / 40). Cần package `spark-sql-kafka-0-10_2.12:3.5.9`, đã có trong cache `Spark/data/ivy2cache`:

```bash
cp b13_spark_kafka.py ../Spark/jobs/
MSYS_NO_PATHCONV=1 docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --conf spark.jars.ivy=/opt/spark-data/ivy2cache \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  --executor-memory 512m --total-executor-cores 2 /opt/spark-apps/b13_spark_kafka.py
```

Demo RetailStream cũ (producer, consumer, spark_kafka_job) nằm trong `_archive_retailstream/Kafka/`.
