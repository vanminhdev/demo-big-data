"""
bai_tap_1_top_customers.py
Bài tập thực hành 1 - PySpark DataFrame: Phân tích Top khách hàng

Yêu cầu:
1. Đọc dữ liệu `orders_sample.csv` với Schema tường minh (Explicit Schema).
2. Lọc bỏ các đơn hàng có giá trị âm (dữ liệu lỗi).
3. Tính tổng chi tiêu của từng khách hàng (`total_spend`).
4. Xác định phương thức thanh toán có tổng chi tiêu lớn nhất của mỗi khách hàng
   (nếu hòa điểm chi tiêu, lấy phương thức có tên tăng dần theo bảng chữ cái).
5. In ra Top 5 khách hàng chi tiêu nhiều nhất (giảm dần theo `total_spend`).
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.window import Window

# Cấu hình môi trường (hoạt động tốt cả trong container Docker và máy cá nhân)
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "local[*]")
DATA_DIR = os.environ.get("PYSPARK_DATA_DIR", "/opt/spark-data/session10_pyspark")
if not os.path.exists(DATA_DIR):
    # Fallback khi chạy trực tiếp từ thư mục PySpark
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

ORDERS_PATH = os.path.join(DATA_DIR, "orders_sample.csv")

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_time", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("total_amount", DoubleType(), True),
    ]
)


def main():
    spark = (
        SparkSession.builder.appName("retailstream-bai-tap-1-top-customers")
        .master(SPARK_MASTER_URL)
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )

    print("\n" + "=" * 80)
    print(f"BÀI TẬP 1: TOP 5 KHÁCH HÀNG CHI TIÊU NHIỀU NHẤT")
    print(f"Nguồn dữ liệu: {ORDERS_PATH}")
    print("=" * 80)

    # 1. Đọc với Schema rõ ràng
    orders = (
        spark.read.schema(ORDERS_SCHEMA)
        .option("header", True)
        .csv(ORDERS_PATH)
    )

    # 2. Làm sạch: lọc đơn có giá trị hợp lệ (>= 0)
    orders_clean = orders.filter(F.col("total_amount") >= 0)

    # 3. Tính tổng chi tiêu mỗi khách hàng
    customer_total = orders_clean.groupBy("customer_id").agg(
        F.sum("total_amount").alias("total_spend")
    )

    # 4. Tính chi tiêu theo từng cặp (customer_id, payment_method)
    payment_total = orders_clean.groupBy("customer_id", "payment_method").agg(
        F.sum("total_amount").alias("payment_spend")
    )

    # Dùng Window Function xếp hạng phương thức thanh toán được dùng nhiều nhất
    w_payment = Window.partitionBy("customer_id").orderBy(
        F.desc("payment_spend"), F.asc("payment_method")
    )

    top_payment = (
        payment_total.withColumn("rank", F.row_number().over(w_payment))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    # 5. Join kết quả và sắp xếp giảm dần theo tổng chi tiêu, lấy Top 5
    result = (
        customer_total.join(top_payment, "customer_id", "inner")
        .orderBy(F.desc("total_spend"), F.asc("customer_id"))
        .select("customer_id", "total_spend", "payment_method")
        .limit(5)
    )

    print("\nKẾT QUẢ TOP 5 KHÁCH HÀNG:")
    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())

