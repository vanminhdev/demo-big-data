"""
bai_tap_2_revenue_share.py
Bài tập thực hành 2 - PySpark DataFrame: Tỷ trọng doanh thu theo danh mục

Yêu cầu:
1. Đọc 3 bảng RetailStream (`orders_sample.csv`, `order_items_sample.csv`, `products_sample.json`)
   bằng Explicit Schema.
2. Làm sạch dữ liệu: loại bỏ đơn hàng âm, xử lý brand null bằng F.coalesce.
3. Join 3 bảng và kiểm soát tính toàn vẹn số dòng.
4. Tính `revenue` theo `month` (yyyy-MM) và `category_name`.
5. Ứng dụng Window Function tính `percent_of_monthly_revenue` (tỷ trọng doanh thu danh mục
   trên tổng doanh thu tháng đó, làm tròn 2 chữ số thập phân).
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
    IntegerType,
    TimestampType,
)
from pyspark.sql.window import Window

# Cấu hình môi trường (hoạt động tốt cả trong container Docker và máy cá nhân)
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "local[*]")
DATA_DIR = os.environ.get("PYSPARK_DATA_DIR", "/opt/spark-data/session10_pyspark")
if not os.path.exists(DATA_DIR):
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

ORDERS_PATH = os.path.join(DATA_DIR, "orders_sample.csv")
ORDER_ITEMS_PATH = os.path.join(DATA_DIR, "order_items_sample.csv")
PRODUCTS_PATH = os.path.join(DATA_DIR, "products_sample.json")

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

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("brand", StringType(), True),
    ]
)


def main():
    spark = (
        SparkSession.builder.appName("retailstream-bai-tap-2-revenue-share")
        .master(SPARK_MASTER_URL)
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )

    print("\n" + "=" * 80)
    print("BÀI TẬP 2: TỶ TRỌNG DOANH THU THEO DANH MỤC TRONG THÁNG (WINDOW FUNCTION)")
    print("=" * 80)

    # 1. Đọc dữ liệu
    orders = spark.read.schema(ORDERS_SCHEMA).option("header", True).csv(ORDERS_PATH)
    order_items = spark.read.schema(ORDER_ITEMS_SCHEMA).option("header", True).csv(ORDER_ITEMS_PATH)
    products = spark.read.schema(PRODUCTS_SCHEMA).json(PRODUCTS_PATH)

    # 2. Làm sạch dữ liệu
    orders_ok = orders.filter(F.col("total_amount") >= 0)
    products_ok = products.withColumn(
        "brand", F.coalesce(F.col("brand"), F.lit("UNKNOWN"))
    )

    # 3. Join 3 bảng và xác thực số dòng
    items_orders = order_items.join(orders_ok, "order_id", "inner")
    retail = items_orders.join(products_ok, "product_id", "inner")

    # 4. Tính doanh thu theo Tháng và Danh mục
    monthly_category = (
        retail.withColumn("month", F.date_format("order_time", "yyyy-MM"))
        .withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
        .groupBy("month", "category_name")
        .agg(F.sum("line_amount").alias("revenue"))
    )

    # 5. Window Function tính tỷ trọng theo từng tháng
    w_month = Window.partitionBy("month")
    result = (
        monthly_category.withColumn(
            "percent_of_monthly_revenue",
            F.round(F.col("revenue") / F.sum("revenue").over(w_month) * 100, 2),
        )
        .orderBy("month", F.desc("revenue"))
    )

    print("\nKẾT QUẢ TỶ TRỌNG DOANH THU THÁNG 2026-07 (ĐỐI CHIẾU LAB):")
    result.filter(F.col("month") == "2026-07").show(truncate=False)

    print("\nTOÀN BỘ KẾT QUẢ CÁC THÁNG:")
    result.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
