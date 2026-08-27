"""
process_retailstream.py
Buoi 10 - Lap trinh du lieu lon voi PySpark (RetailStream)

Doc orders + order_items + products (muc sample, dung Data Contract chung),
lam sach du lieu loi/thieu co chu y, join dung 3 bang, tinh doanh thu theo
thang va danh muc, ghi Parquet co partition theo thang.

Chi doi khoi CONFIG ben duoi de chuyen doi giua local[*] va Spark Standalone
cluster (spark://spark-master:7077) da dung o Spark/docker-compose.yml.
KHONG sua logic xu ly ben duoi khi doi che do.
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

# =====================================================================
# CONFIG TAP TRUNG - DAY LA NOI DUY NHAT CAN SUA DE DOI CHE DO CHAY
# =====================================================================
# Giai doan 1: "local[*]"                       -> chay tren 1 may, khong can cluster
# Giai doan 2: "spark://spark-master:7077"       -> chay that tren Spark Standalone
#              cluster da dung o Spark/docker-compose.yml (1 master + 2 worker)
#
# Co the override bang bien moi truong SPARK_MASTER_URL khi goi spark-submit,
# khong bat buoc sua code:
#   docker exec -e SPARK_MASTER_URL=spark://spark-master:7077 spark-master \
#       /opt/spark/bin/spark-submit ... process_retailstream.py
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "local[*]")

APP_NAME = "retailstream-session10-pyspark"

# Thu muc du lieu dau vao / dau ra. Mac dinh la duong dan BEN TRONG container
# Spark (mount tu Spark/data qua /opt/spark-data, xem README.md cua PySpark/).
# Khi chay local[*] tren may ca nhan khong qua Docker, doi 2 bien nay sang
# duong dan tuong doi toi PySpark/data va PySpark/output.
DATA_DIR = os.environ.get("PYSPARK_DATA_DIR", "/opt/spark-data/session10_pyspark")
OUTPUT_DIR = os.environ.get(
    "PYSPARK_OUTPUT_DIR", os.path.join(DATA_DIR, "output")
)

SHUFFLE_PARTITIONS = int(os.environ.get("PYSPARK_SHUFFLE_PARTITIONS", "4"))
# =====================================================================
# HET CONFIG - TU DAY TRO XUONG LA LOGIC XU LY, KHONG DOI KHI DOI CHE DO
# =====================================================================

ORDERS_PATH = os.path.join(DATA_DIR, "orders_sample.csv")
ORDER_ITEMS_PATH = os.path.join(DATA_DIR, "order_items_sample.csv")
PRODUCTS_PATH = os.path.join(DATA_DIR, "products_sample.json")
REVENUE_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "revenue_by_month_category")


def log(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def build_spark_session():
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    )
    return builder.getOrCreate()


# ---------------------------------------------------------------------
# Explicit schema (KHONG dung schema inference) - dung 00_DATA_CONTRACT.md
# ---------------------------------------------------------------------

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

PRODUCT_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("color", StringType(), True),
        StructField("warranty_months", IntegerType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("category_id", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("attributes", PRODUCT_ATTRIBUTES_SCHEMA, True),
        StructField("updated_at", TimestampType(), True),
    ]
)


def read_sources(spark):
    log("V01 - Doc 3 nguon du lieu voi EXPLICIT SCHEMA (khong inferSchema)")

    orders_df = (
        spark.read.schema(ORDERS_SCHEMA)
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")
        .csv(ORDERS_PATH)
    )
    order_items_df = (
        spark.read.schema(ORDER_ITEMS_SCHEMA)
        .option("header", "true")
        .csv(ORDER_ITEMS_PATH)
    )
    products_df = (
        spark.read.schema(PRODUCTS_SCHEMA)
        .option("multiLine", "true")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")
        .json(PRODUCTS_PATH)
    )

    print("-- orders schema (explicit) --")
    orders_df.printSchema()
    print("orders count:", orders_df.count())

    print("-- order_items schema (explicit) --")
    order_items_df.printSchema()
    print("order_items count:", order_items_df.count())

    print("-- products schema (explicit) --")
    products_df.printSchema()
    print("products count:", products_df.count())

    return orders_df, order_items_df, products_df


def clean_data(orders_df, order_items_df, products_df):
    log("Lam sach du lieu loi/thieu co chu y (xem 00_shared_data/README.md)")

    bad_orders = orders_df.filter(F.col("total_amount") < 0)
    bad_orders_count = bad_orders.count()
    print("So don hang co total_amount < 0 (loi co chu y):", bad_orders_count)
    bad_orders.show(truncate=False)

    orders_clean_df = orders_df.filter(F.col("total_amount") >= 0)
    print(
        "orders truoc lam sach:",
        orders_df.count(),
        "-> sau lam sach:",
        orders_clean_df.count(),
    )

    null_brand_count = products_df.filter(F.col("brand").isNull()).count()
    print("So san pham co brand = null (loi co chu y):", null_brand_count)

    products_clean_df = products_df.withColumn(
        "brand", F.coalesce(F.col("brand"), F.lit("UNKNOWN"))
    )
    print(
        "products voi brand = null sau khi lam sach (phai = 0):",
        products_clean_df.filter(F.col("brand").isNull()).count(),
    )

    return orders_clean_df, order_items_df, products_clean_df


def join_tables(orders_df, order_items_df, products_df):
    log("V02 - Join 3 bang va kiem tra khong nhan ban dong (row count)")

    order_items_count = order_items_df.count()
    print("order_items (dau vao join):", order_items_count)

    joined_orders = order_items_df.join(orders_df, on="order_id", how="inner")
    joined_orders_count = joined_orders.count()
    print(
        "sau join order_items + orders (inner, mat cac dong cua don loi da bi loc):",
        joined_orders_count,
    )

    full_df = joined_orders.join(products_df, on="product_id", how="inner")
    full_df_count = full_df.count()
    print("sau join them products:", full_df_count)

    if full_df_count == joined_orders_count:
        print(
            "V02 PASS: so dong khong doi sau khi join products "
            "(khong bi nhan ban do join sai / product_id khong unique)."
        )
    else:
        print(
            "V02 FAIL: so dong thay doi sau join products -> nghi ngo join "
            "bi nhan ban, can kiem tra product_id trung."
        )

    return full_df


def compute_revenue(full_df):
    log("V03 - Tinh doanh thu theo thang va danh muc")

    enriched_df = full_df.withColumn(
        "line_amount", F.col("quantity") * F.col("unit_price")
    ).withColumn("month", F.date_format(F.col("order_time"), "yyyy-MM"))

    revenue_df = (
        enriched_df.groupBy("month", "category_name")
        .agg(
            F.sum("line_amount").alias("revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.count(F.lit(1)).alias("line_item_count"),
        )
        .orderBy("month", "category_name")
    )

    print("Doanh thu theo thang + danh muc:")
    revenue_df.show(50, truncate=False)

    total_revenue = revenue_df.agg(F.sum("revenue")).collect()[0][0]
    print("Tong doanh thu tat ca thang/danh muc:", total_revenue)

    return revenue_df


def explain_query(revenue_df):
    log("V05 - explain() cho truy van tong hop doanh thu")
    revenue_df.explain(mode="extended")


def write_parquet(revenue_df):
    log("V04 - Ghi Parquet co partition theo thang")
    (
        revenue_df.write.mode("overwrite")
        .partitionBy("month")
        .parquet(REVENUE_OUTPUT_PATH)
    )
    print("Da ghi Parquet vao:", REVENUE_OUTPUT_PATH)


def main():
    print("SPARK_MASTER_URL =", SPARK_MASTER_URL)
    print("DATA_DIR =", DATA_DIR)
    print("OUTPUT_DIR =", OUTPUT_DIR)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("spark.master (thuc te) =", spark.sparkContext.master)
    print("applicationId =", spark.sparkContext.applicationId)

    try:
        orders_df, order_items_df, products_df = read_sources(spark)
        orders_clean_df, order_items_clean_df, products_clean_df = clean_data(
            orders_df, order_items_df, products_df
        )
        full_df = join_tables(orders_clean_df, order_items_clean_df, products_clean_df)
        revenue_df = compute_revenue(full_df)
        explain_query(revenue_df)
        write_parquet(revenue_df)

        log("HOAN TAT - doc lai Parquet de xac nhan doi chieu")
        check_df = spark.read.parquet(REVENUE_OUTPUT_PATH)
        print("So dong doc lai tu Parquet:", check_df.count())
        check_df.orderBy("month", "category_name").show(50, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
