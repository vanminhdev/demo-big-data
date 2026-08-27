"""
train_pipeline.py
Buoi 12 - Hoc may phan tan voi Spark MLlib (RetailStream)

Du doan nguy co don hang bi huy (status = CANCELLED) tu du lieu orders +
customers (muc "lab", 00_shared_data/lab/, xem ly do chon muc du lieu trong
MLlib/README.md va LOCAL_REPORT.md - sample chi co 150 don, qua nho/mat can
bang de train mo hinh co y nghia thong ke).

MOT file duy nhat dung chung cho 2 che do local[*] va Spark Standalone
cluster. Chi doi khoi CONFIG ben duoi (hoac cac bien moi truong tuong ung)
de doi che do - KHONG sua logic xu ly.
"""

import json
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Imputer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

# =====================================================================
# CONFIG TAP TRUNG - DAY LA NOI DUY NHAT CAN SUA DE DOI CHE DO CHAY
# =====================================================================
# Giai doan 1: "local[*]"                     -> chay tren 1 may, khong can cluster
# Giai doan 2: "spark://spark-master:7077"     -> chay that tren Spark Standalone
#              cluster da dung o Spark/docker-compose.yml (1 master + 2 worker)
#
# Co the doi bang bien moi truong SPARK_MASTER_URL khi goi spark-submit,
# khong bat buoc sua code truc tiep:
#   docker exec -e SPARK_MASTER_URL=spark://spark-master:7077 \
#       -e MLLIB_RUN_TAG=cluster_mode spark-master \
#       /opt/spark/bin/spark-submit ... train_pipeline.py
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "local[*]")

APP_NAME = "retailstream-session12-mllib"

# RUN_TAG tach output/model cua tung che do chay (local_mode / cluster_mode)
# de khong ghi de len nhau, phuc vu so sanh metric giua 2 giai doan.
RUN_TAG = os.environ.get("MLLIB_RUN_TAG", "local_mode")

# Thu muc du lieu / output / model. Mac dinh la duong dan BEN TRONG container
# Spark (mount tu Spark/data qua /opt/spark-data). Khi chay ngoai Docker, doi
# 3 bien nay sang duong dan tuong doi toi MLlib/data, MLlib/output, MLlib/models.
DATA_DIR = os.environ.get("MLLIB_DATA_DIR", "/opt/spark-data/mllib_session12")
OUTPUT_DIR = os.environ.get(
    "MLLIB_OUTPUT_DIR", os.path.join(DATA_DIR, "output", RUN_TAG)
)
MODEL_DIR = os.environ.get(
    "MLLIB_MODEL_DIR", os.path.join(DATA_DIR, "models", RUN_TAG)
)

SEED = int(os.environ.get("MLLIB_SEED", "42"))
TEST_FRACTION = float(os.environ.get("MLLIB_TEST_FRACTION", "0.2"))
SHUFFLE_PARTITIONS = int(os.environ.get("MLLIB_SHUFFLE_PARTITIONS", "8"))
# =====================================================================
# HET CONFIG - TU DAY TRO XUONG LA LOGIC XU LY, KHONG DOI KHI DOI CHE DO
# =====================================================================

ORDERS_PATH = os.path.join(DATA_DIR, "orders.csv")
CUSTOMERS_PATH = os.path.join(DATA_DIR, "customers.csv")
PIPELINE_MODEL_PATH = os.path.join(MODEL_DIR, "pipeline_model")
METRICS_PATH = os.path.join(OUTPUT_DIR, "metrics.json")


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

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_segment", StringType(), True),
        StructField("city", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def read_sources(spark):
    log("V01a - Doc orders + customers voi EXPLICIT SCHEMA (khong inferSchema)")

    orders_df = (
        spark.read.schema(ORDERS_SCHEMA)
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")
        .csv(ORDERS_PATH)
    )
    customers_df = (
        spark.read.schema(CUSTOMERS_SCHEMA)
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")
        .csv(CUSTOMERS_PATH)
    )

    print("orders count (raw):", orders_df.count())
    print("customers count (raw):", customers_df.count())

    return orders_df, customers_df


def clean_data(orders_df, customers_df):
    log("V01b - Xu ly missing value / ban ghi loi co chu y (xem manifest.json)")

    # orders.csv: ban ghi cuoi cung co total_amount = -1.0 (loi co chu y trong
    # generator, xem 00_shared_data/lab/manifest.json -> intentional_data_issues).
    # Coi day la missing value (khong phai gia tri hop le) thay vi xoa dong,
    # de Imputer (fit tren TRAIN) xu ly dung quy trinh tranh ro ri du lieu.
    n_negative = orders_df.filter(F.col("total_amount") < 0).count()
    print("So don co total_amount < 0 (coi la missing, se Impute sau):", n_negative)

    orders_df = orders_df.withColumn(
        "total_amount",
        F.when(F.col("total_amount") < 0, None).otherwise(F.col("total_amount")),
    )

    # customers.csv: 1 ban ghi trung customer_id (CUST00001, loi co chu y).
    # Khu trung xac dinh (deterministic) bang row_number() theo created_at,
    # giu ban ghi som nhat, tranh join bi nhan ban dong (data leakage kieu
    # "1 don hang bi dem 2 lan feature khac nhau").
    n_customers_raw = customers_df.count()
    dedup_window = Window.partitionBy("customer_id").orderBy(
        F.col("created_at").asc_nulls_last()
    )
    customers_clean_df = (
        customers_df.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    n_customers_clean = customers_clean_df.count()
    print(
        "customers truoc khu trung:", n_customers_raw,
        "-> sau khu trung customer_id:", n_customers_clean,
    )

    return orders_df, customers_clean_df


def build_feature_table(orders_df, customers_df):
    log("V01c - Tao bang feature + label (KHONG de status/derived label lot vao feature)")

    orders_labeled_df = orders_df.withColumn(
        "label", F.when(F.col("status") == "CANCELLED", F.lit(1.0)).otherwise(F.lit(0.0))
    ).withColumn(
        "order_hour", F.hour("order_time").cast(DoubleType())
    ).withColumn(
        "order_dow", F.dayofweek("order_time").cast(DoubleType())
    )

    # LEFT JOIN de giu lai cac don hang ma customer_id khong (con) khop sau
    # khu trung/loi du lieu; dien "UNKNOWN" cho categorical bi thieu thay vi
    # loai bo ban ghi.
    joined_df = orders_labeled_df.join(customers_df, on="customer_id", how="left")

    feature_df = (
        joined_df.select(
            "order_id",
            F.coalesce(F.col("customer_segment"), F.lit("UNKNOWN")).alias("customer_segment"),
            F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city"),
            F.coalesce(F.col("payment_method"), F.lit("UNKNOWN")).alias("payment_method"),
            "total_amount",
            "order_hour",
            "order_dow",
            "label",
        )
    )

    n_unmatched_customer = joined_df.filter(F.col("customer_segment").isNull()).count()
    print(
        "So don hang khong khop duoc customer (customer_segment=null truoc khi dien UNKNOWN):",
        n_unmatched_customer,
    )

    print("Phan bo label (0 = khong huy, 1 = CANCELLED):")
    feature_df.groupBy("label").count().orderBy("label").show()

    # Xac nhan KHONG con cot 'status' hoac cac cot dinh danh trong bang feature
    remaining_cols = set(feature_df.columns)
    forbidden_cols = {"status"}
    leaked = remaining_cols & forbidden_cols
    if leaked:
        raise RuntimeError(f"Phat hien cot bi cam lot vao feature table: {leaked}")
    print("Xac nhan: khong co cot 'status' trong bang feature ->", "OK" if not leaked else "FAIL")

    # orders.csv/customers.csv la file don le, kich thuoc nho (< khoi HDFS
    # mac dinh) nen Spark doc thanh DUY NHAT 1 partition. repartition() o day
    # de co nhieu task/partition hon SHUFFLE_PARTITIONS, phuc vu quan sat
    # phan tan task tren nhieu executor/worker khi chay cluster (V06). Ap
    # dung GIONG HET nhau o ca 2 che do (local[*] va cluster) nen train/test
    # split (dua tren cung so partition + cung seed) van tuong duong nhau.
    feature_df = feature_df.repartition(SHUFFLE_PARTITIONS)

    return feature_df


def split_train_test(feature_df):
    log("V02 - Chia train/test TRUOC khi fit bat ky transformer nao (tranh ro ri du lieu)")

    train_df, test_df = feature_df.randomSplit(
        [1.0 - TEST_FRACTION, TEST_FRACTION], seed=SEED
    )
    train_df = train_df.cache()
    test_df = test_df.cache()

    n_train = train_df.count()
    n_test = test_df.count()
    print(f"train: {n_train} dong, test: {n_test} dong (seed={SEED}, test_fraction={TEST_FRACTION})")

    print("Phan bo label tren TRAIN:")
    train_df.groupBy("label").count().orderBy("label").show()
    print("Phan bo label tren TEST:")
    test_df.groupBy("label").count().orderBy("label").show()

    # Trong so lop (class weight) de xu ly mat can bang lop CANCELLED - tinh
    # HOAN TOAN tu thong ke tren TRAIN (khong dung thong tin tu TEST) nen
    # khong gay ro ri du lieu.
    counts = {row["label"]: row["count"] for row in train_df.groupBy("label").count().collect()}
    total = sum(counts.values())
    n_classes = len(counts)
    class_weights = {lbl: total / (n_classes * cnt) for lbl, cnt in counts.items()}
    print("Class weight tinh tu TRAIN:", class_weights)

    weight_expr = F.when(F.col("label") == 1.0, F.lit(class_weights.get(1.0, 1.0))).otherwise(
        F.lit(class_weights.get(0.0, 1.0))
    )
    train_df = train_df.withColumn("class_weight", weight_expr)

    return train_df, test_df, class_weights


def build_pipeline():
    log("V03a - Xay Pipeline (Estimator/Transformer) - StringIndexer/OneHotEncoder/Imputer/VectorAssembler/LogisticRegression")

    categorical_cols = ["customer_segment", "city", "payment_method"]
    indexed_cols = [f"{c}_idx" for c in categorical_cols]
    ohe_cols = [f"{c}_ohe" for c in categorical_cols]

    indexer = StringIndexer(
        inputCols=categorical_cols, outputCols=indexed_cols, handleInvalid="keep"
    )
    encoder = OneHotEncoder(inputCols=indexed_cols, outputCols=ohe_cols)

    # Imputer xu ly missing value cho total_amount (bao gom gia tri -1.0 da
    # duoc chuyen thanh null o buoc clean_data). fit() se chay ben trong
    # Pipeline.fit(train_df) => CHI hoc median tu TRAIN, khong dung TEST.
    imputer = Imputer(
        inputCols=["total_amount"], outputCols=["total_amount_imputed"], strategy="median"
    )

    assembler = VectorAssembler(
        inputCols=ohe_cols + ["total_amount_imputed", "order_hour", "order_dow"],
        outputCol="features",
        handleInvalid="keep",
    )

    classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        maxIter=50,
        regParam=0.01,
        elasticNetParam=0.0,
    )

    pipeline = Pipeline(stages=[indexer, encoder, imputer, assembler, classifier])
    return pipeline


def evaluate(model, test_df):
    log("V03b - Danh gia bang Evaluator (AUC + precision/recall/F1 vi CANCELLED la lop thieu so)")

    predictions = model.transform(test_df).cache()

    bin_evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    auc = bin_evaluator.evaluate(predictions)

    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    f1 = f1_evaluator.evaluate(predictions)

    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = accuracy_evaluator.evaluate(predictions)

    precision_cancelled_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction",
        metricName="precisionByLabel", metricLabel=1.0,
    )
    precision_cancelled = precision_cancelled_evaluator.evaluate(predictions)

    recall_cancelled_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction",
        metricName="recallByLabel", metricLabel=1.0,
    )
    recall_cancelled = recall_cancelled_evaluator.evaluate(predictions)

    print(f"AUC (areaUnderROC)              : {auc:.4f}")
    print(f"Accuracy                        : {accuracy:.4f}  (CANH BAO: lop CANCELLED thieu so, accuracy don thuan de gay hieu lam)")
    print(f"F1 (weighted)                   : {f1:.4f}")
    print(f"Precision (label=1 CANCELLED)   : {precision_cancelled:.4f}")
    print(f"Recall (label=1 CANCELLED)      : {recall_cancelled:.4f}")

    print("\nConfusion matrix (label thuc te x prediction):")
    confusion_df = (
        predictions.groupBy("label", "prediction").count().orderBy("label", "prediction")
    )
    confusion_df.show()
    confusion_rows = [row.asDict() for row in confusion_df.collect()]

    metrics = {
        "auc": auc,
        "accuracy": accuracy,
        "f1_weighted": f1,
        "precision_label1_cancelled": precision_cancelled,
        "recall_label1_cancelled": recall_cancelled,
        "confusion_matrix": confusion_rows,
    }
    predictions.unpersist()
    return metrics


def save_and_reload_model(model, test_df, spark):
    log("V04 - Luu PipelineModel (.save()) va nap lai (.load()), ap dung lai tren du lieu moi")

    model.write().overwrite().save(PIPELINE_MODEL_PATH)
    print("Da luu PipelineModel vao:", PIPELINE_MODEL_PATH)

    reloaded_model = PipelineModel.load(PIPELINE_MODEL_PATH)
    print("Da nap lai PipelineModel tu:", PIPELINE_MODEL_PATH)

    sample_new_records = test_df.limit(5).drop("label")
    print("5 ban ghi 'moi' (lay tu test, bo cot label) de kiem tra model nap lai:")
    sample_new_records.show(truncate=False)

    reloaded_predictions = reloaded_model.transform(sample_new_records)
    print("Du doan cua PipelineModel NAP LAI:")
    reloaded_predictions.select(
        "order_id", "customer_segment", "city", "payment_method", "prediction", "probability"
    ).show(truncate=False)

    # Doi chieu: du doan cua model goc (chua .stop()) tren cung 5 ban ghi phai
    # giong het du doan cua model nap lai tu disk.
    original_predictions = model.transform(sample_new_records)
    joined_check = (
        original_predictions.select("order_id", F.col("prediction").alias("pred_original"))
        .join(
            reloaded_predictions.select("order_id", F.col("prediction").alias("pred_reloaded")),
            on="order_id",
        )
    )
    mismatch_count = joined_check.filter(F.col("pred_original") != F.col("pred_reloaded")).count()
    print(
        "So ban ghi du doan KHAC nhau giua model goc va model nap lai (phai = 0):",
        mismatch_count,
    )
    return mismatch_count == 0


def main():
    print("SPARK_MASTER_URL =", SPARK_MASTER_URL)
    print("RUN_TAG =", RUN_TAG)
    print("DATA_DIR =", DATA_DIR)
    print("OUTPUT_DIR =", OUTPUT_DIR)
    print("MODEL_DIR =", MODEL_DIR)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("spark.master (thuc te) =", spark.sparkContext.master)
    print("applicationId =", spark.sparkContext.applicationId)
    print("defaultParallelism =", spark.sparkContext.defaultParallelism)

    try:
        orders_df, customers_df = read_sources(spark)
        orders_df, customers_df = clean_data(orders_df, customers_df)
        feature_df = build_feature_table(orders_df, customers_df)

        # V02 - so partition cua buoc feature_df truoc split (ghi lai de doi
        # chieu voi Spark UI / REST API job-stage-task khi chay tren cluster)
        print("So partition cua feature_df:", feature_df.rdd.getNumPartitions())

        train_df, test_df, class_weights = split_train_test(feature_df)

        pipeline = build_pipeline()
        model = pipeline.fit(train_df)
        print("Da fit xong PipelineModel. Cac stage:")
        for stage in model.stages:
            print(" -", type(stage).__name__)

        metrics = evaluate(model, test_df)
        reload_ok = save_and_reload_model(model, test_df, spark)

        metrics["run_tag"] = RUN_TAG
        metrics["spark_master_url_config"] = SPARK_MASTER_URL
        metrics["spark_master_actual"] = spark.sparkContext.master
        metrics["application_id"] = spark.sparkContext.applicationId
        metrics["seed"] = SEED
        metrics["test_fraction"] = TEST_FRACTION
        metrics["class_weights_from_train"] = {str(k): v for k, v in class_weights.items()}
        metrics["reload_prediction_match"] = reload_ok
        metrics["feature_df_num_partitions"] = feature_df.rdd.getNumPartitions()

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(METRICS_PATH, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)
        print("\nDa ghi metrics vao:", METRICS_PATH)
        print(json.dumps(metrics, indent=2, ensure_ascii=False))

        log("HOAN TAT train_pipeline.py")
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
