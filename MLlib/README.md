# Buổi 12 — Học máy phân tán với Spark MLlib (RetailStream)

Pipeline dự đoán nguy cơ đơn hàng bị hủy (`status = CANCELLED`) từ dữ liệu
`orders` + `customers` của RetailStream, xây dựng bằng Spark ML Pipeline API.
Một Pipeline MLlib gồm các bước biến đổi dữ liệu nối tiếp nhau (gọi là
Transformer, ví dụ mã hóa cột phân loại) và bước học mô hình từ dữ liệu (gọi
là Estimator, ví dụ hồi quy logistic) — pipeline này gồm:
(`StringIndexer` / `OneHotEncoder` / `Imputer` / `VectorAssembler` /
`LogisticRegression`). Một file `train_pipeline.py` duy nhất dùng chung cho
cả hai chế độ chạy — `local[*]` và Spark Standalone Cluster — chỉ đổi biến
môi trường, không đổi logic xử lý.

Kết quả trong tài liệu này là số liệu **mới nhất** (2026-08-22), chạy trên
dữ liệu `00_shared_data/lab/` đã được cấy tương quan giả lập có chủ đích
giữa `customer_segment` / `payment_method` và `status = CANCELLED` (mức
`sample` không bị thay đổi). AUC hiện tại là **0,6893**.

## 1. Bài toán và dữ liệu

Nhãn (label): giá trị cần dự đoán. Đặc trưng (feature): các thông tin đầu
vào dùng để dự đoán nhãn đó.

- **Nhãn**: `label = 1.0` nếu `status == 'CANCELLED'`, ngược lại `0.0`. Cột
  `status` bị loại khỏi bảng đặc trưng; mã kiểm tra tự động và `raise` lỗi
  nếu cột này vô tình còn sót lại.
- **Đặc trưng phân loại**: `customer_segment`, `city` (lấy từ bảng
  `customers`, join qua `customer_id`), `payment_method` (từ bảng `orders`)
  — mã hóa bằng `StringIndexer` + `OneHotEncoder`.
- **Đặc trưng số**: `total_amount` (điền khuyết bằng trung vị), `order_hour`,
  `order_dow` (trích từ `order_time`) — đều là thông tin biết được tại thời
  điểm tạo đơn, không rò rỉ thông tin tương lai.
- **Nguồn dữ liệu**: `00_shared_data/lab/orders.csv` (50.000 đơn, 5.047
  `CANCELLED` ≈ 10,1%) và `00_shared_data/lab/customers.csv` (20.001 dòng,
  có 1 bản ghi trùng `customer_id` được cố tình đưa vào). Dùng mức `lab`
  thay vì `sample` (chỉ 150 đơn, 19 `CANCELLED`) vì `sample` quá nhỏ để chia
  train/test có ý nghĩa thống kê.
- **Giá trị thiếu có chủ đích**: `orders.csv` có 1 bản ghi `total_amount =
  -1.0` (xem `manifest.json` → `intentional_data_issues`), được coi là
  `null` rồi để `Imputer` (fit trên TRAIN) điền trung vị. `customers.csv` có
  1 bản ghi trùng `customer_id` (`CUST00001`), được khử trùng xác định bằng
  `row_number()` theo `created_at`, giữ bản ghi sớm nhất.

## 2. Tránh rò rỉ dữ liệu

Nếu các bước xử lý (như tính trung vị để điền giá trị thiếu, hoặc gán mã số
cho cột phân loại) được học từ TOÀN BỘ dữ liệu (bao gồm cả phần sẽ dùng để
kiểm tra), mô hình gián tiếp "nhìn thấy" trước một phần đáp án, khiến điểm
đánh giá cao hơn thực tế — đây gọi là rò rỉ dữ liệu (data leakage).

- `randomSplit([0.8, 0.2], seed=42)` được gọi **ngay sau khi có bảng đặc
  trưng**, trước khi bất kỳ Estimator nào được `fit()`.
- Toàn bộ `StringIndexer`, `OneHotEncoder`, `Imputer` nằm **trong**
  `Pipeline`, chỉ `fit()` một lần duy nhất qua `pipeline.fit(train_df)`.
  `test_df` chỉ đi qua `model.transform(test_df)`, không bao giờ `fit()`.
- Trọng số lớp (`class_weight`, xử lý mất cân bằng CANCELLED) được tính
  hoàn toàn từ `train_df.groupBy("label").count()`, không dùng thống kê
  từ `test_df`:

  ```text
  train: 39.948 dòng, test: 9.831 dòng (seed=42, test_fraction=0.2)
  Class weight tính từ TRAIN: {0.0: 0.556, 1.0: 4.944}
  ```

## 3. Pipeline MLlib

```python
categorical_cols = ["customer_segment", "city", "payment_method"]

# StringIndexer: chuyển từng cột chuỗi phân loại thành cột số nguyên (chỉ
# số của giá trị, ví dụ "VIP" -> 0.0, "Regular" -> 1.0). handleInvalid="keep"
# nghĩa là giữ lại giá trị lạ chưa từng thấy khi huấn luyện (gán một mã
# riêng) thay vì báo lỗi khi gặp ở dữ liệu test hoặc dữ liệu mới.
indexer = StringIndexer(inputCols=categorical_cols,
                         outputCols=[f"{c}_idx" for c in categorical_cols],
                         handleInvalid="keep")
# OneHotEncoder: chuyển mã số của StringIndexer thành vector nhị phân
# (one-hot), tránh việc mô hình hiểu nhầm các mã số này có quan hệ thứ tự.
encoder = OneHotEncoder(inputCols=[f"{c}_idx" for c in categorical_cols],
                         outputCols=[f"{c}_ohe" for c in categorical_cols])
# Imputer: điền giá trị thiếu (null) của cột số bằng trung vị, tính từ TRAIN.
imputer = Imputer(inputCols=["total_amount"],
                   outputCols=["total_amount_imputed"], strategy="median")
# VectorAssembler: gộp tất cả cột đặc trưng (đã mã hóa/điền khuyết) thành
# một cột vector duy nhất "features" mà Spark ML yêu cầu làm đầu vào huấn
# luyện.
assembler = VectorAssembler(
    inputCols=[f"{c}_ohe" for c in categorical_cols]
             + ["total_amount_imputed", "order_hour", "order_dow"],
    outputCol="features", handleInvalid="keep")
# LogisticRegression: mô hình phân loại nhị phân. weightCol="class_weight"
# cho mô hình biết trọng số ưu tiên của mỗi dòng dữ liệu khi lớp bị mất cân
# bằng (lớp CANCELLED ít hơn hẳn lớp còn lại), để tránh mô hình bỏ qua lớp
# hiếm.
classifier = LogisticRegression(featuresCol="features", labelCol="label",
                                 weightCol="class_weight",
                                 maxIter=50, regParam=0.01, elasticNetParam=0.0)

pipeline = Pipeline(stages=[indexer, encoder, imputer, assembler, classifier])
model = pipeline.fit(train_df)
```

Đánh giá dùng `BinaryClassificationEvaluator` (AUC) và
`MulticlassClassificationEvaluator` (accuracy, F1, precision/recall theo
lớp `CANCELLED`, `metricLabel=1.0`).

## 4. Kết quả (tập test: 9.831 dòng, 1.207 CANCELLED)

| Metric | local[*] | Spark Standalone cluster |
|---|---|---|
| AUC (areaUnderROC) | **0,6893** | **0,6893** |
| Accuracy | 0,6741 | 0,6741 |
| F1 (weighted) | 0,7282 | 0,7282 |
| Precision (label=1, CANCELLED) | 0,2098 | 0,2098 |
| Recall (label=1, CANCELLED) | 0,5982 | 0,5982 |
| applicationId | `local-1787408130388` | `app-20260822142024-0001` |
| Số partition của `feature_df` | 8 | 8 |

Confusion matrix (nhãn thực tế × dự đoán), giống hệt nhau ở cả hai chế độ:

| label | prediction | count |
|---|---|---|
| 0.0 | 0.0 | 5.905 |
| 0.0 | 1.0 | 2.719 |
| 1.0 | 0.0 | 485 |
| 1.0 | 1.0 | 722 |

AUC 0,6893 (0,5 = đoán ngẫu nhiên, 1,0 = hoàn hảo) phản ánh mô hình có khả
năng phân biệt vừa phải: học được tín hiệu thật từ dữ liệu nhưng không hoàn
hảo. Precision của lớp CANCELLED chỉ 0,2098 (trong 100 đơn dự đoán "sẽ hủy"
chỉ khoảng 21 đơn thực sự bị hủy) minh họa rõ nguyên tắc không đồng nhất
Accuracy cao với chất lượng mô hình khi lớp mất cân bằng.

Metric giữa `local[*]` và cluster **giống hệt nhau** (không chỉ tương
đương) vì cùng `seed=42`, cùng `spark.sql.shuffle.partitions=8`, cùng
`repartition(8)`, và toàn bộ pipeline tất định ngoài `randomSplit`/khởi tạo
trọng số hồi quy logistic (đều do cùng seed và thuật toán tối ưu hội tụ
tất định của Spark ML kiểm soát).

`PipelineModel` lưu bằng `model.write().overwrite().save(...)` và nạp lại
bằng `PipelineModel.load(...)`; dự đoán trên 5 bản ghi mới của model nạp
lại khớp tuyệt đối với model gốc ở cả hai chế độ
(`reload_prediction_match: true`).

## 5. Hướng dẫn chạy các kịch bản thử nghiệm

Pipeline dùng lại cụm Spark Standalone (1 master + 2 worker, image
`apache/spark:3.5.9-python3`) đã dựng ở `Spark/docker-compose.yml`.

```bash
cd Spark && docker compose up -d
curl -s http://localhost:8080/json/    # xác nhận ALIVE, 2 worker
```

`pyspark.ml` cần `numpy`, không có sẵn trong image chính thức. Cài một lần
vào thư mục bind-mount dùng chung cho cả 3 container. Dùng `--target=` vì
user chạy trong container không có quyền ghi vào thư mục cài đặt gói Python
hệ thống:

```bash
export MSYS_NO_PATHCONV=1
docker exec spark-master pip3 install --no-cache-dir \
  --target=/opt/spark-data/mllib_session12/pylibs numpy
```

Copy job và dữ liệu vào thư mục mà mọi container truy cập được:

```bash
cp MLlib/train_pipeline.py Spark/jobs/train_pipeline.py
cp MLlib/data/orders.csv MLlib/data/customers.csv Spark/data/mllib_session12/
```

### Chạy `local[*]`

Lệnh dưới đây chạy trong `spark-worker-1` (không phải `spark-master`) vì
container `spark-master` có `mem_limit: 512m` (mặc định trước khi tăng, xem
mục 6) — `local[*]` cùng tiến trình Master trong cùng container dễ vượt
RAM.

```bash
export MSYS_NO_PATHCONV=1
docker exec \
  -e SPARK_MASTER_URL="local[*]" \
  -e MLLIB_RUN_TAG="local_mode" \
  -e PYTHONPATH="/opt/spark-data/mllib_session12/pylibs" \
  spark-worker-1 \
  /opt/spark/bin/spark-submit \
  --master local[2] \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
  --driver-memory 512m \
  /opt/spark-apps/train_pipeline.py
```

### Chạy trên Spark Standalone Cluster

```bash
export MSYS_NO_PATHCONV=1
docker run --rm \
  --network spark_spark-net \
  -v "//d/school/Big Data/Spark/data:/opt/spark-data" \
  -v "//d/school/Big Data/Spark/jobs:/opt/spark-apps:ro" \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e MLLIB_RUN_TAG="cluster_mode" \
  -e PYTHONPATH="/opt/spark-data/mllib_session12/pylibs" \
  apache/spark:3.5.9-python3 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
  --executor-memory 512m --executor-cores 1 --total-executor-cores 2 \
  --driver-memory 512m \
  /opt/spark-apps/train_pipeline.py
```

`--executor-memory 512m` phải được chỉ định rõ: mỗi worker chỉ có 640MB khả
dụng, nhỏ hơn mức mặc định 1024MB của Spark — nếu không chỉ định, job kẹt
vĩnh viễn ở trạng thái "Initial job has not accepted any resources".

Xác nhận job thực sự chạy trên cluster (không phải `local[*]`):
`applicationId` dạng `app-...` (không phải `local-...`), log có 2 dòng
`Executor added: .../0 on worker-...-172.20.0.3-...` và
`.../1 on worker-...-172.20.0.4-...`, và
`curl -s http://localhost:8080/json/` cho ứng dụng vừa chạy `"cores": 2`.

### Thử nghiệm dừng một worker

```bash
docker stop spark-worker-2
curl -s http://localhost:8080/json/    # chỉ còn 1 worker ALIVE
# submit job trong lúc chỉ còn 1 worker — vẫn chạy xong, chỉ còn 1 executor
docker start spark-worker-2
curl -s http://localhost:8080/json/    # cả 2 worker ALIVE trở lại
```

## 6. Cấu hình cụm cần lưu ý

`Spark/docker-compose.yml` — `mem_limit` của service `spark-master` đã được
tăng từ 512MB lên **1536MB** để chạy được pipeline MLlib trên dữ liệu thật
mà không bị Docker OOMKill (driver JVM của `local[*]` cộng với tiến trình
Master vượt quá 512MB). Nếu chạy lại trên máy có RAM Docker hạn chế hơn,
tổng RAM các container Spark hiện là 1536 + 800 + 800 = 3136MB.

## 7. Lỗi thường gặp

- **`ModuleNotFoundError: No module named 'numpy'`**: image
  `apache/spark:3.5.9-python3` không cài sẵn `numpy`. Cài vào thư mục
  bind-mount dùng chung và truyền `PYTHONPATH` cho cả driver
  (`-e PYTHONPATH=...`) lẫn executor (`--conf
  spark.executorEnv.PYTHONPATH=...`).
- **`pip3 install numpy` báo `Permission denied: '/nonexistent'`**: user
  `spark` (uid 185) trong container không có quyền ghi vào site-packages hệ
  thống. Dùng `--target=` trỏ tới thư mục bind-mount.
- **Container `spark-master` bị OOMKilled khi chạy `local[*]` ngay trong
  đó**: chạy trong `spark-worker-1` thay vì `spark-master`, hoặc tăng
  `mem_limit` như mục 6.
- **`spark-submit` vào cluster kẹt ở "Initial job has not accepted any
  resources"**: thiếu `--executor-memory`; mỗi worker chỉ có 640MB, nhỏ hơn
  mức mặc định 1024MB của Spark. Chỉ định rõ `--executor-memory 512m
  --executor-cores 1 --total-executor-cores 2`.
- **Đồng nhất Accuracy cao với mô hình tốt**: với lớp `CANCELLED` chỉ chiếm
  ~10%, một mô hình luôn đoán "không hủy" đạt Accuracy cao hơn cả mô hình
  đã huấn luyện nhưng vô dụng (Recall = 0). Luôn đọc AUC/Precision/Recall
  theo từng lớp cùng với Accuracy.

## 8. Cấu trúc thư mục

```text
MLlib/
├── README.md
├── train_pipeline.py                    (file chính, có khối CONFIG tập trung)
├── data/
│   ├── orders.csv
│   ├── customers.csv
│   └── manifest.json
├── scripts/
│   ├── prepare-data.sh                  (chuẩn bị dữ liệu từ 00_shared_data và cài numpy)
│   ├── train-local.sh                   (huấn luyện mô hình local[*] và xuất metrics)
│   └── train-cluster.sh                 (huấn luyện trên cụm Standalone thật)
├── models/
│   ├── local_mode_v2/pipeline_model/    (PipelineModel local[*], số liệu hiện tại)
│   └── cluster_mode_v2/pipeline_model/  (PipelineModel cluster, số liệu hiện tại)
└── output/
    ├── local_mode_v2/metrics.json
    └── cluster_mode_v2/metrics.json
```

## 9. Hướng dẫn chạy nhanh bằng Script (Khuyến nghị)

Toàn bộ quy trình sao chép dữ liệu từ `00_shared_data/lab/`, cài đặt thư viện phụ thuộc (`numpy`) vào container và nộp job huấn luyện mô hình đã được đóng gói sẵn trong thư mục `MLlib/scripts/`.

### Môi trường khuyến nghị:
- **Git Bash** (trên Windows) hoặc Terminal Linux/macOS.
- Nếu dùng **PowerShell**: hãy gọi qua Git Bash bằng `bash scripts/<tên_script>.sh`.

### Thư mục làm việc (Working Directory):
Mở terminal và chuyển vào thư mục `MLlib`:
```bash
cd "d:/school/Big Data/MLlib"
```

### Thứ tự thực hiện:

#### Bước 1: Chuẩn bị dữ liệu và môi trường Python
```bash
bash scripts/prepare-data.sh
```
*Lệnh này làm gì:*
1. Tự động kiểm tra và khởi động cụm Spark (`cd ../Spark && docker compose up -d`) nếu cụm chưa chạy.
2. Sao chép 2 tệp dữ liệu quy mô lab (`orders.csv` và `customers.csv`) từ nguồn dùng chung `../00_shared_data/lab/` vào thư mục mount của Spark (`Spark/data/mllib_session12/`).
3. Sao chép mã nguồn `train_pipeline.py` vào thư mục mount.
4. Tự động kiểm tra và chạy `pip3 install --target=... numpy` ngay trong container để nạp thư viện tính toán mà không bị lỗi quyền ghi hệ thống.

#### Bước 2A: Huấn luyện ở chế độ Local (`local[*]`)
```bash
# Chạy với tag mặc định local_mode_v2:
bash scripts/train-local.sh

# Hoặc truyền tag tùy chỉnh (ví dụ thử nghiệm tham số mới):
bash scripts/train-local.sh my_local_test
```
*Lệnh này làm gì:* Nộp job huấn luyện chạy `local[*]` bên trong container `spark-master`, tự động truyền đường dẫn thư viện `pylibs`, lưu mô hình vào `models/` và in các chỉ số đánh giá (`metrics.json`) ra màn hình.

#### Bước 2B: Huấn luyện trên cụm Spark Standalone thật (Cluster Mode)
```bash
# Chạy với tag mặc định cluster_mode_v2:
bash scripts/train-cluster.sh

# Hoặc truyền tag tùy chỉnh:
bash scripts/train-cluster.sh my_cluster_test
```
*Lệnh này làm gì:* Nộp job phân tán lên `spark://spark-master:7077`, cấu hình sẵn các tham số tối ưu bộ nhớ (`--executor-memory 512m`, `--executor-cores 1`, `--total-executor-cores 2`) tránh lỗi thiếu RAM trên Worker, lưu mô hình phân tán và xuất kết quả `metrics.json`.

#### Bước 3: Dừng cụm khi kết thúc
```bash
cd ../Spark && bash scripts/stop-cluster.sh
```

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| **Feature (đặc trưng)** | Các thông tin đầu vào dùng để dự đoán (ví dụ: khách hàng thuộc phân khúc nào, thanh toán bằng gì). |
| **Label (nhãn)** | Kết quả cần dự đoán (ví dụ: đơn hàng có bị hủy hay không). |
| **Train / Test split** | Chia dữ liệu thành 2 phần: phần "Train" để mô hình học, phần "Test" để kiểm tra xem mô hình học tốt tới đâu trên dữ liệu nó CHƯA từng thấy. |
| **Pipeline (MLlib)** | Một chuỗi bước xử lý dữ liệu + huấn luyện mô hình được đóng gói lại để chạy lại dễ dàng, đúng thứ tự mỗi lần. |
| **Transformer / Estimator** | Transformer = bước biến đổi dữ liệu (ví dụ mã hóa cột phân loại). Estimator = bước học mô hình từ dữ liệu (ví dụ hồi quy logistic). Một Pipeline MLlib gồm các Transformer nối tiếp và kết thúc bằng một Estimator. |
| **VectorAssembler** | Công cụ gộp nhiều cột feature riêng lẻ thành 1 "vector" (danh sách số) để mô hình học máy có thể xử lý. |
| **StringIndexer / OneHotEncoder** | Cách chuyển dữ liệu dạng chữ (ví dụ "Hà Nội", "TP.HCM") thành dạng số để mô hình hiểu được. |
| **Imputer** | Công cụ điền giá trị thiếu (null) của cột số bằng một giá trị thống kê (ví dụ trung vị), tính từ tập TRAIN. |
| **Evaluator** | Công cụ chấm điểm xem mô hình dự đoán tốt tới đâu. |
| **AUC** | Một điểm số (0 đến 1) đo mức độ mô hình phân biệt đúng 2 nhóm (ví dụ "sẽ hủy" và "không hủy"). AUC = 0.5 nghĩa là mô hình đoán chẳng khác gì tung đồng xu; AUC càng gần 1 thì càng phân biệt tốt. |
| **Precision / Recall / F1** | 3 cách đo độ chính xác khác nhau khi 2 nhóm cần dự đoán bị lệch số lượng (ví dụ ít đơn bị hủy hơn nhiều so với đơn thành công) — dùng khi chỉ nhìn "% đoán đúng chung" (accuracy) dễ gây hiểu lầm. |
| **Data leakage (rò rỉ dữ liệu)** | Lỗi để mô hình "nhìn trộm" thông tin từ tập Test trong lúc học ở tập Train — làm điểm số trông tốt giả tạo, không phản ánh đúng thực tế. |
| **PipelineModel (save/load)** | Sau khi huấn luyện xong, mô hình được lưu lại thành file để lần sau dùng lại ngay, không phải huấn luyện lại từ đầu. |
| **Spark Standalone Cluster** | Cụm Spark tự quản lý (không cần YARN/Kubernetes), gồm 1 Master (điều phối) + nhiều Worker (thực thi việc). |
| **local[\*]** | Chế độ chạy Spark ngay trên 1 máy (không dùng cluster), dùng để học/thử nhanh. `local[*]` nghĩa là dùng tất cả CPU core có sẵn của máy đó. |
| **Executor** | Tiến trình thực sự chạy trên từng Worker để xử lý dữ liệu — nếu thấy có executor chạy trên nhiều Worker khác nhau, nghĩa là job thật sự chạy phân tán (không phải chạy 1 mình trên máy local). |
| **mem_limit / OOM** | `mem_limit` là giới hạn RAM tối đa cấp cho 1 container, khai báo trong `docker-compose.yml`. Nếu container cần nhiều RAM hơn mức này sẽ bị OOM (Out Of Memory — hết bộ nhớ, hệ điều hành tự tắt tiến trình để bảo vệ máy). |
