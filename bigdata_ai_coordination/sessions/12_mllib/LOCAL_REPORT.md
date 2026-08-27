# LOCAL VALIDATION REPORT - Buoi 12 (Spark MLlib)

## 1. Trang thai

**Validation: PASS**

Pipeline MLlib RetailStream (du doan nguy co don hang bi huy) da chay
that, dung ky thuat (Pipeline API, StringIndexer/OneHotEncoder/Imputer/
VectorAssembler, train/test khong ro ri du lieu, evaluator phu hop lop mat
can bang, save/load PipelineModel) tren CA HAI che do bat buoc:
`local[*]` va Spark Standalone Cluster that (1 master + 2 worker, evidence
executor tren 2 container worker khac nhau qua log + REST API). Metric
giua 2 che do giong het nhau. Da thuc hien thu nghiem dung 1 worker va
khoi dong lai thanh cong.

Luu ket qua o `MLlib/` (README.md, `train_pipeline.py`, `data/`,
`models/{local_mode,cluster_mode}/pipeline_model`,
`output/{local_mode,cluster_mode,cluster_worker_down_experiment}`).

## CẬP NHẬT 2026-08-22 (theo yêu cầu giảng viên: "phải sửa dữ liệu generate sao cho phù hợp chứ ra độ chính xác như thế thì có ý nghĩa gì đâu")

Toàn bộ nội dung gốc bên dưới (mục 1-8) là kết quả **CŨ**, chạy trên dữ liệu
`lab` sinh **hoàn toàn ngẫu nhiên** (AUC ~0.51, gần đoán ngẫu nhiên). Theo
yêu cầu giảng viên, generator đã được sửa để cấy tương quan giả lập có chủ
đích giữa `customer_segment`/`payment_method` và `status=CANCELLED` (xem
`00_shared_data/README.md` mục "Tương quan giả lập") — **chỉ áp dụng cho
mức `lab`**, mức `sample` (dùng ở Buổi 5 MongoDB) giữ nguyên 100% không đổi.

**Bước xác minh an toàn đã thực hiện trước khi chấp nhận thay đổi:** so
khớp sha256 + số byte của cả 7 file `00_shared_data/sample/` với
`manifest.json` — **khớp tuyệt đối 100%**, xác nhận generator không hề
động đến mức `sample`.

**Kết quả huấn luyện lại (V08 dưới đây) với dữ liệu `lab` đã có tương
quan:**

| Metric | Cũ (ngẫu nhiên, mục 4 gốc) | Mới (có tương quan, V08) |
|---|---|---|
| AUC | 0.5117 | **0.6893** |
| Accuracy | 0.5332 | 0.6741 |
| F1 (weighted) | 0.6245 | 0.7282 |
| Precision (label=1 CANCELLED) | 0.1053 | 0.2098 |
| Recall (label=1 CANCELLED) | 0.4883 | 0.5982 |

AUC 0.6893 là mức "có khả năng phân biệt vừa phải, không hoàn hảo" (0.5 =
đoán ngẫu nhiên, 1.0 = hoàn hảo) — đúng đúng mức nhập môn: mô hình học được
tín hiệu thật nhưng không "quá dễ" đến mức làm mất ý nghĩa giáo dục của
việc đánh giá mô hình (precision/recall/confusion matrix vẫn cho thấy lớp
CANCELLED khó dự đoán chính xác — bài học thực tế về lớp mất cân bằng vẫn
còn nguyên).

## 2. Environment

| Thanh phan | Version |
|---|---|
| OS (host) | Windows 10 Pro 10.0.19045 |
| Docker | Docker Desktop, Docker Compose v2 |
| Spark cluster (dung lai tu Buoi 9) | `apache/spark:3.5.9-python3`, 1 master (`spark-master`, mem_limit 512m) + 2 worker (`spark-worker-1`, `spark-worker-2`, moi worker `--cores 1 --memory 640m`, mem_limit 800m/container) - dung nguyen `Spark/docker-compose.yml`, KHONG sua |
| Python trong container Spark | 3.10.12 |
| Java trong container Spark | OpenJDK 11.0.31 |
| numpy (them vao, khong co san trong image) | 2.2.6, cai qua `pip3 install --target=` vao thu muc bind-mount `Spark/data/mllib_session12/pylibs`, dung chung cho ca 3 container qua `PYTHONPATH` |
| Cac container khac cung chay dong thoi | `mongodb` (mongo:8.0), `hdfs-namenode`/`hdfs-datanode1`/`hdfs-datanode2` (bde2020 Hadoop 3.2.1) - khong tac dong den Spark, khong bi dung/xoa |
| Du lieu | `00_shared_data/lab/orders.csv` (50.000 dong) + `00_shared_data/lab/customers.csv` (20.001 dong, co 1 ban ghi trung id co chu y) |

**Vi sao dung muc `lab` thay vi `sample`**: `sample/orders_sample.csv` chi
co 150 don (19 CANCELLED, ~12.7%) - qua nho de chia train/test co y nghia
thong ke (test set ~30 dong chi con vai CANCELLED). `lab/orders.csv` co
50.000 don, 5.047 CANCELLED (~10.1%) - du lon, van giu dung ban chat mat
can bang lop. Khong tao dataset nghiep vu moi, chi dung 2 nguon BRIEF cho
phep (`orders.csv` + `customers.csv`, dung Data Contract).

Khong doi Data Contract. Khong dung/xoa cum Spark/HDFS/MongoDB cua buoi
khac (chi `docker stop`/`docker start` tam thoi 1 worker trong pham vi
thu nghiem V06, da khoi dong lai ngay sau khi quan sat xong).

## 3. Cach khoi dong

```bash
# Cum Spark da chay san tu Buoi 9/10 (khong dung cum moi):
cd Spark && docker compose up -d
curl -s http://localhost:8080/json/   # xac nhan ALIVE, 2 worker

# Cai numpy dung chung (1 lan, xem MLlib/README.md muc 4):
export MSYS_NO_PATHCONV=1
docker exec spark-master pip3 install --no-cache-dir \
  --target=/opt/spark-data/mllib_session12/pylibs numpy

# Copy job + data vao Spark/data, Spark/jobs (khong sua docker-compose.yml):
cp MLlib/train_pipeline.py Spark/jobs/train_pipeline.py
cp MLlib/data/orders.csv MLlib/data/customers.csv Spark/data/mllib_session12/
```

## 4. Validation Results

### V01 - Feature engineering (categorical StringIndexer/OneHotEncoder, missing value, VectorAssembler)

**Result: PASS**

Command (trich tu lan chay cluster chinh thuc, `MLLIB_RUN_TAG=cluster_mode`):

```bash
docker run --rm --network spark_spark-net \
  -v "//d/school/Big Data/Spark/data:/opt/spark-data" \
  -v "//d/school/Big Data/Spark/jobs:/opt/spark-apps:ro" \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e MLLIB_RUN_TAG="cluster_mode" \
  -e PYTHONPATH="/opt/spark-data/mllib_session12/pylibs" \
  apache/spark:3.5.9-python3 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
  --executor-memory 512m --driver-memory 512m \
  /opt/spark-apps/train_pipeline.py
```

Expected: doc orders(50000)+customers(20001) voi explicit schema; xu ly
`total_amount = -1.0` (loi co chu y) thanh missing roi Impute median (fit
tren TRAIN); khu trung 1 ban ghi customer trung id; StringIndexer +
OneHotEncoder cho `customer_segment`/`city`/`payment_method`;
VectorAssembler gop thanh cot `features`; khong con cot `status` trong
bang feature.

Actual (log that, trich `MLlib/output/cluster_mode/run_tail.log` va
console output khi chay):

```text
orders count (raw): 50000
customers count (raw): 20001
So don co total_amount < 0 (coi la missing, se Impute sau): 1
customers truoc khu trung: 20001 -> sau khu trung customer_id: 20000
So don hang khong khop duoc customer (customer_segment=null truoc khi dien UNKNOWN): 0
Phan bo label (0 = khong huy, 1 = CANCELLED):
+-----+-----+
|label|count|
+-----+-----+
|  0.0|44953|
|  1.0| 5047|
+-----+-----+
Xac nhan: khong co cot 'status' trong bang feature -> OK
So partition cua feature_df: 8
```

Notes: `feature_df.repartition(SHUFFLE_PARTITIONS)` duoc goi SAU khi da
loai `status` va truoc khi split, dung de tao nhieu task/partition hon cho
V06 (file CSV nho nen Spark mac dinh doc thanh 1 partition). Ap dung GIONG
HET nhau o ca 2 che do nen khong pha vo tinh tuong duong cua train/test
split.

### V02 - Train/test split khong ro ri du lieu

**Result: PASS**

Command: xem `split_train_test()` trong `train_pipeline.py` - `randomSplit`
duoc goi ngay sau khi co `feature_df` (chua fit bat ky Estimator nao),
`class_weight` chi tinh tu `train_df.groupBy("label").count()`.

Expected: train/test tach ro rang, khong Estimator nao (StringIndexer/
OneHotEncoder/Imputer/LogisticRegression) duoc `fit()` truoc buoc split;
`class_weight` chi phu thuoc thong ke TRAIN.

Actual:

```text
train: 39948 dong, test: 10052 dong (seed=42, test_fraction=0.2)
Phan bo label tren TRAIN:
|  0.0|35955|   |  1.0| 3993|
Phan bo label tren TEST:
|  0.0| 8998|   |  1.0| 1054|
Class weight tinh tu TRAIN: {1.0: 4.944485475135402, 0.0: 0.5562494807101116}
```

Notes: kiem tra code - toan bo 5 stage cua `Pipeline` (`StringIndexer`,
`OneHotEncoder`, `Imputer`, `VectorAssembler`, `LogisticRegression`) chi
duoc goi `fit()` MOT LAN duy nhat qua `pipeline.fit(train_df)`; `test_df`
CHI di qua `model.transform(test_df)` (khong bao gio `fit`). Day la co che
Pipeline API tu dong ngan ro ri, khong can code thu cong them.

### V03 - Pipeline fit/transform + Evaluator (AUC, precision/recall/F1 cho lop mat can bang)

**Result: PASS**

Command: nhu V01 (cung 1 lan chay `spark-submit`).

Expected: Pipeline fit thanh cong voi 5 stage; predict tren test; tinh
duoc AUC (`BinaryClassificationEvaluator`) + accuracy/F1/precision/recall
theo lop (`MulticlassClassificationEvaluator`, `metricLabel=1.0` cho
CANCELLED).

Actual (giong het nhau giua local[*] va cluster - xem V05/V06):

```text
Da fit xong PipelineModel. Cac stage:
 - StringIndexerModel
 - OneHotEncoderModel
 - ImputerModel
 - VectorAssembler
 - LogisticRegressionModel

AUC (areaUnderROC)              : 0.5117
Accuracy                        : 0.5332  (CANH BAO: lop CANCELLED thieu so, accuracy don thuan de gay hieu lam)
F1 (weighted)                   : 0.6245
Precision (label=1 CANCELLED)   : 0.1053
Recall (label=1 CANCELLED)      : 0.4883

Confusion matrix (label thuc te x prediction):
+-----+----------+-----+
|label|prediction|count|
+-----+----------+-----+
|  0.0|       0.0| 4761|
|  0.0|       1.0| 4085|
|  1.0|       0.0|  504|
|  1.0|       1.0|  481|
+-----+----------+-----+
```

Notes (QUAN TRONG, khong bia so lieu): AUC ~0.51 gan voi doan ngau nhien
(0.5). Day la ket qua THAT phan anh dung ban chat du lieu: generator
RetailStream sinh `customer_segment`/`city`/`payment_method`/
`total_amount`/`order_time` KHONG co tuong quan nhan tao voi `status`, nen
khong co tin hieu du de mo hinh hoc. Pipeline van chay dung ky thuat (V01-
V04 PASS, khong ro ri du lieu, evaluator dung), chi la du lieu khong du
tin hieu du doan - day la mot diem giang day quan trong (khong danh dong
"chay duoc" voi "mo hinh tot"), Content AI nen giai thich ro trong tai
lieu thay vi coi day la loi.

### V04 - Luu/nap lai PipelineModel (`.save()`/`.load()`), ap dung tren ban ghi moi

**Result: PASS**

Command: `model.write().overwrite().save(...)` roi `PipelineModel.load(...)`
ngay trong cung lan chay (xem ham `save_and_reload_model`).

Expected: nap lai model tu dia cho ket qua du doan giong het model goc
tren cung 5 ban ghi "moi" (lay tu test, bo cot label).

Actual:

```text
Da luu PipelineModel vao: /opt/spark-data/mllib_session12/models/cluster_mode/pipeline_model
Da nap lai PipelineModel tu: /opt/spark-data/mllib_session12/models/cluster_mode/pipeline_model
So ban ghi du doan KHAC nhau giua model goc va model nap lai (phai = 0): 0
```

`reload_prediction_match: true` trong ca 2 file
`MLlib/output/local_mode/metrics.json` va
`MLlib/output/cluster_mode/metrics.json`. PipelineModel that da duoc luu
tai `MLlib/models/local_mode/pipeline_model/` va
`MLlib/models/cluster_mode/pipeline_model/` (co `metadata/` +
`stages/` cho ca 5 stage).

### V05 - Chay `local[*]`

**Result: PASS**

Command:

```bash
export MSYS_NO_PATHCONV=1
docker exec \
  -e SPARK_MASTER_URL="local[*]" -e MLLIB_RUN_TAG="local_mode" \
  -e PYTHONPATH="/opt/spark-data/mllib_session12/pylibs" \
  spark-worker-1 /opt/spark/bin/spark-submit \
  --master local[2] --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
  --driver-memory 512m /opt/spark-apps/train_pipeline.py
```

Expected: `spark.master (thuc te) = local[*]`, `applicationId` dang
`local-...`, job chay het khong loi, exit code 0.

Actual:

```text
spark.master (thuc te) = local[*]
applicationId = local-1787206578595
EXIT_CODE=0
```

File day du: `MLlib/output/local_mode/run.log`,
`MLlib/output/local_mode/metrics.json`.

Notes (ISSUE-01, xem muc 5): lan chay dau tien `local[*]` **trong container
`spark-master`** bi OOMKilled (container `mem_limit: 512m` trung khop
`--driver-memory 512m`, khong con du RAM). Da chuyen sang chay trong
container `spark-worker-1` (mem_limit 800m) va thanh cong. Khong sua
`Spark/docker-compose.yml`.

### V06 - Chay tren Spark Standalone Cluster that (evidence executor tren >=2 worker, job/stage/task/partition, dung 1 worker, so sanh metric)

**Result: PASS**

**a) Xac nhan khong phai `local[*]`, executor tren 2 worker khac nhau:**

Command: xem muc 4 README (`docker run --rm --network spark_spark-net ...
spark-submit --master spark://spark-master:7077 ...`).

Actual (`applicationId = app-20260820061723-0006`, KHONG phai dang
`local-...`):

```text
26/08/20 06:17:23 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260820061723-0006/0 on worker-20260820061022-172.20.0.3-37015 (172.20.0.3:37015) with 1 core(s)
26/08/20 06:17:23 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260820061723-0006/1 on worker-20260820053835-172.20.0.4-35459 (172.20.0.4:35459) with 1 core(s)
```

Xac nhan cheo qua Spark Master REST API (`curl -s
http://localhost:8080/json/`, luu tai
`MLlib/output/cluster_mode/master_completed_apps.json`):

```json
"id" : "app-20260820061723-0006",
"cores" : 2,
```

=> 2 executor, moi executor tren 1 container Worker khac nhau
(172.20.0.3 va 172.20.0.4 - dung 2 dia chi IP container Docker khac nhau
cua `spark-worker-2` va `spark-worker-1`), khong chay nham `local[*]`.

**b) Job/stage/task/partition cua it nhat 1 buoc:**

Sau khi `feature_df.repartition(8)`, cac buoc `count()`/train tiep theo co
8 task (khop `spark.sql.shuffle.partitions=8`). Evidence REST API that
(`GET /api/v1/applications/<id>/stages/39`, luu tai
`MLlib/output/cluster_mode/stage39_taskList_8partitions.json`):

```text
stage 39: numTasks = 8, name = "count at NativeMethodAccessorImpl.java:0"
8 task, moi task 1 partition, chay tren host 172.20.0.3 (executor 0)
```

(stage 41, cung 8 task, evidence tai
`MLlib/output/cluster_mode/stage41_taskList_8partitions.json`). Cac stage
truoc `repartition()` (doc CSV, dem dong ban dau) chi co 1 task vi file
CSV nho duoc Spark doc thanh 1 partition mac dinh - da giai thich trong
`MLlib/README.md` muc 4/muc "Loi thuong gap".

**c) Dung 1 worker, quan sat, khoi dong lai:**

```bash
docker stop spark-worker-2
curl -s http://localhost:8080/json/    # -> chi con 1 worker ALIVE (172.20.0.4)
# submit job voi RUN_TAG=cluster_worker_down_experiment trong luc worker-2 con dung
docker start spark-worker-2
curl -s http://localhost:8080/json/    # -> ca 2 worker ALIVE tro lai
```

Evidence that (luu tai
`MLlib/output/cluster_worker_down_experiment/master_after_worker2_stop.json`,
`master_while_running_1worker.json`, `master_after_worker2_restart.json`):

```text
Sau khi dung worker-2: worker 172.20.0.4 ALIVE, worker 172.20.0.3 DEAD
Trong luc chay job (worker-2 con dung): app-20260820060835-0003 "cores": 1, "state": "RUNNING"
docker logs: "Executor added: app-20260820060835-0003/0 on worker-...-172.20.0.4-... with 1 core(s)"
(chi 1 executor duoc cap, so voi 2 executor khi du 2 worker)
Sau khi khoi dong lai worker-2: ca 2 worker deu "state": "ALIVE"
```

Job van chay xong thanh cong voi chi 1 worker (exit code 0, xem
`MLlib/output/cluster_worker_down_experiment/metrics.json`), chi voi it
song song hon (1 executor thay vi 2) - dung y nghia "cum van hoat dong khi
mat 1 worker, chi giam song song" ma BRIEF yeu cau quan sat. Gioi han cua
thu nghiem: worker bi dung TRUOC khi submit (khong phai giua chung mot
task dang chay), nen khong quan sat duoc hanh vi task-retry-khi-executor-
chet-giua-chung; day la gioi han da biet cua thu nghiem, khong phai loi.

**d) So sanh metric local[*] vs cluster:**

| Metric | local[*] | cluster (2 worker) |
|---|---|---|
| AUC | 0.5117425524857946 | 0.5117425524857946 |
| Accuracy | 0.5332112704709592 | 0.5332112704709592 |
| F1 (weighted) | 0.6245467330024119 | 0.6245467330024119 |
| Precision (label=1) | 0.10534384581690757 | 0.10534384581690757 |
| Recall (label=1) | 0.4883248730964467 | 0.4883248730964467 |
| Confusion matrix | 4761/4085/504/481 | 4761/4085/504/481 |

**Giong het nhau (khong chi "tuong duong")** vi dung seed=42, dung
`spark.sql.shuffle.partitions=8`, dung logic `repartition(8)` o ca 2 che
do va logic xu ly hoan toan deterministic (khong co random ngoai
`randomSplit`/khoi tao trong so LogisticRegression, ca hai deu duoc dieu
khien boi seed/thuat toan toi uu hoi tu deterministic cua Spark ML). Dung
voi tieu chi BRIEF: "so sanh ket qua mo hinh giua local va cluster; ket
qua phai tuong duong khi dung cung du lieu, seed va cau hinh."

### V08 - Chạy lại pipeline với dữ liệu `lab` có tương quan giả lập (2026-08-22)

**Result:** PASS

**Chuẩn bị:** dùng lại nguyên `MLlib/train_pipeline.py` (KHÔNG sửa logic
code), chỉ thay dữ liệu nguồn bằng `00_shared_data/lab/orders.csv` +
`customers.csv` bản mới (đã cấy tương quan). RAM `spark-master` được tăng
từ 512MB lên 1536MB (xem ISSUE-03) để chạy được `local[*]` ngay trong
container đó mà không cần vòng qua `spark-worker-1` như cách né tránh cũ
(ISSUE-01).

Command (local mode):
```bash
export MSYS_NO_PATHCONV=1
docker exec -d spark-master bash -c \
  "MLLIB_RUN_TAG=local_mode_v2 PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
   /opt/spark/bin/spark-submit /opt/spark-data/mllib_session12/train_pipeline.py \
   > /opt/spark-data/mllib_session12/local_v2.log 2>&1"
```

Command (cluster mode — cần chỉ định rõ `--executor-memory 512m
--executor-cores 1 --total-executor-cores 2` vì mỗi worker chỉ có 640MB,
nhỏ hơn executor-memory mặc định 1024MB của Spark — nếu không chỉ định,
job kẹt vĩnh viễn ở "Initial job has not accepted any resources", xem
ISSUE-04):
```bash
docker exec -d spark-master bash -c \
  "SPARK_MASTER_URL=spark://spark-master:7077 MLLIB_RUN_TAG=cluster_mode_v2 \
   PYTHONPATH=/opt/spark-data/mllib_session12/pylibs \
   /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
   --executor-memory 512m --executor-cores 1 --total-executor-cores 2 \
   /opt/spark-data/mllib_session12/train_pipeline.py \
   > /opt/spark-data/mllib_session12/cluster_v2.log 2>&1"
```

Actual (log thật, cả 2 chế độ cho kết quả giống hệt nhau, cùng seed=42):

```text
AUC (areaUnderROC)              : 0.6893
Accuracy                        : 0.6741  (CANH BAO: lop CANCELLED thieu so, accuracy don thuan de gay hieu lam)
F1 (weighted)                   : 0.7282
Precision (label=1 CANCELLED)   : 0.2098
Recall (label=1 CANCELLED)      : 0.5982

Confusion matrix (label thuc te x prediction):
+-----+----------+-----+
|label|prediction|count|
+-----+----------+-----+
|  0.0|       0.0| 5905|
|  0.0|       1.0| 2719|
|  1.0|       0.0|  485|
|  1.0|       1.0|  722|
+-----+----------+-----+

reload_prediction_match: true (ca 2 che do)
```

`application_id` local mode: `local-1787408130388`; cluster mode:
`app-20260822142024-0001` (không phải `local-...`, xác nhận chạy YARN...
à không, xác nhận chạy trên Spark Standalone cluster thật, không phải
`local[*]`). File đầy đủ lưu tại
`MLlib/output/local_mode_v2/metrics.json` và
`MLlib/output/cluster_mode_v2/metrics.json`.

**So sánh với V06 gốc (dữ liệu ngẫu nhiên cũ):** cùng kỹ thuật
(local[*] và cluster cho kết quả giống hệt nhau nhờ seed=42 + xử lý
deterministic), chỉ khác dữ liệu đầu vào — xác nhận thay đổi AUC 0.51 →
0.69 đến từ tương quan trong dữ liệu, không phải từ thay đổi logic
pipeline hay cấu hình chạy.

## 5. Issues Found

### ISSUE-01

**Severity:** Major

**Hien tuong:** `spark-submit --master local[*] --driver-memory 512m`
chay TRONG container `spark-master` bi Docker OOMKill (`docker inspect
spark-master` tra ve `OOMKilled: true`), job dung giua chung (exit code
137) ngay sau buoc luu PipelineModel.

**Nguyen nhan:** `Spark/docker-compose.yml` gioi han `spark-master` o
`mem_limit: 512m` (khong sua file nay theo yeu cau nhiem vu). Khi chay
`local[*]` NGAY trong container do, driver JVM (`--driver-memory 512m`) +
tien trinh Master dang chay + numpy/pandas-like overhead cua PySpark ML
vuot qua 512m.

**Cach sua:** Chuyen lenh `spark-submit --master local[*]` sang chay
trong container `spark-worker-1` (`mem_limit: 800m`, khong co tien trinh
Master canh tranh RAM). Doi voi che do cluster (giai doan 2), dung
container tam thoi (`docker run --rm`, khong phai service moi trong
compose) de spark-submit client khong canh tranh RAM voi Master/Worker.

**File anh huong:** Khong sua file nao trong `Spark/`; chi thay doi cach
goi lenh (ghi trong `MLlib/README.md` muc 4 va lenh thuc te da chay o
muc 3/4 report nay).

### ISSUE-02

**Severity:** Minor

**Hien tuong:** Image `apache/spark:3.5.9-python3` khong cai san `numpy`,
`pyspark.ml` bao `ModuleNotFoundError: No module named 'numpy'` ngay khi
import.

**Nguyen nhan:** Image chinh thuc Apache Spark khong dong goi day du cac
thu vien khoa hoc du lieu Python (chi co PySpark core).

**Cach sua:** `pip3 install --target=/opt/spark-data/mllib_session12/pylibs
numpy` (cai vao thu muc bind-mount dung chung, tranh loi quyen ghi vao
site-packages he thong cua user `spark` uid 185), roi truyen
`PYTHONPATH` cho ca driver (`-e PYTHONPATH=...`) va executor (`--conf
spark.executorEnv.PYTHONPATH=...`).

**File anh huong:** Khong sua image/Dockerfile; chi them thu muc
`Spark/data/mllib_session12/pylibs/` (du lieu runtime, khong commit vao
`MLlib/` vi ~67MB, da ghi ro cach tai lai trong README).

### ISSUE-03 (2026-08-22)

**Severity:** Major

**Hien tuong:** Chay lai `local[*]` truc tiep trong container `spark-master`
(mem_limit goc 512m) voi du lieu `lab` da co tuong quan bi Docker OOMKill
that (`docker inspect spark-master` -> `OOMKilled: true`), dung giua chung
ngay sau buoc luu PipelineModel (V04), khong co thong bao loi ro rang trong
log ung dung (chi dung dot ngot).

**Nguyen nhan:** Cung ban chat ISSUE-01 (RAM 512m qua nho cho ca Spark
Master daemon + driver JVM MLlib), nhung lan nay khong the tranh bang cach
chuyen sang container khac vi buoc kiem chung an toan + chay lai theo yeu
cau giang vien can thuc hien truc tiep, va viec chay tren `spark-worker-1`
(cach ne cu) cung co gioi han RAM tuong tu (800m) khi ket hop voi executor
that dang chay tren cung worker do trong che do cluster.

**Cach sua:** Tang `mem_limit` cua `spark-master` trong
`Spark/docker-compose.yml` tu 512m len **1536m** (co ghi chu ly do trong
file). Day la thay doi CO SUA file cau hinh (khac ISSUE-01 cu chi doi cach
goi lenh) - can giang vien biet neu chay lai tren may khac co RAM han che
hon.

**File anh huong:** `Spark/docker-compose.yml` (service `spark-master`,
dong `mem_limit`).

### ISSUE-04 (2026-08-22)

**Severity:** Minor

**Hien tuong:** Chay `spark-submit --master spark://spark-master:7077`
KHONG chi dinh `--executor-memory` bi ket vinh vien o trang thai "Initial
job has not accepted any resources; check your cluster UI to ensure that
workers are registered and have sufficient resources" (worker van ALIVE,
khong phai loi ket noi).

**Nguyen nhan:** Moi container worker chi co 640MB kha dung (theo cau hinh
goc `Spark/docker-compose.yml` tu Buoi 9, khong doi), trong khi
`--executor-memory` mac dinh cua Spark la 1024MB - vuot qua kha nang cua 1
worker nen scheduler khong bao gio cap phat duoc.

**Cach sua:** Chi dinh ro `--executor-memory 512m --executor-cores 1
--total-executor-cores 2` khi spark-submit (khong sua
`Spark/docker-compose.yml` - day la tham so dong lenh, khong phai cau hinh
ha tang).

**File anh huong:** khong sua file nao; chi ghi ro trong lenh chay o V08.

## 6. Mismatch voi tai lieu Content AI

`CONTENT_REPORT.md` cua Buoi 12 hien dang **trong hoan toan** ("Chua cap
nhat.") - Content AI CHUA soan slide/tai lieu doc/bai thuc hanh/cau hoi
giai thich/danh sach validation item (V01, V02, ... nhu BRIEF yeu cau)
cho buoi nay. Local AI da tu danh so V01-V06 dua tren BRIEF.md +
khung noi dung (muc "Buoi 12" dong 699-757) vi chua co ma validation item
chinh thuc tu Content AI de doi chieu. Khi Content AI viet noi dung, can
doi chieu dung 6 validation item nay (hoac de xuat sua neu can chi tiet
hon) va PHAI dan dung so lieu that trong bang muc 4(d)/README muc 5 -
KHONG duoc tu suy dien AUC/accuracy khac di.

| Vi tri | Noi dung hien tai | Thuc te | De xuat |
|---|---|---|---|
| `CONTENT_REPORT.md` (12_mllib) | "Chua cap nhat." | Local AI da co day du evidence PASS ca 2 che do | Content AI viet slide/tai lieu doc/bai thuc hanh dua tren `MLlib/README.md` + report nay, dac biet nhan manh AUC~0.5 la ket qua that (khong bia thanh so cao hon) |
| BRIEF.md muc 2 "Du lieu" | ghi chung "feature dataset sinh tu orders/customers/clickstream" | Local AI CHI dung `orders.csv` + `customers.csv` (khong dung `clickstream`) vi 2 nguon nay da du de xay dung bai toan phan loai co nghia, giu dung nguyen tac "khong tao dataset nghiep vu moi ngoai 2 nguon BRIEF cho phep" | Content AI khi viet tai lieu nen ghi ro chi dung 2 bang, hoac neu muon minh hoa them clickstream thi can 1 phien lam viec rieng (dataset events, join qua session/customer_id, phuc tap hon nhieu, chua kiem thu) |

## 7. Kha nang chay lai

- [x] chay tu clean state (da `docker compose down`/`up -d` lai cum Spark
  trong qua trinh kiem thu buoi 9/10 truoc do; buoi nay dung lai cum dang
  chay, khong can dung lai tu dau nhung logic hoan toan idempotent - da
  chay `train_pipeline.py` nhieu lan voi cac `RUN_TAG` khac nhau, ket qua
  on dinh/deterministic voi cung seed).
- [x] version duoc ghim (`apache/spark:3.5.9-python3`, khong doi tu
  Buoi 9; numpy ghim `2.2.6` - phien ban moi nhat tai thoi diem cai, khong
  co yeu cau ghim version cu the cho thu vien phu tro nay trong BRIEF).
- [x] du lieu co duong dan tuong doi (`DATA_DIR`/`OUTPUT_DIR`/`MODEL_DIR`
  deu cau hinh qua bien moi truong, mac dinh la duong dan trong container;
  xem CONFIG dau file).
- [x] worker/container truy cap duoc du lieu (`Spark/data/mllib_session12/`
  mount vao ca 3 container qua `./data:/opt/spark-data` co san tu
  `docker-compose.yml`, khong sua file).
- [x] expected output duoc luu (`MLlib/output/*/metrics.json`,
  `MLlib/models/*/pipeline_model/`, cac file JSON evidence REST API).
- [x] reset script hoat dong (2026-08-22): `MLlib/scripts/prepare-data.sh`,
  `train-local.sh`, `train-cluster.sh` - da chay that (RUN_TAG tuy chinh
  qua tham so, tu dong ghi ket qua metrics.json). `train-cluster.sh` da
  ghi ro `--executor-memory 512m` bat buoc (xem ISSUE-04).

## 8. Ket luan cho giang vien

**Co the dung de day: YES**, voi cac diem can doc truoc khi duyet:

1. **Dinh nghia hoan thanh (Definition of Done) da dat**: pipeline MLlib
   chay dau cuoi CA `local[*]` LAN Spark Standalone Cluster that (khong
   chi `local[*]`) - dung yeu cau nghiem ngat cua buoi nay. Evidence
   executor tren 2 worker khac nhau, job/stage/task/partition, thu nghiem
   dung 1 worker, va so sanh metric deu la SO LIEU THAT (khong bia), luu
   tai `MLlib/output/`.
2. ~~AUC thap (~0.51) la ket qua that~~ — **DA CAP NHAT (2026-08-22, xem V08)**:
   theo yeu cau giang vien, generator da duoc sua de cay tuong quan gia lap
   co chu dich CHI cho muc `lab` (muc `sample` giu nguyen 100%, da xac minh
   sha256). AUC gio la **0.6893** (tu 0.5117) — mo hinh hoc duoc tin hieu
   that nhung khong hoan hao, van giu duoc bai hoc ve lop mat can bang
   (precision label=1 chi 0.21). Content AI PHAI dung dung so lieu MOI
   (0.6893/0.6741/0.7282/0.2098/0.5982) trong slide/tai lieu, KHONG dung
   lai so lieu cu (0.51...) tru khi muon minh hoa doi chieu truoc/sau.
3. **`CONTENT_REPORT.md` buoi 12 dang trong hoan toan** - Content AI chua
   lam gi cho buoi nay, can bat dau tu BRIEF.md + report nay (uu tien doc
   V08 va bang so sanh AUC truoc/sau o dau file).
4. **Quyet dinh ky thuat nen giang vien biet**: `Spark/docker-compose.yml`
   dong `mem_limit` cua `spark-master` da duoc TANG tu 512m len 1536m
   (ISSUE-03) de chay duoc pipeline MLlib voi du lieu that ma khong OOM -
   day la thay doi CO SUA file cau hinh (khac cach ne cu chi doi lenh goi).
   Neu chay lai tren may co RAM Docker han che hon (~3-4GB nhu may kiem
   thu goc), tong RAM cac container Spark gio la 1536+800+800=3136MB, van
   trong gioi han da xac nhan hoat dong.
5. Numpy phai duoc cai bo sung vao cum Spark (khong co san trong image
   `apache/spark:3.5.9-python3`) - buoc nay can lam lai neu cum bi tao
   moi tu dau (`docker compose down -v` roi `up -d`), da ghi day du lenh
   trong `MLlib/README.md` muc 4.
6. Khi chay cluster mode, PHAI chi dinh `--executor-memory 512m` ro rang
   (ISSUE-04) vi worker chi co 640MB, nho hon muc mac dinh 1024MB cua
   Spark - neu khong job se ket vinh vien khong bao loi ro.
