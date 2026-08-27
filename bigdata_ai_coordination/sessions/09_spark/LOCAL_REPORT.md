# LOCAL VALIDATION REPORT – Buổi 9 (Apache Spark)

## 1. Trạng thái

**Validation:** PASS

Cụm Spark Standalone (1 Master + 2 Worker) chạy thật bằng Docker Compose, đã
`spark-submit` một job PySpark thật dùng dữ liệu RetailStream (orders +
order_items + web_logs, mức sample), xác nhận job chạy phân tán trên 2
executor nằm ở 2 container Worker khác nhau (không phải `local[*]`), có bằng
chứng job/stage/task/partition/shuffle trích từ Spark Master REST API
(`:8080/json/`) và Spark Application REST API (`:4040/api/v1/...`) khi job
đang chạy, cùng log `spark-submit` đầy đủ.

## 2. Environment

| Thành phần | Version |
|---|---|
| OS | Windows 10 Pro 10.0.19045, Docker Desktop (WSL2 backend) |
| Docker | 29.7.2 |
| Docker Compose | v5.4.0 |
| Image Spark (Master + 2 Worker) | `apache/spark:3.5.9-python3` (Apache Spark 3.5.9, Scala 2.12.18, OpenJDK 11.0.31, Python 3) — ghim version, không dùng `latest` |
| Số worker | 2 (spark-worker-1, spark-worker-2), mỗi worker 1 core / 640 MiB Spark-memory (`--cores 1 --memory 640m`) |
| RAM cấp cho mỗi container (Docker `mem_limit`) | spark-master 512m, spark-worker-1 800m, spark-worker-2 800m |
| RAM Docker Desktop VM (tổng, dùng chung cho mọi buổi) | 3.826 GiB, 2 CPU |
| Dữ liệu | `00_shared_data/sample/orders_sample.csv` (150 bản ghi), `order_items_sample.csv`, `web_logs_sample.jsonl` (200 dòng) — copy nguyên vẹn vào `Spark/data/`, không sửa |

**Đổi image so với gợi ý ban đầu (bitnami/spark → apache/spark):** tại thời
điểm triển khai, Docker Hub báo `bitnami/spark` "no longer available for free
through Docker Hub" (Bitnami đã chuyển sang mô hình Secure Images trả phí,
kiểm tra qua `GET https://hub.docker.com/v2/repositories/bitnami/spark/` thấy
`full_description` xác nhận). Đã đổi sang ảnh chính thức của dự án Apache
Spark, `docker.io/apache/spark:3.5.9-python3`, vẫn ghim version cụ thể. Không
vi phạm `00_DECISIONS.md` (D004 chỉ yêu cầu "Spark Standalone Docker cluster
1 master + 2 worker", không chỉ định nhà cung cấp image) nên không cần mở
quyết định mới, nhưng ghi rõ ở đây để giảng viên biết.

## 3. Cách khởi động

```bash
export MSYS_NO_PATHCONV=1     # Git Bash trên Windows
cd Spark
docker compose up -d
sleep 10
curl -s http://localhost:8080/json/     # kiểm tra status=ALIVE, 2 worker ALIVE
```

Chạy job demo:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=6 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/demo_job.py
```

## 4. Validation Results

### V01 – Cụm Spark Standalone khởi động đủ 1 Master + 2 Worker, đều ALIVE

**Result:** PASS

Command:

```bash
curl -s http://localhost:8080/json/
```

Actual (rút gọn, sau `docker compose up -d`, chờ ~10s):

```json
{
  "status": "ALIVE",
  "workers": [
    {"id": "worker-20260820053007-172.20.0.3-44357", "host": "172.20.0.3", "cores": 1, "memory": 512, "state": "ALIVE"},
    {"id": "worker-20260820053007-172.20.0.4-45547", "host": "172.20.0.4", "cores": 1, "memory": 512, "state": "ALIVE"}
  ]
}
```

(Lần chạy này ghi lại trước khi tăng `--memory` worker lên 640m; các lần sau
memory hiển thị 640 — không ảnh hưởng kết quả PASS.)

Notes: đã lặp lại kiểm thử này 3 lần trong phiên làm việc (kể cả sau
`docker compose down && up -d` từ trạng thái sạch hoàn toàn) — luôn PASS,
cụm luôn có đúng 2 worker ALIVE trong vòng 10-12 giây sau khi start.

---

### V02 – `spark-submit` chạy job thật thành công (exit code 0, có output)

**Result:** PASS

Command:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=6 \
  --executor-memory 512m --driver-memory 512m \
  /opt/spark-apps/demo_job.py
echo "exit code: $?"
```

Actual:

```text
exit code: 0
```

Output ghi ra `Spark/data/output/revenue_by_status_csv/part-00000-*.csv`:

```text
status,total_revenue,order_count,line_item_count
SHIPPED,3.926282E9,63,151
PAID,2.879375E9,48,117
CREATED,1.779847E9,20,60
CANCELLED,1.195253E9,19,52
```

`Spark/data/output/web_logs_status_code_csv/part-00000-*.csv`:

```text
status_code,count
200,183
500,11
404,6
```

Đối chiếu độc lập (không dùng Spark) bằng script Python đọc trực tiếp
`orders_sample.csv` + `order_items_sample.csv` / `web_logs_sample.jsonl`
(join + groupBy thủ công bằng `csv.DictReader`/`collections.Counter`) cho
kết quả khớp 100% với output của Spark ở trên (cả 4 dòng doanh thu theo
status và 3 dòng status_code).

---

### V03 – Job chạy phân tán trên cluster thật, không phải `local[*]`

**Result:** PASS

Bằng chứng 1 — `sc.master` trong log job (in trực tiếp từ `demo_job.py`):

```text
Spark master (deploy) : spark://spark-master:7077
Application ID        : app-20260820053546-0000
Default parallelism   : 2
```

`spark://spark-master:7077` là URL Standalone Master, không phải `local[*]`.

Bằng chứng 2 — log `spark-submit` cho thấy 2 executor được cấp phát trên 2
container Worker khác nhau (địa chỉ IP nội bộ Docker network `spark-net`):

```text
INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20260820053546-0000
INFO StandaloneSchedulerBackend: Granted executor ID app-20260820053546-0000/0 on hostPort 172.20.0.3:33677 with 1 core(s), 512.0 MiB RAM
INFO StandaloneSchedulerBackend: Granted executor ID app-20260820053546-0000/1 on hostPort 172.20.0.4:43117 with 1 core(s), 512.0 MiB RAM
INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260820053546-0000/0 is now RUNNING
INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260820053546-0000/1 is now RUNNING
```

Task thực thi xen kẽ trên cả hai IP (trích một số dòng `TaskSetManager`):

```text
Starting task 0.0 in stage 0.0 (TID 0) (172.20.0.4, executor 1, partition 0, PROCESS_LOCAL, ...)
Starting task 0.0 in stage 1.0 (TID 1) (172.20.0.3, executor 0, partition 0, PROCESS_LOCAL, ...)
Starting task 0.0 in stage 4.0 (TID 4) (172.20.0.4, executor 1, partition 0, PROCESS_LOCAL, ...)
```

Bằng chứng 3 — Spark **Application REST API** (`http://localhost:4040`,
lấy được khi curl trong lúc job còn đang chạy) liệt kê executor "driver"
(bên trong container `spark-master`) và 2 executor "0"/"1" trên
`172.20.0.4:...` / `172.20.0.3:...`, đúng số task hoàn thành khớp tổng số
task của job (trích rút gọn):

```json
[
  {"id": "driver", "hostPort": "83f27c48716d:41593", "totalCores": 0, "completedTasks": 0},
  {"id": "1", "hostPort": "172.20.0.3:38953", "totalCores": 1, "completedTasks": 14, "totalShuffleRead": 16376, "totalShuffleWrite": 16419},
  {"id": "0", "hostPort": "172.20.0.4:40581", "totalCores": 1, "completedTasks": 19, "totalShuffleRead": 16708, "totalShuffleWrite": 29834}
]
```

Bằng chứng 4 — Spark **Master REST API** sau khi job kết thúc, mục
`completedapps`:

```json
{"id": "app-20260820053546-0000", "name": "RetailStream-Buoi9-DemoJob", "cores": 2, "state": "FINISHED", "duration": 35682}
```

`"cores": 2` khớp đúng 2 executor x 1 core, xác nhận job dùng tài nguyên của
cả 2 worker trong cluster (không phải chạy trên 1 tiến trình local).

---

### V04 – Partition có thể quan sát và điều khiển được

**Result:** PASS

`demo_job.py` gọi `.repartition(4)` cho `orders` và `order_items`, in số
partition thực tế trước khi xử lý:

```text
orders partitions      : 4
order_items partitions : 4
```

Log DAGScheduler xác nhận số task submit đúng bằng số partition tại các
stage đọc/ghi dữ liệu đã repartition (ví dụ `ResultStage 9`, `ShuffleMapStage
11`, `ShuffleMapStage 33`, `ShuffleMapStage 59`):

```text
INFO DAGScheduler: Submitting 4 missing tasks from ResultStage 9 (MapPartitionsRDD[47] at collect at /opt/spark-apps/demo_job.py:84) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
```

Ghi chú sư phạm: job đặt `spark.sql.shuffle.partitions=6`, nhưng vì Spark
3.5 bật **AQE (Adaptive Query Execution)** mặc định, số partition sau
shuffle được **coalesce xuống 4** ở nhiều stage (do dữ liệu mẫu quá nhỏ để
cần 6 partition riêng) — đây là ví dụ tốt để giảng "Spark không luôn dùng
đúng số partition cấu hình tĩnh, AQE có thể tối ưu lại lúc runtime", nên đưa
vào tài liệu đọc Buổi 9/10 nếu Content AI thấy phù hợp.

---

### V05 – Shuffle quan sát được qua join + groupBy (wide transformation)

**Result:** PASS

Physical plan (`explain(mode="formatted")`, trích) cho thấy 2 điểm `Exchange`
(shuffle boundary) quanh `BroadcastHashJoin` và các tầng `HashAggregate`:

```text
(31) Exchange
Arguments: RoundRobinPartitioning(4), REPARTITION_BY_NUM, [plan_id=112]
(32) BroadcastHashJoin
Join type: Inner
(35) Exchange
Arguments: hashpartitioning(status#20, order_id#17, 6), ENSURE_REQUIREMENTS, [plan_id=125]
(38) Exchange
Arguments: hashpartitioning(status#20, 6), ENSURE_REQUIREMENTS, [plan_id=129]
(40) Exchange
Arguments: rangepartitioning(total_revenue#97 DESC NULLS LAST, 6), ENSURE_REQUIREMENTS, [plan_id=132]
```

Số liệu shuffle thật (đọc/ghi qua network/disk giữa các executor) lấy từ
Spark Application REST API (`/api/v1/applications/<id>/executors`), executor
"0" và "1" đều có `totalShuffleRead`/`totalShuffleWrite` > 0 (16.7 KB / 29.8
KB và 16.4 KB / 16.4 KB tương ứng) — xác nhận dữ liệu thật sự di chuyển giữa
2 container Worker qua mạng Docker `spark-net`, không chỉ tính trong 1 tiến
trình.

Toàn bộ ứng dụng phát sinh **14 Spark job** và **31 stage** (đếm số dòng
`Submitting N missing tasks` trong log `spark-submit`, phù hợp với việc job
demo gọi nhiều action: `collect()`, `show()`, và 2 lần `.write.csv()`).

---

### V06 – Job có thể chạy lại từ trạng thái sạch (clean state)

**Result:** PASS

Command:

```bash
cd Spark
docker compose down
docker compose up -d
sleep 12
curl -s http://localhost:8080/json/   # -> status ALIVE, 2 worker ALIVE
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --conf spark.sql.shuffle.partitions=6 \
  --executor-memory 512m --driver-memory 512m /opt/spark-apps/demo_job.py
echo "exit code: $?"
```

Actual: cụm lên lại đúng 2 worker ALIVE, job chạy lại thành công,
`exit code: 0`, Application ID mới (`app-20260820053854-0000`), output CSV
được ghi đè thành công (không có lỗi permission/mount).

---

### V07 – Chạy song song an toàn với MongoDB (buổi 5) + HDFS 3 container (buổi 6)

**Result:** PASS (không cần tắt container nào)

Trước khi bật Spark, `docker stats --no-stream` cho thấy Mongo (1 container)
+ HDFS (NameNode + 2 DataNode) đang chạy, tổng RAM dùng khoảng 830 MiB /
3.826 GiB giới hạn Docker Desktop VM. Sau khi bật đủ cụm Spark (3 container)
và chạy xong job, `docker stats --no-stream` ghi nhận:

```text
NAME             MEM USAGE / LIMIT
spark-worker-2   154.9MiB / 800MiB
spark-worker-1   159.8MiB / 800MiB
spark-master     73MiB / 512MiB
hdfs-datanode1   205.2MiB / 3.826GiB
hdfs-datanode2   221.1MiB / 3.826GiB
hdfs-namenode    284.5MiB / 3.826GiB
mongodb          98.64MiB / 3.826GiB
```

Tổng ~1.2 GiB trên 3.826 GiB giới hạn — còn dư nhiều, không cần tắt MongoDB.
**Quyết định:** giữ nguyên MongoDB và HDFS chạy suốt phiên làm việc Buổi 9,
không `docker compose down` bất kỳ dịch vụ nào của buổi khác. Kết thúc phiên,
cả 7 container (Mongo, 3 HDFS, 3 Spark) đều đang chạy — không cần khởi động
lại gì thêm.

## 5. Issues Found

### ISSUE-01

**Severity:** Minor

**Hiện tượng:** Image `bitnami/spark` (gợi ý trong đề bài dạng "ví dụ") không
còn kéo được miễn phí từ Docker Hub.

**Nguyên nhân:** Bitnami chuyển sang mô hình "Bitnami Secure Images" thương
mại từ giữa 2025; kiểm tra `GET
https://hub.docker.com/v2/repositories/bitnami/spark/` xác nhận thông báo
"no longer available for free through Docker Hub".

**Cách sửa:** đổi sang ảnh chính thức `apache/spark:3.5.9-python3`
(`docker.io/apache/spark`), ghim version cụ thể, hoạt động tương đương cho
mục tiêu nhập môn Spark Standalone.

**File ảnh hưởng:** `Spark/docker-compose.yml`, `Spark/README.md` (đã ghi
chú rõ lý do đổi image).

### ISSUE-02

**Severity:** Minor

**Hiện tượng:** Container Master/Worker của `apache/spark` thoát ngay lập
tức nếu dùng lệnh khởi động kiểu `sbin/start-master.sh`/`start-worker.sh`
(cách phổ biến trong nhiều hướng dẫn Spark Standalone Docker cũ, vốn viết
cho image `bitnami/spark`).

**Nguyên nhân:** entrypoint của `apache/spark` không có logic giữ tiến
trình foreground cho các script `sbin/*` — các script này tự daemonize
(spark-daemon.sh) rồi trả về, khiến tiến trình PID 1 của container kết
thúc ngay.

**Cách sửa:** dùng trực tiếp `bin/spark-class
org.apache.spark.deploy.master.Master`/`Worker` làm `command:` trong
`docker-compose.yml` — chạy foreground, giữ container sống.

**File ảnh hưởng:** `Spark/docker-compose.yml`.

### ISSUE-03

**Severity:** Minor

**Hiện tượng:** `spark-submit` báo lỗi `INVALID_DRIVER_MEMORY` /
`INVALID_EXECUTOR_MEMORY` khi để giá trị mặc định hoặc quá thấp
(ví dụ 384m).

**Nguyên nhân:** Spark 3.5 yêu cầu tối thiểu ~450 MiB cho
`UnifiedMemoryManager`, thấp hơn sẽ bị chặn ngay khi khởi tạo
`SparkContext`.

**Cách sửa:** đặt `--driver-memory 512m --executor-memory 512m` (đã cập
nhật `mem_limit` của Worker container lên 800m để đủ chỗ cho JVM executor
512m + overhead).

**File ảnh hưởng:** `Spark/README.md` (mục 5, mục 7), `Spark/docker-compose.yml`.

## 6. Mismatch với tài liệu Content AI

| Vị trí | Nội dung hiện tại | Thực tế | Đề xuất |
|---|---|---|---|
| `bigdata_ai_coordination/sessions/09_spark/CONTENT_REPORT.md` | File hiện đang **trống hoàn toàn** (0 nội dung) | Local Validation đã PASS, có đầy đủ evidence (job/stage/task/partition/shuffle) sẵn sàng làm ngữ cảnh | Content AI cần viết slide/tài liệu đọc/bài thực hành Buổi 9 dựa trên `LOCAL_REPORT.md` này (đặc biệt mục V03-V05 để minh họa driver/executor/stage/shuffle bằng số liệu thật, không phải số liệu giả định), rồi tự cập nhật `CONTENT_REPORT.md` |
| Đề bài gợi ý `bitnami/spark` | — | `bitnami/spark` không còn free trên Docker Hub | Đã tự thay bằng `apache/spark:3.5.9-python3`, ghi rõ lý do ở README + báo cáo này; nếu Content AI có ví dụ lệnh Docker tham chiếu `bitnami/spark`, cần sửa lại theo `apache/spark` |

## 7. Khả năng chạy lại

- [x] chạy từ clean state (`docker compose down` rồi `up -d`, chạy lại job thành công — mục V06);
- [x] version được ghim (`apache/spark:3.5.9-python3`, không dùng `latest`);
- [x] dữ liệu có đường dẫn tương đối (`./data`, `./jobs` trong `docker-compose.yml`, copy từ `00_shared_data/sample/`);
- [x] worker/container truy cập được dữ liệu (cả 3 container mount `./data` -> `/opt/spark-data`, `./jobs` -> `/opt/spark-apps`);
- [x] expected output được lưu (`Spark/data/output/*.csv`, đối chiếu độc lập bằng script Python ngoài Spark, khớp 100%);
- [x] reset script hoạt động (2026-08-22): `Spark/scripts/start-cluster.sh`
      (chờ healthy), `stop-cluster.sh` (cảnh báo nếu Kafka đang gắn network),
      `run-demo-job.sh` (có `--executor-memory 512m` bắt buộc, xem lưu ý
      RAM worker) — đã chạy thật thành công.

## 8. Kết luận cho giảng viên

Có thể dùng để dạy: **YES**

Các điểm cần đọc trước khi duyệt:

1. Đã đổi image `bitnami/spark` (không còn free) sang `apache/spark:3.5.9-python3` — không vi phạm `00_DECISIONS.md` (D004 không ràng buộc nhà cung cấp image) nhưng khác với ví dụ nêu trong đề bài, cần Content AI/giảng viên cập nhật mọi tài liệu tham chiếu tên image cũ.
2. Không cần tắt MongoDB hay HDFS trong suốt phiên làm việc — RAM đủ dư (dùng ~1.2 GiB / 3.826 GiB) khi chạy đồng thời cả 3 công nghệ; nếu giảng viên demo trên máy yếu hơn nên theo dõi `docker stats` trước.
3. `Spark/` chưa có thư mục `scripts/` (start/stop/reset) như `MongoDb/`/`Hdfs/` — hiện dùng trực tiếp `docker compose up -d`/`down`, đơn giản và đủ dùng cho quy mô bài Buổi 9 (3 container, không cấu hình phức tạp).
4. `CONTENT_REPORT.md` Buổi 9 hiện trống — cần Content AI hoàn thiện dựa trên báo cáo này trước khi công bố tài liệu học phần.
5. Evidence job/stage/task/partition/shuffle trong mục 4 (V01-V07) là số liệu thật lấy trực tiếp từ Spark Master/Application REST API và log `spark-submit` thật, không suy đoán — an toàn để Content AI trích dẫn nguyên văn vào slide/tài liệu.
