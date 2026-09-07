# Spark Standalone cluster – Buổi 9 (RetailStream)

Cụm Spark Standalone tối thiểu (1 Master + 2 Worker) chạy bằng Docker Compose,
dùng để chạy một job PySpark thật (`spark-submit`) trên dữ liệu RetailStream
mức sample.

Khi dữ liệu quá lớn cho một máy, Spark chia công việc ra nhiều máy chạy song
song — hiểu công việc được chia và phối hợp thế nào (qua driver/executor,
partition, job/stage/task) giúp giải thích được vì sao job chạy nhanh hay
chậm, và shuffle (bước xáo trộn dữ liệu qua mạng giữa các máy) là nguyên nhân
phổ biến nhất khiến job chậm.

## 1. Yêu cầu môi trường

Spark Standalone Cluster gồm 1 Master (điều phối, phân việc cho các Worker,
không trực tiếp xử lý dữ liệu) và nhiều Worker (máy thực thi công việc thật,
chạy các executor) — cụm trong tài liệu này dùng 1 Master + 2 Worker.

| Mục | Yêu cầu | Ghi chú |
|---|---|---|
| Docker Engine / Docker Desktop | hỗ trợ Compose v2 (`docker compose`) | đã kiểm thử với Docker Compose v2 |
| RAM dành cho Docker | tối thiểu 2 GB dành riêng cho cụm Spark | cụm 3 container dùng khoảng 450–500 MiB RSS lúc idle, tăng khi chạy job |
| Đĩa trống | tối thiểu 2 GB | image `apache/spark:3.5.9-python3` ~ 900 MB |
| Cổng trống trên host | 7077, 8080, 8081, 8082, 4040 | xem mục "Lỗi thường gặp" nếu bị chiếm |

Image dùng: `apache/spark:3.5.9-python3` (Apache Spark 3.5.9, Scala 2.12.18,
OpenJDK 11.0.31, Python 3), ghim version cụ thể, không dùng `latest`.

**Vì sao không dùng `bitnami/spark`:** tại thời điểm triển khai, Docker Hub
trả về thông báo `This image is no longer available for free through Docker
Hub` cho `bitnami/spark` (Bitnami đã chuyển sang mô hình Secure Images trả
phí). Dự án dùng ảnh chính thức của dự án Apache Spark
(`docker.io/apache/spark`) thay thế.

## 2. Cấu trúc thư mục

```text
Spark/
├── README.md
├── docker-compose.yml
├── jobs/
│   └── demo_job.py          # job PySpark demo trên orders/order_items/web_logs
├── data/
│   ├── orders_sample.csv        # 150 bản ghi
│   ├── order_items_sample.csv
│   ├── web_logs_sample.jsonl    # 200 dòng
│   └── output/                  # sinh ra sau khi chạy demo_job.py
└── scripts/
    ├── start-cluster.sh     # khởi động cụm Spark Standalone và chờ healthy
    ├── run-demo-job.sh      # nộp job demo với thông số RAM tối ưu
    └── stop-cluster.sh      # dừng cụm an toàn (kiểm tra liên kết Kafka)
```

`./data` và `./jobs` được mount vào cả 3 container tại `/opt/spark-data` và
`/opt/spark-apps` (đọc/ghi được — job cần ghi kết quả CSV ra
`/opt/spark-data/output`).

## 3. Khởi động cụm

```bash
cd Spark
docker compose up -d
```

Đợi khoảng 10–12 giây rồi kiểm tra cụm đã lên đủ 1 Master + 2 Worker ở trạng
thái `ALIVE`:

```bash
curl -s http://localhost:8080/json/
```

Đây là REST API do chính Spark Master expose ra để truy vấn trạng thái cụm
dưới dạng JSON — không cần cài thêm công cụ nào ngoài `curl`.

Kết quả kỳ vọng (rút gọn):

```json
{
  "status": "ALIVE",
  "workers": [
    {"host": "172.20.0.3", "cores": 1, "memory": 640, "state": "ALIVE"},
    {"host": "172.20.0.4", "cores": 1, "memory": 640, "state": "ALIVE"}
  ]
}
```

Giao diện web:

- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081 (worker 1), http://localhost:8082 (worker 2)
- Spark Application UI: http://localhost:4040 — chỉ tồn tại khi có job đang
  chạy (driver còn sống bên trong container `spark-master`)

## 4. Chạy job demo trên cụm thật

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=6 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/demo_job.py
```

Giải thích các flag chính: `--master spark://spark-master:7077` chỉ định
job kết nối vào cụm Standalone thật (thay vì chạy `local[*]` trên 1 máy);
`--conf spark.sql.shuffle.partitions=6` đặt số partition Spark tạo ra sau
mỗi bước shuffle (mặc định là 200, quá nhiều cho dữ liệu sample nhỏ nên
giảm xuống 6); `--executor-memory`/`--driver-memory` đặt RAM cấp cho mỗi
executor và cho driver.

Trên Windows dùng Git Bash, đặt `export MSYS_NO_PATHCONV=1` trước khi chạy
lệnh trên để tránh Git Bash tự chuyển đường dẫn `/opt/...` thành đường dẫn
Windows (`C:/opt/...`).

`demo_job.py` đọc `orders_sample.csv` và `order_items_sample.csv`, join theo
`order_id`, tính tổng doanh thu (`quantity * unit_price`) và số đơn theo
`status`; đồng thời đọc `web_logs_sample.jsonl` và đếm số request theo
`status_code`. Cả hai kết quả được ghi ra `data/output/` dưới dạng CSV.

**Lưu ý bộ nhớ:** `--executor-memory`/`--driver-memory` phải >= 450m — Spark
3.5 yêu cầu tối thiểu khoảng 450 MiB cho `UnifiedMemoryManager`; dưới mức đó
`spark-submit` báo lỗi `INVALID_DRIVER_MEMORY`/`INVALID_EXECUTOR_MEMORY` ngay
khi khởi tạo `SparkContext`.

## 5. Kết quả tham chiếu (đã kiểm thử thật)

Chạy thành công với `exit code: 0`. File
`data/output/revenue_by_status_csv/part-00000-*.csv`:

```text
status,total_revenue,order_count,line_item_count
SHIPPED,3.926282E9,63,151
PAID,2.879375E9,48,117
CREATED,1.779847E9,20,60
CANCELLED,1.195253E9,19,52
```

File `data/output/web_logs_status_code_csv/part-00000-*.csv`:

```text
status_code,count
200,183
500,11
404,6
```

Kết quả đã được đối chiếu độc lập bằng một script Python đọc trực tiếp
`orders_sample.csv` + `order_items_sample.csv` / `web_logs_sample.jsonl`
(join/groupBy thủ công bằng `csv.DictReader`/`collections.Counter`, không
dùng Spark) — khớp 100% với output của Spark ở trên.

## 6. Xác nhận job chạy phân tán, không phải `local[*]`

Phần này đưa ra 3 bằng chứng độc lập cho thấy job thật sự chạy trên cụm
Standalone (nhiều máy), không phải chạy đơn lẻ trên máy driver.

### Bằng chứng 1: log driver

Log job in trực tiếp từ `demo_job.py` xác nhận cụm Standalone thật:

```text
Spark master (deploy) : spark://spark-master:7077
Application ID        : app-20260820053546-0000
Default parallelism   : 2
```

Log `spark-submit` cho thấy 2 executor được cấp phát trên 2 container Worker
khác nhau (địa chỉ IP nội bộ Docker network `spark-net`):

```text
Granted executor ID app-20260820053546-0000/0 on hostPort 172.20.0.3:33677 with 1 core(s), 512.0 MiB RAM
Granted executor ID app-20260820053546-0000/1 on hostPort 172.20.0.4:43117 with 1 core(s), 512.0 MiB RAM
```

### Bằng chứng 2: Spark Application REST API

Spark Application REST API (`http://localhost:4040/api/v1/applications/<id>/executors`,
lấy được khi curl trong lúc job đang chạy) liệt kê 2 executor với số task
hoàn thành và dữ liệu shuffle đọc/ghi thật (rút gọn):

```json
[
  {"id": "1", "hostPort": "172.20.0.3:38953", "completedTasks": 14, "totalShuffleRead": 16376, "totalShuffleWrite": 16419},
  {"id": "0", "hostPort": "172.20.0.4:40581", "completedTasks": 19, "totalShuffleRead": 16708, "totalShuffleWrite": 29834}
]
```

### Bằng chứng 3: Spark Master REST API sau khi job kết thúc

`completedapps`: `{"cores": 2, "state": "FINISHED", "duration": 35682}` —
khớp đúng 2 executor x 1 core.

Toàn bộ ứng dụng phát sinh **14 Spark job** và **31 stage** (đếm số dòng
`Submitting N missing tasks` trong log `spark-submit`, do job gọi nhiều
action: `collect()`, `show()` và 2 lần `.write.csv()`).

## 7. Quan sát partition và Adaptive Query Execution (AQE)

**Adaptive Query Execution (AQE)** là cơ chế Spark tự động điều chỉnh lại kế
hoạch thực thi (ví dụ số partition sau shuffle) dựa trên số liệu thống kê
thật đo được lúc chạy, thay vì chỉ dựa vào cấu hình tĩnh đặt sẵn.
**Coalesce** ở đây nghĩa là gộp bớt các partition nhỏ lại với nhau để giảm số
lượng partition, tránh việc có quá nhiều partition nhỏ gây lãng phí overhead.

`demo_job.py` gọi `.repartition(4)` cho `orders` và `order_items`, in số
partition thực tế:

```text
orders partitions      : 4
order_items partitions : 4
```

Job đặt `spark.sql.shuffle.partitions=6`, nhưng vì Spark 3.5 bật AQE mặc
định, số partition sau shuffle bị **coalesce xuống 4** ở nhiều stage (dữ liệu
mẫu quá nhỏ để cần 6 partition riêng). Đây là ví dụ thật cho thấy cấu hình
partition tĩnh chỉ là gợi ý ban đầu, AQE có thể tối ưu lại lúc runtime.

## 8. Chạy lại từ trạng thái sạch

```bash
cd Spark
docker compose down
docker compose up -d
```

Đợi khoảng 12 giây, kiểm tra lại `curl -s http://localhost:8080/json/` (2
worker `ALIVE`), rồi chạy lại lệnh `spark-submit` ở mục 4. Đã kiểm thử: cụm
lên lại đúng 2 worker `ALIVE`, job chạy lại thành công với Application ID
mới, output CSV được ghi đè không lỗi permission/mount.

## 9. Dừng cụm

```bash
cd Spark
docker compose down
```

Không cần thêm `-v` — không có volume dữ liệu quan trọng cần dọn; giữ `-v`
lại phòng khi cần xóa hẳn output cũ.

## 10. Chạy song song với MongoDB và HDFS (buổi khác)

Cụm Spark (3 container) đã kiểm thử chạy đồng thời với MongoDB (buổi 5) và
HDFS NameNode + 2 DataNode (buổi 6) trên cùng máy mà không cần tắt container
nào. Tổng RAM sử dụng khi cả 7 container cùng chạy khoảng 1,2 GiB, trên máy
kiểm thử có 3,826 GiB cấp cho Docker Desktop VM — còn dư nhiều. Nếu máy có ít
RAM hơn, kiểm tra trước bằng `docker stats --no-stream` và tạm
`docker compose down` (không kèm `-v`) ở thư mục `MongoDb/` hoặc `Hdfs/` để
giải phóng bộ nhớ nếu cần.

## 11. Lỗi thường gặp

- **`INVALID_DRIVER_MEMORY` / `INVALID_EXECUTOR_MEMORY`**: bộ nhớ cấp cho
  driver/executor dưới ngưỡng tối thiểu Spark 3.5 yêu cầu (~450 MiB). Tăng
  `--driver-memory`/`--executor-memory` lên >= 512m.
- **Container Master/Worker thoát ngay sau khi start**: xảy ra nếu đổi
  `command:` trong `docker-compose.yml` sang `sbin/start-master.sh`/
  `start-worker.sh` — hai script này daemonize (tách tiến trình con chạy nền
  rồi tự thoát) tiến trình rồi thoát ngay, khiến PID 1 của container kết
  thúc theo. Theo quy tắc Docker, tiến trình PID 1 là tiến trình chính của
  container; nếu nó kết thúc — kể cả khi đã "daemonize" một tiến trình con
  vẫn đang chạy — Docker coi container đã xong việc và tự dừng container,
  bất kể tiến trình con kia còn sống hay không. Giữ nguyên cách gọi trực tiếp
  `bin/spark-class org.apache.spark.deploy.master.Master` /
  `org.apache.spark.deploy.worker.Worker` (chạy foreground) như trong
  `docker-compose.yml`.
- **Không đủ RAM khi chạy cùng MongoDB + HDFS**: xem mục 10, dùng
  `docker stats --no-stream` để kiểm tra trước khi bật thêm Spark.
- **Cổng 4040 không phản hồi**: UI Application chỉ tồn tại khi driver còn
  sống (trong lúc `spark-submit` đang chạy); sau khi job kết thúc,
  `SparkContext.stop()` tắt UI này — xem lại job đã chạy tại
  `http://localhost:8080` (mục "Completed Applications" của Spark Master).

## 12. Hướng dẫn chạy nhanh bằng Script (Khuyến nghị)

Toàn bộ quy trình khởi động cụm, kiểm thử cấu hình bộ nhớ và nộp job đã được đóng gói sẵn trong thư mục `scripts/` (định dạng Bash `.sh`).

### Môi trường khuyến nghị:
- **Git Bash** (trên Windows) hoặc Terminal Linux/macOS.
- Nếu dùng **PowerShell**: gọi qua Git Bash bằng `bash scripts/<tên_script>.sh`.

### Thư mục làm việc (Working Directory):
Mở terminal và chuyển vào thư mục `Spark`:
```bash
cd "d:/school/Big Data/Spark"
```

### Thứ tự thực hiện:

#### Bước 1: Khởi động cụm Spark Standalone (1 Master + 2 Worker)
```bash
bash scripts/start-cluster.sh
```
*Lệnh này làm gì:*
1. Kích hoạt `docker compose up -d` khởi động `spark-master`, `spark-worker-1`, `spark-worker-2`.
2. Lặp kiểm tra healthcheck của container `spark-master` cho đến khi `healthy` (tối đa 100s).
3. In ra trạng thái các container và hiển thị liên kết Web UI của Master.

#### Bước 2: Nộp và thực thi job demo
```bash
bash scripts/run-demo-job.sh
```
*Lệnh này làm gì:*
1. Dùng `docker exec spark-master` nộp job PySpark `demo_job.py` lên cụm Standalone (`spark://spark-master:7077`).
2. Thiết lập sẵn các tham số tối ưu bộ nhớ đã qua kiểm thử thực tế (`--executor-memory 512m`, `--executor-cores 1`, `--total-executor-cores 2`, `--conf spark.sql.shuffle.partitions=6`) để tránh lỗi thiếu RAM làm job bị treo vĩnh viễn ở trạng thái WAITING.
3. Đọc dữ liệu từ `data/`, thực hiện các phép biến đổi, in mẫu kết quả ra terminal và lưu kết quả CSV vào `data/output/`.

#### Bước 3: Quan sát trên Spark Web UI
Trong và sau khi chạy job:
- Mở trình duyệt: [http://localhost:8080](http://localhost:8080) (Spark Master UI: quan sát 2 Worker đang hoạt động và danh sách job trong mục *Completed Applications*).

#### Bước 4: Dừng cụm an toàn
Khi hoàn tất buổi thực hành:
```bash
bash scripts/stop-cluster.sh
```
*Lệnh này làm gì:*
1. Tự động kiểm tra xem container `kafka` (của Buổi 13) có đang chạy gắn vào network `spark_spark-net` hay không. Nếu có, script sẽ cảnh báo và hỏi xác nhận để tránh làm gián đoạn Kafka.
2. Thực thi `docker compose down` hạ cụm an toàn mà không làm mất dữ liệu trong thư mục `data/`.

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích đơn giản |
|---|---|
| **Container** | Một "hộp" chạy phần mềm biệt lập, giống một máy ảo thu nhỏ. Một cụm (Hadoop/Spark/Kafka) trong dự án này gồm nhiều container chạy trên CÙNG một máy thật, giả lập nhiều máy. |
| **Docker Compose** | Công cụ mô tả "cần bao nhiêu container, cấu hình ra sao" trong 1 file (`docker-compose.yml`), rồi bật/tắt tất cả cùng lúc bằng 1 lệnh. |
| **Image / ghim version** | "Bản cài đặt đóng gói sẵn" của một phần mềm (ví dụ `mongo:8.0`). "Ghim version" nghĩa là chỉ rõ đúng phiên bản (`8.0`) thay vì `latest` (bản mới nhất, có thể đổi bất cứ lúc nào) — để lần sau chạy lại vẫn ra kết quả giống hệt. |
| **Spark Standalone Cluster** | Cụm Spark tự quản lý (không cần YARN/Kubernetes), gồm 1 Master (điều phối) + nhiều Worker (thực thi việc). |
| **Driver** | Chương trình chính điều khiển toàn bộ job Spark (chạy trên máy nộp job). |
| **Executor** | Tiến trình thực sự chạy trên từng Worker để xử lý dữ liệu — nếu thấy có executor chạy trên nhiều Worker khác nhau, nghĩa là job thật sự chạy phân tán (không phải chạy 1 mình trên máy local). |
| **local[\*]** | Chế độ chạy Spark ngay trên 1 máy (không dùng cluster), dùng để học/thử nhanh. `local[*]` nghĩa là dùng tất cả CPU core có sẵn của máy đó. |
| **Job / Stage / Task** | 1 hành động (ví dụ "tính tổng doanh thu") tạo ra 1 Job. Job được chia thành nhiều Stage (mỗi lần cần "xáo trộn" dữ liệu giữa các máy = 1 ranh giới Stage mới). Mỗi Stage lại chia nhỏ thành nhiều Task chạy song song. |
| **Partition** | Một "phần" của dữ liệu được xử lý độc lập — dữ liệu càng được chia thành nhiều partition thì càng chạy song song được nhiều. |
| **Shuffle** (trong Spark) | Bước tốn kém khi phải trộn/chuyển dữ liệu giữa các máy (ví dụ khi `join` hoặc `groupBy` dữ liệu nằm rải rác). |
| **spark-submit** | Lệnh dùng để "nộp" 1 chương trình Spark cho cluster chạy. |
