# LOCAL VALIDATION REPORT – Buổi 13 (Apache Kafka)

## 1. Trạng thái

**Validation:** PASS

Toàn bộ 5 validation item (V01–V05) chạy PASS với bằng chứng thật (command +
output thật, không suy đoán). Chi tiết log đầy đủ được lưu tại `Kafka/logs/`
và checkpoint thật tại `Kafka/checkpoint/`.

## 2. Environment

| Thành phần | Version |
|---|---|
| OS | Windows 10 Pro 10.0.19045, Docker Desktop (WSL2 backend) |
| Docker | 29.7.2 |
| Kafka broker (Docker) | `apache/kafka:3.7.1` (KRaft combined mode, 1 node, không Zookeeper) |
| Spark cluster (Docker, dùng lại từ Buổi 9) | `apache/spark:3.5.9-python3`, 1 Master + 2 Worker |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9` (tải qua `--packages`/Ivy lúc chạy) |
| Python (host, chạy producer/consumer) | 3.11.4 |
| kafka-python (host) | 3.0.11 |
| RAM cấp cho Docker | 3.826 GiB (Docker Desktop VM), 2 CPU |
| Container chạy đồng thời trong phiên | `kafka` (462 MiB/800 MiB), `spark-master` (512m limit), `spark-worker-1`/`spark-worker-2` (800m limit mỗi cái), `hdfs-namenode`, `hdfs-datanode1`, `hdfs-datanode2`, `mongodb` — cả 8 container chạy cùng lúc không cần tắt cái nào (xem `docker stats` mục 4 V05) |

## 3. Cách khởi động

```bash
export MSYS_NO_PATHCONV=1

# 0) Cụm Spark PHẢI chạy trước (Kafka gắn vào network spark_spark-net do Spark tạo)
cd Spark && docker compose up -d && cd ..

# 1) Kafka
cd Kafka
docker compose up -d
docker inspect --format='{{.State.Health.Status}}' kafka   # -> healthy

# 2) Tạo topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic clickstream --bootstrap-server localhost:29092 \
  --partitions 4 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic product_events --bootstrap-server localhost:29092 \
  --partitions 2 --replication-factor 1

# 3) Cài client Python trên host
pip install kafka-python
```

## 4. Validation Results

### V01 – Topic nhiều partition

**Result:** PASS

Command:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic clickstream \
  --bootstrap-server localhost:29092 --partitions 4 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic clickstream \
  --bootstrap-server localhost:29092
```

Actual:

```text
Created topic clickstream.
Topic: clickstream	TopicId: DslnZIFxSeO-pgLgzCDNCQ	PartitionCount: 4	ReplicationFactor: 1	Configs:
	Topic: clickstream	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 3	Leader: 1	Replicas: 1	Isr: 1
```

Notes: topic `product_events` cũng được tạo (2 partition) để đúng theo mục
2 BRIEF ("dữ liệu: clickstream events, product_events"), nhưng toàn bộ demo
producer/consumer/Spark ở buổi này tập trung vào `clickstream` (đúng nội
dung khung "Thực hành tích hợp trên lớp" – chỉ nhắc `clickstream`). Không
sửa Data Contract.

### V02 – Producer gửi sự kiện + key/partition (2 chiến lược key)

**Result:** PASS

Command:

```bash
cd Kafka
python producer.py --key-strategy session_id
python producer.py --key-strategy product_id
```

Actual (lần chạy `session_id`, sau khi topic được recreate sạch):

```text
Tong so ban ghi doc duoc : 200
Gui thanh cong           : 200
Loi                      : 0

Phan phoi theo partition (key_strategy=session_id):
  partition 0:   55 message(s)  (so key phan biet: 13)
  partition 1:   55 message(s)  (so key phan biet: 13)
  partition 2:   41 message(s)  (so key phan biet: 11)
  partition 3:   49 message(s)  (so key phan biet: 12)
```

Actual (lần chạy `product_id`, cộng dồn lên cùng topic – dùng để so sánh
phân phối):

```text
Tong so ban ghi doc duoc : 200
Gui thanh cong           : 200
Loi                      : 0

Phan phoi theo partition (key_strategy=product_id):
  partition 0:   28 message(s)  (so key phan biet: 9)
  partition 1:   47 message(s)  (so key phan biet: 16)
  partition 2:   60 message(s)  (so key phan biet: 18)
  partition 3:   65 message(s)  (so key phan biet: 16)
```

Log đầy đủ: `Kafka/logs/producer_session_id_run.log`,
`Kafka/logs/producer_product_id_run.log`.

Notes: cả 2 lần gửi đủ 200/200 bản ghi, 0 lỗi. Phân phối khác nhau rõ rệt
giữa 2 chiến lược key vì `clickstream_sample.jsonl` có 45 `session_id` phân
biệt nhưng chỉ khoảng 59 `product_id` phân biệt và tần suất xuất hiện của
từng `product_id` không đều (một số sản phẩm được xem/thêm giỏ nhiều lần
hơn) — minh hoạ đúng yêu cầu BRIEF "khoá sự kiện và chiến lược partition":
key khác nhau tạo phân phối tải khác nhau trên 4 partition, dù cả 2 đều
"cân bằng tương đối" ở mức dữ liệu mẫu này (không có key nào chiếm áp đảo
một partition trong cả 2 trường hợp).

### V03 – Consumer đọc lại + offset tăng dần theo partition

**Result:** PASS

Command:

```bash
python consumer.py --group demo-group-solo-final --consumer-name SOLO \
  --from-beginning --max-messages 600 --duration 30
```

Actual (đọc lại 600 message tích luỹ trong topic tại thời điểm test –
gồm 3 lần producer chạy trước đó: 200 session_id + 200 product_id + 200
session_id):

```text
tong so message da doc            : 600
offset cuoi cung theo partition    :
  partition 0: last_offset=137  count_in_this_run=138
  partition 1: last_offset=156  count_in_this_run=157
  partition 2: last_offset=141  count_in_this_run=142
  partition 3: last_offset=162  count_in_this_run=163
```

(số liệu gốc đầy đủ, không làm tròn: partition 0 offset 0..137 = 138 message,
partition 1 offset 0..156 = 157 message, partition 2 offset 0..141 = 142
message, partition 3 offset 0..162 = 163 message; tổng 138+157+142+163=600,
khớp chính xác tổng số message đã gửi tại thời điểm đó). Log đầy đủ từng
dòng offset: `Kafka/logs/consumer_solo_offsets.log`.

Notes: offset trong mỗi partition tăng đơn điệu từ 0, không có khoảng hở
(mỗi record trong partition có offset kế tiếp) – đúng cơ chế offset của
Kafka (log-structured, append-only per partition), khác hẳn ID nghiệp vụ
`event_id` (không liên tục, không tăng theo partition).

### V04 – Consumer group: 2 consumer cùng group, Kafka tự chia partition

**Result:** PASS

Command (2 tiến trình chạy đồng thời, cùng `--group`):

```bash
python consumer.py --group demo-group-shared-final --consumer-name C1 \
  --from-beginning --duration 25 &
python consumer.py --group demo-group-shared-final --consumer-name C2 \
  --from-beginning --duration 25 &
wait
```

Actual (chạy trên topic đã reset về đúng 200 message – trạng thái sạch):

```text
[C1] TOM TAT: partition assignment (cuoi phien) : [2, 3]
     tong so message da doc            : 90
     partition 2: last_offset=40  count_in_this_run=41
     partition 3: last_offset=48  count_in_this_run=49

[C2] TOM TAT: partition assignment (cuoi phien) : [0, 1]
     tong so message da doc            : 110
     partition 0: last_offset=54  count_in_this_run=55
     partition 1: last_offset=54  count_in_this_run=55
```

Log đầy đủ: `Kafka/logs/consumer_group_C1.log`,
`Kafka/logs/consumer_group_C2.log`.

Đối chiếu: C1 (partition 2+3) đọc 41+49=90 message, C2 (partition 0+1) đọc
55+55=110 message, tổng 90+110=200 — khớp đúng 200 message có trong topic
tại thời điểm test, không trùng lặp, không thiếu partition nào. Kafka group
coordinator **tự động chia đều 4 partition cho 2 consumer** (mỗi consumer 2
partition) mà không cần cấu hình thủ công — đúng cơ chế partition
assignment của consumer group.

### V05 – Spark Structured Streaming đọc từ Kafka source, đối chiếu số lượng

**Result:** PASS

Command:

```bash
export MSYS_NO_PATHCONV=1
docker exec spark-master mkdir -p /opt/spark-data/session13_kafka/checkpoint
docker exec spark-master mkdir -p /opt/spark-data/ivy2cache

docker exec \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
  -e KAFKA_TOPIC="clickstream" \
  -e KAFKA_STARTING_OFFSETS="earliest" \
  -e STREAM_CHECKPOINT_DIR="/opt/spark-data/session13_kafka/checkpoint" \
  -e STREAM_OUTPUT_MODE="update" \
  -e STREAM_WINDOW_DURATION="1 day" \
  -e STREAM_WATERMARK_DELAY="1 day" \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark-data/ivy2cache \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  --driver-memory 512m --executor-memory 512m \
  /opt/spark-apps/spark_kafka_job.py
```

Actual (chạy trên topic đã được reset về đúng 200 message do
`producer.py --key-strategy session_id` gửi 1 lần duy nhất – trạng thái
"clean" để đối chiếu số lượng chính xác):

```text
applicationId          = app-20260820070705-0007
...
batchId=0 numInputRows=200 numRowsDroppedByWatermark=0 startOffset=None endOffset={'clickstream': {'2': 41, '1': 55, '3': 49, '0': 55}}
batchId=1 numInputRows=0 numRowsDroppedByWatermark=0 startOffset={'clickstream': {'2': 41, '1': 55, '3': 49, '0': 55}} endOffset={'clickstream': {'2': 41, '1': 55, '3': 49, '0': 55}}

TONG SO MESSAGE SPARK DA DOC TU KAFKA (tong numInputRows tat ca batch) = 200
```

Đối chiếu: producer đã gửi **200/200** thành công (V02), Spark
Structured Streaming đọc từ Kafka báo **numInputRows=200** ở batch 0 (batch
1 = 0, vì `Trigger.availableNow` đã xử lý hết dữ liệu). `endOffset` của
Spark (`{'2': 41, '1': 55, '3': 49, '0': 55}`) khớp **chính xác từng
partition** với bảng phân phối mà `producer.py` in ra lúc gửi
(partition 0:55, 1:55, 2:41, 3:49). Đây là bằng chứng dữ liệu Spark đọc
được là **dữ liệu Kafka thật** (không phải dữ liệu giả lập), và số lượng
khớp 100% với số sự kiện producer đã gửi.

Job chạy trên **cụm Spark Standalone thật** (không phải `local[*]`):
`applicationId = app-20260820070705-0007` (định dạng `app-...`, không phải
`local-...`), log driver xác nhận 2 executor được cấp trên 2 worker khác
nhau (`172.20.0.3` và `172.20.0.4`, mỗi cái 1 core/512 MiB).

Đã chạy **lặp lại 2 lần** (lần đầu: `Kafka/logs/spark_kafka_job_run1.log`;
lần sau khi reset checkpoint để tái xác nhận:
`Kafka/logs/spark_kafka_job_final_run.log`) — cả 2 lần cho kết quả giống hệt
nhau (numInputRows=200, cùng phân phối partition), xác nhận khả năng chạy
lại (reproducibility) từ trạng thái checkpoint sạch. Checkpoint thật của
lần chạy cuối được lưu tại `Kafka/checkpoint/` (thư mục `commits/`,
`offsets/`, `sources/`, `state/`, `metadata`).

Window aggregation (tumbling 1 ngày trên `event_time`, giống hệt cấu hình
Buổi 11) cho ra kết quả tổng hợp theo ngày + `event_type` khớp với dữ liệu
gốc (ví dụ ngày 2026-08-18: VIEW=14, ngày 2026-08-17: VIEW=13,... xem log
đầy đủ để đối chiếu từng dòng) — chứng minh window/watermark hoạt động
đúng trên dữ liệu đọc từ Kafka, không chỉ là "đọc được" mà còn "xử lý được"
đúng ngữ nghĩa Structured Streaming.

## 5. Issues Found

### ISSUE-04 (2026-08-22, phát hiện khi viết `scripts/run-spark-kafka-job.sh`)

**Severity:** Minor

**Hiện tượng:** `rm -rf ../Spark/data/session13_kafka/checkpoint/*` không xoá
được file ẩn `.metadata.crc`, khiến lần chạy sau bị lỗi thật
`FileAlreadyExistsException: Rename destination file:.../.metadata.crc
already exists`.

**Nguyên nhân:** glob `*` trong shell không khớp file bắt đầu bằng dấu chấm
(dotfile) — lỗi shell kinh điển, không phải lỗi Spark.

**Cách sửa:** xoá hẳn cả thư mục `checkpoint` (`rm -rf checkpoint`) rồi tạo
lại (`mkdir -p checkpoint`) thay vì chỉ xoá nội dung bên trong bằng
wildcard. Đã sửa trong `Kafka/scripts/run-spark-kafka-job.sh` và kiểm thử
lại thành công.

**File ảnh hưởng:** chỉ ảnh hưởng script, không sửa job Spark.

### ISSUE-03 (2026-08-22, phát hiện khi viết `scripts/reset-topics.sh`)

**Severity:** Major

**Hiện tượng:** `kafka-topics.sh --delete --topic clickstream` làm **crash
toàn bộ broker Kafka** (container `kafka` tự thoát, `docker ps` báo
`Exited (1)`). Log thật:

```text
java.nio.file.AccessDeniedException: /var/lib/kafka/data/clickstream-3 ->
/var/lib/kafka/data/clickstream-3.<uuid>-delete
...
ERROR Shutdown broker because all log dirs in /var/lib/kafka/data have failed
```

**Nguyên nhân:** Kafka xoá topic bằng cách **đổi tên** thư mục log
(`<topic>-<partition>` → `<topic>-<partition>.<uuid>-delete`) rồi xoá dần ở
nền. Thư mục `./data` được bind-mount từ Windows (NTFS qua Docker Desktop)
không hỗ trợ đúng semantics rename/xoá mà Kafka cần trên Linux — thao tác
`rename` bị hệ điều hành từ chối quyền, và vì **toàn bộ log dir coi là hỏng
khi 1 thao tác thất bại**, Kafka tự tắt để bảo vệ dữ liệu.

**Cách sửa:** KHÔNG dùng `kafka-topics.sh --delete` để reset trên máy
Windows. Thay vào đó: `docker compose down` (dừng hẳn container) → xoá thư
mục `./data` trên host bằng `rm -rf` (không phải qua lệnh Kafka) → khởi
động lại container từ đầu. Đã đóng gói thành `Kafka/scripts/reset-topics.sh`
và kiểm thử thật thành công nhiều lần.

**File ảnh hưởng:** không sửa `docker-compose.yml`; chỉ ảnh hưởng cách
reset dữ liệu khi demo — quan trọng để giảng viên biết trước khi tự ý gõ
lệnh `--delete` trong lúc dạy (sẽ làm gãy demo giữa buổi, phải khởi động
lại toàn bộ Kafka).

### ISSUE-01

**Severity:** Minor

**Hiện tượng:** `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9`
báo `FileNotFoundException` khi Ivy cố ghi file resolve vào
`/nonexistent/.ivy2/cache/...`.

**Nguyên nhân:** image `apache/spark:3.5.9-python3` chạy container với biến
`$HOME=/nonexistent` (không có quyền ghi), là thư mục mặc định Ivy dùng làm
cache khi tải dependency qua Maven coordinates.

**Cách sửa:** thêm `--conf spark.jars.ivy=/opt/spark-data/ivy2cache` (một
thư mục con trong volume `/opt/spark-data` đã mount ghi được) vào lệnh
`spark-submit`.

**File ảnh hưởng:** không sửa file nào trong repo, chỉ là tham số dòng lệnh
— đã ghi rõ trong `Kafka/README.md` mục 7.

### ISSUE-02

**Severity:** Minor

**Hiện tượng:** ban đầu dùng `volumes: - ./data:/tmp/kraft-combined-logs`
để Kafka giữ dữ liệu qua các lần `docker compose down`/`up`, nhưng sau khi
restart, `kafka-topics.sh --list` trả về rỗng (mất hết topic/dữ liệu).

**Nguyên nhân:** mặc dù `server.properties` mẫu bên trong image ghi
`log.dirs=/tmp/kraft-combined-logs`, khi container thực sự chạy (không set
`KAFKA_LOG_DIRS`), Kafka log xác nhận dùng
`dir=/tmp/kafka-logs` — khác với giá trị trong file mẫu. Đây là hành vi
thật quan sát được qua `docker logs kafka`, không phải suy đoán.

**Cách sửa:** đặt tường minh `KAFKA_LOG_DIRS=/var/lib/kafka/data` trong
`environment:` và mount `volumes: - ./data:/var/lib/kafka/data`. Đã kiểm
thử lại: gửi 200 message → `docker compose down` → `docker compose up -d`
→ `kafka-get-offsets.sh` vẫn trả đúng
`clickstream:0:55 / 1:55 / 2:41 / 3:49` (tổng 200) — dữ liệu sống sót qua
restart.

**File ảnh hưởng:** `Kafka/docker-compose.yml` (đã sửa trong bản hiện tại,
không phải sửa sau khi review — phát hiện và tự khắc phục trong quá trình
kiểm thử).

## 6. Mismatch với tài liệu Content AI

`CONTENT_REPORT.md` của Buổi 13 hiện đang **trống hoàn toàn** (chỉ có dòng
"Chưa cập nhật."), giống tình trạng đã ghi nhận ở các buổi 9–12. Local AI
không thể đối chiếu số liệu/thuật ngữ với slide vì chưa có nội dung để so
sánh. Content AI cần dựa trên `LOCAL_REPORT.md` này (đặc biệt mục 4 V01–V05
và các số liệu thật: 4 partition, 200 message/lần gửi, phân phối partition
theo 2 chiến lược key, 2 consumer chia 2 partition mỗi consumer, Spark đọc
đúng 200/200) để viết slide/tài liệu/bài thực hành + điền
`CONTENT_REPORT.md`.

| Vị trí | Nội dung hiện tại | Thực tế | Đề xuất |
|---|---|---|---|
| `CONTENT_REPORT.md` | "Chưa cập nhật." | N/A | Content AI viết dựa trên LOCAL_REPORT này |

## 7. Khả năng chạy lại

- [x] chạy từ clean state (đã `docker compose down` rồi `up -d` lại cả
      Kafka và xác nhận topic/dữ liệu còn nguyên nhờ volume `./data`; đã
      chạy lại `spark_kafka_job.py` 2 lần với checkpoint sạch, kết quả
      giống hệt nhau);
- [x] version được ghim (`apache/kafka:3.7.1`, `apache/spark:3.5.9-python3`,
      connector `spark-sql-kafka-0-10_2.12:3.5.9` — không dùng `latest` ở
      đâu);
- [x] dữ liệu có đường dẫn tương đối (`producer.py` mặc định đọc
      `../00_shared_data/sample/clickstream_sample.jsonl` tính từ vị trí
      script, có thể override qua `--data-file`);
- [x] worker/container truy cập được dữ liệu (Kafka broker gắn thêm vào
      network `spark_spark-net`, container Spark resolve được hostname
      `kafka` qua DNS nội bộ Docker — đã xác nhận bằng
      `getent hosts kafka`);
- [x] expected output được lưu (`Kafka/logs/*.log` — log console đầy đủ
      của tất cả lần chạy thật; `Kafka/checkpoint/` — checkpoint thật của
      Structured Streaming);
- [x] reset script hoạt động (2026-08-22): `Kafka/scripts/start-cluster.sh`,
      `reset-topics.sh` (dừng container + xoá `./data` trên host + khởi động
      lại — KHÔNG dùng `kafka-topics.sh --delete`, xem ISSUE-03),
      `demo-produce-consume.sh` (kịch bản demo trọn gói: reset → gửi 200 sự
      kiện → 1 consumer đọc lại → 2 consumer chia group), `run-spark-kafka-job.sh`.
      Cả 4 script đã chạy thật thành công end-to-end.

## 8. Kết luận cho giảng viên

Có thể dùng để dạy: **YES**

Các điểm cần đọc trước khi duyệt:

1. Đổi ảnh Kafka gợi ý trong BRIEF sang `apache/kafka:3.7.1` (KRaft, không
   Zookeeper) thay vì `bitnami/kafka`/`confluentinc/cp-kafka` — lý do rủi ro
   "hết free tier" tương tự `bitnami/spark` ở Buổi 9 (cùng nhà cung cấp
   Bitnami), và KRaft đơn giản hơn cho nhập môn (1 container, không cần
   Zookeeper). Không có quyết định kiến trúc nào trong `00_DECISIONS.md` bị
   ảnh hưởng vì BRIEF Buổi 13 chỉ ghi "Kafka Docker", không ràng buộc nhà
   cung cấp image.
2. Kafka được nối vào cụm Spark Standalone đã có sẵn từ Buổi 9 qua network
   Docker `spark_spark-net` (external) — **thứ tự khởi động bắt buộc**:
   `Spark/` phải `docker compose up` trước `Kafka/`, nếu không network chưa
   tồn tại.
3. `spark-submit --packages` cần Internet để tải connector Kafka qua Ivy
   (đã xác nhận `curl` từ container `spark-master` ra ngoài internet trả về
   `200`) — nếu môi trường giảng dạy không có Internet lúc demo, cần tải
   trước jar connector và dùng `--jars` với đường dẫn cục bộ thay vì
   `--packages` (chưa kiểm thử phương án offline này).
4. `producer.py --key-strategy session_id` và `--key-strategy product_id`
   **cộng dồn** message vào topic nếu chạy nhiều lần liên tiếp mà không
   xoá/tạo lại topic — số liệu 200/200 trong V05 chỉ đúng khi topic ở trạng
   thái "vừa mới tạo lại + gửi đúng 1 lần" (đã ghi rõ trong V02/V05, không
   phải tình trạng ngộ nhận).
5. ~~Chưa có script reset-lab~~ — **ĐÃ BỔ SUNG (2026-08-22)**:
   `Kafka/scripts/` giờ có đủ `start-cluster.sh`, `reset-topics.sh`,
   `demo-produce-consume.sh`, `run-spark-kafka-job.sh`. **QUAN TRỌNG**:
   `reset-topics.sh` KHÔNG dùng `kafka-topics.sh --delete` vì lệnh này làm
   crash cả broker trên Windows bind-mount (xem ISSUE-03 mục 5) — thay vào
   đó dừng container + xoá `./data` trên host + khởi động lại.
