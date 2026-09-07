# Buổi 13 – Apache Kafka Docker Lab (RetailStream)

Kafka là một hệ thống truyền message theo mô hình publish-subscribe: nhiều
nguồn (producer) gửi sự kiện vào các luồng có tên gọi là topic, nhiều bên
khác (consumer) đọc lại các sự kiện đó — phù hợp khi nhiều hệ thống cần
trao đổi dữ liệu thời gian thực mà không gọi trực tiếp lẫn nhau.

Gói Docker Compose dựng 1 Kafka broker chế độ **KRaft** (không cần Zookeeper
riêng), tạo topic nhiều partition, gửi/đọc sự kiện `clickstream` bằng
producer/consumer Python, minh họa consumer group + partition assignment,
và nối trực tiếp Kafka làm nguồn (source) cho Spark Structured Streaming –
thay cho File Source đã dùng ở Buổi 11. Dùng lại
`00_shared_data/sample/clickstream_sample.jsonl` (200 dòng) và
`00_shared_data/sample/product_events_sample.jsonl` (80 dòng), không tạo
dataset riêng.

Toàn bộ lệnh dưới đây đã chạy thật trên máy giảng viên (Windows 10, Docker
Desktop WSL2). Số liệu ví dụ trong tài liệu này là kết quả thật của các lần
chạy đó, không phải số liệu minh họa.

## 1. Bảng phiên bản image đã kiểm thử

| Thành phần | Image:tag | Ghi chú |
|---|---|---|
| Kafka broker | `apache/kafka:3.7.1` | KRaft combined mode (broker + controller), 1 node, không Zookeeper |
| Spark cluster (dùng lại từ Buổi 9) | `apache/spark:3.5.9-python3` | 1 Master + 2 Worker |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9` | tải qua `--packages`/Ivy lúc chạy |
| Python (host) | 3.11.4 | chạy producer.py/consumer.py |
| kafka-python (host) | 3.0.11 | client Python cho Kafka |

Dùng ảnh chính thức `apache/kafka` (do chính dự án Apache Kafka phát hành)
thay vì `bitnami/kafka` hay `confluentinc/cp-kafka`: `bitnami/kafka` cùng
nhà phát hành với `bitnami/spark` đã gặp vấn đề ngừng cấp free tier trên
Docker Hub ở Buổi 9, nên tránh rủi ro tương tự. `apache/kafka:3.7.1` hỗ trợ
KRaft mode ngay khi khởi động, không phụ thuộc Zookeeper — phù hợp cho một
node, một bài nhập môn.

## 2. Cấu trúc thư mục

```text
Kafka/
├── README.md
├── docker-compose.yml       (1 Kafka broker KRaft, dual listener BROKER/EXTERNAL)
├── producer.py               (gửi clickstream vào topic, 2 chiến lược key)
├── consumer.py                (consumer đơn giản: offset + partition assignment)
├── spark_kafka_job.py         (Spark Structured Streaming đọc từ Kafka source)
├── data/                      (volume bền vững của Kafka - log.dirs, sinh ra khi chạy)
├── checkpoint/                (checkpoint Structured Streaming của lần chạy thật)
├── logs/                      (log console đầy đủ của các lần chạy thật)
└── scripts/                   (script tiện dụng, xem mục 12)
```

## 3. Kiến trúc mạng: Kafka nối vào cụm Spark đã có sẵn

`docker-compose.yml` gắn broker `kafka` vào **2 network**:

- `kafka-net` — network riêng của gói này;
- `spark_spark-net` — network **external**, do `Spark/docker-compose.yml`
  (Buổi 9) tạo ra. Nhờ vậy container `spark-master`/`spark-worker-*` gọi
  được Kafka qua hostname nội bộ `kafka:29092`, và job Spark đọc dữ liệu
  Kafka thật khi chạy trên cụm.

Vì `spark_spark-net` chỉ tồn tại sau khi cụm Spark đã khởi động,
**thứ tự bắt buộc: khởi động Spark trước, Kafka sau.**

Listener là một "cổng vào" mà Kafka broker lắng nghe kết nối — mỗi listener
phục vụ một loại client khác nhau (bên trong Docker network hay từ máy
host). Dual listener trong `docker-compose.yml`:

| Listener | Dùng cho | Địa chỉ |
|---|---|---|
| `BROKER` | container Spark gọi nội bộ qua Docker network | `kafka:29092` |
| `EXTERNAL` | producer.py/consumer.py chạy trên máy host Windows | `localhost:9092` |
| `CONTROLLER` | KRaft controller nội bộ | `kafka:9093` |

## 4. Khởi động cụm

```bash
export MSYS_NO_PATHCONV=1   # Git Bash trên Windows

# 0) Cụm Spark PHẢI chạy trước (tạo network spark_spark-net)
cd Spark && docker compose up -d && cd ..

# 1) Khởi động Kafka
cd Kafka
docker compose up -d

# Đợi broker chuyển sang trạng thái healthy
docker inspect --format='{{.State.Health.Status}}' kafka
```

## 5. Tạo topic nhiều partition

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic clickstream --bootstrap-server localhost:29092 \
  --partitions 4 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic product_events --bootstrap-server localhost:29092 \
  --partitions 2 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic clickstream --bootstrap-server localhost:29092
```

Kết quả thật:

```text
Created topic clickstream.
Topic: clickstream	TopicId: DslnZIFxSeO-pgLgzCDNCQ	PartitionCount: 4	ReplicationFactor: 1	Configs:
	Topic: clickstream	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 3	Leader: 1	Replicas: 1	Isr: 1
```

Toàn bộ demo producer/consumer/Spark trong gói này tập trung vào topic
`clickstream`; `product_events` được tạo sẵn để khớp bộ dữ liệu chia sẻ
nhưng không có kịch bản demo riêng ở buổi này.

## 6. Producer – gửi sự kiện, hai chiến lược key

Chạy trên **host** (không phải trong container), qua listener `EXTERNAL`
tại `localhost:9092`.

Mỗi message gửi vào Kafka có thể kèm một khóa (key) — Kafka dùng khóa này
để quyết định message rơi vào partition nào (cùng khóa luôn vào cùng 1
partition, giữ đúng thứ tự); nếu không có khóa, Kafka tự rải đều message.

```bash
pip install kafka-python      # đã kiểm thử với kafka-python 3.0.11

cd Kafka
python producer.py --key-strategy session_id   # key = session_id
python producer.py --key-strategy product_id   # key = product_id
python producer.py --key-strategy none          # (tùy chọn) không dùng key
```

Mỗi lần chạy đọc và gửi cả 200 dòng của `clickstream_sample.jsonl`, in ra
bảng phân phối message theo partition. Kết quả thật, chạy trên topic vừa
được tạo lại sạch:

```text
Phan phoi theo partition (key_strategy=session_id):
  partition 0:   55 message(s)  (so key phan biet: 13)
  partition 1:   55 message(s)  (so key phan biet: 13)
  partition 2:   41 message(s)  (so key phan biet: 11)
  partition 3:   49 message(s)  (so key phan biet: 12)

Phan phoi theo partition (key_strategy=product_id):
  partition 0:   28 message(s)  (so key phan biet: 9)
  partition 1:   47 message(s)  (so key phan biet: 16)
  partition 2:   60 message(s)  (so key phan biet: 18)
  partition 3:   65 message(s)  (so key phan biet: 16)
```

Cả hai lần gửi đủ 200/200 bản ghi, 0 lỗi. Phân phối khác nhau rõ rệt giữa
hai chiến lược key vì dữ liệu mẫu có 45 `session_id` phân biệt nhưng chỉ
khoảng 59 `product_id` phân biệt, với tần suất không đều giữa các sản
phẩm — minh họa key khác nhau tạo mức cân bằng tải khác nhau trên cùng 4
partition. Log đầy đủ: `Kafka/logs/producer_session_id_run.log`,
`Kafka/logs/producer_product_id_run.log`.

**Lưu ý quan trọng:** chạy `producer.py` nhiều lần liên tiếp mà không xóa
lại topic sẽ **cộng dồn** message vào topic, không thay thế dữ liệu cũ —
xem mục 9 về cách reset trước khi bắt đầu một vòng demo mới.

## 7. Consumer – offset và consumer group

### Một consumer đọc lại toàn bộ, quan sát offset

```bash
python consumer.py --group demo-group-solo --consumer-name SOLO \
  --from-beginning --max-messages 999999 --duration 30
```

Kết quả thật (đọc lại 600 message tích lũy trong topic tại thời điểm test,
gồm 3 lần producer chạy trước đó):

```text
tong so message da doc            : 600
offset cuoi cung theo partition    :
  partition 0: last_offset=137  count_in_this_run=138
  partition 1: last_offset=156  count_in_this_run=157
  partition 2: last_offset=141  count_in_this_run=142
  partition 3: last_offset=162  count_in_this_run=163
```

138 + 157 + 142 + 163 = 600, khớp đúng tổng số message đã gửi tại thời
điểm đó. Offset trong mỗi partition tăng đơn điệu từ 0, không có khoảng hở.

### Hai consumer cùng group, quan sát Kafka tự chia partition

```bash
python consumer.py --group demo-group-shared --consumer-name C1 \
  --from-beginning --duration 25 &
python consumer.py --group demo-group-shared --consumer-name C2 \
  --from-beginning --duration 25 &
wait
```

Kết quả thật (chạy trên topic đã reset về đúng 200 message):

```text
[C1] partition assignment (cuoi phien) : [2, 3]
     tong so message da doc            : 90

[C2] partition assignment (cuoi phien) : [0, 1]
     tong so message da doc            : 110
```

C1 (partition 2+3) đọc 90 message, C2 (partition 0+1) đọc 110 message,
tổng 200 — khớp đúng số message có trong topic, không trùng lặp, không
thiếu partition nào. Kafka group coordinator tự động chia đều 4 partition
cho 2 consumer mà không cần cấu hình thủ công. Log đầy đủ:
`Kafka/logs/consumer_group_C1.log`, `Kafka/logs/consumer_group_C2.log`.

## 8. Spark Structured Streaming đọc từ Kafka source

Job chính đặt tại `Kafka/spark_kafka_job.py`; vì container Spark chỉ mount
được `Spark/data` và `Spark/jobs` (giống Buổi 10/11/12), một bản sao giống
hệt cũng được đặt ở `Spark/jobs/spark_kafka_job.py`.

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

Kết quả thật (chạy trên topic đã được reset về đúng 200 message):

```text
applicationId          = app-20260820070705-0007
...
batchId=0 numInputRows=200 numRowsDroppedByWatermark=0 startOffset=None endOffset={'clickstream': {'2': 41, '1': 55, '3': 49, '0': 55}}
batchId=1 numInputRows=0 numRowsDroppedByWatermark=0 startOffset={'clickstream': {'2': 41, '1': 55, '3': 49, '0': 55}} endOffset={'clickstream': {'2': 41, '1': 55, '3': 49, '0': 55}}

TONG SO MESSAGE SPARK DA DOC TU KAFKA (tong numInputRows tat ca batch) = 200
```

`endOffset` của Spark (`{'2': 41, '1': 55, '3': 49, '0': 55}`) khớp chính
xác từng partition với bảng phân phối mà `producer.py` in ra lúc gửi —
bằng chứng dữ liệu Spark đọc được là dữ liệu Kafka thật, số lượng khớp
100% với số sự kiện producer đã gửi. Job chạy trên cụm Spark Standalone
thật (`applicationId` dạng `app-...`, không phải `local-...`), 2 executor
trên 2 worker khác nhau (`172.20.0.3` và `172.20.0.4`, mỗi cái 1 core/512
MiB). Đã chạy lặp lại 2 lần với checkpoint sạch, kết quả giống hệt nhau —
xác nhận khả năng chạy lại (reproducibility). Log đầy đủ:
`Kafka/logs/spark_kafka_job_run1.log`,
`Kafka/logs/spark_kafka_job_final_run.log`.

Điểm cần lưu ý khi chạy lại:

- `--packages` tải connector Kafka qua Ivy — **cần Internet** từ container
  `spark-master`.
- Thư mục cache Ivy mặc định (`$HOME/.ivy2`) không ghi được vì `$HOME` là
  `/nonexistent` trong image `apache/spark:3.5.9-python3` — bắt buộc trỏ
  `--conf spark.jars.ivy=` sang một thư mục ghi được trong
  `/opt/spark-data`, nếu không sẽ gặp `FileNotFoundException`.
- Job dùng `Trigger.availableNow=True`: đọc hết dữ liệu đang có trong topic
  (từ `earliest`) rồi tự dừng — phù hợp để đối chiếu tổng số message.
- Window/watermark: tumbling 1 ngày trên `event_time`, watermark 1 ngày —
  giữ nguyên logic/API của Buổi 11, chỉ đổi nguồn đọc từ File Source sang
  Kafka source (`spark.readStream.format("kafka")` thay vì
  `format("json")`). watermark: ngưỡng thời gian Spark chờ dữ liệu đến
  muộn trước khi coi một cửa sổ tổng hợp là "đã đóng" và không cập nhật
  nữa.

## 9. Reset dữ liệu giữa các lần demo

`producer.py` cộng dồn message vào topic mỗi lần chạy. Để đối chiếu số
liệu chính xác (ví dụ đúng 200/200 như trong tài liệu này), topic phải ở
trạng thái "vừa tạo lại, chưa nhận message nào" trước khi chạy producer.

**Không dùng** `kafka-topics.sh --delete` để xóa topic trên máy Windows.
Lệnh này làm Kafka đổi tên thư mục log
(`clickstream-3` → `clickstream-3.<uuid>-delete`) rồi xóa dần ở nền; trên
thư mục dữ liệu bind-mount từ Windows (NTFS qua Docker Desktop), thao tác
đổi tên/xóa này bị hệ điều hành từ chối quyền
(`AccessDeniedException`), và vì Kafka coi toàn bộ log dir là hỏng ngay
khi một thao tác thất bại, **toàn bộ broker Kafka tự tắt** để bảo vệ dữ
liệu (`Shutdown broker because all log dirs in /var/lib/kafka/data have
failed`, container thoát `Exited (1)`). Đây là hành vi thật đã kiểm thử,
không phải giả định — reset bằng cách xóa topic sẽ làm gãy demo giữa
buổi và phải khởi động lại toàn bộ Kafka.

Cách reset an toàn, đã kiểm thử thành công nhiều lần:

```bash
cd Kafka
docker compose down          # dừng hẳn container, KHÔNG dùng --delete topic

rm -rf data                  # xóa thư mục dữ liệu Kafka trên host
mkdir -p data

docker compose up -d         # khởi động lại từ đầu, topic đã bị xóa hết

# đợi healthy rồi tạo lại topic (xem mục 5)
docker inspect --format='{{.State.Health.Status}}' kafka
```

## 10. Dừng cụm

```bash
cd Kafka
docker compose down     # KHÔNG kèm -v: dữ liệu topic được giữ trong ./data
                         # (volume bind-mount tới log.dirs của Kafka)
```

**Lưu ý về persistence:** image `apache/kafka:3.7.1` mặc định dùng
`log.dirs=/tmp/kafka-logs` khi chạy thực tế (khác với dòng
`log.dirs=/tmp/kraft-combined-logs` ghi trong `server.properties` mẫu bên
trong image — sai lệch này đã được phát hiện khi kiểm thử). Vì vậy
`docker-compose.yml` đặt tường minh `KAFKA_LOG_DIRS=/var/lib/kafka/data` và
mount `./data:/var/lib/kafka/data` để dữ liệu sống sót qua
`docker compose down && docker compose up -d` — đã kiểm thử thật: gửi 200
message, `down`/`up`, `kafka-get-offsets.sh` vẫn trả đúng 200 message trên
4 partition.

## 11. Lỗi thường gặp

- **`docker compose up -d` ở `Kafka/` báo lỗi network không tồn tại**:
  network `spark_spark-net` do `Spark/docker-compose.yml` tạo ra chưa
  chạy. Luôn `cd Spark && docker compose up -d` trước.
- **`FileNotFoundException` khi `spark-submit --packages`**: chưa set
  `--conf spark.jars.ivy=<thư mục ghi được>` (xem mục 8).
- **Spark container không resolve được hostname `kafka`**: Kafka broker
  chưa join network `spark_spark-net` — kiểm tra `networks:` trong
  `docker-compose.yml`, xác nhận bằng
  `docker exec spark-master getent hosts kafka`.
- **Số liệu không khớp giữa producer, consumer và Spark (ví dụ không còn
  đúng 200)**: topic chưa được reset giữa các lần demo, dữ liệu đã cộng
  dồn từ các lần chạy trước — xem mục 9. KHÔNG dùng
  `kafka-topics.sh --delete` để sửa việc này.
- **`kafka-topics.sh --describe` báo topic không tồn tại sau khi
  `docker compose down`/`up`**: kiểm tra `docker-compose.yml` có
  `KAFKA_LOG_DIRS=/var/lib/kafka/data` và mount `./data:/var/lib/kafka/data`
  hay chưa (xem mục 10).
- **RAM**: Kafka container giới hạn `mem_limit: 800m`, đã kiểm thử chạy
  đồng thời với 7 container khác (MongoDB, 3 container HDFS, 3 container
  Spark) trên máy có khoảng 3.8 GiB cấp cho Docker — không cần tắt
  container nào.

## 12. Hướng dẫn chạy nhanh bằng Script (Khuyến nghị)

Thư mục `Kafka/scripts/` cung cấp 4 script tự động hóa trọn gói toàn bộ các kịch bản demo: từ khởi động 2 cụm mạng, tạo topic, chạy mô phỏng producer/consumer đa luồng, đến tích hợp Spark Structured Streaming.

### Môi trường khuyến nghị:
- **Git Bash** (trên Windows) hoặc Terminal Linux/macOS.
- Nếu dùng **PowerShell**: hãy gọi qua Git Bash bằng `bash scripts/<tên_script>.sh`.

### Thư mục làm việc (Working Directory):
Mở terminal và chuyển vào thư mục `Kafka`:
```bash
cd "d:/school/Big Data/Kafka"
```

### Thứ tự thực hiện:

#### Bước 1: Khởi động cụm Spark và Kafka KRaft
```bash
bash scripts/start-cluster.sh
```
*Lệnh này làm gì:*
1. Tự động kiểm tra và khởi động cụm Spark (`../Spark/docker-compose.yml`) trước để tạo Docker network chia sẻ `spark_spark-net`.
2. Kích hoạt `docker compose up -d` cho Kafka broker (chạy chế độ KRaft không cần Zookeeper).
3. Thăm dò healthcheck chờ container `kafka` đạt trạng thái `healthy`.
4. Tự động khởi tạo 2 topic chuẩn nếu chưa có: `clickstream` (4 partitions, rf 1) và `product_events` (2 partitions, rf 1).

#### Bước 2: Chạy demo Producer & Consumer (Offset & Chia Partition)
```bash
bash scripts/demo-produce-consume.sh
```
*Lệnh này làm gì:*
1. Gọi `reset-topics.sh` để đưa topic về trạng thái trống 0 message.
2. Chạy `producer.py` gửi 200 bản ghi clickstream từ `00_shared_data/sample/clickstream_sample.jsonl` vào 4 partition của topic `clickstream`.
3. Chạy 1 consumer độc lập đọc tuần tự 200 bản ghi từ đầu để quan sát offset tăng dần.
4. Chạy đồng thời 2 consumer (C1 và C2 chạy nền song song) trong cùng một consumer group `demo-shared` để minh họa Kafka tự động gán partition (mỗi consumer nhận 2 partition, đọc song song không trùng lặp).

#### Bước 3: Chạy Spark Structured Streaming đọc dữ liệu từ Kafka
```bash
bash scripts/run-spark-kafka-job.sh
```
*Lệnh này làm gì:*
1. Tự động copy `spark_kafka_job.py` vào thư mục chia sẻ của Spark (`../Spark/jobs/`) và cấu hình thư mục cache Ivy.
2. Dọn sạch checkpoint cũ để Spark đọc lại từ `earliest`.
3. Dùng `docker exec spark-master` nộp job lên cụm Spark Standalone thật, tự động tải package `spark-sql-kafka-0-10_2.12:3.5.9`, xử lý luồng sự kiện theo cửa sổ thời gian (window/watermark) với `Trigger.availableNow=True` và in tổng số message đã đọc (đúng 200 message).

#### Bước 4: Đặt lại topic về trạng thái sạch (Reset khi cần)
Nếu muốn dọn dẹp các message cũ để chạy lại demo từ đầu:
```bash
bash scripts/reset-topics.sh
```
*Lệnh này làm gì:* Hạ container, xóa dữ liệu nhị phân trên đĩa `./data/*`, và gọi lại `start-cluster.sh` để tái tạo 2 topic trắng. *(Cách này an toàn tuyệt đối trên Windows NTFS, tránh lỗi crash do lệnh delete topic của Kafka).*

#### Bước 5: Dừng cụm khi kết thúc
Khi hoàn tất buổi thực hành:
```bash
docker compose down
cd ../Spark && bash scripts/stop-cluster.sh
```

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| **Streaming (xử lý luồng)** | Xử lý dữ liệu liên tục, mới đến đâu xử lý đến đó — khác với "batch" (xử lý theo lô, gom đủ dữ liệu rồi chạy 1 lần). |
| **Kafka broker** | 1 "máy chủ" Kafka nhận và lưu tạm các sự kiện gửi tới. |
| **Topic** | Một "kênh" để gửi/nhận sự kiện trong Kafka (giống 1 hàng đợi có tên). |
| **Partition (trong Kafka)** | 1 topic được chia thành nhiều partition để nhiều máy có thể đọc/ghi song song — sự kiện trong CÙNG 1 partition mới được đảm bảo đúng thứ tự. |
| **Producer / Consumer** | Producer = bên gửi sự kiện vào Kafka. Consumer = bên đọc sự kiện ra. |
| **Consumer group** | Một nhóm consumer cùng đọc 1 topic — Kafka tự chia các partition cho từng consumer trong nhóm để không ai đọc trùng nhau. |
| **Offset** | "Số thứ tự" của 1 sự kiện trong 1 partition — dùng để biết đã đọc tới đâu. |
| **KRaft** | Cách Kafka tự quản lý nội bộ mà KHÔNG cần một phần mềm phụ trợ tên là Zookeeper (cách cũ) — đơn giản hoá triển khai, không ảnh hưởng khái niệm topic/partition/consumer group. |
| **Window (cửa sổ thời gian)** | Gom các sự kiện lại theo từng khoảng thời gian cố định để tính toán (ví dụ "đếm số lượt xem mỗi 5 phút"). |
| **Watermark** | Một "hạn chót" cho phép dữ liệu đến muộn bao lâu vẫn được tính. Sự kiện đến sau hạn này sẽ bị bỏ qua (tuỳ chế độ). Có watermark giúp hệ thống biết khi nào "chốt" một cửa sổ thời gian thay vì chờ mãi. |
| **Checkpoint** | "Điểm lưu tạm" ghi lại job đã xử lý tới đâu — nếu job bị dừng và chạy lại, nó đọc checkpoint để tiếp tục đúng chỗ, không phải xử lý lại từ đầu. |
| **Output mode (complete / append / update)** | Cách kết quả được ghi ra mỗi lần cập nhật: `complete` = ghi lại toàn bộ kết quả từ đầu mỗi lần; `append` = chỉ ghi thêm dòng mới; `update` = chỉ ghi những dòng có thay đổi. |
| **mem_limit / OOM** | `mem_limit` là giới hạn RAM tối đa cấp cho 1 container, khai báo trong `docker-compose.yml`. Nếu container cần nhiều RAM hơn mức này sẽ bị OOM (Out Of Memory — hết bộ nhớ, hệ điều hành tự tắt tiến trình để bảo vệ máy). |
