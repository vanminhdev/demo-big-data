# Buổi 13 – Thực hành Apache Kafka & Spark Streaming (RetailStream)

Hệ thống truyền sự kiện phân tán theo mô hình Publish/Subscribe: Bên phát (*Producer*) đẩy sự kiện vào các luồng có tên gọi là *Topic*; bên nhận (*Consumer*) kéo các sự kiện đó về xử lý độc lập theo tốc độ riêng. Đây là "xương sống" kết nối thời gian thực giữa các hệ thống trong kiến trúc Dữ liệu lớn hiện đại.

Tài liệu này hướng dẫn chi tiết từ **mức nhập môn dễ hiểu nhất** (thử nghiệm gửi/nhận tức thì không cần cài đặt thêm) đến **kịch bản phân tán chuyên sâu** (chiến lược băm Partition, Consumer Group tự chia việc, và tích hợp Spark Structured Streaming).

---

## 🚀 Hướng dẫn Chạy Nhanh trong 3 phút (Quickstart - Dành cho Người mới)

Nếu Thầy/Cô và sinh viên muốn chạy thử nghiệm ngay lập tức để kiểm chứng toàn bộ hệ thống hoạt động, chỉ cần thực hiện 3 bước sau:

### Môi trường khuyến nghị:
- **PowerShell** (có sẵn trên Windows/VS Code) HOẶC **Git Bash**.
- Đảm bảo **Docker Desktop** đã được bật và đang chạy.

```powershell
# Chuyển vào thư mục Kafka:
cd "d:\school\Big Data\Kafka"

# BƯỚC 1: Khởi động cụm Spark và Kafka KRaft (tự tạo 2 topic clickstream & product_events)
.\scripts\start-cluster.ps1         # Trên PowerShell
# hoặc: bash scripts/start-cluster.sh  # Trên Git Bash

# BƯỚC 2: Chạy thử nghiệm Producer gửi 200 tin & Consumer đọc phân chia Partition
.\scripts\demo-produce-consume.ps1  # Trên PowerShell
# hoặc: bash scripts/demo-produce-consume.sh

# BƯỚC 3: Chạy Spark Structured Streaming đọc 200 tin từ Kafka xử lý theo Window
.\scripts\run-spark-kafka-job.ps1   # Trên PowerShell
# hoặc: bash scripts/run-spark-kafka-job.sh
```

---

## 💡 Trải nghiệm Tương tác Thời gian thực (Zero-Install CLI)

> [!TIP]
> **Không cần cài đặt Python hay bất kỳ thư viện nào trên máy tính!**  
> Container Docker `apache/kafka:3.7.1` đã tích hợp sẵn công cụ dòng lệnh Producer và Consumer. Đây là cách trực quan nhất để cảm nhận ngay mô hình Publish/Subscribe trong 30 giây:

Mở **2 cửa sổ Terminal** (hoặc dùng tính năng Split Terminal của VS Code) đặt cạnh nhau:

```text
┌──────────────────────────────────────────────┬──────────────────────────────────────────────┐
│  TERMINAL 1 (BÊN PHẢI): CONSUMER NGHE SỰ KIỆN │  TERMINAL 2 (BÊN TRÁI): PRODUCER GÕ SỰ KIỆN  │
└──────────────────────────────────────────────┴──────────────────────────────────────────────┘
```

### 1. Tại Terminal 1 (Bên phải) – Mở Consumer chờ sẵn:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic clickstream \
  --from-beginning
```
*(Terminal này sẽ giữ kết nối và chờ đón mọi thông điệp gửi tới topic `clickstream`)*.

### 2. Tại Terminal 2 (Bên trái) – Mở Producer để gõ tin:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:29092 \
  --topic clickstream
```

### 3. Trải nghiệm gửi nhận:
- Tại **Terminal 2**, gõ một dòng bất kỳ rồi nhấn **Enter**:
  ```text
  > Khach hang CUST001 vua them iPhone 15 vao gio hang!
  > Don hang ORD999 vua thanh toan thanh cong 25000000 VND
  ```
- **Quan sát Terminal 1**: Dòng chữ lập tức xuất hiện ngay trên màn hình trong tích tắc!
- Nhấn `Ctrl + C` ở cả hai terminal khi muốn dừng phiên thử nghiệm.

---

## 1. Bảng Phiên bản Môi trường đã Kiểm thử

| Thành phần | Image / Phiên bản | Ghi chú kỹ thuật |
|---|---|---|
| **Kafka broker** | `apache/kafka:3.7.1` | Chế độ **KRaft combined mode** (broker + controller tích hợp trong 1 tiến trình), không cần Zookeeper riêng, khởi động cực nhanh và tiết kiệm RAM. |
| **Spark cluster** | `apache/spark:3.5.9-python3` | Cụm phân tán gồm 1 Master (`spark-master`) + 2 Workers (`spark-worker-1`, `spark-worker-2`) dùng lại từ Buổi 9. |
| **Spark Kafka Connector** | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9` | Tự động nạp qua `--packages` và cache vào `/opt/spark-data/ivy2cache`. |
| **Python Client (Host)** | Python 3.10 / 3.11 / 3.12 + `kafka-python` | Dùng để chạy `producer.py` và `consumer.py` từ máy Host Windows. |

---

## 2. Cấu trúc Thư mục Bài thực hành

```text
Kafka/
├── README.md                  # Tài liệu hướng dẫn toàn diện
├── docker-compose.yml         # Khởi tạo Kafka KRaft, Dual Listener (BROKER/EXTERNAL)
├── producer.py                # Gửi sự kiện: hỗ trợ 3 chiến lược Key, chế độ gõ tay và gửi chậm
├── consumer.py                # Đọc sự kiện: theo dõi Offset, phân chia Partition trong Consumer Group
├── spark_kafka_job.py         # Spark Structured Streaming đọc trực tiếp từ Kafka Topic
├── data/                      # Thư mục lưu trữ bền vững (Commit Log) mount từ container
├── checkpoint/                # Thư mục checkpoint của Spark Streaming phục hồi lỗi
├── logs/                      # Log thực tế đối chiếu của các lần chạy thành công
└── scripts/                   # Bộ script tự động hóa (hỗ trợ cả .ps1 và .sh)
    ├── start-cluster.ps1 / .sh          # Khởi động cụm Spark + Kafka + tạo Topic
    ├── demo-produce-consume.ps1 / .sh   # Chạy kịch bản Producer/Consumer chuẩn
    ├── run-spark-kafka-job.ps1 / .sh    # Chạy Spark Structured Streaming trên cụm thật
    └── reset-topics.ps1 / .sh           # Dọn sạch dữ liệu topic về 0 để thử nghiệm lại
```

---

## 3. Kiến trúc Mạng Docker và Dual Listeners

Để cả ứng dụng trên máy Host Windows và các container Spark trong Docker đều kết nối được vào Kafka, `docker-compose.yml` cấu hình **2 mạng và 3 Listeners**:

```text
[ Máy Host Windows ]                [ Mạng Docker nội bộ (spark_spark-net) ]
  producer.py / consumer.py                    spark-master / spark-workers
        │                                                    │
        ▼ (Port 9092)                                        ▼ (Port 29092)
  ┌────────────────────────────────────────────────────────────────────────┐
  │ Listener EXTERNAL: localhost:9092     Listener BROKER: kafka:29092     │
  │                                                                        │
  │                     KAFKA BROKER (apache/kafka:3.7.1)                  │
  │                     Topic "clickstream" (4 Partitions)                 │
  └────────────────────────────────────────────────────────────────────────┘
```

| Listener | Cổng | Dành cho ai kết nối? | Địa chỉ kết nối |
|---|:---:|---|---|
| `EXTERNAL` | `9092` | Ứng dụng chạy trực tiếp trên máy Host Windows (`producer.py`, `consumer.py`) | `localhost:9092` |
| `BROKER` | `29092`| Container Spark chạy trong mạng Docker nội bộ (`spark_kafka_job.py`) | `kafka:29092` |
| `CONTROLLER`| `9093` | Giao tiếp nội bộ quản lý cụm KRaft (Quorum Controller) | `kafka:9093` |

> [!IMPORTANT]
> **Thứ tự khởi động**: Kafka gắn thêm vào mạng Docker `spark_spark-net` do cụm Spark tạo ra. Do đó, script `start-cluster` luôn tự động kiểm tra và khởi động Spark trước, sau đó mới bật Kafka.

---

## 4. Quản lý Topic với Công cụ CLI

Khởi tạo 2 topic phục vụ bài toán RetailStream:
- `clickstream`: 4 partitions, replication-factor 1 (dùng cho toàn bộ demo).
- `product_events`: 2 partitions, replication-factor 1.

```bash
# 1. Tạo topic clickstream gồm 4 partition:
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic clickstream --bootstrap-server localhost:29092 \
  --partitions 4 --replication-factor 1

# 2. Xem thông tin chi tiết cấu hình và danh sách partition:
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic clickstream --bootstrap-server localhost:29092
```

Kết quả xuất màn hình chuẩn:
```text
Topic: clickstream	TopicId: ...	PartitionCount: 4	ReplicationFactor: 1	Configs:
	Topic: clickstream	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
	Topic: clickstream	Partition: 3	Leader: 1	Replicas: 1	Isr: 1
```

---

## 5. Thực nghiệm Producer: Khóa Phân vùng (Key Strategy)

Chạy trên máy Host Windows thông qua thư viện `kafka-python`. Nếu chưa cài, mở terminal cài đặt:
```bash
pip install kafka-python
```

### Kịch bản 1: Gửi chậm thời gian thực để quan sát dòng chảy sự kiện
```bash
# Gửi 200 sự kiện, mỗi sự kiện cách nhau 0.1 giây:
python producer.py --key-strategy session_id --delay 0.1
```

### Kịch bản 2: Chế độ tương tác gõ tay từ bàn phím (`--interactive`)
```bash
python producer.py --interactive
```
Sinh viên có thể nhập từng hành vi (`VIEW`, `ADD_TO_CART`, `PURCHASE`) để thấy ngay thông báo:
`==> DA GUI! Roi vao Partition=2, Offset=45 (Key='SESS00001')`.

### Kịch bản 3: Đối chiếu 2 chiến lược Key và Hiện tượng Lệch tải (Hot Partition)
```bash
python producer.py --key-strategy session_id
python producer.py --key-strategy product_id
```

**Bảng so sánh phân phối thực tế đo được trên 200 bản ghi:**
| Phân vùng | Chiến lược `key=session_id` (Phân bổ đều) | Chiến lược `key=product_id` (Lệch tải - Skew) |
|:---:|:---:|:---:|
| **Partition 0** | **55** bản ghi (13 keys phân biệt) | **28** bản ghi (9 keys) |
| **Partition 1** | **55** bản ghi (13 keys phân biệt) | **47** bản ghi (16 keys) |
| **Partition 2** | **41** bản ghi (11 keys phân biệt) | **60** bản ghi (18 keys) |
| **Partition 3** | **49** bản ghi (12 keys phân biệt) | **65** bản ghi (16 keys - Gấp 2.3 lần P0!) |

> [!NOTE]
> **Giải thích bản chất sư phạm**:
> - Khi dùng `key = session_id`: Các phiên người dùng xuất hiện đồng đều $\implies$ Tải chia đều cho 4 partition.
> - Khi dùng `key = product_id`: Một số sản phẩm "hot" (Flash Sale) được xem và mua liên tục $\implies$ Partition chứa sản phẩm đó bị dồn nén dữ liệu (*Hot Partition*), trong khi partition khác nhàn rỗi.

---

## 6. Thực nghiệm Consumer: Tọa độ Offset và Consumer Group

### Kịch bản 1: Một Consumer đơn lẻ đọc từ đầu (`--from-beginning`)
```bash
# Đọc và in chi tiết tọa độ Offset của từng tin nhắn:
python consumer.py --group demo-solo --consumer-name SOLO --from-beginning --max-messages 200
```
Kết quả ghi nhận:
```text
[SOLO] partition=0 | offset=  0 | key=SESS00039    | event_id=CEV000001 (ADD_TO_CART)
[SOLO] partition=0 | offset=  1 | key=SESS00039    | event_id=CEV000002 (VIEW)
...
--- TOM TAT KET QUA DOC [SOLO] ---
Phan vung duoc phan cong : [0, 1, 2, 3]  (Đọc trọn vẹn cả 4 partition)
Tong so message da doc  : 200
```

### Kịch bản 2: Hai Consumer cùng Group – Cơ chế Tự chia Partition
Mở 2 terminal song song và cùng tham gia group `demo-shared`:
- **Terminal A**:
  ```bash
  python consumer.py --group demo-shared --consumer-name C1 --from-beginning --duration 0
  ```
- **Terminal B**:
  ```bash
  python consumer.py --group demo-shared --consumer-name C2 --from-beginning --duration 0
  ```

**Kết quả tự động phân bổ của Kafka**:
- `[C1] GIAO PHAN VUNG: [2, 3]` $\implies$ Đọc được **90** bản ghi.
- `[C2] GIAO PHAN VUNG: [0, 1]` $\implies$ Đọc được **110** bản ghi.
- Tổng số bản ghi: $90 + 110 = 200$ (Khớp chính xác 100%, không trùng lặp, không thất thoát).

---

## 7. Tích hợp Spark Structured Streaming với Kafka Source

Job Spark đặt tại `Kafka/spark_kafka_job.py`. Khác với File Source ở Buổi 11, Spark đọc dữ liệu trực tiếp từ Kafka Topic bằng giao thức phân tán:

```python
# 1. Đọc luồng dữ liệu từ Kafka broker:
kafka_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Giải nén trường 'value' (mảng byte JSON) theo Schema:
parsed = kafka_raw.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.from_json(F.col("value").cast("string"), CLICKSTREAM_SCHEMA).alias("data")
).select("kafka_key", "kafka_partition", "kafka_offset", "data.*")

# 3. Tính toán theo Tumbling Window 1 ngày và Watermark 1 ngày:
windowed = parsed.withWatermark("event_time", "1 day") \
    .groupBy(F.window(F.col("event_time"), "1 day"), F.col("event_type")) \
    .count()
```

### Chạy Job trên cụm Spark Standalone thật:
```powershell
.\scripts\run-spark-kafka-job.ps1       # Trên PowerShell
# hoặc: bash scripts/run-spark-kafka-job.sh
```

Kết quả in ra từ Spark Console:
```text
batchId=0 numInputRows=200 endOffset={'clickstream': {'2': 41, '1': 55, '3': 49, '0': 55}}
TONG SO MESSAGE SPARK DA DOC TU KAFKA = 200
```
`endOffset` của Spark ghi nhận chính xác từng partition: $P_0=55, P_1=55, P_2=41, P_3=49$ $\implies$ Khớp 100% với số liệu Producer đã gửi!

---

## 8. Quy trình Reset Dữ liệu Sạch (Tránh lỗi Crash trên Windows)

> [!CAUTION]
> **Tuyệt đối KHÔNG dùng lệnh `kafka-topics.sh --delete` trên Windows!**  
> Khi xóa topic trên thư mục bind-mount từ Windows NTFS, Kafka thực hiện đổi tên thư mục log ở tầng nền (`clickstream-0` $\to$ `clickstream-0.<uuid>-delete`). Hệ điều hành Windows từ chối quyền (*AccessDeniedException*), khiến Kafka coi toàn bộ ổ đĩa bị hỏng và tự tắt broker (*Crash Exited 1*).

**Cách reset an toàn và chuẩn tắc 100%**:
Chỉ cần chạy script reset (hạ container, dọn sạch thư mục nhị phân `./data/`, và khởi động lại container mới):
```powershell
.\scripts\reset-topics.ps1         # Trên PowerShell
# hoặc: bash scripts/reset-topics.sh
```

---

## 9. Xử lý Sự cố Thường gặp (Troubleshooting)

1. **Lỗi `network spark_spark-net not found` khi khởi động Kafka**:
   - *Nguyên nhân*: Mạng Docker của cụm Spark chưa được tạo.
   - *Khắc phục*: Chạy `cd ..\Spark && docker compose up -d` trước, hoặc chỉ cần chạy script `start-cluster.ps1`.
2. **Lỗi `ImportError: cannot import name 'get_event_loop'` khi chạy Python**:
   - *Nguyên nhân*: Máy tính đang dùng Python 3.12+ không tương thích với bản `kafka-python 3.0.x`.
   - *Khắc phục*: Chuyển sang dùng công cụ CLI có sẵn trong container (Mục "Trải nghiệm Tương tác Thời gian thực" ở trên), hoặc cài đặt bản sửa lỗi: `pip install kafka-python-ng`.
3. **Lỗi `FileNotFoundException` khi Spark tải gói `--packages`**:
   - *Nguyên nhân*: Thư mục cache mặc định `$HOME/.ivy2` không ghi được trong image Spark.
   - *Khắc phục*: Job đã cấu hình sẵn `--conf spark.jars.ivy=/opt/spark-data/ivy2cache` để lưu cache vào thư mục có quyền ghi.
4. **Số lượng tin nhắn bị dồn lên 400 hoặc 600 tin**:
   - *Nguyên nhân*: Chạy `producer.py` nhiều lần mà chưa reset topic.
   - *Khắc phục*: Chạy `.\scripts\reset-topics.ps1` để đưa dữ liệu về 0 trước khi bắt đầu bài demo mới.

---

## 10. Phụ lục: Bảng Thuật ngữ Bằng Ẩn dụ Đời thường

| Thuật ngữ | Ẩn dụ Đời thường | Giải thích Kỹ thuật |
|---|---|---|
| **Kafka Broker** | *Bưu cục / Quầy giao dịch trung tâm* | Máy chủ lưu trữ và điều phối các dòng sự kiện. |
| **Topic** | *Hòm thư có dán nhãn phân loại* | Kênh logic để gửi và nhận dữ liệu (vd topic `clickstream`). |
| **Partition** | *Các ngăn kéo song song trong hòm thư* | Đơn vị chia nhỏ để ghi đĩa và đọc dữ liệu song song trên nhiều máy. |
| **Producer** | *Người gửi bưu phẩm* | Ứng dụng đẩy dữ liệu vào topic (Web, Mobile App). |
| **Consumer** | *Người nhận bưu phẩm* | Ứng dụng kéo dữ liệu từ topic về để tính toán. |
| **Offset** | *Số thứ tự trên phiếu lấy đồ* | Tọa độ nguyên tăng dần đánh dấu vị trí của bản tin trong partition. |
| **Consumer Group** | *Nhóm nhân viên chia nhau xử lý hòm thư* | Nhóm các consumer cùng chia sẻ partition để không ai đọc trùng dữ liệu của nhau. |
| **Commit Log** | *Cuốn sổ cái kế toán bìa cứng* | Nhật ký ghi nối tiếp xuống đĩa, đọc không xóa, cho phép tua lại (Replay). |
| **KRaft** | *Cơ chế tự quản không cần trợ lý* | Thuật toán đồng thuận nội bộ của Kafka thay thế cho Zookeeper cũ. |
| **Watermark** | *Hạn chót đóng cửa quầy* | Ngưỡng thời gian chờ dữ liệu đến muộn trong xử lý luồng Spark. |
