# Buổi 11 – Spark Structured Streaming (RetailStream / clickstream)

Job này đọc sự kiện `clickstream` qua **File Source** của Spark Structured
Streaming, tổng hợp số sự kiện theo `event_type` trong **tumbling window 5
phút** trên `event_time`, dùng **watermark 3 phút** để xử lý dữ liệu đến
muộn, ghi kết quả ra console sink, và dùng **checkpoint** để có thể dừng và
khởi động lại mà không xử lý lại từ đầu.

Giải thích nhanh các thuật ngữ trên:

- **File Source**: nguồn dữ liệu streaming của Spark đọc các file mới xuất
  hiện trong một thư mục theo thời gian, coi mỗi file mới là một đợt dữ
  liệu tới (không cần hệ thống message queue như Kafka).
- **Tumbling window**: cửa sổ thời gian cố định, không chồng lấn, ví dụ mỗi
  5 phút gộp dữ liệu một lần.
- **Watermark**: ngưỡng thời gian Spark chờ dữ liệu đến muộn trước khi coi
  một cửa sổ là "đã đóng" và không tính thêm dữ liệu đến sau ngưỡng đó nữa.
- **Checkpoint**: nơi lưu tiến trình xử lý (đã đọc đến đâu, trạng thái tổng
  hợp hiện tại) để có thể dừng/khởi động lại mà không mất dữ liệu hay xử lý
  lại từ đầu.

Nguồn dùng **file stream** (không dùng Kafka — Kafka được ghép ở Buổi 13)
để cô lập khái niệm streaming. Job chạy `local[*]` bên trong container Spark
đã dựng sẵn từ Buổi 9 (không bắt buộc chạy trên cluster Standalone thật,
khác với Buổi 9/10/12).

## 1. Cấu trúc thư mục

```text
StructuredStreaming/
├── README.md
├── streaming_job.py            (job chính: schema tường minh, window, watermark, checkpoint)
├── compress_timeline.py        (nén trục thời gian clickstream_sample.jsonl xuống 90 phút)
├── prepare_batches_v2.py       (chia dữ liệu đã nén thành 19 file cửa sổ 5 phút)
├── data_source_v2/             (clickstream_compressed.jsonl + các file batch)
├── batches_staging_v2/         (19 file win_XX_HHMM.jsonl + manifest.json mô tả late record)
├── scripts/
│   ├── prepare-data.sh         (tự động nén và chia 19 batch cửa sổ 5 phút)
│   ├── run-demo.sh             (bật cụm Spark, rót dữ liệu và chạy streaming)
│   └── run-demo-checkpoint-restart.sh (chứng minh fault tolerance: rót 10 batch -> chạy -> rót 9 batch -> chạy tiếp)
├── prepare_batches.py          (bản gốc: chia theo ngày, window 1 ngày — tham khảo lịch sử)
├── batches_staging/            (15 file batch theo ngày — tham khảo lịch sử)
└── logs_v2/                    (log console đầy đủ của các lần chạy với window 5 phút)
    ├── run1_update_mode_win1-10.log
    ├── run2_update_mode_win11-19_restart.log
    └── run3_complete_mode_all19_FULL.log
```

Checkpoint thật (bằng chứng chạy thật, giữ nguyên trạng thái) nằm tại
`Spark/data/session11_streaming/checkpoint_v2/` vì container Spark chỉ mount
được thư mục `Spark/data` và `Spark/jobs` (cùng kỹ thuật như Buổi 10
PySpark — xem `PySpark/README.md` mục 2).

## 2. Dữ liệu và cách mô phỏng luồng dữ liệu

Dữ liệu gốc `00_shared_data/sample/clickstream_sample.jsonl` (200 dòng,
đúng Data Contract: `event_id`, `event_time`, `customer_id`, `session_id`,
`product_id`, `event_type`) có `event_time` trải trên 15 ngày. Vì window
thực hành cần ở mức "vài phút" cho dễ quan sát, `compress_timeline.py` nén
trục thời gian của 200 dòng này xuống còn **90 phút**, giữ nguyên thứ tự và
tỉ lệ khoảng cách tương đối giữa các sự kiện, **không sửa** bất kỳ trường
nào khác và không sửa file gốc. Kết quả:
`data_source_v2/clickstream_compressed.jsonl`.

`prepare_batches_v2.py` chia 200 dòng đã nén thành **19 file** theo cửa sổ 5
phút: `win_01_0900.jsonl` .. `win_19_1030.jsonl`. Mỗi file mới xuất hiện
trong thư mục nguồn tương ứng với "một đợt dữ liệu mới tới" mà File Source
của Structured Streaming sẽ tự phát hiện.

**Late data có chủ đích:** bản ghi `event_id = CEV000068`, thuộc cửa sổ
`[09:10, 09:15)`, được cố ý chèn vào file của cửa sổ `[09:30, 09:35)` (file
thứ 7 trong thứ tự xử lý) — mô phỏng độ trễ khoảng 4 cửa sổ (~20 phút).
Bản ghi không bị sửa bất kỳ trường nào, chỉ đổi file nó xuất hiện. Chi tiết
đầy đủ trong `batches_staging_v2/manifest.json`.

## 3. Cấu hình window/watermark hiện tại

- Tumbling window: **5 phút** trên `event_time`.
- Watermark delay: **3 phút** (`withWatermark("event_time", "3 minutes")`).
- Khai báo trong khối `CONFIG` của `streaming_job.py`, có thể override qua
  biến môi trường `STREAM_WINDOW_DURATION` và `STREAM_WATERMARK_DELAY`.

```python
WINDOW_DURATION = os.environ.get("STREAM_WINDOW_DURATION", "5 minutes")
WATERMARK_DELAY = os.environ.get("STREAM_WATERMARK_DELAY", "3 minutes")
```

## 4. Cách chạy

```bash
export MSYS_NO_PATHCONV=1   # cần thiết trên Git Bash / Windows

# 1) Khởi động cụm Spark (nếu chưa chạy sẵn)
cd Spark
docker compose up -d
curl -s http://localhost:8080/json/    # xác nhận status=ALIVE, 2 worker ALIVE

# 2) Đảm bảo job và dữ liệu đã có trong container, theo đúng đường dẫn mount:
#    Spark/jobs/streaming_job.py
#    Spark/data/session11_streaming/data_source_v2/   (file .jsonl cửa sổ 5 phút)

# 3) Chạy job ở output mode "update" — Trigger.availableNow xử lý hết dữ
#    liệu đang có trong SOURCE_DIR rồi tự dừng (phù hợp để demo nhiều đợt
#    rót dữ liệu + checkpoint restart)
docker exec \
  -e SPARK_MASTER_URL="local[*]" \
  -e STREAM_SOURCE_DIR="/opt/spark-data/session11_streaming/data_source_v2" \
  -e STREAM_CHECKPOINT_DIR="/opt/spark-data/session11_streaming/checkpoint_v2/update_mode" \
  -e STREAM_OUTPUT_MODE="update" \
  -e STREAM_WINDOW_DURATION="5 minutes" \
  -e STREAM_WATERMARK_DELAY="3 minutes" \
  spark-master /opt/spark/bin/spark-submit \
  --master local[*] --driver-memory 512m \
  /opt/spark-apps/streaming_job.py
```

Để so sánh output mode, đổi `STREAM_OUTPUT_MODE` thành `complete` hoặc
`append` và trỏ `STREAM_CHECKPOINT_DIR` sang một thư mục checkpoint khác —
Spark không cho phép đổi output mode khi tái sử dụng checkpoint đã tạo cho
mode khác. Lý do: checkpoint lưu trạng thái xử lý gắn liền với ngữ nghĩa
của một output mode cụ thể (ví dụ dữ liệu nào đã được in ra, dữ liệu nào
còn chờ); đổi mode giữa chừng sẽ làm trạng thái đã lưu không còn đúng nghĩa
nữa, nên Spark chặn luôn thao tác này.

### Kiểm chứng checkpoint được tái sử dụng (dừng rồi khởi động lại)

```bash
# Lần chạy 1: chỉ có 10 file đầu (win_01..win_10) trong data_source_v2/
# -> chạy lệnh ở mục trên, checkpoint mới, batch kết thúc ở 10 (batch rỗng)

# Thêm 9 file còn lại (win_11..win_19) vào CÙNG data_source_v2/,
# KHÔNG xoá checkpoint, rồi chạy lại đúng lệnh cũ ở trên.

# Xác nhận: batch tiếp tục từ 11 (không quay lại 0)
ls Spark/data/session11_streaming/checkpoint_v2/update_mode/commits/ \
  | grep -v crc | sort -n
# 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
```

## 5. Kết quả kiểm thử thật

Toàn bộ kết quả dưới đây được chạy thật (không suy đoán), log và checkpoint
gốc lưu tại `logs_v2/` và `Spark/data/session11_streaming/checkpoint_v2/`.

**Schema tường minh** (không dùng `inferSchema`):

```text
struct<event_id:string, event_time:timestamp, customer_id:string,
       session_id:string, product_id:string, event_type:string>
```

**Window aggregation** — ví dụ cửa sổ `{09:40:00, 09:45:00}` (`update` mode):
VIEW = 6, PURCHASE = 2, ADD_TO_CART = 3, khớp đúng dữ liệu batch nguồn.

**Watermark / late data:** bản ghi muộn `CEV000068` (thuộc cửa sổ
`[09:10,09:15)`, đến ở batch xử lý cửa sổ `[09:30,09:35)`) bị loại đúng
1 bản ghi:

```text
batchId=6 numInputRows=10 numRowsDroppedByWatermark=1
```

`numRowsDroppedByWatermark` là số bản ghi bị watermark coi là "đến quá
muộn" (thuộc một cửa sổ đã đóng) và bị loại khỏi kết quả tổng hợp.

**Checkpoint restart:** lần chạy 2 (thêm 9 file còn lại, cùng checkpoint)
in ra `WARN ... The state for version 11 doesn't exist in loadedMaps.
Reading snapshot file...` rồi tiếp tục từ `Batch: 11` đến `Batch: 19` —
xác nhận state được nạp lại từ checkpoint, không xử lý lại từ batch 0.

**So sánh output mode:**

| Output mode | Late data (`CEV000068`) | Khi nào một cửa sổ được in ra | Số dòng in ra tích lũy |
|---|---|---|---|
| `update` | Bị loại (watermark) | Ngay khi có thay đổi | Chỉ dòng thay đổi |
| `append` | Bị loại (watermark) | Chỉ khi watermark đã vượt qua cửa sổ (trễ) | Mỗi cửa sổ in đúng 1 lần, không sửa lại |
| `complete` | **Không bị loại** (được tính) | Mọi batch, in lại toàn bộ | Toàn bộ bảng mỗi batch |

Ở `complete` mode, bảng cuối cùng có **45 dòng** (tổ hợp cửa sổ ×
`event_type`), tổng **= 200** — khớp 100% tổng số dòng gốc của
`clickstream_compressed.jsonl`. Hành vi `append` mô tả trong bảng trên được
xác nhận với window 1 ngày ở lần thử nghiệm gốc (cùng cơ chế API, chỉ khác
đơn vị thời gian) — xem chi tiết trong `LOCAL_REPORT.md` mục V05.

## 6. Lưu ý tài nguyên (RAM container `spark-master`)

Container `spark-master` trong `Spark/docker-compose.yml` có giới hạn
`mem_limit: 1536m`. Nếu một job khác cũng chạy `local[*]` **đồng thời trong
cùng container** này (ví dụ job MLlib), RAM có thể cạn kiệt và tiến trình bị
kernel/JVM chấm dứt giữa chừng. Trước khi chạy, kiểm tra `docker stats` /
`docker top spark-master`; nếu có tiến trình `local[*]` khác đang chạy, đợi
nó kết thúc rồi chạy lại — checkpoint không bị hỏng, chỉ mất tối đa 1 batch
chưa commit.

## 7. Hướng dẫn chạy nhanh bằng Script (Khuyến nghị)

Toàn bộ quy trình nén dữ liệu, chia 19 batch, nộp streaming job và kiểm chứng cơ chế phục hồi checkpoint đã được tự động hóa trong thư mục `StructuredStreaming/scripts/`.

### Môi trường khuyến nghị:
- **Git Bash** (trên Windows) hoặc Terminal Linux/macOS.
- Nếu dùng **PowerShell**: hãy gọi qua Git Bash bằng `bash scripts/<tên_script>.sh`.

### Thư mục làm việc (Working Directory):
Mở terminal và chuyển vào thư mục `StructuredStreaming`:
```bash
cd "d:/school/Big Data/StructuredStreaming"
```

### Thứ tự thực hiện:

#### Bước 1: Chuẩn bị dữ liệu micro-batches
Tạo 19 file cửa sổ 5 phút kèm 1 bản ghi đến muộn (late record) từ `00_shared_data/sample/clickstream_sample.jsonl`:
```bash
bash scripts/prepare-data.sh
```
*Lệnh này làm gì:*
1. Chạy `compress_timeline.py`: Nén trục thời gian 200 bản ghi clickstream xuống còn 90 phút.
2. Chạy `prepare_batches_v2.py`: Chia 200 bản ghi thành 19 file batch (`win_01` đến `win_19`) vào thư mục `batches_staging_v2/`, đồng thời cố ý gán sự kiện `CEV000068` (xảy ra lúc 10:28 nhưng đến muộn ở batch 10:55) để kiểm thử watermark.

#### Bước 2: Chạy demo trọn gói Streaming với Output Mode mong muốn
```bash
# Chạy với chế độ mặc định update (khuyến nghị để quan sát dòng thay đổi):
bash scripts/run-demo.sh update

# Hoặc thử nghiệm với complete mode (in lại toàn bộ bảng tổng hợp mỗi batch):
bash scripts/run-demo.sh complete
```
*Lệnh này làm gì:*
1. Tự động kiểm tra và khởi động cụm Spark (`cd ../Spark && docker compose up -d`).
2. Dọn sạch checkpoint cũ để chạy từ đầu.
3. Rót toàn bộ 19 file batch vào thư mục nguồn của Spark (`Spark/data/session11_streaming/data_source_v2/`) và copy `streaming_job.py` vào `Spark/jobs/`.
4. Kích hoạt `spark-submit` chạy với `Trigger.availableNow` và in kết quả từng micro-batch ra terminal.

#### Bước 3: Kiểm chứng khả năng phục hồi từ Checkpoint (Fault Tolerance)
Để chứng minh Spark ghi nhớ trạng thái và tiếp tục xử lý chính xác khi có dữ liệu mới tới:
```bash
bash scripts/run-demo-checkpoint-restart.sh
```
*Lệnh này làm gì:*
1. **Lần 1:** Chỉ rót 10 file đầu (`win_01` .. `win_10`) vào thư mục nguồn và chạy job -> Job kết thúc ở batch 10.
2. **Lần 2:** Giữ nguyên checkpoint cũ, rót tiếp 9 file còn lại (`win_11` .. `win_19`) vào thư mục và chạy lại -> Spark đọc checkpoint, phát hiện dữ liệu mới và bắt đầu chạy tiếp từ batch 11 đến batch 19 mà không hề xử lý lại từ batch 0.

#### Bước 4: Dừng cụm khi hoàn tất
```bash
cd ../Spark && bash scripts/stop-cluster.sh
```

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích đơn giản |
|---|---|
| **Container** | Một "hộp" chạy phần mềm biệt lập, giống một máy ảo thu nhỏ. Một cụm (Hadoop/Spark/Kafka) trong dự án này gồm nhiều container chạy trên CÙNG một máy thật, giả lập nhiều máy. |
| **Docker Compose** | Công cụ mô tả "cần bao nhiêu container, cấu hình ra sao" trong 1 file (`docker-compose.yml`), rồi bật/tắt tất cả cùng lúc bằng 1 lệnh. |
| **mem_limit** | Giới hạn RAM tối đa cấp cho 1 container, khai báo trong `docker-compose.yml`. Nếu container cần nhiều RAM hơn mức này sẽ bị OOM (hết bộ nhớ, bị hệ điều hành tự tắt). |
| **Spark Standalone Cluster** | Cụm Spark tự quản lý (không cần YARN/Kubernetes), gồm 1 Master (điều phối) + nhiều Worker (thực thi việc). |
| **local[\*]** | Chế độ chạy Spark ngay trên 1 máy (không dùng cluster), dùng để học/thử nhanh. `local[*]` nghĩa là dùng tất cả CPU core có sẵn của máy đó. |
| **spark-submit** | Lệnh dùng để "nộp" 1 chương trình Spark cho cluster chạy. |
| **Streaming (xử lý luồng)** | Xử lý dữ liệu liên tục, mới đến đâu xử lý đến đó — khác với "batch" (xử lý theo lô, gom đủ dữ liệu rồi chạy 1 lần). |
| **Window (cửa sổ thời gian)** | Gom các sự kiện lại theo từng khoảng thời gian cố định để tính toán (ví dụ "đếm số lượt xem mỗi 5 phút"). |
| **Watermark** | Một "hạn chót" cho phép dữ liệu đến muộn bao lâu vẫn được tính. Sự kiện đến sau hạn này sẽ bị bỏ qua (tuỳ chế độ). Có watermark giúp hệ thống biết khi nào "chốt" một cửa sổ thời gian thay vì chờ mãi. |
| **Late data (dữ liệu đến muộn)** | Sự kiện có thời gian xảy ra (event time) sớm nhưng vì lý do mạng/hệ thống lại "đến nơi" trễ hơn các sự kiện khác. |
| **Checkpoint** | "Điểm lưu tạm" ghi lại job đã xử lý tới đâu — nếu job bị dừng và chạy lại, nó đọc checkpoint để tiếp tục đúng chỗ, không phải xử lý lại từ đầu. |
| **Output mode (complete / append / update)** | Cách kết quả được ghi ra mỗi lần cập nhật: `complete` = ghi lại toàn bộ kết quả từ đầu mỗi lần; `append` = chỉ ghi thêm dòng mới; `update` = chỉ ghi những dòng có thay đổi. |
