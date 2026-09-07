# Tối ưu hoá Spark trên RetailStream — Buổi 15

Ba kịch bản minh hoạ các vấn đề tối ưu Spark phổ biến, chạy thật trên dữ liệu RetailStream
và trên cụm Spark Standalone đã dựng ở Buổi 9 (`Spark/docker-compose.yml`, image
`apache/spark:3.5.9-python3`, 1 master + 2 worker):

- **Data skew** (lệch tải): khi một số giá trị khóa (key) xuất hiện nhiều hơn hẳn các giá trị
  khác, phần lớn dữ liệu bị dồn vào một partition duy nhất, khiến executor xử lý partition đó
  chạy lâu hơn hẳn các executor khác — cả job phải chờ executor chậm nhất.
- **Small files problem** (quá nhiều tệp nhỏ): khi dữ liệu bị ghi thành rất nhiều tệp kích
  thước nhỏ thay vì ít tệp lớn hơn, hệ thống lưu trữ (đặc biệt HDFS) tốn nhiều chi phí quản lý
  metadata và mở/đóng tệp hơn mức cần thiết.
- **Shuffle lớn** (xáo trộn dữ liệu lớn): khi join hoặc gom nhóm dữ liệu theo khóa mà không lọc
  bớt trước, Spark phải di chuyển một lượng lớn dữ liệu qua mạng giữa các executor để dữ liệu
  cùng khóa gặp nhau — tốn băng thông mạng và đĩa.

Bốn kịch bản minh hoạ trong tài liệu này:

1. **Data skew** — `data_skew_demo.py`
2. **Small files problem** — `small_files_demo.py`
3. **Shuffle lớn khi join** — `shuffle_demo.py`
4. **Hot partition (Kafka)** — không chạy job riêng ở buổi này; dùng lại số liệu đã đo ở
   Buổi 13 (xem mục 6).

**Phạm vi số liệu:** toàn bộ demo chạy ở quy mô lab (50.000–125.000 dòng, vài MB đến vài
chục MB dữ liệu), trên cụm Docker giả lập 2 worker x 1 core x 640 MB. Đây là demo giáo dục
minh hoạ **cơ chế** vì sao data skew, small files và shuffle lớn gây vấn đề hiệu năng —
**không phải benchmark production**. Số liệu tuyệt đối (byte, mili-giây) chỉ có ý nghĩa so
sánh nội bộ giữa hai biến thể trong cùng một demo, không đại diện hiệu năng thật ở quy mô
GB/TB hay cụm nhiều node. Không suy diễn kiểu "phương án X nhanh hơn Y bao nhiêu phần trăm"
từ các con số này như thể đó là số liệu production.

## 1. Yêu cầu môi trường

- Cụm Spark Standalone đã dựng sẵn từ Buổi 9, khởi động bằng `docker compose up -d` trong
  thư mục `Spark/` (không dựng hạ tầng mới, không sửa `Spark/docker-compose.yml`).
- Dữ liệu nguồn đã được copy vào `Spark/data/session15_optimization/`:
  `clickstream_uniform.jsonl`, `clickstream_skewed.jsonl`, `orders_lab.csv`,
  `order_items_lab.csv`.
- Job Spark chạy thật đặt tại `Spark/jobs/{data_skew_demo,small_files_demo,shuffle_demo}.py`
  (bản sao giống hệt các file cùng tên trong thư mục này).

## 2. Chạy các kịch bản thử nghiệm

Chạy từ Git Bash (PowerShell dùng cú pháp tương đương). Trước tiên xác nhận cụm đang chạy và
tạo thư mục event log trong container (chỉ cần một lần):

```bash
export MSYS_NO_PATHCONV=1
cd Spark
docker compose up -d
docker exec spark-master mkdir -p /opt/spark-data/session15_optimization/event_logs
```

### Data skew

AQE là cơ chế Spark tự động điều chỉnh lại kế hoạch thực thi TRONG LÚC job đang chạy, dựa trên
số liệu thực tế đã quan sát được (ví dụ tự gộp các partition nhỏ lại) — hữu ích trong thực tế
nhưng ở đây làm mất đúng tín hiệu skew cần quan sát nên phải tắt để minh họa.

Bắt buộc tắt Adaptive Query Execution (`spark.sql.adaptive.enabled=false`) — nếu không tắt,
tính năng `CoalescePartitions` của AQE sẽ gộp các partition nhỏ lại ở quy mô demo này và làm
mất khả năng quan sát skew qua nhiều task.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark-data/session15_optimization/event_logs \
  --executor-memory 512m --driver-memory 512m --total-executor-cores 2 \
  /opt/spark-apps/data_skew_demo.py
```

### Small files problem

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark-data/session15_optimization/event_logs \
  --executor-memory 512m --driver-memory 512m --total-executor-cores 2 \
  /opt/spark-apps/small_files_demo.py
```

### Shuffle lớn (join có lọc trước + broadcast)

Cũng bắt buộc tắt AQE, cùng lý do như ở data skew.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark-data/session15_optimization/event_logs \
  --executor-memory 512m --driver-memory 512m --total-executor-cores 2 \
  /opt/spark-apps/shuffle_demo.py
```

### Đọc lại bằng chứng từ event log

Spark UI REST API (`:4040`) chỉ tồn tại trong lúc driver process còn sống, và dự án này không
cấu hình Spark History Server, nên bằng chứng số liệu được lấy lại từ Spark event log SAU KHI
job đã dừng — không phụ thuộc thời điểm scrape:

```bash
python Optimization/parse_event_log.py Spark/data/session15_optimization/event_logs/<app-id>
```

Thay `<app-id>` bằng ID ứng dụng in ra ở dòng `APP_ID:` khi job chạy (ví dụ
`app-20260820073906-0017`).

## 3. Dữ liệu dùng

Không tạo dataset nghiệp vụ mới; dùng nguyên dữ liệu lab RetailStream:
`clickstream` (100.000 dòng), `orders` (50.000 dòng), `order_items` (124.862 dòng hợp lệ).

Biến thể "méo" duy nhất tạo riêng cho buổi này là `data/clickstream_skewed.jsonl`, sinh bởi
`make_skewed_data.py` (seed=42, deterministic): gán lại `product_id = "PROD00001"` cho khoảng
90% bản ghi (thực tế đạt 89,92%, 89.928/100.000 dòng), 10% còn lại giữ nguyên phân phối gốc.
Phân phối gốc gần như đều — `product_id` chiếm nhiều nhất trong bản gốc chỉ khoảng 0,04%.

## 4. Kết quả đo được — Data skew

`df.repartition(8, "product_id")` hash-partition lại toàn bộ 100.000 bản ghi thô (không có
map-side combiner), bộc lộ trực tiếp độ lệch tải giữa các partition:

```text
[UNIFORM] so ban ghi tren tung partition sau repartition(8, product_id):
   [12953, 11934, 12728, 12758, 12513, 12010, 13163, 11941]
   max/min partition ratio: 1.10x

[SKEWED] so ban ghi tren tung partition sau repartition(8, product_id):
   [91210, 1184, 1276, 1297, 1275, 1223, 1290, 1245]
   max/min partition ratio: 77.04x
```

Bằng chứng bổ sung từ Spark event log — shuffle-read byte trên mỗi task:

```text
UNIFORM (Stage 5, 8 task): shuffle_read_bytes min=61626 median=65690 max=68332 (ratio ~1.11x)
SKEWED  (Stage 16, 8 task): shuffle_read_bytes min=6514  median=6948  max=34902 (ratio ~5.36x)
```

**Ghi chú quan trọng khi đọc số liệu này:**

- Chỉ số **số dòng/partition** (77,04x) là bằng chứng chính, mạnh và trực tiếp nhất — không
  bị ảnh hưởng bởi nén dữ liệu.
- Chỉ số **byte shuffle/task** (5,36x) thấp hơn nhiều so với 77,04x vì Spark nén dữ liệu
  shuffle bằng LZ4 mặc định — chuỗi `"PROD00001"` lặp lại 90% nên nén rất tốt, làm giảm biểu
  hiện của skew ở mức byte. Đây là hiện tượng thật, không phải lỗi đo.
- **Thời gian task (mili-giây) không thể hiện skew rõ ở quy mô lab này** (task lớn nhất
  khoảng 200–670ms) vì 91.210 dòng vẫn xử lý trong bộ nhớ dưới 1 giây ở quy mô vài chục MB. Ở
  quy mô GB/TB thật, cùng cơ chế này sẽ khiến một task/executor chạy lâu hơn hẳn phần còn
  lại — đây là suy luận về cơ chế dựa trên bằng chứng số dòng/partition thật, không phải số
  đo thời gian thật ở quy mô lớn.
- `groupBy("product_id").count()` không phù hợp để minh hoạ skew vì Spark tự làm map-side
  partial aggregate (combiner) trước khi shuffle: dữ liệu qua shuffle chỉ là
  `(product_id, partial_count)` đã rút gọn theo số key phân biệt mỗi partition đầu vào, gần
  như đều dù 1 key chiếm 90% số bản ghi thô. Vì vậy demo dùng `repartition()` thay vì
  `groupBy().count()`.

## 5. Kết quả đo được — Small files problem

`repartition(n)` chia lại dữ liệu thành đúng n partition, có xáo trộn (shuffle) dữ liệu qua
mạng để cân bằng. `coalesce(n)` chỉ gộp bớt partition hiện có xuống còn tối đa n, không xáo
trộn qua mạng — nên nếu dữ liệu gốc đã nằm trong ít partition hơn n, `coalesce` không thể
tăng lên được.

Ghi cùng một tập `orders` (50.000 dòng) bằng `repartition(50)` và `coalesce(2)`, sau đó đọc
lại cả hai và ép `count()` toàn bộ:

```text
INPUT_ROWS: 50000
MANY_FILES (repartition 50) -> so file that: 50
FEW_FILES  (coalesce 2)     -> so file that: 1
MANY_FILES read count: 50000   (khop input)
FEW_FILES  read count: 50000   (khop input)
```

Bằng chứng event log:

```text
Stage 3 (shuffle-write cho repartition(50)): 1 task, duration=1919ms, shuffle_write=2.464.236 bytes
Stage 5 (ghi 50 file CSV, phia sau shuffle): 50 task, duration min=118 max=601 median=153 ms
Stage 6 (ghi coalesce(2), khong shuffle): 1 task, duration=770ms

Stage 7 (doc lai 50 file "many"): 2 task, duration min=915 max=1074 ms, input=3.733.387 bytes
Stage 10 (doc lai 1 file "few"):  1 task, duration=163ms, input=3.729.400 bytes
```

**Ghi chú quan trọng:**

- `coalesce(2)` chỉ tạo ra **1 file, không phải 2**: file CSV nguồn (~3,75 MB) được Spark đọc
  thành đúng 1 partition ban đầu (nhỏ hơn ngưỡng chia partition mặc định), và `coalesce()` chỉ
  có thể giảm số partition (không shuffle), không thể tăng từ 1 lên 2. Kết quả được ghi nhận
  trung thực thay vì ép cho ra đúng 2 file.
- Đọc lại 50 file nhỏ chỉ tạo **2 task, không phải 50 task**: Spark dùng cơ chế "bin-packing"
  khi lập kế hoạch đọc file, dựa trên `spark.sql.files.maxPartitionBytes` (mặc định 128 MB) và
  `openCostInBytes` (mặc định 4 MB/file) — 50 file nhỏ được coi có "chi phí ảo" khoảng
  50 × 4 MB = 200 MB, vượt 128 MB nên tách thành 2 partition đọc. Đây chính là cơ chế Spark
  dùng để giảm nhẹ tác hại của small-files problem ở mức task — nhưng vấn đề small-files vẫn
  tồn tại thật ở tầng lưu trữ (50 file thật trên đĩa/HDFS = 50 lần mở file, 50 entry metadata,
  tăng áp lực cho NameNode nếu chạy trên HDFS thật ở quy mô lớn hơn) dù không lộ rõ ở số task
  đọc lại tại quy mô lab này.
- Vì lý do trên, khác biệt task-đọc-lại (2 so với 1, khoảng 2 lần) nhỏ hơn nhiều khác biệt số
  file (50 so với 1, 50 lần) — small-files là vấn đề **tầng lưu trữ** trước khi là vấn đề
  **tầng task**, và framework hiện đại có cơ chế giảm nhẹ một phần nhưng không loại bỏ hoàn
  toàn chi phí gốc.

## 6. Kết quả đo được — Shuffle lớn khi join

Broadcast join: khi một bảng đủ nhỏ, Spark gửi (broadcast) toàn bộ bảng đó tới mọi executor
thay vì phải shuffle dữ liệu của bảng lớn qua mạng để join — nhanh hơn nhiều so với join thông
thường (SortMergeJoin).

Join `orders` (50.000 dòng) với `order_items` (124.862 dòng hợp lệ) theo `order_id`, hai
cách:

- **UNOPT**: join toàn bộ 2 bảng, tắt auto-broadcast
  (`spark.sql.autoBroadcastJoinThreshold=-1`) để ép sort-merge join thật có shuffle cả 2
  phía — ở quy mô lab, 2 bảng đủ nhỏ để Spark mặc định tự broadcast nếu không tắt, nên phải
  tắt để mô phỏng đúng tình huống bảng lớn hơn trong thực tế.
- **OPT**: lọc `orders` theo `status="PAID"` trước khi join (còn 17.519/50.000 dòng), bật lại
  auto-broadcast mặc định (10 MB) — Spark tự động broadcast `orders_paid` (đã đủ nhỏ) thay vì
  shuffle.

```text
orders rows: 50000, order_items rows: 124862
UNOPT join result rows: 124862
orders_paid rows: 17519
OPT join result rows: 43835

Event log - UNOPT (Stage 4 + Stage 5, shuffle-write 2 phia cua join):
   Stage 4 (shuffle-write order_items): 402.719 bytes
   Stage 5 (shuffle-write orders):      287.716 bytes
   Stage 6 (shuffle-read, 8 task):      690.435 bytes  (= 402.719 + 287.716, khop)
   => Tong luu luong shuffle (write+read) cho UNOPT: 1.380.870 bytes

Event log - OPT (Stage 10, broadcast collect orders_paid):
   Stage 10: 1 task, input=3.750.477 bytes, KHONG co Shuffle Write/Read Metrics
   => Khong co stage shuffle nao gan voi phep join OPT (broadcast thay the shuffle hoan toan)
```

**Ghi chú quan trọng:**

- Số dòng kết quả UNOPT (124.862) khác OPT (43.835) là kết quả đúng như dự kiến — OPT lọc
  trước nên join ra ít dòng hơn (chỉ đơn hàng PAID). Đây không phải bug: mục đích minh hoạ là
  "lọc trước khi join giảm khối lượng shuffle", không phải "hai cách cho cùng một kết quả".
- Bằng chứng byte shuffle (690.435 so với ~0) đo trực tiếp qua Shuffle Write/Read Metrics
  trong event log, không suy đoán.
- Việc UNOPT phải tắt `autoBroadcastJoinThreshold` là có chủ đích để mô phỏng kịch bản bảng
  lớn — ở quy mô lab thật, Spark mặc định đã đủ thông minh để tự broadcast, nên nếu không tắt,
  "vấn đề shuffle lớn" sẽ không xảy ra tự nhiên ở quy mô nhỏ này. Ở quy mô dữ liệu lớn thật,
  Spark sẽ tự shuffle nếu không lọc/không có bảng đủ nhỏ để broadcast — đây chính là lý do tối
  ưu "lọc trước + broadcast" quan trọng trong thực tế.

## 7. Hot partition (Kafka) — trích dẫn lại từ bài Kafka

Phần này chỉ trích lại số liệu đã đo ở bài Kafka để đối chiếu, không chạy lại job mới.
Kafka phân phối message vào partition dựa trên khóa (key) của message — nếu chọn khóa
không phù hợp (ví dụ nhiều message có cùng khóa), một partition có thể nhận tải nhiều
hơn hẳn các partition khác, gọi là hiện tượng hot partition.

Số liệu thật đo được khi gửi 200 message với 2 chiến lược chọn khóa khác nhau:

```text
Phan phoi theo partition (key_strategy=session_id):
  partition 0: 55  partition 1: 55  partition 2: 41  partition 3: 49  (tong 200)

Phan phoi theo partition (key_strategy=product_id):
  partition 0: 28  partition 1: 47  partition 2: 60  partition 3: 65  (tong 200)
```

Ở mức dữ liệu mẫu (200 message) của Buổi 13, không có key nào chiếm áp đảo hẳn một partition —
cả hai chiến lược key đều tương đối cân bằng. Đây là minh hoạ đúng cơ chế (key khác nhau dẫn
đến phân phối partition khác nhau, do Kafka phân vùng theo `hash(key) % numPartitions` mặc
định), nhưng không phải một ví dụ hot partition cực đoan (không có partition nào bị quá tải
rõ rệt, mức lệch lớn nhất khoảng 2,3 lần, không phải kiểu 90/10 như data skew ở mục 4).

## 8. Bảng tóm tắt

| # | Tình huống | Bằng chứng chính | Kết luận |
|---|---|---|---|
| 1 | Data skew | `repartition(8,"product_id")`: UNIFORM lệch tối đa 1,10x giữa 8 partition (12.010–13.163 dòng); SKEWED lệch **77,04x** (1 partition 91.210 dòng / 7 partition còn lại ~1.200–1.300 dòng mỗi cái) | Lệch số dòng/partition rõ ràng; lệch thời gian task ở quy mô lab không rõ |
| 2 | Small files | `repartition(50)` tạo 50 file thật; `coalesce(2)` tạo 1 file (input CSV chỉ có 1 partition gốc nên coalesce không tăng được lên 2); đọc lại: 50 file → 2 task, 1 file → 1 task | Số file/task khác biệt thật; cơ chế bin-packing của Spark làm giảm bớt số task so với số file nhưng không loại bỏ chi phí tầng lưu trữ |
| 3 | Shuffle lớn | UNOPT (join đầy đủ, tắt auto-broadcast): tổng shuffle write+read 1.380.870 bytes. OPT (lọc `status=PAID` trước rồi broadcast join): 0 byte shuffle cho join | Khác biệt shuffle byte rõ ràng, đo trực tiếp từ event log |
| 4 | Hot partition | Trích dẫn lại số liệu Buổi 13 (Kafka), không chạy job mới | Minh hoạ đúng cơ chế phân vùng theo key, không phải ví dụ cực đoan |

## 9. Cấu trúc thư mục

```text
Optimization/
├── README.md                  (tài liệu này)
├── make_skewed_data.py        (sinh dữ liệu clickstream lệch riêng cho buổi này)
├── parse_event_log.py         (đọc lại Spark event log JSON -> số liệu task/stage thật)
├── data_skew_demo.py          (bản sao Spark/jobs/data_skew_demo.py)
├── small_files_demo.py        (bản sao Spark/jobs/small_files_demo.py)
├── shuffle_demo.py            (bản sao Spark/jobs/shuffle_demo.py)
├── scripts/
│   └── run-demo.sh            (chạy tự động liên tiếp cả 3 kịch bản: skew, small files, shuffle)
├── data/
│   └── clickstream_skewed.jsonl   (dữ liệu lệch có chủ đích, sinh bởi make_skewed_data.py)
└── evidence/
    ├── data_skew_demo_run.log
    ├── skew_stage_metrics.txt
    ├── small_files_demo_run.log
    ├── small_files_stage_metrics.txt
    ├── shuffle_demo_run.log
    └── shuffle_stage_metrics.txt
```

## 10. Hướng dẫn chạy nhanh bằng Script (Khuyến nghị)

Thư mục `Optimization/scripts/` có sẵn script `run-demo.sh` giúp tự động hóa toàn bộ việc chuẩn bị dữ liệu, cấu hình tắt AQE/bật Event Log và chạy liên tiếp cả 3 kịch bản thử nghiệm trên cụm Spark thật.

### Môi trường khuyến nghị:
- **Git Bash** (trên Windows) hoặc Terminal Linux/macOS.
- Nếu dùng **PowerShell**: hãy gọi qua Git Bash bằng `bash scripts/<tên_script>.sh`.

### Thư mục làm việc (Working Directory):
Mở terminal và chuyển vào thư mục `Optimization`:
```bash
cd "d:/school/Big Data/Optimization"
```

> [!IMPORTANT]
> **Chuẩn bị file dữ liệu kiểm thử:**
> Trước khi chạy, hãy đảm bảo file `clickstream_uniform.jsonl` đã được copy từ `00_shared_data/lab/clickstream.jsonl` vào thư mục mount của Spark:
> ```bash
> mkdir -p ../Spark/data/session15_optimization
> cp ../00_shared_data/lab/clickstream.jsonl ../Spark/data/session15_optimization/clickstream_uniform.jsonl
> ```

### Thứ tự thực hiện:

#### Bước 1: Khởi chạy toàn bộ 3 kịch bản tối ưu hóa
```bash
bash scripts/run-demo.sh
```
*Lệnh này làm gì:*
1. Tự động kiểm tra và kích hoạt cụm Spark (`cd ../Spark && docker compose up -d`).
2. Sinh dữ liệu lệch `clickstream_skewed.jsonl` (nếu chưa có).
3. Sao chép dữ liệu và mã nguồn demo vào các thư mục mount của Spark.
4. Nộp lần lượt 3 job `spark-submit` lên cụm Standalone:
   - **Kịch bản V01 (Data Skew):** Chạy so sánh không salt vs có salt (salting key).
   - **Kịch bản V02 (Small Files):** Chạy so sánh ghi 100 partition file nhỏ vs ghi 10 partition tối ưu.
   - **Kịch bản V03 (Shuffle lớn):** Chạy so sánh Shuffle Hash Join thông thường vs Broadcast Hash Join.
5. Kích hoạt cờ ghi Spark Event Log để phục vụ phân tích số liệu task/stage.

#### Bước 2: Phân tích Event Log đã ghi nhận
Sau khi chạy xong, xem danh sách các event log trong `Spark/data/session15_optimization/spark-events/` và dùng script Python để trích xuất số liệu:
```bash
# Ví dụ phân tích log của kịch bản Data Skew:
python parse_event_log.py ../Spark/data/session15_optimization/spark-events/app-xxx-xxx
```

#### Bước 3: Dừng cụm khi hoàn tất
```bash
cd ../Spark && bash scripts/stop-cluster.sh
```

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| **Data skew (lệch dữ liệu)** | Tình trạng dữ liệu bị dồn không đều — ví dụ 1 sản phẩm chiếm 80-90% lượt xem — khiến máy xử lý phần đó bị quá tải trong khi các máy khác rảnh, làm chậm cả job. |
| **Small files problem** | Có quá nhiều file nhỏ thay vì ít file lớn — làm hệ thống tốn công quản lý/mở từng file, chậm hơn dù tổng dung lượng dữ liệu không đổi. |
| **Hot partition** | Giống data skew nhưng ở cấp độ Kafka — 1 partition nhận quá nhiều sự kiện so với các partition khác (thường do chọn "key" phân vùng không tốt). |
| **Broadcast join** | Một cách tối ưu khi join 2 bảng mà 1 bảng rất nhỏ — thay vì "xáo trộn" (shuffle) cả 2 bảng lớn qua lại giữa các máy, hệ thống gửi hẳn 1 bản sao bảng nhỏ tới TẤT CẢ các máy, tránh phải shuffle bảng lớn. |
| **Shuffle** | Bước Spark di chuyển dữ liệu qua mạng giữa các executor để dữ liệu cùng khóa (key) gặp nhau, ví dụ khi `join` hoặc `groupBy` — tốn băng thông mạng và đĩa, thường là bước tốn kém nhất trong một job. |
| **Partition (trong Spark)** | Một "phần" của dữ liệu được xử lý độc lập bởi 1 task — dữ liệu càng được chia thành nhiều partition thì càng chạy song song được nhiều, nhưng quá nhiều partition nhỏ (ví dụ khi ghi ra file) lại gây ra small files problem. |
| **AQE (Adaptive Query Execution)** | Cơ chế Spark tự động điều chỉnh lại kế hoạch thực thi TRONG LÚC job đang chạy, dựa trên số liệu thực tế đã quan sát được (ví dụ tự gộp các partition nhỏ lại bằng `CoalescePartitions`) — hữu ích trong thực tế nhưng đôi khi che mất tín hiệu cần quan sát khi làm demo tối ưu, nên phải tắt (`spark.sql.adaptive.enabled=false`) trong các demo data skew và shuffle của tài liệu này. |
| **repartition() / coalesce()** | `repartition(n)` chia lại dữ liệu thành đúng n partition, có xáo trộn (shuffle) dữ liệu qua mạng để cân bằng đều. `coalesce(n)` chỉ gộp bớt partition hiện có xuống còn tối đa n, không xáo trộn qua mạng — nên nếu dữ liệu gốc đã nằm trong ít partition hơn n, `coalesce` không thể tăng lên được. |
| **mem_limit** | Giới hạn RAM tối đa cấp cho 1 container, khai báo trong `docker-compose.yml`. Nếu container cần nhiều RAM hơn mức này sẽ bị OOM (Out Of Memory — hệ điều hành tự tắt tiến trình để bảo vệ máy). |
