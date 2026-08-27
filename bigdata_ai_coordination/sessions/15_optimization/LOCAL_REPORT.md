# LOCAL VALIDATION REPORT – Buổi 15 (Tổng kết kiến trúc và tối ưu hệ thống dữ liệu lớn)

## 1. Trạng thái

**Validation:** PASS_WITH_NOTES

Lý do PASS_WITH_NOTES thay vì PASS thuần: 3/3 tình huống bắt buộc (data skew, small files,
shuffle) chạy thật và có bằng chứng số liệu thật (PASS từng mục), nhưng (a) bằng chứng lấy
qua Spark event log thay vì scrape trực tiếp REST API `:4040` như BRIEF gợi ý (lý do kỹ thuật
ở mục 5), và (b) hot partition không chạy job mới mà trích dẫn lại Buổi 13 theo đúng chỉ dẫn
BRIEF cho phép. Cả hai đều đã ghi rõ, không phải lỗi.

## 2. Environment

| Thành phần | Version |
|---|---|
| OS (host) | Windows 10 Pro (MINGW64/Git Bash, MSYS) |
| OS (container) | Ubuntu 22.04.5 LTS (bên trong image `apache/spark:3.5.9-python3`) |
| Docker | 29.7.2 |
| Python (host) | 3.11.4 |
| Java (container) | OpenJDK Temurin 11.0.31 |
| Component chính | Apache Spark 3.5.9 (Standalone cluster có sẵn từ Buổi 9: 1 master 512m + 2 worker 640m/1 core mỗi worker, image `apache/spark:3.5.9-python3`, KHÔNG dựng lại, KHÔNG sửa `Spark/docker-compose.yml`) |

Không dựng hạ tầng mới. Dùng lại nguyên trạng cụm Spark Standalone (`Spark/`) đã kiểm thử ở
Buổi 9/10/11/12/13. Xác nhận trước khi chạy (`docker ps`/`docker stats --no-stream`): 8
container đang chạy ổn định (kafka, spark-master, spark-worker-1/2, hdfs-namenode,
hdfs-datanode1/2, mongodb), tổng RAM dùng lúc idle ~1.75 GiB / 3.826 GiB khả dụng — đủ dư địa
chạy job Spark buổi này (mỗi job dùng tối đa 2 executor x 512m, không chạy job nào khác đồng
thời).

## 3. Cách khởi động

```bash
export MSYS_NO_PATHCONV=1
cd Spark
docker compose up -d          # cụm đã chạy sẵn từ buổi trước, không cần dựng lại
docker exec spark-master mkdir -p /opt/spark-data/session15_optimization/event_logs
```

Dữ liệu nguồn và job đã copy sẵn vào `Spark/data/session15_optimization/` và
`Spark/jobs/{data_skew_demo,small_files_demo,shuffle_demo}.py` (bản sao giống hệt
`Optimization/*.py` ở root repo — xem `Optimization/README.md` mục 4 để chạy lại đầy đủ).

## 4. Validation Results

### V01 – Data skew (groupBy/repartition theo `product_id`)

**Result:** PASS

Command:

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

App ID thật: `app-20260820073906-0017`. Job đọc 2 file JSONL (100.000 dòng mỗi file, cùng
schema, đã kiểm chứng qua `df.count()`), rồi `df.repartition(8, "product_id")` (hash-partition
thật theo key, KHÔNG có map-side combiner — quan trọng, xem ghi chú kỹ thuật trong
`data_skew_demo.py`), sau đó đếm số dòng thật rơi vào từng trong 8 partition bằng
`spark_partition_id()`.

Expected: ở bản UNIFORM (phân phối `product_id` gần đều, `00_shared_data/lab/clickstream.jsonl`
gốc), 8 partition có số dòng gần bằng nhau. Ở bản SKEWED (~90% dòng bị gán `product_id`
="PROD00001"), 1 trong 8 partition phải chứa phần lớn dữ liệu.

Actual (log thật, `Optimization/evidence/data_skew_demo_run.log`):

```text
[UNIFORM] total_rows=100000
[UNIFORM] top5 product_id theo so ban ghi:
   PROD03015: 36
   PROD03233: 36
   PROD04636: 36
   PROD01948: 35
   PROD00924: 34
[UNIFORM] so ban ghi tren tung partition sau repartition(8, product_id):
   [12953, 11934, 12728, 12758, 12513, 12010, 13163, 11941]
[UNIFORM] max/min partition ratio: 1.10x

[SKEWED] total_rows=100000
[SKEWED] top5 product_id theo so ban ghi:
   PROD00001: 89928
   PROD00377: 9
   PROD01392: 9
   PROD02259: 8
   PROD03807: 7
[SKEWED] so ban ghi tren tung partition sau repartition(8, product_id):
   [91210, 1184, 1276, 1297, 1275, 1223, 1290, 1245]
[SKEWED] max/min partition ratio: 77.04x
```

Bằng chứng bổ sung từ Spark event log thật (`Optimization/evidence/skew_stage_metrics.txt`,
parse bằng `Optimization/parse_event_log.py`) — stage shuffle-read sau `repartition()`:

```text
UNIFORM (Stage 5, 8 task): shuffle_read_bytes min=61626 median=65690 max=68332 (ratio ~1.11x)
SKEWED  (Stage 16, 8 task): shuffle_read_bytes min=6514  median=6948  max=34902 (ratio ~5.36x)
```

Notes (giới hạn quan trọng, KHÔNG được bỏ qua khi trích dẫn số liệu này):

1. Chỉ số **số dòng/partition** (77.04x) là bằng chứng chính, mạnh và trực tiếp nhất — không
   bị ảnh hưởng bởi nén.
2. Chỉ số **byte shuffle/task** (5.36x) THẤP HƠN NHIỀU so với 77.04x vì Spark nén dữ liệu
   shuffle (LZ4 mặc định) — chuỗi `"PROD00001"` lặp lại 90% nên nén rất tốt, làm giảm biểu
   hiện của skew ở mức byte. Đây là hiện tượng thật, không phải lỗi đo.
3. **Thời gian task (mili-giây) KHÔNG thể hiện skew rõ ở quy mô lab này** (task lớn nhất ~
   200–670ms, không có task nào "chậm hẳn" tương xứng với 77x số dòng) — vì 91.210 dòng vẫn
   xử lý trong bộ nhớ dưới 1 giây ở quy mô vài chục MB. Ở quy mô GB/TB thật, cùng cơ chế này
   (1 partition nhận phần lớn dữ liệu) sẽ khiến 1 task/executor chạy lâu hơn hẳn phần còn lại
   — đây là suy luận về CƠ CHẾ dựa trên bằng chứng số dòng/partition thật, KHÔNG phải số đo
   thời gian thật ở quy mô lớn (không có số liệu production để trích dẫn).
4. Bắt buộc `--conf spark.sql.adaptive.enabled=false`: Spark 3.5 mặc định bật AQE
   (CoalescePartitions) sẽ gộp các partition nhỏ lại ở quy mô demo này, khiến stage sau
   shuffle chỉ còn 1 task — làm mất khả năng quan sát skew qua nhiều task. Đây là lựa chọn
   có chủ đích cho demo, KHÔNG phải khuyến nghị production (production nên bật AQE).

### V02 – Small files problem (`repartition(50)` vs `coalesce(2)`)

**Result:** PASS

Command:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark-data/session15_optimization/event_logs \
  --executor-memory 512m --driver-memory 512m --total-executor-cores 2 \
  /opt/spark-apps/small_files_demo.py
```

App ID thật: `app-20260820073413-0013`. Job đọc `orders_lab.csv` (50.000 dòng), ghi 2 biến
thể (`repartition(50)` và `coalesce(2)`), rồi đọc lại cả hai và ép `count()` toàn bộ.

Expected: `repartition(50)` tạo nhiều file nhỏ hơn `coalesce(2)`; đọc lại tạo nhiều task hơn.

Actual (log thật + kiểm tra trực tiếp số file trên volume host):

```text
INPUT_ROWS: 50000
MANY_FILES (repartition 50) -> so file that: 50   (ls output/orders_many_files | grep part-)
FEW_FILES  (coalesce 2)     -> so file that: 1
MANY_FILES read count: 50000   (khop input)
FEW_FILES  read count: 50000   (khop input)
```

Bằng chứng event log (`Optimization/evidence/small_files_stage_metrics.txt`):

```text
Stage 3 (shuffle-write cho repartition(50)): 1 task, duration=1919ms, shuffle_write=2.464.236 bytes
Stage 5 (ghi 50 file CSV, phia sau shuffle): 50 task, duration min=118 max=601 median=153 ms
Stage 6 (ghi coalesce(2), khong shuffle): 1 task, duration=770ms

Stage 7 (doc lai 50 file "many"): 2 task, duration min=915 max=1074 ms, input=3.733.387 bytes
Stage 10 (doc lai 1 file "few"):  1 task, duration=163ms, input=3.729.400 bytes
```

Notes (giới hạn/phát hiện quan trọng — KHÔNG suy diễn quá mức):

1. `coalesce(2)` chỉ tạo ra **1 file, không phải 2**: file CSV nguồn (~3.75MB) được Spark đọc
   thành đúng 1 partition ban đầu (kích thước nhỏ hơn ngưỡng chia partition mặc định), và
   `coalesce()` chỉ có thể GIẢM số partition (không shuffle), không thể TĂNG từ 1 lên 2. Đây
   là hành vi Spark thật, đã ghi nhận trung thực thay vì ép cho ra đúng 2 file.
2. Đọc lại 50 file nhỏ chỉ tạo **2 task, không phải 50 task**: Spark có cơ chế "bin-packing"
   khi lập kế hoạch đọc file (dựa trên `spark.sql.files.maxPartitionBytes` mặc định 128MB và
   `openCostInBytes` mặc định 4MB/file — 50 file nhỏ được coi có "chi phí ảo" ≈ 50×4MB=200MB,
   vượt 128MB nên tách thành 2 partition đọc). Đây CHÍNH LÀ cơ chế Spark dùng để GIẢM tác hại
   của "small files problem" ở mức task — nhưng vấn đề small-files vẫn tồn tại thật ở tầng
   lưu trữ (50 file thật trên đĩa/HDFS = 50 lần mở file, 50 entry metadata, tăng áp lực cho
   NameNode nếu chạy trên HDFS thật ở quy mô lớn hơn) dù không lộ rõ ở số task đọc lại tại quy
   mô lab này. Đã ghi rõ để không phóng đại độ chênh lệch.
3. Vì lý do trên, khác biệt task-đọc-lại (2 vs 1, ~2x) NHỎ HƠN NHIỀU khác biệt số file (50 vs
   1, 50x) — đây là điểm học thuật quan trọng cần Content AI truyền tải: small-files là vấn
   đề TẦNG LƯU TRỮ trước khi là vấn đề TẦNG TASK, và framework hiện đại có cơ chế giảm nhẹ một
   phần nhưng không loại bỏ hoàn toàn chi phí gốc.

### V03 – Shuffle lớn (join `orders`/`order_items`: full shuffle vs filter + broadcast)

**Result:** PASS

Command:

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

App ID thật: `app-20260820073729-0016`. Job đọc `orders_lab.csv` (50.000 dòng) và
`order_items_lab.csv` (124.862 dòng hợp lệ), join theo `order_id` 2 cách:

- **UNOPT**: join toàn bộ 2 bảng, `spark.sql.autoBroadcastJoinThreshold=-1` (tắt auto-broadcast
  có chủ đích để ép sort-merge join thật có shuffle cả 2 phía — ở quy mô lab, 2 bảng đủ nhỏ
  để Spark mặc định tự broadcast nếu không tắt, nên phải tắt để mô phỏng đúng tình huống bảng
  lớn hơn trong thực tế).
- **OPT**: lọc `orders` theo `status="PAID"` TRƯỚC khi join (còn 17.519/50.000 dòng), bật lại
  auto-broadcast mặc định (10MB) — Spark tự động broadcast `orders_paid` (đã đủ nhỏ) thay vì
  shuffle.

Actual (log thật + event log):

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

Notes:

1. Số dòng kết quả UNOPT (124.862) khác OPT (43.835) là ĐÚNG như dự kiến — OPT lọc trước nên
   join ra ít dòng hơn (chỉ đơn hàng PAID). Đây KHÔNG phải bug, vì mục đích minh hoạ là "lọc
   trước khi join giảm khối lượng shuffle", không phải "2 cách cho cùng 1 kết quả".
2. Bằng chứng byte shuffle (690.435 vs ~0) là số liệu thật, đo trực tiếp qua
   `Shuffle Write Metrics`/`Shuffle Read Metrics` trong event log — không suy đoán.
3. Việc UNOPT phải tắt `autoBroadcastJoinThreshold` là có chủ đích để MÔ PHỎNG kịch bản bảng
   lớn (đã ghi rõ trong code và ở đây) — ở quy mô lab thật, Spark mặc định đã đủ thông minh để
   tự broadcast, nên nếu không tắt, "vấn đề shuffle lớn" sẽ không xảy ra tự nhiên ở quy mô nhỏ
   này. Sinh viên cần hiểu: ở quy mô dữ liệu lớn thật, Spark sẽ TỰ shuffle nếu không lọc/không
   có bảng đủ nhỏ để broadcast — đây chính là lý do tối ưu "lọc trước + broadcast" quan trọng
   trong thực tế.

### V04 – Hot partition (Kafka) — TRÍCH DẪN LẠI Buổi 13, KHÔNG chạy job mới

**Result:** N/A (không PASS/FAIL riêng — tái sử dụng bằng chứng đã có, không phải tình huống
mới của buổi này, theo đúng gợi ý BRIEF)

Nguồn: `bigdata_ai_coordination/sessions/13_kafka/LOCAL_REPORT.md`, mục V02 (Producer gửi sự
kiện + key/partition, 2 chiến lược key), nguyên văn số liệu đã đo ở buổi đó:

```text
Phan phoi theo partition (key_strategy=session_id):
  partition 0: 55  partition 1: 55  partition 2: 41  partition 3: 49  (tong 200)

Phan phoi theo partition (key_strategy=product_id):
  partition 0: 28  partition 1: 47  partition 2: 60  partition 3: 65  (tong 200)
```

Ghi chú trung thực (đã có sẵn trong LOCAL_REPORT Buổi 13, nhắc lại ở đây để tránh hiểu nhầm):
ở mức dữ liệu sample (200 message) của Buổi 13, **không có key nào chiếm áp đảo hẳn 1
partition** — cả 2 chiến lược key đều "cân bằng tương đối". Đây là minh hoạ đúng CƠ CHẾ (key
khác nhau → phân phối partition khác nhau, do Kafka partition theo `hash(key) % numPartitions`
mặc định), nhưng KHÔNG phải một ví dụ "hot partition" cực đoan (không có partition nào bị quá
tải rõ rệt). Content AI khi viết slide cần nêu rõ ranh giới này, tránh mô tả quá mức thành
"partition 3 bị hot" khi thực tế chỉ lệch nhẹ (65 vs 28, ~2.3x, không phải kiểu 90/10 như V01
data skew ở buổi này).

## 5. Issues Found

### ISSUE-01

**Severity:** Minor

**Hiện tượng:** Cách lấy bằng chứng ban đầu (theo đúng gợi ý BRIEF — chạy job với
`time.sleep()` cuối job, host `curl` REST API `:4040` trong lúc job "ngủ") bị race-condition:
2 lần liên tiếp, việc tự động dò `applicationId` mới nhất qua
`GET /api/v1/applications` trả về **applicationId của lần chạy TRƯỚC ĐÓ đã dừng hẳn** (không
phải lần đang chạy), do độ trễ giữa lúc driver cũ giải phóng cổng 4040 và driver mới bind lại
cổng này chưa đủ để đồng bộ với polling loop từ host; 1 lần `curl -o` ghi file thất bại âm
thầm (thư mục đích không lỗi nhưng file không được tạo, nguyên nhân chưa xác định chắc chắn —
có thể do timing tương tự).

**Nguyên nhân:** REST API `:4040` chỉ tồn tại trong thời gian driver process còn sống; dự án
này không cấu hình Spark History Server nên không có cách nào truy vấn bằng chứng SAU KHI job
dừng qua REST API — bắt buộc phải scrape ĐÚNG lúc job đang "ngủ", và cửa sổ đó dễ bị lệch nếu
launcher (docker exec chạy nền) và script host polling không đồng bộ chặt.

**Cách sửa:** Chuyển hẳn sang bật `spark.eventLog.enabled=true` khi `spark-submit`, ghi event
log JSON Lines vào `Spark/data/session15_optimization/event_logs/` (đã mount ra host qua
volume có sẵn từ Buổi 9), rồi viết `Optimization/parse_event_log.py` đọc lại file này SAU KHI
job đã dừng hẳn (không còn phụ thuộc thời điểm), trích xuất trực tiếp từ sự kiện
`SparkListenerTaskEnd`/`SparkListenerStageCompleted`. Đã bỏ hẳn phần `time.sleep()` trong 3
job (không cần thiết nữa).

**File ảnh hưởng:** `Spark/jobs/data_skew_demo.py`, `Spark/jobs/small_files_demo.py`,
`Spark/jobs/shuffle_demo.py` (và bản sao trong `Optimization/`), `Optimization/parse_event_log.py`.

### ISSUE-02

**Severity:** Minor

**Hiện tượng:** Ở lần chạy đầu tiên của `data_skew_demo.py` (dùng `groupBy("product_id").count()`
trực tiếp, chưa dùng `repartition()`), số liệu shuffle không thể hiện skew: cả bản UNIFORM lẫn
SKEWED đều cho shuffle byte gần như đều nhau giữa các task.

**Nguyên nhân:** Spark tự động thực hiện "map-side partial aggregate" (combiner) cho
`groupBy().count()` — dữ liệu thật sự đi qua shuffle chỉ là `(product_id, partial_count)` đã
rút gọn theo SỐ KEY PHÂN BIỆT mỗi partition đầu vào, không phải theo số bản ghi thô. Vì số key
phân biệt tương đối đều giữa các partition đầu vào (dù 1 key chiếm 90% SỐ BẢN GHI), shuffle
byte vẫn đều — bài học kỹ thuật thật, không phải lỗi đo.

**Cách sửa:** Đổi demo sang `df.repartition(8, "product_id")` (hash-partition lại TOÀN BỘ bản
ghi thô, không có combiner) rồi đếm số dòng thật rơi vào mỗi partition bằng
`spark_partition_id()` — cách này bộc lộ đúng độ lệch 77.04x như mô tả ở V01. Cũng phát hiện
thêm: Spark 3.5 mặc định bật AQE (`spark.sql.adaptive.enabled=true`), tính năng
`CoalescePartitions` của AQE gộp các partition nhỏ lại (do dữ liệu demo quá nhỏ so với
ngưỡng), làm stage sau shuffle chỉ còn 1 task — phải thêm
`--conf spark.sql.adaptive.enabled=false` để giữ đủ 8 partition/task quan sát được.

**File ảnh hưởng:** `Spark/jobs/data_skew_demo.py` (và bản sao `Optimization/data_skew_demo.py`).

## 6. Mismatch với tài liệu Content AI

| Vị trí | Nội dung hiện tại | Thực tế | Đề xuất |
|---|---|---|---|
| `sessions/15_optimization/CONTENT_REPORT.md` | "Chưa cập nhật." (trống hoàn toàn) | Local AI đã hoàn thành 3 tình huống + evidence + README | Content AI cần viết slide/tài liệu/bài thực hành Buổi 15 dựa trên LOCAL_REPORT này, đặc biệt giữ đúng khung "demo giáo dục, không phải benchmark production" (mục 1 README), và giữ đúng 3 điểm giới hạn quan trọng: (1) skew rõ ở số dòng/partition (77.04x) nhưng KHÔNG rõ ở thời gian task tại quy mô lab; (2) small-files là vấn đề tầng lưu trữ trước khi là vấn đề tầng task (Spark bin-pack giảm nhẹ số task đọc lại); (3) hot partition (V04) là trích dẫn lại Buổi 13, không phải ví dụ cực đoan, không được mô tả quá mức |
| BRIEF gợi ý lấy bằng chứng qua "Spark UI REST API" (`stages/.../taskSummary`) | — | Đổi sang Spark event log + `parse_event_log.py` (lý do race-condition, xem ISSUE-01) | Nếu Content AI hoặc giảng viên muốn minh hoạ REST API `:4040` trực tiếp cho sinh viên xem trực quan trong giờ học (không phải để lấy bằng chứng ghi báo cáo), vẫn dùng được bình thường — chỉ vấn đề khi cần SCRIPT TỰ ĐỘNG scrape sau khi job đã dừng |

## 7. Khả năng chạy lại

- [x] chạy từ clean state (cụm Spark Standalone Buổi 9 đã dựng sẵn, `docker compose up -d`
      nếu chưa chạy; không cần dữ liệu/state đặc biệt nào khác).
- [x] version được ghim (`apache/spark:3.5.9-python3`, không đổi so với Buổi 9).
- [x] dữ liệu có đường dẫn tương đối (`00_shared_data/lab/...` → copy vào
      `Spark/data/session15_optimization/`, đường dẫn trong container luôn là
      `/opt/spark-data/session15_optimization/...`).
- [x] worker/container truy cập được dữ liệu (đã xác nhận qua log chạy thật, 2 executor trên
      2 worker khác nhau).
- [x] expected output được lưu (`Optimization/evidence/*.log`, `*_stage_metrics.txt`, event
      log thô giữ nguyên tại `Spark/data/session15_optimization/event_logs/`).
- [x] reset script hoạt động (2026-08-22): `Optimization/scripts/run-demo.sh`
      chạy cả 3 tình huống (skew, small files, shuffle) liên tiếp, tự chuẩn
      bị dữ liệu — đã chạy thật thành công, 3 event log mới xuất hiện đúng.

## 8. Kết luận cho giảng viên

Có thể dùng để dạy: YES

Các điểm cần đọc trước khi duyệt:

1. Cả 3 tình huống bắt buộc (data skew, small files, shuffle lớn) đều chạy THẬT trên cụm Spark
   Standalone có sẵn, có số liệu thật từ Spark event log (không suy đoán), đủ để minh hoạ cơ
   chế cho sinh viên nhập môn.
2. Đây là demo GIÁO DỤC ở quy mô lab (vài chục MB, 2 worker 1 core/640MB) — không phải
   benchmark production. 3 giới hạn quan trọng cần nhắc sinh viên (chi tiết ở mục 4, Notes của
   từng V0x): (a) skew thể hiện rõ ở SỐ DÒNG/partition nhưng không rõ ở THỜI GIAN task tại quy
   mô này; (b) small-files là vấn đề tầng lưu trữ, Spark có cơ chế giảm nhẹ (không loại bỏ) ở
   tầng task; (c) hot partition chỉ là trích dẫn lại Buổi 13, không phải ví dụ cực đoan.
3. `CONTENT_REPORT.md` Buổi 15 hiện đang trống hoàn toàn — Content AI cần viết dựa trên báo
   cáo này (mục 6, Mismatch).
4. Quyết định kỹ thuật đáng chú ý (không cần giảng viên phê duyệt, nhưng nên biết): đổi cách
   lấy bằng chứng từ "scrape REST API :4040 khi job đang chạy" (theo gợi ý BRIEF) sang "đọc lại
   Spark event log sau khi job dừng" vì gặp race-condition thật khi thử theo cách đầu (xem
   ISSUE-01) — không ảnh hưởng độ tin cậy của số liệu, chỉ khác cơ chế thu thập.
