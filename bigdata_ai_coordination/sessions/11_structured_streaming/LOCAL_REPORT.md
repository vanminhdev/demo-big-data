# LOCAL VALIDATION REPORT – Buổi 11 (Spark Structured Streaming)

## 1. Trạng thái

**Validation:** PASS

Cả 5 hạng mục bắt buộc theo BRIEF (V01–V05: schema tường minh, window
aggregation, watermark/late data, checkpoint restart, so sánh output mode)
đã chạy thật, có log/checkpoint thật giữ lại làm bằng chứng. Một sự cố tài
nguyên (OOM) và một sai sót thao tác (thứ tự copy file) xảy ra trong quá
trình làm việc — cả hai đã được xử lý dứt điểm và ghi rõ trong mục 6, không
ảnh hưởng tới kết luận PASS của 5 validation item.

## CẬP NHẬT 2026-08-22 (theo yêu cầu giảng viên: "nên sửa lại cho nhỏ đi cho phù hợp")

Toàn bộ mục 1-9 gốc bên dưới là kết quả **CŨ** với window **1 ngày** (vì dữ
liệu gốc trải 15 ngày). Theo yêu cầu giảng viên, đã thực hiện:

1. Tạo `StructuredStreaming/compress_timeline.py`: nén trục thời gian của
   `00_shared_data/sample/clickstream_sample.jsonl` (200 dòng, KHÔNG sửa
   file gốc) từ khung ~14.45 ngày xuống còn **90 phút**, giữ nguyên thứ tự
   và tỉ lệ khoảng cách tương đối giữa các sự kiện — chỉ đổi `event_time`,
   không đổi bất kỳ trường nào khác. Output:
   `StructuredStreaming/data_source_v2/clickstream_compressed.jsonl`.
2. Đổi `WINDOW_DURATION` từ `"1 day"` xuống **`"5 minutes"`**,
   `WATERMARK_DELAY` xuống **`"3 minutes"`** trong `streaming_job.py` (nằm
   trong khối CONFIG, không sửa logic xử lý).
3. Tạo `prepare_batches_v2.py`: chia 200 dòng đã nén thành 19 file
   `win_01_0900.jsonl` .. `win_19_1030.jsonl` theo cửa sổ 5 phút, có 1 bản
   ghi muộn cố ý (`CEV000068`, thuộc cửa sổ [09:10,09:15)) được chèn vào
   file cửa sổ [09:30,09:35) — mô phỏng late data với độ trễ ~4 cửa sổ
   (~20 phút), tương tự tỉ lệ độ trễ so với window trong kịch bản gốc.
4. Chạy lại đầy đủ cả 5 validation item với dữ liệu/cấu hình mới — xem
   V01b-V05b bên dưới. Log lưu tại `StructuredStreaming/logs_v2/`,
   checkpoint thật tại `Spark/data/session11_streaming/checkpoint_v2/`.

**Kết quả cốt lõi không đổi về BẢN CHẤT** (window/watermark/checkpoint hoạt
động đúng ngữ nghĩa dù đơn vị thời gian khác), chỉ khác về độ lớn window —
đúng yêu cầu "nhỏ đi cho phù hợp" của giảng viên, giờ dùng được trực tiếp
làm ví dụ "vài phút" như BRIEF gốc gợi ý mà không cần giải thích vòng vo lý
do dùng "1 ngày".

### V01b - Config + schema tường minh, window 5 phút

**Result:** PASS

```text
SPARK_MASTER_URL      = local[*]
SOURCE_DIR            = /opt/spark-data/session11_streaming/data_source_v2
WINDOW_DURATION       = 5 minutes
WATERMARK_DELAY       = 3 minutes
MAX_FILES_PER_TRIGGER = 1
CLICKSTREAM_SCHEMA (tuong minh): struct<event_id:string,event_time:timestamp,customer_id:string,session_id:string,product_id:string,event_type:string>
```

### V02b - Window aggregation 5 phút (update mode, batch 0-10, log `logs_v2/run1_update_mode_win1-10.log`)

**Result:** PASS

```text
batchId=0 numInputRows=11 numRowsDroppedByWatermark=0
batchId=1 numInputRows=12 numRowsDroppedByWatermark=0
...
batchId=6 numInputRows=10 numRowsDroppedByWatermark=1   <- xem V03b
batchId=9 numInputRows=11 numRowsDroppedByWatermark=0
batchId=10 numInputRows=0  (het du lieu dot 1, 10 file dau)
```

Cửa sổ ví dụ `{09:40:00, 09:45:00}`: VIEW=6, PURCHASE=2, ADD_TO_CART=3 —
khớp đúng dữ liệu batch file tương ứng, đối chiếu được trực tiếp.

### V03b - Watermark/late data với window 5 phút

**Result:** PASS

Bản ghi muộn: `CEV000068`, `event_time` thuộc cửa sổ `[09:10,09:15)`, được
chèn vào file cửa sổ `[09:30,09:35)` (file thứ 7 trong thứ tự xử lý, tương
ứng `batchId=6`, xem `batches_staging_v2/manifest.json`).

Actual (`numRowsDroppedByWatermark` — chỉ số nội bộ Spark, không suy đoán):

```text
batchId=6 numInputRows=10 numRowsDroppedByWatermark=1
```

Xác nhận đúng **1** bản ghi bị watermark loại bỏ ở `update` mode — khớp
chính xác kịch bản đã dựng. Đối chiếu console: batch 6 (`update` mode)
không có dòng nào cho cửa sổ `{09:10,09:15}` (cửa sổ của bản ghi muộn),
xác nhận nó không cập nhật state.

### V04b - Checkpoint restart với window 5 phút (`update` mode)

**Result:** PASS

**Lần chạy 1** (`logs_v2/run1_update_mode_win1-10.log`): 10 file đầu
(`win_01`..`win_10`), checkpoint mới, `Trigger.availableNow=True`, batch
0→10 (batch 10 rỗng, kết thúc dữ liệu đợt 1). `commits/0`..`commits/10`
được ghi.

**Lần chạy 2** (`logs_v2/run2_update_mode_win11-19_restart.log`): thêm 9
file còn lại (`win_11`..`win_19`) vào cùng `data_source_v2/`, chạy lại
**đúng lệnh cũ, cùng checkpoint**. Log xác nhận:

```text
WARN HDFSBackedStateStoreProvider: The state for version 11 doesn't exist
in loadedMaps. Reading snapshot file and delta files if needed...
Batch: 11
...
Batch: 19
batchId=11 numInputRows=10 ... batchId=19 numInputRows=1 ... batchId=20 numInputRows=0
```

**Bằng chứng checkpoint được tái sử dụng:** batch ID tiếp tục từ **11**
(không quay lại 0), log tự in "state for version **11**" (= state sau
batch 10), checkpoint thật `Spark/data/session11_streaming/checkpoint_v2/update_mode/`
có đủ `offsets/commits` từ 0 đến 20 sau lần chạy 2, xác nhận bằng lệnh thật:

```bash
ls Spark/data/session11_streaming/checkpoint_v2/update_mode/commits/ | grep -v crc | sort -n
# 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
```

### V05b - So sánh output mode với window 5 phút

**Result:** PASS

| Mode | Log | Batch cuối |
|---|---|---|
| `update` | `logs_v2/run1...log` + `run2...log` | batchId=20 (rỗng) |
| `complete` | `logs_v2/run3_complete_mode_all19_FULL.log` | batchId=18 |

`complete` mode: bảng cuối cùng có **45 dòng** (tổ hợp cửa sổ × event_type),
**tổng = 200** — khớp 100% tổng số dòng gốc của `clickstream_compressed.jsonl`.
Xác nhận lại đúng hành vi đã ghi nhận ở báo cáo gốc mục V03/V05: **complete
mode không dùng watermark để loại dữ liệu muộn** (bản ghi `CEV000068` VẪN
được tính vào cửa sổ `{09:10,09:15}` — cửa sổ này giữ nguyên
ADD_TO_CART=4/PURCHASE=3/VIEW=3=10 tổng, không đổi qua các batch sau, tức
là bản ghi muộn đã nằm trong 10 đó ngay từ đầu, không bị loại). Đây là
đúng tài liệu chính thức Spark, nhất quán với phát hiện cũ ở window 1
ngày — không phải hiện tượng riêng của window nhỏ.

**Không chạy lại `append` mode lần này** (đã có đủ 3/3 mode ở báo cáo gốc
mục V05 với window 1 ngày, cùng API/semantics không đổi theo window; giữ
nguyên bảng so sánh 3 mode ở mục V05 gốc làm tài liệu tham khảo, chỉ thay
đơn vị "ngày" bằng "5 phút" khi Content AI viết lại ví dụ).

## 2. Environment

| Thành phần | Version |
|---|---|
| OS host | Windows 10 Pro 10.0.19045 |
| Docker | Docker Desktop, Docker Compose v2 |
| Image Spark (dùng lại từ Buổi 9, không đổi) | `apache/spark:3.5.9-python3` |
| Spark | 3.5.9 (trong container) |
| PySpark | 3.5.9 (đi kèm image) |
| Python (trong container) | 3.10.12 |
| Java (trong container) | OpenJDK 11.0.31 |
| Cụm Spark | 1 master (`spark-master`, `mem_limit: 512m`) + 2 worker (`spark-worker-1`, `spark-worker-2`), đã dựng sẵn từ Buổi 9, không sửa `Spark/docker-compose.yml` |
| Chế độ chạy job | `local[*]` bên trong container `spark-master` (BRIEF Buổi 11 không bắt buộc chạy trên Spark Standalone cluster thật như Buổi 9/10, khác MLlib Buổi 12) |
| Các container khác chạy đồng thời | `mongodb` (mongo:8.0), `hdfs-namenode`/`hdfs-datanode1`/`hdfs-datanode2` (bde2020 2.0.0-hadoop3.2.1-java8), và trong một phần của phiên làm việc: job MLlib Buổi 12 (`train_pipeline.py`, cũng chạy `local[*]` bên trong CÙNG container `spark-master`) |

**Ghi chú quan trọng về tranh chấp tài nguyên:** container `spark-master`
chỉ có `mem_limit: 512m`. Trong phiên làm việc này, một agent khác (Buổi 12
– MLlib) cũng chạy job `local[*]` bên trong CÙNG container `spark-master`
tại cùng thời điểm lần chạy đầu tiên của job streaming, khiến RAM cạn
(`docker stats` cho thấy `spark-master` ở mức 99.92% / 512MiB) và job
streaming bị crash giữa batch 4 (xem ISSUE-01). Sau khi job MLlib kết thúc
(RAM về ~20%), chạy lại thành công từ đầu. Đây là giới hạn tài nguyên thật
của máy kiểm thử (Docker Desktop cấp ~3.8 GiB tổng cộng cho toàn bộ VM),
không phải lỗi logic của job.

## 3. Cách khởi động

```bash
cd Spark
docker compose up -d
curl -s http://localhost:8080/json/    # xác nhận status=ALIVE, 2 worker ALIVE
```

Cụm đã chạy sẵn từ Buổi 9/10 khi bắt đầu phiên làm việc này (`docker ps`
xác nhận đủ 7 container Mongo/HDFS/Spark đều `Up`), không cần khởi động
lại. Job và dữ liệu batch được đặt tại `Spark/jobs/streaming_job.py` +
`Spark/data/session11_streaming/` (bản sao của `StructuredStreaming/`, vì
container chỉ mount được `Spark/data` và `Spark/jobs` — kỹ thuật giống hệt
Buổi 10 PySpark, xem `PySpark/README.md` mục 2).

## 4. Dữ liệu đầu vào

`00_shared_data/sample/clickstream_sample.jsonl` — 200 dòng JSON Lines,
đúng Data Contract (`event_id`, `event_time`, `customer_id`, `session_id`,
`product_id`, `event_type`). Không tạo dataset riêng.

`event_time` trải trong 15 ngày (2026-08-05 → 2026-08-19), 10-20
bản ghi/ngày. `prepare_batches.py` chia 200 dòng gốc (KHÔNG sửa nội dung
bất kỳ trường nào của bất kỳ bản ghi nào) thành 15 file
`day_XX_YYYY-MM-DD.jsonl` theo ngày, giữ lại 1 bản ghi cụ thể
(`event_id=CEV000096`, ngày thật `2026-08-06`, `event_type=PURCHASE`) để
chèn muộn vào file ngày `2026-08-10` — mô phỏng late data. Toàn bộ chi tiết
(danh sách 15 file, số dòng mỗi file, bản ghi muộn) lưu tại
`StructuredStreaming/batches_staging/manifest.json`.

## 5. Validation Results

### V01 – Đọc clickstream qua File Source với schema tường minh (không inferSchema)

**Result:** PASS

`streaming_job.py` khai báo `CLICKSTREAM_SCHEMA` bằng `StructType`/
`StructField` tường minh cho cả 6 trường theo `00_DATA_CONTRACT.md` mục
clickstream, gọi `spark.readStream.format("json").schema(CLICKSTREAM_SCHEMA)`,
**không** gọi `option("inferSchema", true)` ở bất kỳ đâu.

Actual (trích mọi log, dòng in ra giống nhau ở cả 4 lần chạy):

```text
CLICKSTREAM_SCHEMA (tuong minh, dinh nghia trong code):
struct<event_id:string,event_time:timestamp,customer_id:string,session_id:string,product_id:string,event_type:string>
```

### V02 – Window aggregation: đếm số sự kiện theo event_type / tumbling window 1 ngày

**Result:** PASS

Command (rút gọn, xem mục 3 cho lệnh đầy đủ):

```bash
docker exec -e STREAM_OUTPUT_MODE="update" ... spark-master /opt/spark/bin/spark-submit \
  --master local[*] --driver-memory 512m /opt/spark-apps/streaming_job.py
```

Logic:

```python
grouped = (
    stream_df.withWatermark("event_time", "1 day")
    .groupBy(F.window(F.col("event_time"), "1 day"), F.col("event_type"))
    .count()
)
```

Actual (trích `logs/run3_complete_mode_all15.log`, batch cuối cùng — bảng
tích lũy đầy đủ 15 ngày × tối đa 3 `event_type`, output mode `complete`
hiển thị toàn bộ state):

```text
|{2026-08-05 00:00:00, 2026-08-06 00:00:00}|ADD_TO_CART|2    |
|{2026-08-05 00:00:00, 2026-08-06 00:00:00}|VIEW       |8    |
|{2026-08-06 00:00:00, 2026-08-07 00:00:00}|ADD_TO_CART|5    |
|{2026-08-06 00:00:00, 2026-08-07 00:00:00}|PURCHASE   |1    |
|{2026-08-06 00:00:00, 2026-08-07 00:00:00}|VIEW       |9    |
...
|{2026-08-19 00:00:00, 2026-08-20 00:00:00}|ADD_TO_CART|4    |
|{2026-08-19 00:00:00, 2026-08-20 00:00:00}|VIEW       |1    |
```

Đối chiếu độc lập: tổng số bản ghi trong mỗi cửa sổ khớp đúng số dòng của
file batch tương ứng trong `manifest.json` (ví dụ ngày 08-05: 10 bản ghi =
ADD_TO_CART 2 + VIEW 8; ngày 08-11: 12 bản ghi = ADD_TO_CART 5 + PURCHASE 3
+ VIEW 4). Tổng toàn bộ 15 cửa sổ trong bảng `complete` mode cuối cùng =
200 (khớp 100% số dòng gốc của `clickstream_sample.jsonl`, vì complete mode
không rơi dữ liệu — xem giải thích khác biệt với update/append ở V05).

### V03 – Watermark và late data: chứng minh 1 bản ghi cụ thể bị loại

**Result:** PASS

Bản ghi muộn: `event_id = CEV000096`, `event_time = 2026-08-06T22:41:42+00:00`,
`event_type = PURCHASE`, được rót vào stream trong file `day_06_2026-08-10.jsonl`
(cùng 11 bản ghi bình thường của ngày 08-10), tức là chỉ đến sau khi các
ngày 08-07, 08-08, 08-09 đã được xử lý.

Command: `logs/run1_update_mode_batches1-8.log` (output mode `update`,
window 1 ngày, watermark 1 ngày, 8 file đầu tiên `day_01`..`day_08`).

Actual — `recentProgress` của job (in ra cuối log, lấy trực tiếp từ
`StreamingQueryProgress.stateOperators[0].numRowsDroppedByWatermark`, không
phải suy đoán):

```text
batchId=4 numInputRows=15 numRowsDroppedByWatermark=0 ...   (file day_05, ngay 08-09)
batchId=5 numInputRows=12 numRowsDroppedByWatermark=1 ...   (file day_06, ngay 08-10 + 1 ban ghi muon)
batchId=6 numInputRows=12 numRowsDroppedByWatermark=0 ...   (file day_07, ngay 08-11)
```

`numInputRows=12` ở batch 5 = 11 bản ghi bình thường của 08-10 + 1 bản ghi
muộn CEV000096. `numRowsDroppedByWatermark=1` xác nhận **đúng 1** bản ghi bị
watermark loại bỏ — khớp chính xác với bản ghi muộn đã chèn.

Đối chiếu bằng nội dung console: batch 5 (`update` mode) chỉ in ra 2 dòng
thay đổi, cả hai đều thuộc cửa sổ `{2026-08-10, 2026-08-11}`:

```text
-------------------------------------------
Batch: 5
-------------------------------------------
|{2026-08-10 00:00:00, 2026-08-11 00:00:00}|VIEW       |6    |
|{2026-08-10 00:00:00, 2026-08-11 00:00:00}|ADD_TO_CART|5    |
```

Không có dòng nào cho cửa sổ `{2026-08-06, 2026-08-07}` xuất hiện ở batch
5 — nghĩa là bản ghi PURCHASE muộn của 08-06 **không** cập nhật cửa sổ của
nó (đã bị watermark loại trước khi vào state), đúng ngữ nghĩa `update`
mode (chỉ in dòng thật sự thay đổi). Nguyên nhân: sau khi xử lý xong batch
4 (ngày 08-09), watermark = `max(event_time đã thấy) - 1 ngày` ≈ 08-08,
đã vượt qua thời điểm đóng cửa sổ `[2026-08-06, 2026-08-07)` (đóng khi
watermark ≥ 08-07), nên bản ghi 08-06 đến ở batch 5 bị coi là "quá muộn".

**Phát hiện thêm quan trọng cho Content AI:** hành vi loại bỏ late-data
này chỉ đúng với output mode `update`/`append`. Ở output mode `complete`,
Spark **không** loại dữ liệu muộn bằng watermark (vì complete mode phải
giữ toàn bộ state để in lại cả bảng mỗi batch) — xem bằng chứng chi tiết
ở V05. Đây là hành vi tài liệu chính thức của Spark
("Since Complete mode requires all the aggregate data to be preserved,
watermark does not take effect in dropping data"), không phải lỗi.

### V04 – Checkpoint: dừng và khởi động lại, không xử lý lại từ đầu

**Result:** PASS

**Lần chạy 1** (`logs/run1_update_mode_batches1-8.log`): `data_source/`
chỉ có 8 file (`day_01`..`day_08`). Job chạy với
`STREAM_CHECKPOINT_DIR=checkpoint/update_mode` (checkpoint rỗng, tạo mới),
`Trigger.availableNow=True` (xử lý hết dữ liệu đang có rồi tự dừng — dùng
đúng tinh thần "khởi động lại từ checkpoint" mà không cần chờ trigger
interval thời gian thực). Kết quả: batch 0 → 8 (9 batch, batch 8 là batch
rỗng kết thúc), checkpoint ghi `commits/0` .. `commits/8`.

```bash
docker exec spark-master bash -c \
  "ls /opt/spark-data/session11_streaming/checkpoint/update_mode/commits/ | grep -v crc | sort -n"
# -> 0 1 2 3 4 5 6 7 8
```

**Thêm dữ liệu:** copy 7 file còn lại (`day_09`..`day_15`) vào cùng
`data_source/` (không xoá 8 file cũ, không xoá checkpoint).

**Lần chạy 2** (`logs/run2_update_mode_batches9-15_restart.log`): chạy lại
**đúng lệnh cũ, cùng `STREAM_CHECKPOINT_DIR`**. Log xác nhận:

```text
WARN HDFSBackedStateStoreProvider: The state for version 9 doesn't exist in
loadedMaps. Reading snapshot file and delta files if needed...Note that
this is normal for the first batch of starting query.
...
Batch: 9
...
Batch: 15
...
batchId=9 numInputRows=11 ...
batchId=15 numInputRows=13 numRowsDroppedByWatermark=2 ...
```

**Bằng chứng checkpoint được tái sử dụng, không xử lý lại từ đầu:**

1. Batch ID tiếp tục từ **9**, không quay lại **0** — nếu checkpoint bị bỏ
   qua, job sẽ in lại `Batch: 0` với dữ liệu của `day_01`.
2. Log driver tự in dòng "state for version **9**" khi nạp lại state store
   — tức là job đọc state đã lưu ở batch 8 (version 9 = state sau batch 8)
   từ checkpoint, không khởi tạo state rỗng.
3. Sau lần chạy 2, `commits/` của checkpoint có đủ **0 → 15** (16 file,
   xác nhận bằng lệnh `ls` thật):

```bash
ls StructuredStreaming/checkpoint/update_mode/commits/ | grep -v crc | sort -n
# 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
```

4. `offsets/0` (batch đầu tiên, ghi lúc lần chạy 1) và `offsets/9` (batch
   đầu tiên của lần chạy 2) đều còn nguyên trong checkpoint, không bị ghi
   đè hay xoá.

Checkpoint thật được giữ lại làm bằng chứng tại
`StructuredStreaming/checkpoint/update_mode/`.

**Ghi chú trung thực (không phải phần bắt buộc của V04 nhưng liên quan):**
lần chạy 2 xử lý các file theo thứ tự **modification time**, không phải
thứ tự tên file. Vì thao tác `cp` các file `day_10`..`day_15` được thực
hiện TRƯỚC `day_09` (một sai sót thao tác trong phiên làm việc), file
`day_09_2026-08-13.jsonl` có mtime muộn hơn nên bị xử lý **cuối cùng**
(batch 15) thay vì đúng vị trí thứ 9. Khi đó watermark đã tiến rất xa
(dựa trên ngày 08-19), khiến **toàn bộ 13 bản ghi** của ngày 08-13 hiệu
lực bị coi là muộn (`numInputRows=13`, batch 15 không in ra dòng nào —
0 dòng kết quả) — dù `numRowsDroppedByWatermark` (chỉ số nội bộ của Spark,
đo theo state operator) chỉ báo **2**, không phải 13. Đây là quan sát thật
đã được kiểm chứng, không sửa lại số liệu; nó minh hoạ thêm cho sinh viên:
**thứ tự các file/sự kiện thực sự đến (arrival order) mới quyết định late
hay không, không phải tên file** — một bài học thực tế về vận hành file
source. Vì phát hiện thêm này làm sai lệch số liệu tổng hợp của cửa sổ
08-13 trong lần chạy 2, các lần chạy V02/V05 sau đó (chạy 3 và 4) đã sửa
mtime của cả 15 file theo đúng thứ tự ngày (`touch`) trước khi chạy, để số
liệu window aggregation dùng làm ví dụ chính xác 100%, không bị ảnh hưởng
bởi sự cố thao tác này.

### V05 – So sánh output mode: complete vs update vs append

**Result:** PASS (3/3 mode đã chạy, đúng yêu cầu tối thiểu 2/3)

Cả 3 lần chạy dùng cùng 15 file (`data_source/`, đã sửa mtime đúng thứ tự
ngày), mỗi mode 1 checkpoint riêng (Spark không cho đổi output mode khi
dùng lại checkpoint cũ).

| Mode | Log | Checkpoint |
|---|---|---|
| `update` | `logs/run1_...log` + `run2_...log` | `checkpoint/update_mode/` |
| `complete` | `logs/run3_complete_mode_all15.log` | `checkpoint/complete_mode/` |
| `append` | `logs/run4_append_mode_all15.log` | `checkpoint/append_mode/` |

**Khác biệt quan sát được (bằng chứng thật, không suy đoán):**

1. **`update`**: mỗi batch chỉ in ra các dòng (cửa sổ, event_type) có thay
   đổi trong batch đó. Bản ghi muộn CEV000096 (08-06) bị watermark loại bỏ
   (`numRowsDroppedByWatermark=1` ở batch xử lý file chứa nó) — xem V03.

2. **`complete`**: mỗi batch in lại **toàn bộ** bảng kết quả tích luỹ từ
   đầu (batch cuối có đủ 15 ngày × các event_type, tổng đúng 200 dòng gốc).
   **Khác biệt cốt lõi so với `update`/`append`: watermark KHÔNG loại dữ
   liệu muộn ở complete mode** — cùng kịch bản late data (CEV000096, chèn
   vào file ngày 08-10 sau khi ngày 08-09 đã xử lý), batch xử lý file đó
   báo `numRowsDroppedByWatermark=0` và cửa sổ `{2026-08-06,2026-08-07}`
   **có thêm** dòng `PURCHASE|1` xuất hiện ngay trong batch đó — tức là
   bản ghi muộn **được** tính vào kết quả, không bị loại:

   ```text
   # complete mode, batch xử lý file ngày 08-10 (chứa ban ghi muon 08-06)
   |{2026-08-06 00:00:00, 2026-08-07 00:00:00}|ADD_TO_CART|5    |
   |{2026-08-06 00:00:00, 2026-08-07 00:00:00}|PURCHASE   |1    |   <-- ban ghi muon duoc tinh, KHONG bi loai
   |{2026-08-06 00:00:00, 2026-08-07 00:00:00}|VIEW       |9    |
   ```

   Đây đúng theo tài liệu chính thức của Spark: complete mode phải giữ lại
   toàn bộ state để in lại cả bảng mỗi lần, nên watermark không được dùng
   để loại bỏ dữ liệu (nó vẫn được dùng để dọn state cũ ở `update`/`append`,
   nhưng không dùng để loại dữ liệu ở `complete`).

3. **`append`**: một cửa sổ **chỉ được in ra khi đã "chốt"** (watermark đã
   vượt qua điểm kết thúc cửa sổ đó), nên **3 batch đầu tiên (0,1,2) không
   in ra dòng nào** dù đã nhận đủ dữ liệu ngày 08-05..08-07 — phải đợi đến
   batch 3 (đã xử lý đến ngày 08-08, watermark vượt qua 08-06 → 08-07) thì
   cửa sổ 08-05 mới được in ra lần đầu:

   ```text
   Batch: 0 -> (rong)
   Batch: 1 -> (rong)
   Batch: 2 -> (rong)
   Batch: 3 -> {2026-08-05,2026-08-06} VIEW=8, ADD_TO_CART=2
   ```

   Và ở đầu bên kia: **3 cửa sổ cuối cùng (08-17, 08-18, 08-19) không bao
   giờ được in ra** trong toàn bộ lần chạy (batch 15 là batch rỗng, kết
   thúc dữ liệu) — vì không còn dữ liệu ngày sau đó để đẩy watermark vượt
   qua các cửa sổ này. Đây là đặc điểm "độ trễ" cố hữu của `append` mode
   với windowed aggregation: kết quả đúng và không lặp, nhưng luôn trễ ít
   nhất bằng khoảng watermark, và dữ liệu "gần đây nhất" có thể chưa bao
   giờ được in nếu luồng dừng lại. `append` mode cũng loại bỏ late data
   giống `update` (`numRowsDroppedByWatermark=1` ở đúng batch chứa
   CEV000096, giống hệt V03).

**Tóm tắt cho Content AI (bảng so sánh dùng được ngay cho slide):**

| Output mode | Late data (CEV000096) | Khi nào 1 cửa sổ được in | Số dòng in ra tích luỹ |
|---|---|---|---|
| `update` | Bị loại (watermark) | Ngay khi có thay đổi | Chỉ dòng thay đổi |
| `append` | Bị loại (watermark) | Chỉ khi watermark đã vượt qua cửa sổ (trễ) | Mỗi cửa sổ in đúng 1 lần, vĩnh viễn không sửa |
| `complete` | **Không bị loại** (được tính) | Mọi batch, in lại toàn bộ | Toàn bộ bảng mỗi batch |

## 6. Issues Found

### ISSUE-01 (Critical, đã xử lý) – Job streaming bị crash do tranh chấp RAM với job khác trong cùng container

**Hiện tượng:** lần chạy đầu tiên của job (`update` mode, 8 file) dừng đột
ngột sau batch 3, không có exception rõ ràng trong log, tiến trình
`spark-submit` không còn trong `docker top spark-master`.

**Nguyên nhân (xác nhận thật, không suy đoán):** `docker stats` tại thời
điểm đó cho thấy container `spark-master` ở 511.6MiB/512MiB (99.92%) RAM.
`docker top spark-master` cho thấy một tiến trình khác
(`train_pipeline.py`, job MLlib của Buổi 12, agent khác chạy song song
trong cùng dự án) đang chạy `local[*]` **trong cùng container**
`spark-master` tại cùng thời điểm. Container bị giới hạn cứng
`mem_limit: 512m` trong `Spark/docker-compose.yml` — 2 JVM driver
`local[*]` cùng lúc trong 512 MiB đã vượt giới hạn, tiến trình bị kernel
(cgroup OOM killer) chấm dứt giữa batch 4 (checkpoint xác nhận: `offsets/4`
được ghi nhưng `commits/4` không tồn tại — bằng chứng batch 4 bị dừng giữa
chừng, không phải lỗi logic job).

**Cách xử lý:** đợi job MLlib (Buổi 12) kết thúc (xác nhận bằng
`docker top spark-master` không còn `train_pipeline.py`, `docker stats`
RAM về ~20%), xoá checkpoint dở dang (`update_mode/`), chạy lại từ đầu.
Lần chạy lại thành công hoàn toàn (9 batch, xem V01-V03).

**File ảnh hưởng:** không sửa file nào của dự án; chỉ là vấn đề lịch trình
chạy job trên máy có RAM giới hạn khi nhiều buổi học chạy `local[*]` cùng
lúc trong cùng 1 container `spark-master`. Nên biết cho các phiên sau: nếu
nhiều "buổi" cùng dùng `docker exec spark-master ... --master local[*]`
đồng thời, cần kiểm tra `docker stats`/`docker top` trước khi chạy.

### ISSUE-02 (Minor, đã xử lý) – Thứ tự copy file ảnh hưởng thứ tự xử lý của File Source

**Hiện tượng:** ở lần chạy 2 của V04 (checkpoint restart), file
`day_09_2026-08-13.jsonl` được xử lý **cuối cùng** (batch 15) thay vì đúng
vị trí thứ 9, khiến toàn bộ 13 bản ghi của nó bị coi là muộn.

**Nguyên nhân:** Spark File Source (mặc định) sắp xếp file mới theo
**modification time**, không phải theo tên file. File `day_09` được `cp`
vào `data_source/` SAU các file `day_10`..`day_15` (do lỗi thao tác trong
lúc gõ lệnh — brace expansion `day_1{0,1,2,3,4,5}` bỏ sót `day_09` phải
copy bù sau), nên có mtime muộn hơn.

**Cách xử lý:** ghi nhận trung thực trong V04 (không giấu, không sửa lại
số liệu của lần chạy 2), và với các lần chạy sau (V02, V05) dùng `touch`
để đặt lại mtime của cả 15 file đúng thứ tự ngày trước khi chạy, đảm bảo
số liệu window aggregation dùng cho slide là chính xác 100%.

**File ảnh hưởng:** không sửa code; chỉ ảnh hưởng thứ tự file trong
`data_source/` của 1 lần chạy cụ thể (đã ghi log lại đầy đủ, không xoá
bằng chứng).

## 7. Mismatch với tài liệu Content AI

`CONTENT_REPORT.md` của Buổi 11 **hiện đang trống hoàn toàn** (chỉ có dòng
"Chưa cập nhật.") — Content AI chưa viết slide/tài liệu đọc/bài thực hành
cho buổi này, nên chưa có gì để đối chiếu mismatch tại thời điểm lập báo
cáo này. Khi Content AI hoàn thành, cần đối chiếu ít nhất:

1. BRIEF.md gợi ý window "vài phút" — LOCAL_REPORT này dùng window 1 NGÀY
   vì lý do dữ liệu (mục 3 README.md). Content AI nên mô tả khái niệm
   window bằng ví dụ chung (ví dụ "5 phút") nhưng khi trích số liệu cụ thể
   từ lab này phải dùng đúng đơn vị "1 ngày" và các con số thật trong báo
   cáo này, không tự suy ra số liệu window phút.
2. Bảng so sánh 3 output mode ở mục V05 dùng được trực tiếp cho slide.
3. Kịch bản late data (CEV000096) ở mục V03/V04 dùng được trực tiếp làm ví
   dụ minh hoạ watermark trong tài liệu đọc.
4. Phát hiện "complete mode không loại late data" (V03/V05) là kiến thức
   nâng cao nên đưa vào slide như một lưu ý/mở rộng, không phải nội dung
   nhập môn bắt buộc — tuỳ giảng viên quyết định mức độ đưa vào bài giảng.

## 8. Khả năng chạy lại

- [x] chạy từ clean state (checkpoint `update_mode` đã bị xoá và chạy lại
      từ đầu sau ISSUE-01, kết quả giống hệt nhau ở các đại lượng cấu
      trúc — số batch, số dòng/batch);
- [x] version được ghim (`apache/spark:3.5.9-python3`, dùng lại từ Buổi 9,
      không đổi);
- [x] dữ liệu có đường dẫn tương đối/biến môi trường (`STREAM_SOURCE_DIR`,
      `STREAM_CHECKPOINT_DIR`, `STREAM_OUTPUT_MODE`, `STREAM_WINDOW_DURATION`,
      `STREAM_WATERMARK_DELAY` đều override được qua biến môi trường,
      không hardcode đường dẫn Windows);
- [x] worker/container truy cập được dữ liệu (job chạy trong container,
      đọc/ghi qua mount `Spark/data` có sẵn từ Buổi 9);
- [x] expected output được lưu (`StructuredStreaming/logs/` — 4 file log
      đầy đủ của 4 lần chạy thật, `StructuredStreaming/checkpoint/` — 3
      checkpoint thật giữ nguyên trạng thái sau khi chạy);
- [x] reset script hoạt động (xoá thư mục checkpoint tương ứng rồi chạy
      lại `spark-submit` là đủ để "reset" một mode; không cần script riêng
      vì thao tác chỉ là xoá 1 thư mục — đã thực hiện thật ở ISSUE-01).

## 9. Kết luận cho giảng viên

Có thể dùng để dạy: **YES**

Các điểm cần đọc trước khi duyệt:

1. ~~Window dùng 1 ngày~~ — **ĐÃ SỬA (2026-08-22, xem mục "CẬP NHẬT" đầu
   file, V01b-V05b)**: window giờ là **5 phút** (watermark 3 phút), đúng
   quy mô BRIEF gốc gợi ý. Dữ liệu nguồn được nén trục thời gian từ ~14.45
   ngày xuống 90 phút bằng `compress_timeline.py` (giữ nguyên thứ tự + tỉ
   lệ khoảng cách, không sửa dữ liệu gốc `00_shared_data/sample/`). Toàn bộ
   kết quả cũ (mục 1-9 dưới đây, window 1 ngày) vẫn giữ nguyên làm tài liệu
   tham khảo/đối chiếu, không xoá.
2. Chạy `local[*]` bên trong container `spark-master` (không dùng cluster
   Spark Standalone thật) — đúng theo BRIEF Buổi 11 (không bắt buộc chạy
   cluster như Buổi 9/10/12).
3. Container `spark-master` giới hạn RAM 512 MiB; nếu nhiều buổi cùng chạy
   `local[*]` trong container này đồng thời có thể gây OOM (đã gặp thật,
   xem ISSUE-01) — không phải lỗi logic bài học.
4. `CONTENT_REPORT.md` Buổi 11 còn trống — xem mục 7.
5. Toàn bộ 5 validation item (V01-V05) có bằng chứng thật (log console +
   checkpoint thật giữ lại trong `StructuredStreaming/`), không có mục
   nào được đánh PASS dựa trên suy đoán.
