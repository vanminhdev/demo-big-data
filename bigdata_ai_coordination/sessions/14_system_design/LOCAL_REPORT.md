# LOCAL VALIDATION REPORT – Buổi 14 (Thiết kế hệ thống Big Data)

## 1. Trạng thái

**Validation:** PASS

Buổi này không dựng hạ tầng mới — nhiệm vụ của Local AI là **rà soát tính khả thi**
của kiến trúc RetailStream end-to-end bằng cách tổng hợp lại toàn bộ 8 báo cáo kỹ
thuật đã PASS (Buổi 5, 6, 7, 9, 10, 11, 12, 13), xác nhận version tương thích và
dữ liệu luân chuyển đúng giữa các thành phần. Không suy đoán — mọi số liệu dưới
đây lấy từ `docker ps`/`docker stats` chạy thật tại thời điểm viết report
(2026-08-20) và từ các `LOCAL_REPORT.md` đã có.

## 2. Environment — Kiểm kê toàn bộ thành phần đang chạy đồng thời

```text
NAMES            IMAGE                                              STATUS
kafka            apache/kafka:3.7.1                                 healthy
spark-worker-1   apache/spark:3.5.9-python3                         up
spark-worker-2   apache/spark:3.5.9-python3                         up
spark-master     apache/spark:3.5.9-python3                         healthy
hdfs-datanode1   bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8    healthy
hdfs-datanode2   bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8    healthy
hdfs-namenode    bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8    healthy
mongodb          mongo:8.0                                          up
```

**8 container, tất cả version đã ghim (không có `:latest`)**, chạy đồng thời ổn
định. RAM thực đo tại thời điểm rà soát (`docker stats --no-stream`):

| Container | Mem Usage / Limit |
|---|---|
| kafka | 465.2MiB / 800MiB |
| spark-worker-1 | 84.5MiB / 800MiB |
| spark-worker-2 | 159.5MiB / 800MiB |
| spark-master | 76.4MiB / 512MiB |
| hdfs-datanode1 | 193.9MiB / 3.826GiB |
| hdfs-datanode2 | 201.2MiB / 3.826GiB |
| hdfs-namenode | 267.6MiB / 3.826GiB |
| mongodb | 85.5MiB / 3.826GiB |

Tổng RAM thực dùng lúc idle: ~1.53GiB. Máy kiểm thử có Docker Desktop VM
3.826GiB. **Ở trạng thái idle, cả 8 container chạy cùng lúc thoải mái**, nhưng
khi có tải thật (ví dụ MLlib huấn luyện + Structured Streaming cùng lúc), Buổi
11 và 12 đã ghi nhận OOM crash trên `spark-master` (giới hạn 512MB) khi 2
application tranh chấp — xem `sessions/11_structured_streaming/LOCAL_REPORT.md`
ISSUE-01 và `sessions/12_mllib/LOCAL_REPORT.md`. **Kết luận kiến trúc:** demo
tuần tự (từng buổi một) hoàn toàn khả thi trên máy cấu hình tối thiểu đã kiểm
thử; demo đồng thời nhiều job nặng trên cùng cụm Spark cần tăng giới hạn bộ nhớ
container hoặc chạy tuần tự — cần nêu rõ trong slide Buổi 14 để sinh viên không
hiểu nhầm là hệ thống luôn scale vô hạn.

## 3. Kiến trúc RetailStream end-to-end đã kiểm chứng

```text
[MongoDB]           [HDFS]                [Kafka]
 products/orders      web_logs              clickstream/product_events
 (embed+ref demo)     (1 NN + 2 DN)         (KRaft, 4 partition)
      |                   |                       |
      | (buổi 5)          | (buổi 6,7: MapReduce) | (buổi 13)
      v                   v                       v
                    [Spark Standalone Cluster: 1 master + 2 worker]
                    (buổi 9 Spark core, 10 PySpark, 12 MLlib,
                     11 Structured Streaming file-source,
                     13 Structured Streaming Kafka-source)
                              |
                              v
                    [Parquet output / console sink / PipelineModel]
                     (serving layer ở mức nhập môn — chưa có BI/API layer thật)
```

Toàn bộ nguồn dữ liệu đều xuất phát từ **một bộ RetailStream chung**
(`00_shared_data/`, seed=42), không có buổi nào tự tạo dataset song song lệch
nhau — đây là điều kiện tiên quyết để kiến trúc "hợp nhất" ở buổi này có ý nghĩa
thật, không phải ghép hình thức.

## 4. Validation Results

### V01 – Kiểm kê component và version đã ghim

**Result:** PASS

Không có image nào dùng `:latest`. Bảng version đầy đủ:

| Thành phần | Image:tag | Ghi chú |
|---|---|---|
| MongoDB | `mongo:8.0` (thực tế 8.0.29) | Buổi 5 |
| HDFS NameNode/DataNode | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` / `hadoop-datanode` cùng tag (NameNode dùng bản build cục bộ có sẵn python3 từ 2026-08-22) | Buổi 6, dùng lại cho Buổi 7 (MapReduce, giờ chạy YARN thật với ResourceManager+NodeManager, xem cập nhật mục V04) |
| Spark Master/Worker | `apache/spark:3.5.9-python3` | Buổi 9, dùng lại cho Buổi 10, 11, 12, 13 |
| Kafka | `apache/kafka:3.7.1` (KRaft) | Buổi 13 |

### V02 – Tính tương thích định dạng dữ liệu giữa các thành phần

**Result:** PASS

- MongoDB import trực tiếp từ CSV/JSON của `00_shared_data/sample/` (Buổi 5).
- HDFS lưu JSON Lines `web_logs_sample.jsonl` nguyên bản, không cần chuyển đổi (Buổi 6).
- MapReduce (Hadoop Streaming) đọc thẳng JSON Lines từ HDFS bằng mapper Python (Buổi 7).
- PySpark đọc CSV/JSON với **explicit schema** (không suy diễn), ghi Parquet partition theo tháng (Buổi 10).
- MLlib đọc CSV (`00_shared_data/lab/`) qua cùng cơ chế PySpark (Buổi 12).
- Structured Streaming đọc JSON Lines (file source, Buổi 11) và JSON qua Kafka value (Kafka source, Buổi 13) — cùng schema `clickstream`.

Không phát sinh xung đột định dạng nào giữa các buổi — mọi thành phần đọc đúng
schema `00_DATA_CONTRACT.md`, không có buổi nào phải "dịch" lại dữ liệu sang
định dạng riêng.

### V03 – Tính khả thi tích hợp thực tế (không phải giả định trên giấy)

**Result:** PASS

Các điểm nối đã CHẠY THẬT, không phải chỉ vẽ sơ đồ:

- HDFS → MapReduce: Buổi 7 chạy Hadoop Streaming trực tiếp trên cụm HDFS của Buổi 6 (cùng container, không dựng lại).
- Spark cluster dùng chung cho 5 buổi (9, 10, 11, 12, 13) không cần dựng lại — xác nhận Spark Standalone đủ ổn định để nhiều "buổi học" tái sử dụng, giống một cụm dev dùng chung trong thực tế.
- Kafka → Spark Structured Streaming: Buổi 13 kết nối Kafka vào cùng Docker network `spark_spark-net` của cụm Spark, `numInputRows=200` khớp chính xác số message producer gửi — chứng minh pipeline ingest → stream-processing hoạt động thật, không phải 2 thành phần tách rời trên lý thuyết.

### V04 – Giới hạn cần nêu rõ cho sinh viên (không phóng đại khả năng hệ thống)

**Result:** PASS (ghi nhận giới hạn, không phải lỗi)

1. Cụm HDFS/Spark/Kafka đều là **container mô phỏng nhiều node trên một máy**,
   không phải nhiều máy vật lý (D005 trong `00_DECISIONS.md`) — vẫn đúng, không
   đổi.
2. ~~MapReduce chạy ở chế độ LocalJobRunner~~ — **ĐÃ SỬA (2026-08-22)**: cụm
   HDFS (`Hdfs/docker-compose.yml`) giờ có ResourceManager + NodeManager thật,
   job MapReduce đã chạy lại và PASS trên YARN thật (không còn LocalJobRunner).
   Xem `sessions/07_mapreduce/LOCAL_REPORT.md` V06/V07. Giới hạn còn lại: chỉ
   1 NodeManager (vẫn mô phỏng trên 1 máy, đúng D005), không phải nhiều máy
   vật lý — không cần quyết định thêm.
3. Chạy đồng thời nhiều job nặng trên `spark-master` từng gây OOM (Buổi 11 vs
   12) — **đã sửa 1 phần**: `mem_limit` của `spark-master` tăng từ 512MB lên
   1536MB (2026-08-22, xem `sessions/12_mllib/LOCAL_REPORT.md` ISSUE-03) để
   chạy được pipeline MLlib với dữ liệu thật. Vẫn cần lưu ý: kiến trúc "nhiều
   nhóm sinh viên cùng dùng 1 cụm Spark cùng lúc" nên tăng thêm resource limit
   nếu triển khai thật cho lớp đông người — khuyến nghị chạy tuần tự từng job
   nặng vẫn còn giá trị.
4. ~~Mô hình MLlib có AUC ~0.51~~ — **ĐÃ SỬA (2026-08-22)**: generator đã cấy
   tương quan giả lập có chủ đích giữa `customer_segment`/`payment_method` và
   `CANCELLED`, chỉ áp dụng cho mức `lab` (mức `sample` giữ nguyên 100%, đã
   xác minh sha256). AUC giờ là **0.6893** (từ 0.5117), số liệu thật, chạy
   lại cả `local[*]` và cluster mode cho kết quả giống hệt nhau. Xem
   `sessions/12_mllib/LOCAL_REPORT.md` V08.
5. Buổi 11 (Structured Streaming): window đã đổi từ 1 ngày xuống **5 phút**
   (2026-08-22, theo yêu cầu giảng viên "nhỏ đi cho phù hợp") — dữ liệu nguồn
   được nén trục thời gian, không đổi dữ liệu gốc. Xem
   `sessions/11_structured_streaming/LOCAL_REPORT.md` mục "CẬP NHẬT".

## 5. Issues Found

Không phát sinh issue mới ở buổi này — các issue liên quan đã được các buổi
5–13 ghi nhận riêng (xem mục V04). Buổi 14 chỉ tổng hợp lại để giảng viên có
cái nhìn toàn cảnh khi duyệt.

## 6. Mismatch với tài liệu Content AI

| Vị trí | Nội dung hiện tại | Thực tế | Đề xuất |
|---|---|---|---|
| `sessions/14_system_design/CONTENT_REPORT.md` | "Chưa cập nhật" | Toàn bộ 8 LOCAL_REPORT của buổi 5–13 đã PASS và có thể dùng làm nguồn số liệu thật cho bài tập thiết kế kiến trúc RetailStream cuối buổi 14 | Content AI nên trích dẫn đúng version/số liệu trong các LOCAL_REPORT đã có, đặc biệt kiến trúc ở mục 3 báo cáo này, thay vì tự vẽ kiến trúc giả định |

## 7. Khả năng chạy lại

- [x] Toàn bộ 8 container khởi động từ các buổi trước vẫn `healthy`/`up` tại thời điểm rà soát, không cần dựng lại.
- [x] Mọi version đã ghim, không có `:latest`.
- [x] Dữ liệu nguồn dùng chung một bộ (`00_shared_data/`), có seed cố định, tái tạo được.
- [ ] Chưa có 1 script duy nhất "start toàn bộ pipeline 8 container theo đúng thứ tự phụ thuộc" — hiện mỗi buổi có script riêng (`MongoDb/`, `Hdfs/scripts/`, `Spark/`, `Kafka/`). Nếu giảng viên muốn demo end-to-end liền mạch trong 1 buổi 14, nên cân nhắc 1 script tổng hợp gọi lần lượt các `docker compose up -d` theo đúng thứ tự HDFS → Spark → Kafka → MongoDB (độc lập với 3 cái sau).

## 8. Kết luận cho giảng viên

Có thể dùng để dạy: **YES**

Các điểm cần đọc trước khi duyệt:

1. Kiến trúc RetailStream end-to-end **đã được kiểm chứng chạy thật**, không phải
   thiết kế trên giấy — mọi mũi tên trong sơ đồ mục 3 đều có bằng chứng thực thi
   trong các LOCAL_REPORT tương ứng.
2. ~~2 quyết định đang chờ giảng viên~~ — **CẢ 2 ĐÃ GIẢI QUYẾT (2026-08-22)**:
   (a) MapReduce giờ chạy YARN thật (ResourceManager+NodeManager), (b) MLlib
   giờ dùng dữ liệu có tương quan giả lập, AUC 0.6893. Xem mục V04 điểm 2 và 4
   ở trên. Đồng thời Structured Streaming (Buổi 11) đã đổi window 1 ngày →
   5 phút theo yêu cầu giảng viên.
3. Giới hạn RAM khi chạy đồng thời nhiều job nặng trên Spark cluster dùng chung
   — cần nêu rõ trong slide để không phóng đại khả năng "luôn scale" của hệ thống.
   `spark-master` đã tăng RAM lên 1536MB nhưng vẫn nên khuyến nghị chạy tuần tự.
