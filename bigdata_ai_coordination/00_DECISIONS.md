# 00 – DECISION LOG

Tất cả thay đổi ảnh hưởng nhiều buổi phải được ghi tại đây.

---

## D001 – RetailStream là bài toán xuyên suốt

**Trạng thái:** APPROVED

Dùng RetailStream cho phần chuyên môn của học phần.

Trục chính:

- hành vi khách hàng;
- doanh thu;
- nguy cơ hủy đơn.

Không tạo bài toán nghiệp vụ mới nếu không cần thiết.

---

## D002 – Một Data Contract dùng chung

**Trạng thái:** APPROVED

Các AI phải sử dụng `00_DATA_CONTRACT.md`.

Không đổi tên trường theo sở thích từng công nghệ.

---

## D003 – Local AI là Integration/QA owner

**Trạng thái:** APPROVED

Local AI chịu trách nhiệm chạy thử kỹ thuật và xác nhận demo.

Content AI không được đánh dấu `VALIDATED`.

---

## D004 – Spark thực hành dùng hai chế độ

**Trạng thái:** APPROVED

- `local[*]`: học API và debug nhanh.
- Spark Standalone Docker cluster: xác nhận xử lý phân tán.

Tối thiểu khi thực hành cluster:
- 1 Spark Master;
- 2 Spark Worker.

---

## D005 – Không coi Docker container là máy vật lý thật

**Trạng thái:** APPROVED

Docker được dùng để mô phỏng/deploy môi trường nhiều dịch vụ trên máy local nhằm phục vụ học tập.

---

## D006 – MapReduce (Buổi 7) dùng YARN thật thay vì LocalJobRunner

**Ngày:** 2026-08-22
**Trạng thái:** APPROVED (đã triển khai)

### Quyết định

Bổ sung ResourceManager + NodeManager thật vào `Hdfs/docker-compose.yml`
(dùng chung cho Buổi 6 HDFS và Buổi 7 MapReduce). Job MapReduce chạy trên
YARN thật, không còn LocalJobRunner.

### Lý do

Giảng viên yêu cầu trực tiếp: "phải có YARN như thật, phải có python3 sẵn
luôn". LocalJobRunner tuy đúng ngữ nghĩa Map/Shuffle/Reduce nhưng không
minh hoạ được kiến trúc phân tán thật (ResourceManager cấp phát,
NodeManager thực thi, AppMaster theo dõi).

### Ảnh hưởng

- Buổi: 7 (MapReduce), gián tiếp Buổi 6 (HDFS, dùng chung docker-compose.yml), Buổi 14 (System Design, đã cập nhật)
- File: `Hdfs/docker-compose.yml`, `MapReduce/Dockerfile.namenode-with-python3` (mới), `MapReduce/Dockerfile.nodemanager-with-python3` (mới)
- Runtime: cần build 2 image cục bộ trước khi `docker compose up`; RAM NodeManager tăng lên 2560MB (từ mặc định), xem chi tiết sự cố/cách sửa tại `sessions/07_mapreduce/LOCAL_REPORT.md` V06

---

## D007 – MLlib (Buổi 12) dùng dữ liệu có tương quan giả lập

**Ngày:** 2026-08-22
**Trạng thái:** APPROVED (đã triển khai)

### Quyết định

Generator (`00_shared_data/generators/generate_retailstream.py`) cấy tương
quan giả lập có chủ đích giữa `customer_segment`/`payment_method` và
`status=CANCELLED`, **chỉ áp dụng cho mức `lab`**. Mức `sample` (Buổi 5
MongoDB phụ thuộc) giữ nguyên 100% không đổi — đã xác minh sha256.

### Lý do

Giảng viên yêu cầu trực tiếp: "phải sửa dữ liệu generate sao cho phù hợp
chứ ra độ chính xác như thế thì có ý nghĩa gì đâu" — AUC ~0.51 (gần đoán
ngẫu nhiên) không có giá trị minh hoạ cho bài toán "dự đoán nguy cơ hủy
đơn".

### Ảnh hưởng

- Buổi: 12 (MLlib), Buổi 14 (System Design, đã cập nhật)
- File: `00_shared_data/generators/generate_retailstream.py`, `00_shared_data/lab/*` (regenerate), `Spark/docker-compose.yml` (mem_limit spark-master 512m→1536m)
- Runtime: AUC 0.5117 → 0.6893 (local[*] và cluster mode giống hệt nhau), xem `sessions/12_mllib/LOCAL_REPORT.md` V08

---

## D008 – Structured Streaming (Buổi 11) dùng window 5 phút thay vì 1 ngày

**Ngày:** 2026-08-22
**Trạng thái:** APPROVED (đã triển khai)

### Quyết định

Nén trục thời gian của `clickstream_sample.jsonl` (dùng riêng bản sao
`StructuredStreaming/data_source_v2/`, không sửa `00_shared_data/sample/`
gốc) từ ~14.45 ngày xuống 90 phút; đổi `WINDOW_DURATION` từ "1 day" xuống
"5 minutes", `WATERMARK_DELAY` xuống "3 minutes".

### Lý do

Giảng viên yêu cầu trực tiếp: "nên sửa lại cho nhỏ đi cho phù hợp" — window
1 ngày không đúng tinh thần "vài phút" mà BRIEF gốc gợi ý.

### Ảnh hưởng

- Buổi: 11 (Structured Streaming), Buổi 14 (System Design, đã cập nhật)
- File: `StructuredStreaming/compress_timeline.py` (mới), `StructuredStreaming/prepare_batches_v2.py` (mới), `StructuredStreaming/streaming_job.py` (đổi CONFIG)
- Runtime: xem `sessions/11_structured_streaming/LOCAL_REPORT.md` mục "CẬP NHẬT" (V01b-V05b)

---

## Mẫu quyết định mới

## DXXX – Tên quyết định

**Ngày:** YYYY-MM-DD  
**Trạng thái:** PROPOSED / APPROVED / REJECTED

### Quyết định

...

### Lý do

...

### Ảnh hưởng

- Buổi:
- File:
- Runtime:
