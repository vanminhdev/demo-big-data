# 00 – MASTER PLAN
## Học phần Nhập môn dữ liệu lớn

## 1. Mục tiêu dự án

Chuẩn bị đồng bộ toàn bộ học phần 15 buổi, gồm:

- slide giảng dạy;
- tài liệu đọc;
- bài thực hành tích hợp;
- dữ liệu mẫu/lab/cluster;
- mã nguồn demo;
- Docker Compose và môi trường chạy;
- expected output;
- báo cáo kiểm thử;
- tài liệu phục vụ giảng viên;
- gói phát cho sinh viên.

Mục tiêu tổ chức: nhiều AI có thể làm song song nhưng đầu ra cuối vẫn thống nhất về dữ liệu, thuật ngữ, công nghệ, ví dụ và cách dẫn dắt.

---

## 2. Bài toán xuyên suốt

Toàn bộ phần chuyên môn dùng tình huống **RetailStream**:

> Hệ thống bán lẻ đa kênh cần theo dõi hành vi khách hàng, phân tích doanh thu và dự đoán nguy cơ đơn hàng bị hủy.

Các nhóm dữ liệu dùng chung:

- `customers`
- `products`
- `orders`
- `order_items`
- `web_logs`
- `clickstream`
- `product_events`

Không tạo dataset nghiệp vụ mới cho từng buổi nếu không có lý do được phê duyệt.

Quy mô tăng dần:

```text
sample -> lab -> cluster -> stream
```

---

## 3. Mạch kiến thức xuyên suốt

```text
RDBMS
  |
MongoDB
  |
HDFS
  |
MapReduce
  |
Spark
  |
PySpark
  |
Structured Streaming
  |
Kafka
  |
MLlib
  |
Thiết kế hệ thống
  |
Tối ưu
```

Lưu ý: đây là **mạch dẫn dắt sư phạm**, không có nghĩa công nghệ sau luôn thay thế hoàn toàn công nghệ trước.

Mỗi công nghệ mới phải trả lời được ít nhất một câu hỏi:

1. Vấn đề mới nào xuất hiện?
2. Công nghệ hiện tại gặp giới hạn gì?
3. Công nghệ mới giải quyết phần nào?
4. Đánh đổi mới xuất hiện là gì?

---

## 4. Phân vai AI

### 4.1. Content AI – ChatGPT

Chịu trách nhiệm:

- khung slide;
- slide hoàn chỉnh;
- tài liệu đọc;
- bài tập;
- bài thực hành;
- câu hỏi tự kiểm tra;
- code minh họa;
- sơ đồ;
- hướng dẫn giảng dạy;
- xác định các nội dung cần Local AI kiểm thử.

Không được tự tuyên bố demo đã chạy thành công nếu chưa có Local Validation.

### 4.2. Local AI – máy giảng viên

Đóng vai trò:

**Integration Engineer + QA Engineer + Lab Engineer**

Chịu trách nhiệm:

- tạo/cập nhật dataset;
- Docker Compose;
- cấu hình môi trường;
- mã demo thực tế;
- chạy thử từ môi trường sạch;
- xác nhận version;
- lưu expected output;
- chụp/log minh chứng cần thiết;
- phát hiện nội dung slide/code không đúng thực tế;
- đề xuất sửa cho Content AI;
- duy trì khả năng chạy lại.

### 4.3. Giảng viên

Đóng vai trò:

**Project Owner / Reviewer**

Chịu trách nhiệm:

- giao việc lớn;
- duyệt thay đổi;
- đọc report;
- xử lý xung đột giữa AI;
- quyết định mức độ kiến thức phù hợp;
- duyệt gói cuối.

---

## 5. Bảng công việc theo buổi

| Buổi | Chủ đề | Bài toán RetailStream | Content AI | Local AI |
|---|---|---|---|---|
| 1 | Tổng quan Big Data | Nhận diện yêu cầu dữ liệu | Soạn lý thuyết/tình huống | Không bắt buộc |
| 2 | Transaction/ACID | Đặt hàng, thanh toán, tồn kho | Lý thuyết + tình huống | Có thể chuẩn bị demo DB |
| 3 | Indexing | Truy vấn orders | Lý thuyết + execution plan | Chạy MySQL/SQL Server/PostgreSQL |
| 4 | CAP/NoSQL | Phân tán dịch vụ RetailStream | Lý thuyết + lựa chọn C/A | Không bắt buộc |
| 5 | MongoDB | products/orders | Slide + lab | Import/query/index/explain |
| 6 | HDFS | web_logs/orders lớn | Slide + lab | 1 NameNode + >=2 DataNode |
| 7 | MapReduce | đếm lượt xem sản phẩm | Thuật toán + lab | Hadoop Streaming/job mẫu |
| 8 | Giữa kỳ | Buổi đánh giá | Đề/rubric | Không bắt buộc |
| 9 | Spark | dùng lại dữ liệu HDFS | Spark concepts | Spark standalone cluster |
| 10 | PySpark | join orders/items/products | DataFrame lab | Chạy local + cluster |
| 11 | Structured Streaming | clickstream theo window | Streaming lab | File/socket stream + checkpoint |
| 12 | MLlib | dự đoán hủy đơn | ML pipeline | Chạy local + cluster |
| 13 | Kafka | event ingestion | Kafka + Spark lab | Kafka Docker + integration |
| 14 | System Design | ghép RetailStream end-to-end | Kiến trúc tổng hợp | Xác minh pipeline khả thi |
| 15 | Optimization | lỗi/tắc nghẽn RetailStream | Case tối ưu | Tạo/kiểm thử tình huống lỗi |

---

## 6. Quy tắc trạng thái

Mỗi buổi có thể ở một trong các trạng thái:

- `NOT_STARTED`
- `DRAFTING`
- `CONTENT_READY`
- `LOCAL_TESTING`
- `ISSUES_FOUND`
- `VALIDATED`
- `FINALIZED`

Không dùng `FINALIZED` nếu phần kỹ thuật chưa `VALIDATED`.

---

## 7. Definition of Done

Một buổi kỹ thuật chỉ hoàn thành khi:

- nội dung bám đúng mục tiêu học tập;
- dùng đúng RetailStream Data Contract;
- slide và lab thống nhất;
- mọi code chính đã chạy;
- Docker image/version được ghim;
- có expected output tối thiểu;
- lỗi phổ biến đã được ghi nhận;
- Local AI đã lập `LOCAL_REPORT.md`;
- các mismatch giữa tài liệu và runtime đã được xử lý;
- giảng viên duyệt.

---

## 8. Quy tắc ưu tiên

Ưu tiên theo thứ tự:

1. Correctness.
2. Khả năng chạy lại.
3. Khả năng sinh viên hiểu.
4. Tính liên thông giữa các buổi.
5. Tính hiện đại của công nghệ.
6. Tối ưu hiệu năng.

Không hy sinh khả năng giảng dạy để mô phỏng production quá phức tạp.
