# SESSION BRIEF – PySpark

## 1. Vai trò trong mạch RetailStream

Join orders + order_items + products; làm sạch; tính doanh thu theo tháng/danh mục.

## 2. Dữ liệu

- orders.csv
- order_items.csv
- products.json hoặc Parquet

Tuân thủ `00_DATA_CONTRACT.md`.

## 3. Content AI phải tạo

- slide/slide outline theo yêu cầu dự án;
- tài liệu đọc nếu được giao;
- bài thực hành tích hợp;
- ví dụ code/lệnh;
- câu hỏi giải thích;
- danh sách validation item;
- `CONTENT_REPORT.md`.

## 4. Local AI phải thực hiện

- local[*].
- explicit schema.
- join correctness.
- aggregation.
- write Parquet.
- explain().
- chạy lại trên cluster nếu brief cuối yêu cầu.

- tạo `LOCAL_REPORT.md`;
- cập nhật `00_WORK_STATUS.md`.

## 5. Interface bắt buộc

Content AI phải đánh mã các yêu cầu kiểm thử: `V01`, `V02`, ...

Local AI phản hồi đúng từng ID bằng `PASS/FAIL`.

## 6. Definition of Done

- Nội dung đúng mức nhập môn.
- Không đổi Data Contract.
- Dùng RetailStream.
- Demo chính chạy thật.
- Mismatch được xử lý.
- Local Validation PASS.
- Giảng viên duyệt.
