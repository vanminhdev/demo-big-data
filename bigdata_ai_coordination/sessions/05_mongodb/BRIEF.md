# SESSION BRIEF – Hệ quản trị MongoDB

## 1. Vai trò trong mạch RetailStream

Lưu products/orders bằng document; embedding/reference; CRUD; aggregation; index/explain.

## 2. Dữ liệu

- products.json
- orders.json hoặc biểu diễn order_items phù hợp bài học

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

- Import dữ liệu bằng Compass/mongosh.
- CRUD cơ bản.
- Aggregation doanh thu theo danh mục/tháng.
- Tạo index và explain.
- Xác nhận hướng dẫn MongoDB Atlas/Compass nếu có.

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
