# SESSION BRIEF – MapReduce

## 1. Vai trò trong mạch RetailStream

Thống kê lượt xem sản phẩm từ web_logs bằng Map -> Shuffle/Sort -> Reduce.

## 2. Dữ liệu

- web_logs_sample.jsonl
- web_logs lab

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

- Mapper/Reducer chạy được.
- Hadoop Streaming hoặc job mẫu.
- thay đổi số reducer.
- kiểm tra output với sample.

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
