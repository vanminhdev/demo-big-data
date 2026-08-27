# 00 – SHARED CONTEXT

## 1. Đối tượng học

- Sinh viên năm 3 ngành Khoa học máy tính.
- Đã học lập trình và cơ sở dữ liệu ở mức cơ bản.
- Không giả định đã biết distributed systems, Hadoop, Spark hoặc Kafka.
- Học phần mang tính nhập môn, ưu tiên hiểu cơ chế, lựa chọn công nghệ và luồng dữ liệu.

## 2. Phong cách nội dung

- Ngôn ngữ: tiếng Việt.
- Văn phong hàn lâm, rõ ràng, phù hợp giảng dạy đại học.
- Giải thích từ vấn đề -> khái niệm -> cơ chế -> ví dụ -> đánh đổi -> thực hành.
- Không đưa quá nhiều chi tiết vận hành production nếu không phục vụ mục tiêu bài.
- Không dùng thuật ngữ mà không giải thích lần đầu xuất hiện.
- Phải phân biệt rõ kiến thức lịch sử/nền tảng với công nghệ hiện đang phổ biến.

## 3. Bài toán chung

RetailStream là hệ thống bán lẻ đa kênh.

Trục nghiệp vụ:

- người dùng xem sản phẩm;
- thêm vào giỏ;
- tạo đơn;
- thanh toán;
- giao hàng hoặc hủy;
- hệ thống cần batch analytics;
- hệ thống cần near-real-time analytics;
- hệ thống có thể dự đoán nguy cơ hủy đơn.

## 4. Công cụ ưu tiên

- Docker Desktop / Docker Engine.
- Python.
- VS Code.
- Jupyter Notebook khi phù hợp.
- MongoDB Compass + mongosh.
- Apache Hadoop/HDFS.
- Apache Spark + PySpark.
- Spark Structured Streaming.
- Spark MLlib.
- Apache Kafka.

## 5. Quy ước slide

- Dùng Marp nếu tạo slide dạng Markdown.
- Nền sáng, trình bày rõ, hạn chế đoạn văn dài.
- Code phải dùng fenced code block có language để syntax highlighting.
- Mermaid phải render thành ảnh trước khi đưa vào slide.
- Không đưa raw Mermaid vào deck cuối nếu hệ render không bảo đảm.
- Mỗi slide nên có một thông điệp chính.
- Ví dụ code phải đủ ngắn để đọc trên màn chiếu.
- Code dài đặt ở file riêng, slide chỉ trích phần cần giảng.

## 6. Quy ước thực hành

Mỗi lab phải có:

- mục tiêu kỹ năng;
- dữ liệu đầu vào;
- môi trường;
- nhiệm vụ;
- mã/lệnh tối thiểu;
- expected output;
- câu hỏi giải thích;
- lỗi phổ biến;
- sản phẩm nộp;
- tiêu chí hoàn thành.

## 7. Quy tắc AI

- Không tự tạo thêm schema nếu chưa kiểm tra `00_DATA_CONTRACT.md`.
- Không thay version công nghệ âm thầm.
- Không sửa quyết định kiến trúc trong nhiều file cùng lúc mà không cập nhật `00_DECISIONS.md`.
- Khi có giả định, phải ghi rõ trong report.
- Khi không chắc một demo chạy được, gắn nhãn `REQUIRES_LOCAL_VALIDATION`.
