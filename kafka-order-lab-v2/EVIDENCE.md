# Phiếu minh chứng thực hành

Sinh viên nộp các ảnh chụp màn hình theo đúng thứ tự dưới đây. Mỗi ảnh cần thể hiện rõ cửa sổ, lệnh hoặc giao diện liên quan; không cần chụp toàn bộ màn hình nếu phần cần chứng minh đã nhìn thấy rõ.

| # | Minh chứng | Cần thể hiện |
|---|---|---|
| 1 | Docker Compose | Kafka, API, Consumer, Kafka UI đang chạy |
| 2 | Topic | `order-events` có 4 partition |
| 3 | API bất đồng bộ | `POST /orders` trả `202` và trạng thái `PROCESSING` |
| 4 | Partition key | Các event của cùng `orderId` có cùng partition |
| 5 | Offset | Cùng order có offset tăng theo thứ tự event |
| 6 | Consumer | Log có `key`, `partition`, `offset`, `orderId`, `status` |
| 7 | Polling | Trạng thái chuyển từ `PROCESSING` sang trạng thái đã xử lý |
| 8 | Message vẫn còn | Kafka UI vẫn nhìn thấy message sau khi consumer đọc |
| 9 | Consumer group | Hai consumer cùng `order-service` được chia các partition |

## Câu trả lời cuối bài

1. Vì sao `orderId` được dùng làm partition key?
2. Khi consumer đọc message, Kafka có xóa ngay message đó không?
3. `partition` và `offset` trong log consumer cho chúng ta biết điều gì?
4. Vì sao API trả `PROCESSING` trước khi consumer xử lý xong?
