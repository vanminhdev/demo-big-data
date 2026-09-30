# Kế hoạch tái cấu trúc Buổi 9–15 (Spark → Tối ưu)

**Trạng thái (2026-09-30): đã hoàn thành** – 7 deck, 7 PDF, 6 demo chạy thật trên cụm Docker, hướng dẫn thực hành PySpark. Xem D009 trong `00_DECISIONS.md`.

Ngày lập: 2026-09-29. Sao lưu bản cũ: `OneDrive/.../BigData/_backup_slide_2026-09-29/`.

## 1. Đánh giá bản hiện tại

| Vấn đề | Biểu hiện |
|---|---|
| Ngữ cảnh mờ | RetailStream bị ép vào mọi buổi; sinh viên không thấy vì sao cần streaming, Kafka hay ML trong bài toán đó |
| Giọng văn | Tiêu đề phô trương ("Bước nhảy vọt", "Thảm họa", "Mục tiêu Khoa học"), bullet dài như giáo trình |
| Lệch mức nhập môn | Catalyst 4 giai đoạn, Tungsten, Murmur2, RabbitMQ Streams, Pulsar, công thức logistic |
| Thiếu mạch sư phạm | Ít slide "vấn đề → trực giác → cơ chế → ví dụ số → kiểm tra hiểu"; nhiều code dài |
| Hình ảnh | Sơ đồ ASCII, ảnh chụp bài báo, ít ví dụ số theo dõi được từng bước |

## 2. Quyết định (ghi vào D009)

- Bỏ RetailStream cho Buổi 9–15. Dùng một bối cảnh trực quan hơn: **CityRide – ứng dụng gọi xe tại Hà Nội** (hư cấu).
  - Batch: chuyến xe, tài xế, 12 quận (bảng nhỏ → ví dụ tự nhiên cho ghép bảng nhỏ, dữ liệu lệch ở quận trung tâm).
  - Streaming: đếm yêu cầu đặt xe theo quận mỗi 5 phút để bật giá cao điểm.
  - Kafka: một sự kiện "đặt xe" cần tới nhiều dịch vụ (ghép tài xế, thông báo, tính giá, phân tích).
  - MLlib: dự đoán khách hủy chuyến.
- Mỗi buổi theo nhịp: vấn đề thật → trực giác → khái niệm/cơ chế → ví dụ số nhỏ → kiểm tra hiểu → demo → tổng kết.
- Mức nhập môn: giải thích cơ chế bằng hình và ví dụ số; code ngắn (≤ 12 dòng/slide), không đi sâu nội bộ engine.
- Slide theo skill `create-university-marp` (design system HUCE); sơ đồ D2/Graphviz; biểu đồ matplotlib từ số đo demo thật.

## 3. Khung nội dung mới

| Buổi | Câu hỏi trung tâm | Cụm kiến thức |
|---|---|---|
| 9 Spark | Vì sao chuỗi nhiều bước trên MapReduce chậm và Spark làm khác gì? | giới hạn MapReduce; Driver/Cluster Manager/Executor; partition → task; transformation/action, lazy; bước tại chỗ và bước trao đổi dữ liệu (shuffle) → stage; job/stage/task trên Spark UI; phục hồi theo lineage; cache; RDD và DataFrame |
| 10 PySpark | Viết một pipeline phân tích bảng lớn như thế nào cho đúng và rẻ? | SparkSession; đọc CSV/Parquet, schema; select/filter/withColumn/groupBy/join; Spark SQL; lỗi ghép bảng (nhân dòng, mất dòng); null; kế hoạch thực thi và bộ tối ưu (lọc sớm, bỏ cột); gửi bảng nhỏ; Parquet, chia thư mục theo tháng |
| 11 Streaming | Làm sao đếm "ngay bây giờ" khi dữ liệu không bao giờ kết thúc? | batch và stream; bảng không giới hạn, micro-batch; nguồn–truy vấn–đích; output mode; thời điểm sự kiện và thời điểm xử lý; cửa sổ; dữ liệu đến muộn, watermark; trạng thái, checkpoint |
| 12 MLlib | Huấn luyện mô hình trên dữ liệu không vừa một máy thế nào cho khỏi sai? | khi nào cần ML phân tán; đặc trưng số/phân loại, vector; Transformer/Estimator/Pipeline; fit và transform, rò rỉ dữ liệu; hồi quy logistic ở mức trực giác; ma trận nhầm lẫn, precision/recall, AUC, mất cân bằng lớp; lưu và dùng lại mô hình |
| 13 Kafka | Một sự kiện cần tới nhiều hệ thống mà không buộc chặt chúng vào nhau thế nào? | ghép nối trực tiếp; hàng đợi, pub/sub, log; broker, topic, partition, offset; key → partition → thứ tự; consumer group; commit offset, đọc lại, retention; replication; at-most/at-least/exactly-once; nối Spark |
| 14 System design | Từ yêu cầu nghiệp vụ đến kiến trúc dữ liệu thế nào? | yêu cầu dữ liệu và phi chức năng; các lớp thu nhận–lưu trữ–xử lý–phục vụ; batch và stream; Lambda, Kappa; data lake, warehouse, lakehouse; idempotency, replay, chất lượng dữ liệu; thiết kế CityRide từng bước; bài tập nhóm với bối cảnh khác |
| 15 Tối ưu | Pipeline chậm: đo ở đâu, sửa gì, chứng minh thế nào? | quy trình đo → tìm nút thắt → đổi một yếu tố → kiểm chứng; đọc Spark UI; dữ liệu lệch; nhiều file nhỏ; shuffle lớn khi ghép bảng; định dạng; số partition; Kafka partition nóng và độ trễ consumer; tổng kết học phần |

## 4. Demo (D:\school\Big Data)

- `00_shared_data/cityride/`: generator có seed + dữ liệu `sample`/`lab`.
- Viết lại job trong `Spark/`, `PySpark/`, `StructuredStreaming/`, `MLlib/`, `Kafka/`, `Optimization/` theo bối cảnh mới; giữ nguyên docker-compose đã kiểm thử (Spark 3.5.9, Kafka 3.7.1).
- Chạy thật trên cụm Docker, lưu log làm bằng chứng; số liệu trên slide lấy từ log.

## 5. Đầu ra

- `Slide chuẩn/<thư mục buổi>/slides.md` + `assets/` + `slides.pdf`.
- Chép PDF ra `Slide chuẩn/` đè tên cũ: `9. spark.pdf`, `10. pyspark.pdf`, `11. structured streaming.pdf`, `12. mllib.pdf`, `13. kafka.pdf`, `14. system design.pdf`, `15. optimization.pdf`.
- Skill: `BigData/.claude/skills/` (Marp, D2, Graphviz, matplotlib + skill riêng của học phần).
