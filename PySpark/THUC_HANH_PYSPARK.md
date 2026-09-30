# Thực hành PySpark: báo cáo doanh thu CityRide

**Học phần:** Nhập môn dữ liệu lớn · **Buổi 10** · Thời lượng gợi ý: 100 phút trên lớp + bài tập về nhà

## 1. Mục tiêu

Sau bài thực hành, sinh viên:

1. Đọc dữ liệu CSV bằng schema khai báo; kiểm tra chất lượng dữ liệu sau khi đọc.
2. Làm sạch, ghép bảng và tổng hợp bằng DataFrame API và Spark SQL.
3. Phát hiện và sửa hai lỗi ghép bảng: mất dòng (inner join) và nhân dòng (một – nhiều).
4. Ghi Parquet chia thư mục theo tháng; đọc `explain()` để thấy lọc sớm, chọn cột, gửi bảng nhỏ.
5. Chạy cùng chương trình ở `local[*]` và trên cụm 2 worker, đối chiếu kết quả.

## 2. Dữ liệu

Thư mục `00_shared_data/cityride/lab` (xem README trong đó):

| Bảng | Số dòng | Khóa | Ghi chú |
|---|---|---|---|
| `trips.csv` | 300.000 | `trip_id` | chuyến xe; chỉ chuyến `completed` mới có cước |
| `drivers.csv` | 2.000 | `driver_id` | 244 chuyến có `driver_id` không có trong bảng này |
| `zones.csv` | 12 | `zone_id` | bảng nhỏ |
| `trip_payments.csv` | 254.031 | (`trip_id`, `method`) | một chuyến có thể có 2 dòng thanh toán |

## 3. Môi trường

```bash
cd "D:/school/Big Data/Spark"
bash scripts/start-cluster.sh              # 1 master + 2 worker
cd ../PySpark
bash scripts/run-local.sh                  # chạy mẫu ở local[*]
bash scripts/run-cluster.sh                # chạy mẫu trên cụm
```

Chương trình mẫu `b10_cityride_pipeline.py` in ra 8 mục [1]–[8]. Sinh viên đọc mã mẫu, chạy, rồi làm các nhiệm vụ dưới đây trong một tệp mới `b10_<mssv>.py` (chép vào `Spark/jobs/` trước khi chạy).

## 4. Nhiệm vụ trên lớp

### Nhiệm vụ 1: schema và kiểm tra chất lượng

1. Đọc `trips.csv` hai lần: `inferSchema=True` và schema khai báo. Ghi lại thời gian của từng lệnh đọc.
2. Đếm: tổng số chuyến; số chuyến theo `status`; số chuyến hoàn thành có `fare_vnd = 0`; số chuyến hoàn thành thiếu `payment_method`.
3. Tạo `clean`: chỉ chuyến hoàn thành, cước > 0, `payment_method` rỗng thay bằng `unknown`, thêm cột `month` dạng `yyyy-MM`.

**Kết quả cần đạt:** 300.000 → 239.879 → 239.456 dòng.

### Nhiệm vụ 2: ghép bảng không mất dòng

1. Ghép `clean` với `drivers` bằng `inner` và `left`; so số dòng.
2. Liệt kê 5 `driver_id` không có trong `drivers` và số chuyến của mỗi mã.

**Câu hỏi:** Báo cáo doanh thu nên dùng kiểu ghép nào? Vì sao?

### Nhiệm vụ 3: ghép bảng không nhân dòng

1. Ghép `clean` với `trip_payments` theo `trip_id`. So số dòng trước và sau.
2. Tính tổng `fare_vnd` sau khi ghép và so với tổng trước khi ghép. Giải thích chênh lệch.
3. Viết lại để có **doanh thu theo hình thức thanh toán** (`wallet`, `cash`, `card`) đúng: tổng các hình thức phải bằng 15.762.641.000 đồng.

### Nhiệm vụ 4: báo cáo doanh thu tháng theo quận

1. Viết bằng DataFrame API: `month`, `zone_name`, số chuyến, doanh thu, cước trung bình.
2. Viết lại bằng Spark SQL; kiểm tra hai kết quả giống nhau.
3. Gọi `explain()`: tìm `BroadcastHashJoin`, `Exchange`, `PushedFilters`, `ReadSchema`. Mỗi từ khóa cho biết điều gì?

### Nhiệm vụ 5: Parquet và chia thư mục

1. Ghi `clean` ra Parquet, `partitionBy("month")`. Liệt kê cây thư mục kết quả.
2. So dung lượng với cùng dữ liệu ghi ra CSV.
3. Đọc lại Parquet, lọc tháng `2026-08`, chọn 2 cột; tìm `PartitionFilters` trong `explain()`.

### Nhiệm vụ 6: local và cụm

Chạy chương trình của bạn ở hai chế độ. Lập bảng so sánh số dòng, tổng doanh thu và thời gian. Giải thích vì sao với 300.000 dòng, cụm có thể chậm hơn local.

## 5. Bài tập về nhà

**Bài 1. Tỷ lệ khách hủy theo quận và giờ.** Từ `trips`, tính theo (`pickup_zone`, giờ đặt xe): số yêu cầu, số khách hủy, tỷ lệ hủy. Lưu Parquet chia theo `pickup_zone`. Chỉ ra 3 khung giờ có tỷ lệ hủy cao nhất ở Q01 và giải thích bằng `eta_min`, `surge`.

**Bài 2. Top 10 tài xế theo doanh thu tháng 8.** Chỉ tính tài xế có trong `drivers`. Kèm `vehicle_type` và `rating`. Kiểm tra: tổng doanh thu của mọi tài xế cộng với doanh thu của các chuyến có `driver_id` lạ phải bằng tổng doanh thu tháng 8.

## 6. Nộp bài

| Sản phẩm | Yêu cầu |
|---|---|
| `b10_<mssv>.py` | chạy được bằng `spark-submit` ở cả hai chế độ, không sửa code |
| Log hai lần chạy | local và cụm |
| Báo cáo ngắn (PDF, ≤ 3 trang) | bảng số liệu các nhiệm vụ; trả lời các câu hỏi; ảnh Spark UI có 2 executor |

## 7. Tiêu chí chấm

| Tiêu chí | Tỷ trọng | Đạt khi |
|---|---|---|
| Schema và làm sạch đúng | 20% | số dòng khớp mục 4.1 |
| Ghép bảng đúng | 30% | giải thích được mất dòng, nhân dòng; tổng tiền theo hình thức thanh toán khớp |
| Báo cáo và Spark SQL | 20% | hai cách cho cùng kết quả; đọc đúng các từ khóa trong `explain()` |
| Parquet, chia thư mục | 15% | có `PartitionFilters`; so sánh dung lượng |
| Local và cụm | 15% | cùng kết quả; giải thích thời gian |
