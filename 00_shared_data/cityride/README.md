# Bộ dữ liệu CityRide (Buổi 9–15)

CityRide là ứng dụng gọi xe hư cấu tại Hà Nội, dùng chung cho các buổi Spark, PySpark, Structured Streaming, MLlib, Kafka, thiết kế hệ thống và tối ưu. Bộ dữ liệu này thay cho RetailStream từ Buổi 9 trở đi. Các buổi 5–7 vẫn dùng RetailStream trong `00_shared_data/sample` và `lab`.

## Sinh lại dữ liệu

```bash
python generate_cityride.py --scale sample   # 200 chuyến, xem nhanh
python generate_cityride.py --scale lab      # 300.000 chuyến, 07–09/2026
```

Seed cố định (2026), nên chạy lại luôn ra cùng dữ liệu.

## Các bảng

| Tệp | Số dòng (lab) | Nội dung |
|---|---|---|
| `zones.csv` | 12 | `zone_id`, `zone_name`, `is_center` |
| `drivers.csv` | 2.000 | `driver_id`, `vehicle_type`, `join_date`, `rating` |
| `trips.csv` | 300.000 | chuyến xe (xem cột bên dưới) |
| `trip_payments.csv` | 254.031 | các phần thanh toán; khoảng 6% chuyến trả làm 2 phần (`split`) |

Cột của `trips.csv`: `trip_id, request_time, pickup_zone, dropoff_zone, driver_id, vehicle_type, distance_km, duration_min, eta_min, surge, is_raining, fare_vnd, payment_method, status`.

- `status`: `completed` (239.879), `rider_cancelled` (51.107), `driver_cancelled` (9.014).
- Tỷ lệ khách hủy tăng theo `eta_min`, `surge`, `is_raining` và giờ cao điểm (dùng cho Buổi 12).
- Nhu cầu lệch về quận trung tâm: Q01 Hoàn Kiếm nhiều chuyến nhất.

## Lỗi dữ liệu cài cố ý (Buổi 10)

| Lỗi | Số lượng (lab) | Dùng để dạy |
|---|---|---|
| chuyến hoàn thành có `fare_vnd = 0` | 423 | làm sạch |
| chuyến hoàn thành thiếu `payment_method` | 2.452 | xử lý null |
| `driver_id` không có trong `drivers.csv` | 244 chuyến (sau làm sạch) | inner join làm mất dòng |
| chuyến trả làm 2 phần trong `trip_payments` | 14.409 | ghép bảng làm nhân dòng |
| chuyến bị hủy không có cước, thanh toán, thời gian | toàn bộ chuyến hủy | rò rỉ dữ liệu khi học máy |
