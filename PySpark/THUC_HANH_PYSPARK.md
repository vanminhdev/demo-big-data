# THỰC HÀNH PYSPARK: RETAILSTREAM

> Làm theo thứ tự. Mỗi lab có một kết quả cần kiểm tra. Không dùng `inferSchema` trong Lab 2-6.

## 1. Tóm lược lõi kiến thức

| Nội dung | RDD | DataFrame |
|---|---|---|
| Dạng dữ liệu | Bản ghi tổng quát | Bảng có cột, kiểu dữ liệu |
| Tối ưu hóa | Tự quản lý nhiều hơn | Catalyst biết schema và tối ưu kế hoạch |
| Nên dùng khi | Xử lý mức thấp, đặc thù | ETL, SQL, báo cáo, bài lab này |

| Nhóm lệnh | Ví dụ | Spark thực thi ngay? |
|---|---|---|
| Narrow transformation | `select`, `filter`, `withColumn` | Chưa, Spark ghi kế hoạch |
| Wide transformation | `groupBy`, `join`, `orderBy` | Chưa, có thể gây shuffle khi chạy |
| Action | `show`, `count`, `write`, `collect` | Có |

- **Lazy evaluation:** Spark gom chuỗi transformation thành một kế hoạch trước khi chạy action.
- **Catalyst:** dùng schema để cắt cột thừa, đẩy điều kiện lọc gần dữ liệu và chọn kế hoạch join.
- **Không dùng `collect()` cho dữ liệu lớn:** nó kéo toàn bộ kết quả về Driver.

| Định dạng | Cách lưu | Khi phù hợp |
|---|---|---|
| CSV | Text theo dòng | Trao đổi dữ liệu, dễ xem bằng tay |
| JSON | Text bán cấu trúc | Dữ liệu lồng nhau, API |
| Parquet | Nhị phân theo cột | Phân tích dữ liệu lớn, nén và chỉ đọc cột cần thiết |

Partition theo `month` tạo thư mục như `month=2026-07/`. Bộ lọc theo tháng cho phép Spark bỏ qua các thư mục không liên quan.

## 2. Môi trường và lệnh khởi động

> **Lưu ý quan trọng:** Toàn bộ môi trường thực hành đã được đóng gói sẵn trong Docker. Sinh viên **không cần cài đặt Java hay Apache Spark** trên máy cá nhân để tránh lỗi xung đột phiên bản trên Windows.

### Chuẩn bị

- [ ] Đã bật Docker Desktop.
- [ ] Đã mở terminal tại thư mục dự án `D:\school\Big Data`.
- [ ] Nếu dùng Git Bash trên Windows, luôn chạy `export MSYS_NO_PATHCONV=1` trước các lệnh `docker exec`.

### Bước 1: Khởi động cụm Spark Standalone bằng Docker Compose

Mở terminal và di chuyển vào thư mục `Spark`:

```bash
cd "D:/school/Big Data/Spark"
docker compose up -d
```

Kiểm tra giao diện Spark Master Web UI tại: `http://localhost:8080` (phải thấy 2 Worker đang ở trạng thái `ALIVE`).

### Bước 2: Mở PySpark Shell tương tác (Dùng cho Lab 1)

Để gõ từng dòng lệnh PySpark tương tác trực tiếp trong môi trường Docker:

```bash
docker exec -it spark-master /opt/spark/bin/pyspark --master "local[*]"
```

*(Nhấn `Ctrl + D` hoặc gõ `exit()` khi muốn thoát khỏi PySpark Shell).*

### Bước 3: Thực thi kịch bản xử lý tự động (Dùng cho Lab 2 - 6 và Bài tập)

**Nếu dùng Git Bash / Linux / macOS:**

```bash
cd "D:/school/Big Data/PySpark"
export MSYS_NO_PATHCONV=1
./scripts/run-local.sh       # Chạy chế độ local[*]
./scripts/run-cluster.sh     # Chạy trên cụm Spark Master + 2 Workers
```

**Nếu dùng PowerShell:**

```powershell
cd 'D:\school\Big Data\PySpark'
# Copy các script sang thư mục Spark/jobs để container nhìn thấy:
Copy-Item process_retailstream.py ..\Spark\jobs\ -Force
Copy-Item bai_tap_1_top_customers.py ..\Spark\jobs\ -Force
Copy-Item bai_tap_2_revenue_share.py ..\Spark\jobs\ -Force

# Chạy pipeline chính:
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/process_retailstream.py
```

## 3. Hands-on Labs

### Lab 1 - DataFrame và thao tác cột

- [ ] Mở terminal và kết nối vào PySpark shell trong container:
  ```bash
  docker exec -it spark-master /opt/spark/bin/pyspark --master "local[*]"
  ```
- [ ] Chạy lần lượt từng khối lệnh bên dưới:

```python
from pyspark.sql import functions as F

data = [("Alice", 85), ("Bob", 92), ("Charlie", 78)]
df = spark.createDataFrame(data, ["name", "score"])

df.select("name", "score") \
  .filter(F.col("score") >= 80) \
  .withColumn("passed", F.col("score") >= 50) \
  .show()

df.groupBy("passed").count().show()
```

**Tự kiểm tra:** `Alice` và `Bob` xuất hiện trong kết quả lọc; `show()` là action.

### Lab 2 - Đọc RetailStream bằng schema rõ ràng và làm sạch

- [ ] Đọc `PySpark/data/orders_sample.csv` với `ORDERS_SCHEMA` trong `process_retailstream.py`.
- [ ] Lọc `total_amount >= 0`.
- [ ] Đọc `products_sample.json`, đổi `brand = null` thành `UNKNOWN` bằng `F.coalesce`.

```python
orders_ok = orders.filter(F.col("total_amount") >= 0)
products_ok = products.withColumn(
    "brand", F.coalesce(F.col("brand"), F.lit("UNKNOWN"))
)
```

**Tự kiểm tra:** `orders.count() = 150`, `orders_ok.count() = 149`; có 3 brand `null` trước khi làm sạch và 0 sau khi làm sạch.

### Lab 3 - Join ba bảng và kiểm soát số dòng

```python
items_orders = order_items.join(orders_ok, "order_id", "inner")
retail = items_orders.join(products_ok, "product_id", "inner")

assert order_items.count() == 380
assert items_orders.count() == 376
assert retail.count() == 376
```

- [ ] Nối `order_items` với `orders_ok` bằng `order_id`.
- [ ] Nối kết quả với `products_ok` bằng `product_id`.
- [ ] Đếm sau từng phép nối.

**Giải thích:** bốn dòng item của đơn âm bị loại. `product_id` duy nhất nên join sản phẩm không làm nhân bản dòng.

### Lab 4 - Doanh thu theo tháng và danh mục, đối chiếu SQL

```python
report = (retail
    .withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("month", F.date_format("order_time", "yyyy-MM"))
    .groupBy("month", "category_name")
    .agg(F.sum("line_amount").alias("revenue"))
    .orderBy("month", "category_name"))

report.show(100, truncate=False)
retail.createOrReplaceTempView("retail")
```

```sql
SELECT date_format(order_time, 'yyyy-MM') AS month,
       category_name,
       SUM(quantity * unit_price) AS revenue
FROM retail
GROUP BY 1, 2
ORDER BY 1, 2
```

- [ ] Chạy SQL bằng `spark.sql("""...""")`.
- [ ] So sánh kết quả với `report`.

### Lab 5 - Parquet và partitioning

```python
(report.write.mode("overwrite")
 .partitionBy("month")
 .parquet("output/revenue_by_month_category"))

spark.read.parquet("output/revenue_by_month_category") \
     .filter("month = '2026-07'") \
     .select("category_name", "revenue") \
     .show()
```

- [ ] Mở thư mục kết quả và tìm các thư mục `month=...`.
- [ ] So sánh dung lượng bằng `Get-ChildItem -Recurse` (PowerShell) hoặc `du -sh` (Git Bash).
- [ ] Giải thích vì sao truy vấn tháng 07 không cần quét partition tháng 02.

### Lab 6 - Đọc execution plan

```python
report.explain(mode="extended")
```

- [ ] Tìm `FileScan` hoặc `PushedFilters` khi có filter trên nguồn Parquet.
- [ ] Tìm `BroadcastHashJoin` khi Spark broadcast bảng `products` nhỏ.
- [ ] Tìm `Exchange`: đó là điểm Spark shuffle dữ liệu giữa stage.

## 4. Bài tập thực hành bắt buộc

### Bài tập 1 - Top khách hàng

**Tệp phải tạo:** `bai_tap_1_top_customers.py`

Yêu cầu: đọc `orders_sample.csv` bằng explicit schema, bỏ đơn âm, tính tổng chi tiêu mỗi khách hàng; chọn phương thức có tổng thanh toán lớn nhất của mỗi khách hàng (hòa thì lấy tên phương thức tăng dần); in top 5 giảm dần theo `total_spend`.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

clean = orders.filter(F.col("total_amount") >= 0)
customer_total = clean.groupBy("customer_id").agg(F.sum("total_amount").alias("total_spend"))
payment_total = clean.groupBy("customer_id", "payment_method").agg(F.sum("total_amount").alias("payment_spend"))
window = Window.partitionBy("customer_id").orderBy(F.desc("payment_spend"), F.asc("payment_method"))
top_payment = payment_total.withColumn("rn", F.row_number().over(window)).filter("rn = 1")
result = customer_total.join(top_payment, "customer_id").orderBy(F.desc("total_spend"), "customer_id").limit(5)
```

| customer_id | total_spend | payment_method |
|---|---:|---|
| CUST00064 | 558698000.0 | CREDIT_CARD |
| CUST00045 | 349651000.0 | E_WALLET |
| CUST00059 | 334550000.0 | CREDIT_CARD |
| CUST00013 | 309953000.0 | CREDIT_CARD |
| CUST00006 | 262945000.0 | CREDIT_CARD |

### Bài tập 2 - Tỷ trọng doanh thu danh mục bằng Window Function

**Tệp phải tạo:** `bai_tap_2_revenue_share.py`

Yêu cầu: dùng `retail` từ Lab 3, tính doanh thu theo `month`, `category_name`; thêm `percent_of_monthly_revenue`, làm tròn 2 chữ số; trong mỗi tháng tổng tỷ trọng xấp xỉ 100%.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

monthly_category = (retail
    .withColumn("month", F.date_format("order_time", "yyyy-MM"))
    .withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
    .groupBy("month", "category_name")
    .agg(F.sum("line_amount").alias("revenue")))

w = Window.partitionBy("month")
result = monthly_category.withColumn(
    "percent_of_monthly_revenue",
    F.round(F.col("revenue") / F.sum("revenue").over(w) * 100, 2),
).orderBy("month", F.desc("revenue"))
```

**Tự đối chiếu cho tháng `2026-07`**

| category_name | revenue | percent_of_monthly_revenue |
|---|---:|---:|
| Thuc pham | 449889000.0 | 28.20 |
| Dien tu | 328590000.0 | 20.59 |
| Sach | 245079000.0 | 15.36 |
| Thoi trang | 204112000.0 | 12.79 |
| My pham | 191018000.0 | 11.97 |
| Gia dung | 176805000.0 | 11.08 |

## 5. Nộp bài và rubric

### Checklist nộp bài

- [ ] `bai_tap_1_top_customers.py`
- [ ] `bai_tap_2_revenue_share.py`
- [ ] Thư mục kết quả Parquet của Lab 5
- [ ] Ảnh terminal có lệnh chạy và kết quả
- [ ] Ảnh Spark Master UI (`:8080`) và Spark Application UI (`:4040`) khi job chạy

| Tiêu chí | Điểm |
|---|---:|
| Schema rõ ràng, đọc và làm sạch đúng | 2.0 |
| Join đúng, có kiểm tra số dòng | 2.0 |
| Bài tập 1 đúng kết quả và sắp xếp | 2.0 |
| Bài tập 2 dùng Window Function đúng | 2.0 |
| Parquet, ảnh minh chứng và mã dễ đọc | 2.0 |

**Tổng: 10 điểm.**
