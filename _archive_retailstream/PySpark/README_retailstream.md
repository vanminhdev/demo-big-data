# PySpark RetailStream: Xử lý và Phân tích Dữ liệu Lớn

Tài liệu hướng dẫn và mã nguồn thực hành PySpark DataFrame thuộc học phần **Dữ liệu lớn**.

> 📖 **HƯỚNG DẪN THỰC HÀNH CHI TIẾT:** Sinh viên mở tệp **[`THUC_HANH_PYSPARK.md`](./THUC_HANH_PYSPARK.md)** để thực hiện tuần tự 6 bài Lab và 2 Bài tập thực hành bắt buộc kèm tiêu chí đánh giá (Rubric).

---

## 1. Cấu trúc thư mục

```text
PySpark/
├── THUC_HANH_PYSPARK.md          # Cẩm nang thực hành chi tiết (Lab 1 -> 6, Bài tập 1 & 2)
├── README.md                     # Tài liệu tổng quan này
├── process_retailstream.py       # Pipeline chính: nạp 3 bảng, làm sạch, join, tính doanh thu, ghi Parquet
├── bai_tap_1_top_customers.py    # Bài tập 1: Top 5 khách hàng chi tiêu nhiều nhất & phương thức thanh toán
├── bai_tap_2_revenue_share.py    # Bài tập 2: Tỷ trọng doanh thu danh mục theo tháng (Window Function)
├── data/                         # Dữ liệu mẫu RetailStream (chuẩn Data Contract)
│   ├── orders_sample.csv         # 150 dòng, chứa 1 dòng lỗi cố ý (total_amount = -1.0)
│   ├── order_items_sample.csv    # 380 dòng chi tiết đơn hàng
│   ├── products_sample.json      # 60 sản phẩm (chứa 3 sản phẩm brand = null)
│   └── manifest.json             # Metadata kiểm tra tính toàn vẹn
├── scripts/
│   ├── run-local.sh              # Script chạy pipeline chế độ local[*] trong Docker
│   └── run-cluster.sh            # Script khởi động cụm và chạy trên cụm Spark Standalone
└── output/                       # Thư mục lưu trữ kết quả Parquet được phân vùng theo tháng (partitionBy month)
    ├── local_mode/revenue_by_month_category/
    └── cluster_mode/revenue_by_month_category/
```

---

## 2. Môi trường thực thi chuẩn hóa (Docker)

Toàn bộ môi trường thực hành đã được cài đặt sẵn bên trong Docker container sử dụng ảnh chính thức `apache/spark:3.5.9-python3` (Spark 3.5.9, PySpark 3.5.9, Python 3.10.12, Java OpenJDK 11, Scala 2.12.18).

Sinh viên **không cần cài đặt Java hay Spark trên hệ điều hành Windows** để tránh xung đột phiên bản.

### Bước 1: Khởi động cụm Spark

Mở terminal tại thư mục `Spark`:

```bash
cd "D:/school/Big Data/Spark"
docker compose up -d
```

Kiểm tra:
- Spark Master Web UI: `http://localhost:8080` (trạng thái 2 Worker: `ALIVE`).

### Bước 2: Vào PySpark Shell tương tác (Dùng cho Lab 1)

```bash
docker exec -it spark-master /opt/spark/bin/pyspark --master "local[*]"
```

*(Gõ code trực tiếp, thoát bằng `exit()` hoặc `Ctrl + D`).*

### Bước 3: Chạy kịch bản tự động

#### Cách 1: Sử dụng Git Bash (Khuyên dùng trên Windows)

```bash
cd "/d/school/Big Data/PySpark"
export MSYS_NO_PATHCONV=1

# 1. Chạy pipeline xử lý chính:
./scripts/run-local.sh       # Chạy chế độ local[*]
./scripts/run-cluster.sh     # Chạy trên cụm Spark Standalone (1 Master + 2 Workers)

# 2. Chạy Bài tập 1:
cp bai_tap_1_top_customers.py ../Spark/jobs/
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit \
  --master "local[*]" /opt/spark-apps/bai_tap_1_top_customers.py

# 3. Chạy Bài tập 2:
cp bai_tap_2_revenue_share.py ../Spark/jobs/
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit \
  --master "local[*]" /opt/spark-apps/bai_tap_2_revenue_share.py
```

#### Cách 2: Sử dụng Windows PowerShell

```powershell
cd 'D:\school\Big Data\PySpark'

# Đồng bộ file code vào thư mục Spark/jobs để container nhìn thấy:
Copy-Item process_retailstream.py ..\Spark\jobs\ -Force
Copy-Item bai_tap_1_top_customers.py ..\Spark\jobs\ -Force
Copy-Item bai_tap_2_revenue_share.py ..\Spark\jobs\ -Force

# Chạy pipeline chính:
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/process_retailstream.py

# Chạy Bài tập 1:
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/bai_tap_1_top_customers.py

# Chạy Bài tập 2:
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit --master "local[*]" /opt/spark-apps/bai_tap_2_revenue_share.py
```

---

## 3. Nội dung cốt lõi của Pipeline

1. **Explicit Schema:** Khai báo cấu trúc bảng tường minh bằng `StructType`/`StructField`, loại bỏ chi phí suy diễn kiểu dữ liệu `inferSchema`.
2. **Làm sạch có kiểm soát:**
   - Lọc bỏ 1 đơn hàng lỗi âm (`total_amount = -1.0`), giữ lại 149/150 đơn hợp lệ.
   - Điền giá trị mặc định cho 3 sản phẩm có `brand = null` thành `"UNKNOWN"` bằng `F.coalesce`.
3. **Join an toàn 3 bảng:**
   - Nối `order_items` (380 dòng) với `orders` hợp lệ $\rightarrow$ 376 dòng (loại 4 items của đơn lỗi).
   - Nối tiếp với `products` (60 dòng duy nhất) $\rightarrow$ giữ nguyên 376 dòng (không nhân bản dòng).
4. **Tính toán doanh thu:**
   - Tạo cột doanh thu từng món: `line_amount = quantity * unit_price`.
   - Trích xuất tháng: `month = date_format(order_time, 'yyyy-MM')`.
   - Tổng hợp doanh thu theo Tháng $\times$ Danh mục sản phẩm.
5. **Lưu trữ tối ưu với Apache Parquet:**
   - Ghi dữ liệu nén Snappy, phân vùng theo tháng: `partitionBy("month")`.
   - Kích hoạt tối ưu hóa **Column Projection** (chỉ nạp cột cần) và **Partition Pruning** (chỉ đọc đúng thư mục tháng cần truy vấn).
6. **Kiểm tra kế hoạch thực thi:**
   - Dùng `df.explain(mode="extended")` để phân tích `FileScan`, `PushedFilters`, `BroadcastHashJoin` và các điểm shuffle `Exchange`.

---

## 4. Báo cáo nộp bài

Xem chi tiết danh mục hồ sơ nộp bài và tiêu chí chấm điểm tại [Mục 5 trong `THUC_HANH_PYSPARK.md`](./THUC_HANH_PYSPARK.md#5-nộp-bài-và-rubric).
