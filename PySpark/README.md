# BÀI THỰC HÀNH: XỬ LÝ DỮ LIỆU LỚN VỚI APACHE PYSPARK
**Học phần:** Nhập môn Dữ liệu lớn (Introduction to Big Data)  
**Chủ đề:** Xây dựng Data Pipeline Phân tán, Tối ưu hóa Truy vấn với Catalyst Optimizer và Triển khai trên Cụm Spark Standalone

---

## I. MỤC TIÊU SƯ PHẠM (Learning Outcomes)

Sau khi hoàn thành bài thực hành này, sinh viên có khả năng:

1. **Về kiến thức (Knowledge):**
   - Hiểu rõ kiến trúc vận hành phân tán của Apache Spark: vai trò của **Driver Coordinator**, **Cluster Master**, **Worker Nodes**, và các tiến trình thực thi **Executors**.
   - Phân tích được vòng đời tối ưu hóa truy vấn của **Catalyst Optimizer**: từ Kế hoạch Luận lý (Logical Plan) đến Kế hoạch Vật lý (Physical Plan).
   - Nắm vững bản chất của cơ chế **Lazy Evaluation**, ranh giới phân tách Stage (**Shuffle Boundary**), và các chiến lược liên kết dữ liệu (**Broadcast Hash Join** so với **Sort Merge Join**).

2. **Về kỹ năng lập trình dữ liệu (Engineering Skills):**
   - Xây dựng được pipeline ETL chuẩn công nghiệp với **Schema tường minh (Explicit Schema)**, loại bỏ hoàn toàn việc suy diễn kiểu tự động (`inferSchema`).
   - Thành thạo kỹ thuật kiểm soát chất lượng dữ liệu (Data Cleansing & Validation), xử lý ngoại lệ số học và giá trị khuyết thiếu (`NULL`) có chủ đích.
   - Làm chủ các phép toán biến đổi quan hệ: phép nối bảng đa nguồn không làm biến dạng số lượng dòng (Row Cardinality Verification), tính toán tổng hợp đa chiều theo thời gian và danh mục.
   - Tổ chức lưu trữ dữ liệu phân tích theo định dạng cột tối ưu (**Apache Parquet**) kết hợp kỹ thuật phân vùng (**Partitioning**).

3. **Về kỹ năng điều phối hệ thống (Operations & System Deployment):**
   - Sử dụng thành thạo công cụ điều phối `spark-submit` để đóng gói và nộp ứng dụng PySpark.
   - Chuyển đổi linh hoạt giữa môi trường kiểm thử đơn node (**`local[*]`**) và môi trường cụm phân tán thực tế (**Spark Standalone Cluster**).
   - Đọc hiểu, phân tích và gỡ lỗi kế hoạch thực thi thông qua phương thức `explain(mode="extended")` và giao diện giám sát Spark Web UI.

---

## II. KIẾN TRÚC HỆ THỐNG & CƠ SỞ LÝ THUYẾT

### 1. Kiến trúc Cụm Spark Standalone (Cluster Architecture)

Trong bài thực hành, hệ thống được triển khai theo mô hình cụm phân tán giả lập thông qua Docker Compose (định nghĩa tại `Spark/docker-compose.yml`):

```text
                               +----------------------------------+
                               |     SPARK DRIVER APPLICATION     |
                               |    (spark-submit / Container)    |
                               +-----------------+----------------+
                                                 |
                                     Đăng ký App & Xin tài nguyên
                                                 v
                               +----------------------------------+
                               |       SPARK MASTER NODE          |
                               |      (spark-master:7077)         |
                               |      Web UI: localhost:8080      |
                               +--------+----------------+--------+
                                        |                |
                       Phân bổ Executor |                | Phân bổ Executor
                                        v                v
                        +---------------+--+          +--+---------------+
                        |  SPARK WORKER 1  |          |  SPARK WORKER 2  |
                        | (spark-worker-1) |          | (spark-worker-2) |
                        | 1 Core / 640M RAM|          | 1 Core / 640M RAM|
                        +--------+---------+          +--------+---------+
                                 |                             |
                                 v                             v
                        +------------------+          +------------------+
                        |    EXECUTOR 0    |          |    EXECUTOR 1    |
                        |  (Task execution)|          |  (Task execution)|
                        +------------------+          +------------------+
                                 \                             /
                                  \                           /
                          +--------v-------------------------v--------+
                          |      SHARED STORAGE (DATA VOLUMES)        |
                          |  /opt/spark-data     /opt/spark-apps      |
                          +-------------------------------------------+
```

- **Spark Master (`spark-master:7077`)**: Tiếp nhận yêu cầu khởi tạo ứng dụng từ Driver, quản lý trạng thái tài nguyên toàn cụm (tổng số CPU Cores, tổng dung lượng RAM khả dụng).
- **Spark Workers (`spark-worker-1`, `spark-worker-2`)**: Quản lý tài nguyên phần cứng trên từng node vật lý/container; chịu trách nhiệm khởi tạo và giám sát vòng đời của các Executor. Mỗi Worker trong phòng lab được cấp phát cố định 1 Core và 640 MB RAM.
- **Executors**: Tiến trình JVM được cấp phát riêng cho từng ứng dụng, trực tiếp thực thi các Task phân tán và lưu trữ dữ liệu đệm trên bộ nhớ/ổ đĩa.
- **Shared Volumes**: Hai thư mục mount dùng chung giữa máy chủ (Host) và các container:
  - `./data` $\rightarrow$ `/opt/spark-data`: Nơi chứa dữ liệu nguồn và thư mục lưu trữ kết quả đầu ra.
  - `./jobs` $\rightarrow$ `/opt/spark-apps`: Nơi chứa mã nguồn kịch bản xử lý Python (`.py`).

### 2. Nguyên lý Tối ưu hóa của Catalyst Optimizer

Khi thực thi mã PySpark DataFrame API, Spark không tính toán dữ liệu ngay lập tức (**Lazy Evaluation**) mà chuyển đổi chuỗi phép biến đổi thành một Đồ thị có hướng không chu trình (**DAG - Directed Acyclic Graph**). Bộ tối ưu hóa Catalyst xử lý truy vấn qua 4 giai đoạn chính:

```text
[Mã DataFrame/SQL API]
         |
         v
[Unresolved Logical Plan]  (Chỉ chứa cú pháp, chưa kiểm tra cột/bảng có tồn tại không)
         |
         v (Catalog Resolution: đối chiếu metadata bảng và kiểu dữ liệu)
[Analyzed Logical Plan]
         |
         v (Optimization Rules: Predicate Pushdown, Column Pruning, Constant Folding)
[Optimized Logical Plan]
         |
         v (Cost Model & Physical Planning: chọn BroadcastHashJoin vs SortMergeJoin)
[Selected Physical Plan]
         |
         v (Tạo mã bytecode Java trực tiếp tại runtime - Tungsten Engine)
[RDD Code Generation & Execution]
```

Hai kỹ thuật tối ưu hóa cốt lõi cần quan sát trong bài:
1. **Predicate Pushdown (Đẩy vị từ xuống sâu)**: Đẩy điều kiện lọc dữ liệu (`WHERE` / `filter`) xuống trực tiếp tầng đọc file (`FileScan`). Điều này giúp lọc bỏ các bản ghi không hợp lệ ngay khi nạp từ đĩa vào bộ nhớ, giảm thiểu tối đa băng thông I/O và dung lượng bộ nhớ cần xử lý ở các tầng kế tiếp.
2. **Broadcast Hash Join (BHJ)**: Khi một trong hai bảng tham gia phép nối có kích thước nhỏ (mặc định dưới 10 MB), Spark Driver sẽ phát thanh (broadcast) toàn bộ bảng nhỏ này tới từng Executor. Mỗi Executor lưu bảng nhỏ vào một bảng băm (Hash Table) trong bộ nhớ và tiến hành so khớp với các partition của bảng lớn cục bộ. Kỹ thuật này triệt tiêu hoàn toàn giai đoạn **Shuffle** (không cần xáo trộn dữ liệu qua mạng), mang lại hiệu năng vượt trội.

---

## III. THIẾT KẾ DỮ LIỆU & QUY CÁCH SCHEMA (Data Specification)

Pipeline xử lý tích hợp 3 thực thể dữ liệu trong hệ thống bán lẻ RetailStream (tập dữ liệu `sample` theo quy ước `00_DATA_CONTRACT.md`):

### 1. Bảng đặc tả Schema tường minh

| Tệp dữ liệu | Bản ghi | Quy cách Schema tường minh (`StructType`) | Mục đích nghiệp vụ |
|---|---|---|---|
| `orders_sample.csv` | 150 | `order_id` (String, Not Null)<br>`customer_id` (String)<br>`order_time` (Timestamp, định dạng ISO-8601)<br>`status` (String)<br>`payment_method` (String)<br>`total_amount` (Double) | Chứa thông tin giao dịch đơn hàng và thời gian phát sinh giao dịch. |
| `order_items_sample.csv` | 380 | `order_id` (String, Not Null)<br>`product_id` (String, Not Null)<br>`quantity` (Integer)<br>`unit_price` (Double) | Chi tiết các dòng sản phẩm trong từng đơn hàng (1 đơn có từ 1–4 mặt hàng). |
| `products_sample.json` | 60 | `product_id` (String, Not Null)<br>`category_id` (String)<br>`category_name` (String)<br>`product_name` (String)<br>`brand` (String)<br>`price` (Double)<br>`attributes` (StructType: `color`, `warranty_months`)<br>`updated_at` (Timestamp) | Danh mục sản phẩm, có cấu trúc lồng nhau (`attributes`). |

### 2. Dị biệt dữ liệu có chủ đích & Kỹ thuật xử lý (Data Quality Handling)

Trong môi trường thực tế, dữ liệu từ các hệ thống giao dịch (OLTP) luôn tiềm ẩn lỗi. Tập dữ liệu thí nghiệm được thiết kế có chủ đích hai lỗi điển hình để sinh viên thực hành làm sạch:

1. **Dị biệt số học (Negative Values):**
   - *Hiện tượng:* Bản ghi cuối cùng trong `orders_sample.csv` (`order_id = "ORD000150"`) có giá trị bất thường: `total_amount = -1.0`.
   - *Giải pháp kỹ thuật:* Sử dụng vị từ lọc `F.col("total_amount") >= 0`. Phép lọc này loại bỏ đơn hàng lỗi khỏi tập `orders` (từ 150 dòng còn 149 dòng hợp lệ).
   - *Hệ quả trong phép nối:* Do sử dụng phép nối nội (**`INNER JOIN`**), 4 dòng sản phẩm liên kết với `ORD000150` trong `order_items_sample.csv` sẽ tự động bị loại bỏ (từ 380 dòng còn 376 dòng), ngăn chặn việc đưa doanh thu sai vào báo cáo tài chính.

2. **Khuyết thiếu thông tin phân loại (Missing Values / Nulls):**
   - *Hiện tượng:* 3 bản ghi trong `products_sample.json` có trường thương hiệu bị rỗng (`brand = null`).
   - *Giải pháp kỹ thuật:* Sử dụng hàm xử lý giá trị khuyết chuẩn học thuật:  
     `F.coalesce(F.col("brand"), F.lit("UNKNOWN"))`  
     Kỹ thuật này điền giá trị thế thân mặc định mà không làm mất thông tin của sản phẩm khi thực hiện các phép gom nhóm sau này.

---

## IV. CẤU TRÚC THƯ MỤC DỰ ÁN

```text
PySpark/
├── README.md                      # Giáo trình hướng dẫn thực hành chuyên sâu (tài liệu này)
├── process_retailstream.py        # Mã nguồn pipeline PySpark chính (thiết kế theo module chuẩn)
├── data/                          # Dữ liệu nguồn mẫu cục bộ (phục vụ đối chiếu và chạy native)
│   ├── orders_sample.csv          # 150 giao dịch (chứa 1 bản ghi lỗi âm tiền cố ý)
│   ├── order_items_sample.csv     # 380 dòng chi tiết mặt hàng
│   ├── products_sample.json       # 60 sản phẩm (chứa 3 bản ghi brand null và struct attributes)
│   └── manifest.json              # Checksum SHA-256 và chữ ký kiểm định toàn vẹn dữ liệu
├── output/                        # Thư mục lưu trữ kết quả đối chứng đã kiểm định thực tế
│   ├── local_mode/                # Kết quả kết xuất từ chế độ chạy local[*]
│   └── cluster_mode/              # Kết quả kết xuất từ chế độ chạy Spark Standalone cluster
├── scripts/                       # Bộ kịch bản tự động hóa quy trình chuẩn
│   ├── run-local.sh               # Kịch bản triển khai chế độ Local trên container Master
│   └── run-cluster.sh             # Kịch bản triển khai phân tán trên cụm Master - Workers
├── local_run_stage1.log           # Nhật ký thực thi chi tiết chế độ Local (minh chứng đối soát)
└── cluster_run_stage2.log         # Nhật ký thực thi chi tiết chế độ Cluster (minh chứng đối soát)
```

---

## V. QUY TRÌNH THỰC HÀNH TỪNG BƯỚC (Lab Execution Procedure)

Sinh viên thực hiện tuần tự theo các giai đoạn dưới đây. Toàn bộ các thao tác được thực hiện từ thư mục gốc của phân hệ `PySpark`:

```bash
cd "d:/school/Big Data/PySpark"
```

> [!WARNING]
> **Đặc thù môi trường Windows (Git Bash / MSYS2 Path Translation):**  
> Khi gọi trình thực thi nhị phân Windows (`docker.exe`) từ môi trường Git Bash, bộ thông dịch MSYS2 mặc định tự động biên dịch các đường dẫn bắt đầu bằng dấu gạch chéo `/` (ví dụ `/opt/spark/...`) thành đường dẫn Windows vật lý (`C:/Program Files/Git/opt/spark/...`).  
> **Hệ quả:** Container Linux không tồn tại ổ đĩa `C:`, dẫn đến lỗi:  
> `OCI runtime exec failed: ... exec: "C:/Program Files/Git/opt/spark/bin/spark-submit": no such file or directory`  
> **Giải pháp bắt buộc:** Luôn khai báo chỉ thị vô hiệu hóa biên dịch đường dẫn trước khi gọi lệnh:  
> `export MSYS_NO_PATHCONV=1`

---

### GIAI ĐOẠN 1: Kiểm thử Logic Nghiệp vụ trên Môi trường Local (`local[*]`)

*Mục đích:* Xác thực tính đúng đắn của logic chuyển đổi dữ liệu, kiểm tra tính khớp của Explicit Schema và xác nhận kết quả làm sạch trước khi nộp lên môi trường phân tán.

#### Lệnh thực thi:
```bash
# 1. Đồng bộ mã nguồn xử lý vào thư mục mount của Spark
mkdir -p ../Spark/jobs
cp process_retailstream.py ../Spark/jobs/process_retailstream.py

# 2. Vô hiệu hóa biến đổi đường dẫn trên Git Bash
export MSYS_NO_PATHCONV=1

# 3. Nộp job thực thi tại chỗ trong container spark-master
docker exec \
  -e SPARK_MASTER_URL="local[*]" \
  spark-master /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --conf spark.sql.shuffle.partitions=4 \
  --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py
```

#### Phân tích các tham số kỹ thuật:
- `--master "local[*]"`: Chỉ định Driver tạo một SparkContext cục bộ, sử dụng toàn bộ số luồng CPU có sẵn của máy trạm (không yêu cầu Cluster Manager phân bổ Worker).
- `--conf spark.sql.shuffle.partitions=4`: Cấu hình số lượng partition sau các giai đoạn Shuffle (mặc định của Spark là 200). Với dữ liệu mẫu nhỏ, việc ép về 4 partitions giúp tiết kiệm tài nguyên bộ nhớ và tránh chi phí quản lý task vụn vặt.
- `--driver-memory 512m`: Giới hạn ngưỡng RAM an toàn cho tiến trình JVM của Driver, nằm trong hạn mức `mem_limit: 1536m` của container Master.

#### Tiêu chí đánh giá kết quả Giai đoạn 1:
- Thuộc tính `applicationId` trong log có tiền tố dạng: `local-xxxxxxxxxxxxx`.
- Không xuất hiện bất kỳ cảnh báo schema mismatch hay ép kiểu dữ liệu thất bại nào.
- Dữ liệu kết xuất được ghi vào thư mục `/opt/spark-data/session10_pyspark/output/revenue_by_month_category`.

---

### GIAI ĐOẠN 2: Triển khai Phân tán trên Spark Standalone Cluster Thật

*Mục đích:* Đưa ứng dụng vào môi trường cụm đa tiến trình thực tế, quan sát cơ chế thương thảo tài nguyên giữa Master và Worker, và theo dõi việc phân bổ Task song song trên các Executor độc lập.

#### Lệnh thực thi:
```bash
# 1. Đảm bảo cụm Spark Standalone đang hoạt động ở trạng thái Healthy
(cd ../Spark && docker compose up -d)

# 2. Đồng bộ mã nguồn mới nhất vào thư mục mount
mkdir -p ../Spark/jobs
cp process_retailstream.py ../Spark/jobs/process_retailstream.py

# 3. Khai báo cờ môi trường Git Bash
export MSYS_NO_PATHCONV=1

# 4. Nộp job lên Spark Master điều phối
docker exec \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=4 \
  --executor-memory 512m \
  --executor-cores 1 \
  --total-executor-cores 2 \
  --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py
```

#### Phân tích các tham số phân bổ tài nguyên:
- `--master spark://spark-master:7077`: Trỏ tiến trình Driver kết nối trực tiếp với Master RPC của cụm Standalone.
- `--executor-memory 512m`: **Tham số trọng yếu.** Do mỗi container Worker chỉ có dung lượng khả dụng 640 MB (khai báo tại `docker-compose.yml`), nếu không cấu hình tham số này, Spark sẽ mặc định yêu cầu `1024m` cho mỗi Executor $\rightarrow$ Cụm không đủ tài nguyên đáp ứng $\rightarrow$ Ứng dụng rơi vào trạng thái bế tắc (`WAITING: Initial job has not accepted any resources`). Mức `512m` đảm bảo cấp phát thành công.
- `--executor-cores 1` và `--total-executor-cores 2`: Yêu cầu Master cấp phát tổng cộng 2 Cores trên toàn cụm, chia đều cho 2 Executor độc lập nằm trên 2 Worker riêng biệt (`spark-worker-1` và `spark-worker-2`).

#### Minh chứng phân bổ phân tán từ Nhật ký thực thi (Execution Evidence):
Sinh viên kiểm tra log thực thi trên terminal hoặc mở giao diện Spark Master UI tại [http://localhost:8080](http://localhost:8080) để xác nhận 2 Executor được cấp phát trên 2 địa chỉ IP mạng nội bộ khác nhau:

```text
INFO StandaloneSchedulerBackend: Granted access to 2 executors and 2 cpus
INFO StandaloneSchedulerBackend: Executor added: app-20260820054748-0001/0 on worker-...-172.20.0.3-36933 (172.20.0.3:36933) with 1 core(s)
INFO StandaloneSchedulerBackend: Executor added: app-20260820054748-0001/1 on worker-...-172.20.0.4-35459 (172.20.0.4:35459) with 1 core(s)
```
- `applicationId` mang định dạng định danh cụm: `app-YYYYMMDDHHMMSS-xxxx` (minh chứng chạy trên cụm thật, hoàn toàn khác biệt với `local-xxxx`).

---

### GIAI ĐOẠN 3: Tự động hóa Quy trình bằng Shell Script (Automation Practice)

Để đảm bảo tính tái lập (Reproducibility) và hỗ trợ kiểm thử tự động (CI/CD), hệ thống cung cấp 2 shell script chuẩn hóa tại thư mục `scripts/`:

| Tên script | Mục đích thực thi | Cú pháp chạy (từ thư mục `PySpark/`) |
|---|---|---|
| `run-local.sh` | Tự động đồng bộ code và chạy kiểm thử chế độ `local[*]` | `bash scripts/run-local.sh` |
| `run-cluster.sh` | Tự động bật cụm Spark, đồng bộ code và nộp job phân tán lên cụm | `bash scripts/run-cluster.sh` |

*Lưu ý đối với sinh viên sử dụng PowerShell:* Hãy mở cửa sổ **Git Bash** để gọi các lệnh trên, hoặc gọi gián tiếp qua cú pháp `bash scripts/run-cluster.sh`.

#### Dừng và giải phóng tài nguyên sau buổi học:
```bash
cd ../Spark && bash scripts/stop-cluster.sh
```

---

## VI. ĐỌC HIỂU & PHÂN TÍCH KẾ HOẠCH THỰC THI (Catalyst Query Plan Analysis)

Phương thức `revenue_df.explain(mode="extended")` cho phép sinh viên quan sát trực tiếp tư duy tối ưu hóa của bộ xử lý Catalyst. Dưới đây là phân tích chi tiết các khối lệnh vật lý cốt lõi trích xuất từ nhật ký thực tế:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Final Output Sort ==
   *(6) Sort [month#xx ASC NULLS FIRST, category_name#yy ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(month#xx ASC NULLS FIRST, category_name#yy ASC NULLS FIRST, 4)
      +- == Stage 2: Final Hash Aggregate ==
         *(5) HashAggregate(keys=[month#xx, category_name#yy], functions=[sum(line_amount#zz), count(distinct order_id#aa), count(1)])
         +- Exchange hashpartitioning(month#xx, category_name#yy, 4)
            +- == Stage 1: Partial Hash Aggregate ==
               *(4) HashAggregate(keys=[month#xx, category_name#yy], functions=[partial_sum(line_amount#zz), partial_count(distinct order_id#aa), partial_count(1)])
               +- == Stage 0: Join Tree ==
                  *(3) Project [quantity#b * unit_price#c AS line_amount#zz, date_format(order_time#d, yyyy-MM) AS month#xx, ...]
                  +- *(3) BroadcastHashJoin [product_id#a], [product_id#e], Inner, BuildRight
                     :- *(3) BroadcastHashJoin [order_id#f], [order_id#g], Inner, BuildRight
                     :  :- *(3) FileScan csv [order_id#f,product_id#a,quantity#b,unit_price#c] File: order_items_sample.csv
                     :  +- BroadcastExchange HashedRelationBroadcastMode(...)
                     :     +- *(1) Filter (isnotnull(total_amount#h) AND (total_amount#h >= 0.0))
                     :        +- *(1) FileScan csv [order_id#g,order_time#d,total_amount#h] File: orders_sample.csv
                     :           PushedFilters: [IsNotNull(total_amount), GreaterThanOrEqual(total_amount,0.0)]
                     +- BroadcastExchange HashedRelationBroadcastMode(...)
                        +- *(2) Project [product_id#e, category_name#yy, coalesce(brand#k, UNKNOWN) AS brand#m]
                           +- *(2) FileScan json [product_id#e,category_name#yy,brand#k] File: products_sample.json
```

### Các điểm nhấn kiến trúc cần bảo vệ trong bài thu hoạch:

1. **Vị trí của Predicate Pushdown (`PushedFilters`):**
   Quan sát thấy tại khối `FileScan csv ... orders_sample.csv`, điều kiện `GreaterThanOrEqual(total_amount,0.0)` được gán trực tiếp vào tầng đọc tệp. Spark không đọc dòng lỗi lên bộ nhớ rồi mới gọi `Filter`, mà loại bỏ dòng `ORD000150` ngay từ luồng đọc I/O.

2. **Cơ chế BroadcastHashJoin lồng nhau:**
   Cả hai phép nối (`order_items` với `orders`, và kết quả nối với `products`) đều được Catalyst lựa chọn chiến lược `BroadcastHashJoin [BuildRight]`. Do bảng `orders` (sau lọc còn 149 dòng) và `products` (60 dòng) đều có kích thước dưới 100 KB, Driver phát thanh trực tiếp hai bảng này sang các Executor. Quá trình nối hoàn tất hoàn toàn trong bộ nhớ nội bộ của Executor mà không phát sinh bất kỳ chi phí truyền tải mạng (Network Shuffle) nào tại tầng Join.

3. **Cơ chế HashAggregate hai pha (Two-phase Aggregation):**
   Quá trình tính tổng doanh thu và đếm đơn hàng được chia thành:
   - **Pha 1 (`partial_sum`, `partial_count`):** Mỗi Executor tự tính tổng tạm thời trên các partition dữ liệu mà mình nắm giữ. Điều này giúp nén hàng trăm dòng giao dịch xuống chỉ còn vài dòng nhóm.
   - **Pha 2 (`Exchange hashpartitioning`):** Spark phát sinh một ranh giới Shuffle để gom các khóa `(month, category_name)` giống nhau về cùng một Executor.
   - **Pha 3 (`Final HashAggregate`):** Executor tổng hợp các kết quả trung gian để đưa ra số liệu chính thức cuối cùng.

---

## VII. ĐỐI CHIẾU & ĐÁNH GIÁ KẾT QUẢ ĐỊNH LƯỢNG (Validation Results)

### 1. Bảng đối soát số liệu qua từng công đoạn xử lý

| Công đoạn xử lý | Tệp dữ liệu / Biến DataFrame | Số dòng trước xử lý | Số dòng sau xử lý | Sai lệch & Nguyên nhân nghiệp vụ |
|---|---|:---:|:---:|---|
| **Nạp dữ liệu (Read)** | `orders_df`<br>`order_items_df`<br>`products_df` | —<br>—<br>— | 150<br>380<br>60 | Nạp đủ 100% dữ liệu từ các tệp nguồn, cấu trúc khớp hoàn toàn với Explicit Schema. |
| **Làm sạch (Clean)** | `orders_clean_df` | 150 | 149 | **Giảm đúng 1 dòng**: Đơn hàng `ORD000150` có `total_amount = -1.0` bị loại bỏ. |
| | `products_clean_df` | 60 | 60 | **Không đổi số dòng**: 3 giá trị `brand = null` được chuyển thành `"UNKNOWN"`. |
| **Nối lần 1 (Join)** | `order_items` $\bowtie$ `orders` | 380 | 376 | **Giảm đúng 4 dòng**: 4 mặt hàng thuộc đơn hàng lỗi `ORD000150` bị loại bởi `INNER JOIN`. |
| **Nối lần 2 (Join)** | `joined` $\bowtie$ `products` | 376 | 376 | **Số dòng giữ nguyên tuyệt đối**: Xác nhận 100% `product_id` đều tồn tại và duy nhất (không bị bùng nổ dòng). |
| **Tổng hợp (Aggregate)** | `revenue_df` | 376 | **41** | Thu gọn thành 41 tổ hợp `(Tháng × Danh mục)`, trải dài từ tháng `2026-02` đến `2026-08`. |

### 2. Kiểm chứng tính toàn vẹn và Nhất quán Phân tán
- **Tổng doanh thu toàn hệ thống:** Đạt giá trị số học chính xác tuyệt đối:  
  $$\sum \text{revenue} = 9,650,372,000.0 \text{ VNĐ}$$
- **Kiểm định tính đồng nhất giữa Local và Cluster:**  
  Dữ liệu đầu ra định dạng Parquet tại `output/local_mode` và `output/cluster_mode` được đối soát chéo bằng hàm kiểm định `pandas.testing.assert_frame_equal()`. Kết quả trả về `True` (khớp chính xác 41/41 dòng, 100% các ô giá trị và kiểu dữ liệu), chứng minh sự độc lập của kết quả xử lý đối với kiến trúc thực thi hạ tầng.

### 3. Cấu trúc Phân vùng Lưu trữ (Partitioning Layout)
Kết quả phân tích được lưu trữ tại `output/.../revenue_by_month_category/` tuân thủ nghiêm ngặt chuẩn cấu trúc Hive Partitioning:
```text
revenue_by_month_category/
├── _SUCCESS                          # Tệp đánh dấu job ghi thành công toàn vẹn
├── month=2026-02/                    # Phân vùng dữ liệu tháng 2/2026
│   └── part-00000-...c000.snappy.parquet
├── month=2026-03/
│   └── part-00000-...c000.snappy.parquet
├── ...
└── month=2026-08/                    # Phân vùng dữ liệu tháng 8/2026
    └── part-00000-...c000.snappy.parquet
```

---

## VIII. CÁC BẪY KỸ THUẬT & HƯỚNG DẪN XỬ LÝ SỰ CỐ (Troubleshooting Guide)

| Hiện tượng lỗi | Nguyên nhân gốc rễ (Root Cause) | Giải pháp xử lý chuẩn kỹ thuật |
|---|---|---|
| `OCI runtime exec failed: ... exec: "C:/Program Files/Git/...": no such file or directory` | Trình biên dịch MSYS2 của Git Bash tự ý chuyển đổi đường dẫn Unix `/opt/...` sang đường dẫn Windows. | Thực thi lệnh `export MSYS_NO_PATHCONV=1` trong phiên làm việc hiện tại trước khi gọi `docker exec`. |
| Job phân tán bị treo vĩnh viễn ở trạng thái `WAITING`, không nhận Worker | Cấu hình `--executor-memory` vượt quá ngưỡng RAM khả dụng của Worker (mặc định Spark xin 1024m trong khi Worker chỉ có 640m). | Khai báo tường minh tham số tài nguyên: `--executor-memory 512m --executor-cores 1 --total-executor-cores 2`. |
| Lỗi `AnalysisException: Path does not exist: /opt/spark-data/...` | Thư mục dữ liệu chưa được đưa vào mount point của Spark container (`Spark/data`). | Đảm bảo dữ liệu đã được sao chép vào `Spark/data/session10_pyspark/` trước khi thực thi. |
| Container `spark-master` bị dừng đột ngột (Exit Code 137 / OOMKilled) | Chạy đồng thời nhiều job PySpark `local[*]` bên trong container Master làm vượt ngưỡng giới hạn RAM (`mem_limit: 1536m`). | Đóng các phiên làm việc cũ trước khi chạy job mới (`docker top spark-master`), hoặc chuyển sang chạy trên Cluster mode. |

---

## IX. CÂU HỎI THẢO LUẬN & BÀI TẬP MỞ RỘNG (Review Questions & Lab Assignments)

Sinh viên chuẩn bị câu trả lời cho các câu hỏi sau để bảo vệ bài thực hành:

1. **Về Chiến lược Nối dữ liệu (Join Strategies):**  
   *Câu hỏi:* Tại sao phép nối giữa `order_items` và `products` được Catalyst tự động chọn là `BroadcastHashJoin`? Trong kịch bản thực tế khi bảng `products` tăng trưởng lên 50 triệu bản ghi (dung lượng ~15 GB), Catalyst sẽ chuyển sang thuật toán nối nào? Phân tích chi phí mạng (Network I/O) của thuật toán đó.

2. **Về Thiết kế Lưu trữ Phân vùng (Partitioning Strategy):**  
   *Câu hỏi:* Trong bài tập, chúng ta thực hiện `partitionBy("month")`. Giả sử hệ thống nhận thêm yêu cầu phân vùng kết quả theo cả mã danh mục sản phẩm: `.partitionBy("month", "category_id")`. Hãy đánh giá ưu điểm và rủi ro kỹ thuật (đặc biệt là vấn đề **Small Files Problem**) khi áp dụng chiến lược phân vùng đa cấp này.

3. **Về Phân tích Kế hoạch Thực thi (Execution DAG):**  
   *Câu hỏi:* Dựa vào physical plan ở Mục VI, hãy giải thích tại sao giai đoạn tổng hợp dữ liệu lại xuất hiện 2 lần toán tử `HashAggregate` (Partial HashAggregate và Final HashAggregate)? Kỹ thuật này mang lại lợi ích gì cho hiệu năng đường truyền mạng so với việc chỉ gom nhóm một lần duy nhất tại đích?

4. **Bài tập nâng cao (Hands-on Challenge):**  
   *Yêu cầu:* Hãy hiệu chỉnh kịch bản `process_retailstream.py` để bổ sung chỉ số:  
   **Tỷ trọng doanh thu của từng danh mục sản phẩm so với tổng doanh thu của toàn bộ tháng đó (Percentage of Monthly Revenue)**.  
   *Gợi ý:* Sử dụng kỹ thuật hàm cửa sổ (**PySpark Window Function**) với `Window.partitionBy("month")` kết hợp cùng `F.sum("revenue").over(...)`.
