# PySpark — RetailStream: doanh thu theo tháng và danh mục
# PySpark RetailStream Lab

Buổi này học cách xử lý dữ liệu thật có lỗi/thiếu (dữ liệu "bẩn") bằng
PySpark — ví dụ số tiền âm không hợp lệ, trường bị thiếu (`null`) — thay vì
giả định dữ liệu luôn sạch. Buổi này cũng học cách tối ưu hiệu năng đọc dữ
liệu bằng cách khai báo rõ schema (kiểu dữ liệu từng cột) thay vì để Spark
tự đoán.
Tài liệu học chính: [THUC_HANH_PYSPARK.md](C:/Users/Vanmi/OneDrive/Documents/Giảng%20dạy/BigData/Slide%20chuẩn/10_pyspark/THUC_HANH_PYSPARK.md).

Cụ thể, job PySpark xử lý end-to-end ba nguồn dữ liệu RetailStream
(`orders`, `order_items`, `products`, mức `sample`): đọc với explicit
schema, làm sạch dữ liệu lỗi/thiếu có chủ ý, join ba bảng không nhân bản
dòng, tính doanh thu theo tháng và danh mục, ghi kết quả ra Parquet có
partition theo tháng.
Repo này cung cấp dữ liệu RetailStream và pipeline PySpark chạy được ở hai chế độ:

Đã kiểm thử thật (không chỉ đọc code rồi giả định) trên cả `local[*]` và
Spark Standalone cluster thật (1 master + 2 worker) — kết quả, log và số
liệu thật trích trong tài liệu này đều lấy từ các lần chạy đó.
| Chế độ | Lệnh | Khi dùng |
|---|---|---|
| Local Docker | `./scripts/run-local.sh` | Học lệnh Spark trên một container. |
| Spark Standalone | `./scripts/run-cluster.sh` | Quan sát Master và 2 Worker. |
| Local cài trực tiếp | `spark-submit --master "local[*]" process_retailstream.py` | Máy đã có Spark/Java. |

## 1. Cấu trúc thư mục
## 1. Cấu trúc

```text
PySpark/
├── README.md
├── process_retailstream.py       # file xử lý chính, có khối CONFIG tập trung
├── data/
│   ├── orders_sample.csv
│   ├── order_items_sample.csv
│   ├── products_sample.json
│   └── manifest.json
├── output/
│   ├── local_mode/revenue_by_month_category/     # kết quả chạy local[*]
│   └── cluster_mode/revenue_by_month_category/    # kết quả chạy trên cluster thật
├── scripts/
│   ├── run-local.sh              # chạy tự động chế độ local[*] trong container
│   └── run-cluster.sh            # khởi động cụm Spark và chạy trên Cluster thật
├── local_run_stage1.log          # log đầy đủ lần chạy local[*]
└── cluster_run_stage2.log        # log đầy đủ lần chạy trên Spark Standalone cluster
├── data/                         # orders, order_items, products
├── output/                       # kết quả minh họa đã sinh
├── process_retailstream.py       # pipeline đầy đủ
└── scripts/
    ├── run-local.sh
    └── run-cluster.sh
```

Hai thư mục `output/local_mode` và `output/cluster_mode` đã được đối chiếu
bằng pandas (`DataFrame.equals()` = `True`, 41 dòng khớp từng ô).
Pipeline có các bước: đọc schema rõ ràng, lọc đơn âm, thay brand rỗng, join 3 bảng, tính doanh thu tháng x danh mục, ghi Parquet theo `month`, và in `explain()`.

## 2. Dữ liệu đầu vào
## 2. Chuẩn bị

| File | Số dòng | Ghi chú |
|---|---|---|
| `orders_sample.csv` | 150 | `order_id` unique 100%; 1 dòng lỗi cố ý `total_amount = -1.0` |
| `order_items_sample.csv` | 380 | mọi `order_id`/`product_id` đều tồn tại trong `orders`/`products` |
| `products_sample.json` | 60 | `product_id` unique 100%; 3 sản phẩm `brand = null` cố ý; có struct lồng `attributes` (`color`, `warranty_months`) |
- Docker Desktop với Docker Compose v2.
- Git Bash hoặc PowerShell.
- Nếu chạy trực tiếp ngoài Docker: Java và Spark/PySpark tương thích.

Job khai báo `StructType`/`StructField` tường minh cho cả ba nguồn
(`ORDERS_SCHEMA`, `ORDER_ITEMS_SCHEMA`, `PRODUCTS_SCHEMA`), gọi
`spark.read.schema(...)`, **không** dùng `inferSchema` ở bất kỳ đâu.
Từ thư mục này, chạy bằng Git Bash:

Nếu không khai báo schema, mặc định Spark phải đọc hết dữ liệu một lượt
(`inferSchema=true`) chỉ để đoán kiểu dữ liệu của từng cột — chậm hơn vì tốn
thêm một lượt đọc, và có thể đoán sai kiểu nếu dữ liệu không đồng nhất (ví
dụ một cột số nhưng có vài dòng lẫn chữ). Đây là lý do khai báo schema tường
minh (explicit schema) tốt hơn khi đã biết trước cấu trúc dữ liệu.

## 3. Yêu cầu môi trường

Job đã được kiểm thử thật bên trong image `apache/spark:3.5.9-python3`
(Spark 3.5.9, PySpark 3.5.9, Python 3.10.12, Java OpenJDK 11.0.31, Scala
2.12.18), chạy qua `spark-submit`. Cần Docker + Docker Compose để dựng cụm
Spark Standalone dùng cho phần chạy cluster (cấu hình trong
`Spark/docker-compose.yml`, không cần sửa gì thêm).

## 4. Chạy trên local[\*]

Đứng từ thư mục `PySpark/`:

```bash
# 1) Đồng bộ file code vào thư mục jobs được mount của Spark
mkdir -p ../Spark/jobs
cp process_retailstream.py ../Spark/jobs/process_retailstream.py

# 2) Thực thi job (cần export MSYS_NO_PATHCONV=1 trên Git Bash Windows)
export MSYS_NO_PATHCONV=1
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.sql.shuffle.partitions=4 \
  --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py
./scripts/run-local.sh
./scripts/run-cluster.sh
```

Lưu ý: biến môi trường `-e SPARK_MASTER_URL="local[*]"` ở đây chỉ dùng để
job in ra log cho biết đang chạy ở chế độ nào (đọc bằng
`os.environ.get(...)` trong `process_retailstream.py`). Giá trị thật quyết
định job chạy local hay chạy trên cluster là flag `--master` truyền thẳng
cho `spark-submit`; đổi `-e SPARK_MASTER_URL` mà không đổi `--master` sẽ
không làm job chạy khác đi, chỉ làm log hiển thị sai.
Trong PowerShell, dùng Git Bash cho hai script `.sh`, hoặc chạy pipeline trực tiếp như phần dưới.

Kết quả mong đợi: một Spark Session khởi tạo ở chế độ `local[*]`, chạy xong
không lỗi, in ra số dòng đã xử lý ở mỗi bước và ghi kết quả Parquet ra
`output/local_mode/`.
## 3. Chạy trực tiếp `local[*]`

Kết quả thực tế: `spark.master (thuc te) = local[*]`, `applicationId` dạng
`local-1787204815347` — một JVM driver duy nhất, không có executor phân tán.

## 5. Chạy trên Spark Standalone cluster thật

Đứng từ thư mục `PySpark/`:

```bash
# 1) Khởi động cụm Spark nếu chưa chạy và copy code vào Spark/jobs
(cd ../Spark && docker compose up -d)
mkdir -p ../Spark/jobs
cp process_retailstream.py ../Spark/jobs/process_retailstream.py

# 2) Nộp job lên cụm Standalone (cần export MSYS_NO_PATHCONV=1 trên Git Bash Windows)
export MSYS_NO_PATHCONV=1
docker exec -e SPARK_MASTER_URL="spark://spark-master:7077" spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=4 \
  --executor-memory 512m \
  --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py
```powershell
cd 'D:\school\Big Data\PySpark'
$env:PYSPARK_DATA_DIR = "$PWD\data"
$env:PYSPARK_OUTPUT_DIR = "$PWD\output\student_local"
spark-submit --master "local[*]" .\process_retailstream.py
```

Xác nhận job chạy thật trên cluster (không phải `local[*]`):
Kết quả hợp lệ phải có:

```bash
curl -s http://localhost:8080/json/     # completedapps -> "cores": 2
```
| Kiểm tra | Giá trị |
|---|---:|
| Orders đầu vào | 150 |
| Orders hợp lệ | 149 |
| Order items đầu vào | 380 |
| Sau join orders | 376 |
| Sau join products | 376 |

`applicationId = app-20260820054748-0001`; log driver ghi rõ 2 executor được
cấp trên 2 địa chỉ IP worker khác nhau:
## 4. Chạy với Docker cluster

```text
Executor added: app-20260820054748-0001/0 on worker-...-172.20.0.3-36933 (172.20.0.3:36933) with 1 core(s)
Executor added: app-20260820054748-0001/1 on worker-...-172.20.0.4-35459 (172.20.0.4:35459) with 1 core(s)
```
`run-cluster.sh` tự khởi động các container trong `../Spark`, sao chép job rồi nộp nó tới `spark://spark-master:7077`.

## 6. Chuyển đổi giữa hai chế độ mà không sửa logic xử lý

Toàn bộ cấu hình nằm ở đầu `process_retailstream.py`:

```python
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "local[*]")
DATA_DIR = os.environ.get("PYSPARK_DATA_DIR", "/opt/spark-data/session10_pyspark")
OUTPUT_DIR = os.environ.get("PYSPARK_OUTPUT_DIR", os.path.join(DATA_DIR, "output"))
```

Chỉ cần đổi tham số `--master` khi gọi `spark-submit` (và tuỳ chọn biến môi
trường `SPARK_MASTER_URL` để log in đúng giá trị); phần logic đọc schema,
làm sạch, join, tính doanh thu, ghi Parquet, `explain()` giữ nguyên giữa hai
giai đoạn.

## 7. Chạy local[\*] trực tiếp trên máy cá nhân (không qua Docker)

Chưa được kiểm thử thật trong lần chạy validation gần nhất — dùng khi không
có Docker và chấp nhận tự kiểm tra tương thích Python/Java/PySpark:

```bash
export PYSPARK_DATA_DIR="./data"
export PYSPARK_OUTPUT_DIR="./output/native_local_mode"
python process_retailstream.py
export MSYS_NO_PATHCONV=1
./scripts/run-cluster.sh
```

## 8. Các lỗi/thiếu dữ liệu cố ý và cách job xử lý
- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040 khi job đang chạy

- `orders_sample.csv`: dòng cuối (`ORD000150`) có `total_amount = -1.0` →
  job lọc bỏ bằng `orders_df.filter(F.col("total_amount") >= 0)`, kéo theo
  4 dòng `order_items` của đơn này bị loại khỏi kết quả join (INNER JOIN —
  chỉ giữ lại các dòng có khóa khớp ở CẢ HAI bảng, dòng không khớp bị loại
  bỏ hoàn toàn; khác với LEFT JOIN vốn giữ lại toàn bộ dòng bên trái kể cả
  khi không khớp).
- `products_sample.json`: 3 sản phẩm có `brand = null` → job thay bằng chuỗi
  `"UNKNOWN"` bằng `F.coalesce(F.col("brand"), F.lit("UNKNOWN"))` (không ảnh
  hưởng gom nhóm vì gom theo `category_name`, không theo `brand`).
Mỗi Worker được cấp 1 core và 640 MB RAM. Script dùng `--executor-memory 512m`, `--executor-cores 1`, `--total-executor-cores 2` để cluster đủ tài nguyên.

## 9. Kết quả thực tế
## 5. Dữ liệu và đầu ra

- `orders`: 150 dòng → 149 dòng sau làm sạch.
- `order_items`: 380 dòng → 376 dòng sau join (mất đúng 4 dòng của đơn lỗi,
  không nhân bản).
- `products`: 60 dòng, `product_id` unique.
- Doanh thu tổng hợp: **41 dòng** (tháng × danh mục), từ `2026-02` đến
  `2026-08`, tổng doanh thu toàn bộ = `9650372000.0` — giống hệt nhau giữa
  `local[*]` và cluster (`DataFrame.equals() == True` khi so sánh hai thư
  mục Parquet đầu ra bằng pandas).
- Output Parquet có 7 thư mục con `month=2026-02` … `month=2026-08`, mỗi
  thư mục có file `.parquet`, cộng một file `_SUCCESS` ở thư mục gốc.
| Tệp | Nội dung |
|---|---|
| `orders_sample.csv` | 150 đơn hàng; có 1 đơn âm để thực hành làm sạch |
| `order_items_sample.csv` | 380 dòng chi tiết đơn hàng |
| `products_sample.json` | 60 sản phẩm; có 3 brand thiếu |
| `output/.../revenue_by_month_category` | Parquet partition theo `month` |

## 10. Đọc logical/physical plan
Không xóa `_SUCCESS` hoặc các thư mục `month=...` khi nộp kết quả Parquet.

Trước khi đọc log plan thật, dưới đây là giải thích ngắn gọn từng khái niệm
sẽ xuất hiện:
## 6. Bài tập bắt buộc

- **Catalyst Optimizer** là bộ tối ưu truy vấn của Spark, tự động biến đổi
  kế hoạch thực thi ban đầu (logical plan — "làm gì") thành kế hoạch vật lý
  (physical plan — "làm như thế nào cụ thể") hiệu quả hơn, tương tự query
  optimizer trong một hệ quản trị CSDL quan hệ.
- **Predicate pushdown**: đẩy điều kiện lọc (`filter`) xuống càng sớm càng
  tốt, gần ngay bước đọc dữ liệu, để giảm số dòng cần xử lý ở các bước sau.
- **BroadcastHashJoin**: khi một bảng đủ nhỏ (mặc định dưới 10 MB), Spark
  gửi (broadcast) toàn bộ bảng đó tới mọi executor, thay vì phải xáo trộn
  (shuffle) dữ liệu của bảng lớn hơn qua mạng — nhanh hơn nhiều so với join
  thông thường.
- **Exchange**: bước Spark phải xáo trộn (shuffle) dữ liệu giữa các executor
  qua mạng, thường là bước tốn tài nguyên nhất trong physical plan.
- **HashAggregate**: bước tính toán gộp nhóm (như `groupBy().agg()`),
  thường có nhiều pha — gộp sơ bộ (partial) tại từng partition, rồi gộp lần
  cuối (merge/final) sau khi dữ liệu đã được shuffle theo đúng key.
Xem phần 4 của Lab Guide. Cần nộp hai script:

`revenue_df.explain(mode="extended")` (log đầy đủ trong
`local_run_stage1.log`, dòng 159–229) cho thấy các khái niệm trên xuất hiện
trong plan thật của job:
- `bai_tap_1_top_customers.py`: Top 5 khách hàng và phương thức thanh toán chính.
- `bai_tap_2_revenue_share.py`: Tỷ trọng doanh thu danh mục theo tháng bằng Window Function.

- **Predicate pushdown**: điều kiện `total_amount >= 0.0 AND
  isnotnull(order_id)` xuất hiện ngay tại `FileScan csv orders_sample.csv`
  thay vì ở một `Filter` riêng sau khi đọc toàn bộ file.
- **BroadcastHashJoin** cho cả hai phép join: `orders` và `products` sau khi
  lọc đủ nhỏ để broadcast toàn bộ sang mỗi executor thay vì shuffle bảng
  `order_items` lớn hơn.
- 4 điểm `Exchange` (shuffle): 3 phục vụ `HashAggregate` nhiều pha (partial →
  merge → final) và 1 cho `Sort` trước khi trả kết quả.
## 7. Lỗi thường gặp

## 11. Dữ liệu và job dùng trong cụm Docker

Container Spark (`spark-master`, `spark-worker-1`, `spark-worker-2`) chỉ đọc
được đường dẫn đã mount sẵn trong `Spark/docker-compose.yml`
(`./data -> /opt/spark-data`, `./jobs -> /opt/spark-apps`, mount vào cả ba
container). Vì vậy bản dùng để chạy thật trên cụm (giống hệt nội dung của
`PySpark/process_retailstream.py` và `PySpark/data/*`) được đặt thêm ở:

- `Spark/data/session10_pyspark/{orders_sample.csv, order_items_sample.csv, products_sample.json}`
- `Spark/jobs/process_retailstream.py`

`PySpark/` là bản chính thức để đọc/nộp bài; `Spark/data/session10_pyspark/`
và `Spark/jobs/process_retailstream.py` chỉ là bản thực thi giúp cả ba
container cùng đọc được. Không có file nào trong `Spark/` bị ghi đè, không
sửa `Spark/docker-compose.yml`.

## 12. Hướng dẫn chạy nhanh bằng Script (Khuyến nghị)

Thư mục `PySpark/scripts/` cung cấp sẵn 2 script tự động hóa cho cả hai giai đoạn thực thi (local và cluster).

### Môi trường khuyến nghị:
- **Git Bash** (trên Windows) hoặc Terminal Linux/macOS.
- Nếu dùng **PowerShell**: hãy gọi qua Git Bash bằng `bash scripts/<tên_script>.sh`.

### Thư mục làm việc (Working Directory):
Mở terminal và chuyển vào thư mục `PySpark`:
```bash
cd "d:/school/Big Data/PySpark"
```

> [!NOTE]
> **Về đường dẫn dữ liệu & code khi chạy Docker:**
> Khi chạy trong container Spark, các script sẽ tự động đồng bộ file mã nguồn `process_retailstream.py` sang thư mục mount `../Spark/jobs/`. Dữ liệu được đọc từ thư mục mount `../Spark/data/session10_pyspark/` đã được chuẩn bị sẵn.

### Thứ tự thực hiện:

#### Bước 1: Chạy thử nghiệm chế độ Local (`local[*]`)
Chạy job PySpark ngay trong container `spark-master` để kiểm tra logic tính toán:
```bash
bash scripts/run-local.sh
```
*Lệnh này làm gì:*
1. Copy `process_retailstream.py` sang `../Spark/jobs/`.
2. Dùng `docker exec` gọi `spark-submit` với cờ `--master "local[*]"` chạy trong container `spark-master`.
3. In kết quả tính toán doanh thu theo tháng và danh mục ra màn hình terminal.

#### Bước 2: Chạy trên cụm Spark Standalone thật (Cluster Mode)
Chạy job phân tán trên cụm gồm Master và 2 Worker:
```bash
bash scripts/run-cluster.sh
```
*Lệnh này làm gì:*
1. Tự động kiểm tra và khởi động cụm Spark (`cd ../Spark && docker compose up -d`) nếu cụm chưa chạy.
2. Đồng bộ `process_retailstream.py` vào `../Spark/jobs/`.
3. Nộp job lên cụm với Master URL `spark://spark-master:7077`, tự động cấu hình các tham số RAM và Cores tối ưu (`--executor-memory 512m`, `--executor-cores 1`, `--total-executor-cores 2`, `--driver-memory 512m`).
4. In kết quả ra terminal và ghi dữ liệu kết quả phân tán vào thư mục output.

#### Bước 3: Đối chiếu kết quả 2 chế độ
Kết quả của cả 2 lần chạy cho ra 41 dòng dữ liệu hoàn toàn trùng khớp từng ô (xem so sánh chi tiết ở mục 6).

#### Bước 4: Dừng cụm khi kết thúc
Khi không còn sử dụng cụm Spark:
```bash
cd ../Spark && bash scripts/stop-cluster.sh
```

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích đơn giản |
| Triệu chứng | Cách xử lý |
|---|---|
| **Container** | Một "hộp" chạy phần mềm biệt lập, giống một máy ảo thu nhỏ. Một cụm (Hadoop/Spark/Kafka) trong dự án này gồm nhiều container chạy trên CÙNG một máy thật, giả lập nhiều máy. |
| **Docker Compose** | Công cụ mô tả "cần bao nhiêu container, cấu hình ra sao" trong 1 file (`docker-compose.yml`), rồi bật/tắt tất cả cùng lúc bằng 1 lệnh. |
| **Image / ghim version** | "Bản cài đặt đóng gói sẵn" của một phần mềm (ví dụ `mongo:8.0`). "Ghim version" nghĩa là chỉ rõ đúng phiên bản (`8.0`) thay vì `latest` (bản mới nhất, có thể đổi bất cứ lúc nào) — để lần sau chạy lại vẫn ra kết quả giống hệt. |
| **Explicit schema / schema tường minh** | Khai báo rõ tên cột và kiểu dữ liệu (ví dụ "cột `price` là số thập phân") thay vì để phần mềm tự đoán — tránh đoán sai. |
| **Spark Standalone Cluster** | Cụm Spark tự quản lý (không cần YARN/Kubernetes), gồm 1 Master (điều phối) + nhiều Worker (thực thi việc). |
| **Driver** | Chương trình chính điều khiển toàn bộ job Spark (chạy trên máy nộp job). |
| **Executor** | Tiến trình thực sự chạy trên từng Worker để xử lý dữ liệu — nếu thấy có executor chạy trên nhiều Worker khác nhau, nghĩa là job thật sự chạy phân tán (không phải chạy 1 mình trên máy local). |
| **local[\*]** | Chế độ chạy Spark ngay trên 1 máy (không dùng cluster), dùng để học/thử nhanh. `local[*]` nghĩa là dùng tất cả CPU core có sẵn của máy đó. |
| **Partition** | Một "phần" của dữ liệu được xử lý độc lập — dữ liệu càng được chia thành nhiều partition thì càng chạy song song được nhiều. |
| **Shuffle** (trong Spark) | Bước tốn kém khi phải trộn/chuyển dữ liệu giữa các máy (ví dụ khi `join` hoặc `groupBy` dữ liệu nằm rải rác). |
| **explain()** | Lệnh xem "Spark định làm gì" (kế hoạch thực thi) trước/sau khi tối ưu, giúp hiểu vì sao 1 câu lệnh chạy nhanh/chậm. |
| **spark-submit** | Lệnh dùng để "nộp" 1 chương trình Spark cho cluster chạy. |
| `C:/Program Files/Git/opt/spark/...` | Trong Git Bash chạy `export MSYS_NO_PATHCONV=1`. |
| `WAITING: Initial job has not accepted any resources` | Dùng `run-cluster.sh`, không tăng `--executor-memory` vượt 512m. |
| Không thấy `:4040` | Application UI chỉ tồn tại trong thời gian driver còn chạy. |
| Lỗi không tìm thấy dữ liệu khi chạy local | Đặt `PYSPARK_DATA_DIR` và `PYSPARK_OUTPUT_DIR` như mục 3. |
