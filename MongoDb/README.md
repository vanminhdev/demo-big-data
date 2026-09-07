# MongoDB — CRUD, Index, Aggregation trên dữ liệu RetailStream

Đến buổi này, dữ liệu RetailStream (`products`, `orders`, `order_items`) đã
quen thuộc qua các bảng có cấu trúc cố định (dạng CSV/JSON theo schema).
MongoDB đại diện cho một hướng khác trong lưu trữ dữ liệu: **NoSQL hướng
tài liệu (document)** — mỗi bản ghi là một **document** dạng JSON (lưu bên
trong dưới dạng nhị phân **BSON**), nhóm lại thành một **collection** (tương
đương khái niệm "bảng" nhưng không ép buộc mọi document phải cùng cấu trúc
cột). Bài thực hành này dùng đúng bộ dữ liệu RetailStream chung để: thao tác
CRUD cơ bản, tạo index tăng tốc truy vấn, viết **aggregation pipeline** (một
chuỗi bước biến đổi/gộp dữ liệu, giống `GROUP BY` nhiều bước trong SQL) tính
doanh thu theo danh mục và tháng, xem kế hoạch thực thi truy vấn bằng
`explain()`, và so sánh hai cách mô hình hoá dữ liệu quan hệ trong MongoDB:
**embedding** (nhúng dữ liệu con vào ngay bên trong document cha) và
**referencing** (tách thành collection riêng, liên kết qua một trường khoá
giống khoá ngoại).

## 1. Yêu cầu môi trường

| Mục | Giá trị đã kiểm thử |
|---|---|
| Docker Desktop / Docker Engine | Docker 29.7.2, Docker Compose v5.4.0 |
| MongoDB (image) | `mongo:8.0` (phiên bản thật khi chạy: 8.0.29) — **ghim version**, không dùng `latest` |
| Cổng cần trống trên host | `27017` |

## 2. Cấu trúc thư mục

```text
MongoDb/
├── README.md
├── docker-compose.yml          # 1 service mongodb, image mongo:8.0, volume ./data
├── demo.mongodb.js             # toàn bộ script demo: CRUD, index, aggregation, explain, embedding vs referencing
├── demo_output.txt             # log kết quả chạy thật của demo.mongodb.js
├── data_import/                # dữ liệu JSON đã chuẩn bị sẵn để mongoimport
└── scripts/
    └── prepare_import_data.py  # sinh data_import/ từ dữ liệu RetailStream chung (00_shared_data/)
```

## 3. Khởi động MongoDB

### Môi trường & Thư mục làm việc:
- Mở terminal và chuyển vào thư mục `MongoDb`:
```bash
cd "d:/school/Big Data/MongoDb"
```
- Nếu dùng **Git Bash trên Windows**, hãy chạy trước lệnh sau để tránh lỗi Git Bash tự ý chuyển đổi đường dẫn Unix `/tmp/...`:
```bash
export MSYS_NO_PATHCONV=1
```

### Thứ tự thực hiện:

#### Bước 1: Khởi động container MongoDB
```bash
docker compose up -d
docker exec mongodb mongosh --quiet --eval "db.version()"
```
*Lệnh này làm gì:* Khởi chạy container `mongodb` (phiên bản MongoDB 8.0) và kiểm tra kết nối với engine cơ sở dữ liệu. Kết quả mong đợi: in ra phiên bản `8.0.29`.

#### Bước 2: Chuẩn bị và chuyển đổi dữ liệu
Dữ liệu được lấy từ bộ RetailStream dùng chung (`00_shared_data/sample/`). Chạy script chuyển đổi mô hình:
```bash
python scripts/prepare_import_data.py
```
*Lệnh này làm gì:* Đọc `orders_sample.csv` và `order_items_sample.csv`, ép kiểu dữ liệu và tạo ra 3 tệp mô hình hóa dữ liệu trong `data_import/`:
- `orders_embedded.json` — nhúng mảng chi tiết `items` trực tiếp vào từng đơn hàng (biến thể embedding).
- `orders_ref.json` — đơn hàng riêng lẻ không kèm chi tiết (biến thể referencing).
- `order_items.json` — từng dòng sản phẩm trong đơn, liên kết qua `order_id`.
*(Lưu ý: Tệp `products.json` chứa 60 sản phẩm được lấy trực tiếp từ `00_shared_data/sample/products_sample.json` đã có sẵn trong `data_import/`).*

#### Bước 3: Nạp dữ liệu vào MongoDB bằng `mongoimport`
Copy các tệp dữ liệu vào container và nạp vào database `retailstream`:
```bash
# 1. Sao chép 4 file dữ liệu vào container
docker cp data_import/products.json mongodb:/tmp/products.json
docker cp data_import/orders_embedded.json mongodb:/tmp/orders_embedded.json
docker cp data_import/orders_ref.json mongodb:/tmp/orders_ref.json
docker cp data_import/order_items.json mongodb:/tmp/order_items.json

# 2. Nạp vào từng collection tương ứng
docker exec mongodb mongoimport --db retailstream --collection products --file /tmp/products.json --jsonArray
docker exec mongodb mongoimport --db retailstream --collection orders_embedded --file /tmp/orders_embedded.json --jsonArray
docker exec mongodb mongoimport --db retailstream --collection orders --file /tmp/orders_ref.json --jsonArray
docker exec mongodb mongoimport --db retailstream --collection order_items --file /tmp/order_items.json --jsonArray
```
*Lệnh này làm gì:* `mongoimport` nạp hàng loạt dữ liệu dạng JSON vào database. Cờ `--jsonArray` báo cho MongoDB biết tệp là mảng JSON chứa nhiều document. Kết quả mong đợi: nhập thành công `60 / 150 / 150 / 380` document, 0 lỗi.

#### Bước 4: Chạy toàn bộ kịch bản demo
Chạy script tự động thực thi toàn bộ các thao tác nghiệp vụ (CRUD, compound index, aggregation pipeline, explain plan phân tích hiệu năng, và so sánh embedding vs referencing):
```bash
docker cp demo.mongodb.js mongodb:/tmp/demo.mongodb.js
docker exec mongodb mongosh retailstream /tmp/demo.mongodb.js
```
*(Log chi tiết đầy đủ của một lần chạy chuẩn được lưu tại `demo_output.txt`).*

#### Bước 5: Dọn dẹp khi kết thúc
```bash
docker compose down
```
*(Nếu muốn xóa sạch toàn bộ dữ liệu database để làm lại từ đầu: chạy `docker compose down`, sau đó xóa sạch các tệp nhị phân trong thư mục `./data/*`).*

## 6. CRUD cơ bản (V01)

```js
// Create
db.products.insertOne({ product_id: "PROD99999", ... });
// Read
db.products.findOne({ product_id: "PROD99999" });
// Update
db.products.updateOne({ product_id: "PROD99999" }, { $set: { price: 179000 } });
// Delete
db.products.deleteOne({ product_id: "PROD99999" });
```

**Kết quả mong đợi (trích từ `demo_output.txt`):**

```text
Update -> { acknowledged: true, matchedCount: 1, modifiedCount: 1 }
Delete -> { acknowledged: true, deletedCount: 1 }
```

## 7. Tạo index (V02)

Truy vấn theo thời gian đơn hàng và trạng thái là truy vấn phổ biến nhất
trên `orders` (ví dụ: tính doanh thu các đơn đã thanh toán trong một khoảng
thời gian) — tạo một **compound index** (index gộp nhiều trường) trên
`(order_time, status)` để tăng tốc:

```js
db.orders.createIndex({ order_time: 1, status: 1 });
```

**Kết quả mong đợi:** index tên `order_time_1_status_1` được tạo, xác nhận
qua `db.orders.getIndexes()`.

## 8. Aggregation pipeline: doanh thu theo danh mục và tháng (V03)

Pipeline gồm các bước: `$lookup` (nối dữ liệu giữa các collection, giống
`JOIN` trong SQL) `order_items` với `orders` để lấy trạng thái đơn, lọc các
đơn `PAID`/`SHIPPED`, `$lookup` tiếp với `products` để lấy `category_name`,
rồi `$group` (gộp và tính tổng, giống `GROUP BY`) theo `(category, month)`:

```js
db.order_items.aggregate([
  { $lookup: { from: "orders", localField: "order_id", foreignField: "order_id", as: "order" } },
  { $unwind: "$order" },
  { $match: { "order.status": { $in: ["PAID", "SHIPPED"] } } },
  { $lookup: { from: "products", localField: "product_id", foreignField: "product_id", as: "product" } },
  { $unwind: "$product" },
  { $project: {
      category_name: "$product.category_name",
      month: { $substrCP: ["$order.order_time", 0, 7] },
      line_total: { $multiply: ["$quantity", "$unit_price"] },
  } },
  { $group: {
      _id: { category: "$category_name", month: "$month" },
      revenue: { $sum: "$line_total" },
      order_item_count: { $sum: 1 },
  } },
  { $sort: { "_id.month": 1, revenue: -1 } },
]);
```

`$unwind` tách một mảng kết quả `$lookup` (luôn trả về mảng, kể cả khi chỉ
khớp 1 document) thành từng document phẳng để các bước sau xử lý dễ dàng.

**Kết quả mong đợi (trích thật từ `demo_output.txt`):**

```text
{ category: 'Thuc pham', month: '2026-02' } revenue: 155,063,000  (4 items)
{ category: 'My pham',   month: '2026-02' } revenue: 84,634,000   (4 items)
{ category: 'Gia dung',  month: '2026-02' } revenue: 54,244,000   (3 items)
...
{ category: 'Thuc pham', month: '2026-04' } revenue: 586,942,000  (17 items)
```

Kết quả trải trên các tháng `2026-02` đến `2026-08`, khớp khoảng thời gian
sinh dữ liệu của bộ RetailStream (`--days-back 180`).

## 9. Kiểm tra kế hoạch thực thi bằng `explain()` (V04)

```js
db.orders.find({ status: "PAID" }).sort({ order_time: 1 }).explain("executionStats");
```

**Kết quả mong đợi:** `winningPlan.stage = FETCH`, `inputStage.stage =
IXSCAN` — xác nhận optimizer dùng đúng index `order_time_1_status_1` đã tạo
ở mục 7 thay vì quét toàn bộ collection (`COLLSCAN`). Số liệu thật:
`totalKeysExamined = 150`, `totalDocsExamined = 150`, `nReturned = 48`.

## 10. So sánh embedding vs referencing (V05)

```js
// embedding: mảng items nằm ngay trong document orders_embedded
db.orders_embedded.findOne({ order_id: "ORD000001" });

// referencing: order_items là collection riêng, liên kết qua order_id
db.order_items.find({ order_id: "ORD000001" });
```

**Kết quả mong đợi:** truy vấn embedding trả về 1 document đơn, đã có sẵn
mảng `items` — không cần bước nối dữ liệu nào thêm. Truy vấn referencing trả
về các document `order_items` rời rạc, phải tự nối lại (`$lookup`) nếu muốn
xem cùng lúc với thông tin đơn hàng.

**Đánh đổi giữa hai cách:**

- **Embedding**: đọc nhanh (chỉ 1 round-trip tới MongoDB), phù hợp khi dữ
  liệu con hiếm khi thay đổi độc lập với document cha và số lượng mục con
  không quá lớn (tránh document phình to, vượt giới hạn kích thước BSON
  16MB cho một document).
- **Referencing**: linh hoạt hơn khi cần truy vấn hoặc cập nhật dữ liệu con
  độc lập (ví dụ sửa giá tại thời điểm mua của riêng một dòng đơn hàng),
  tránh document cha phình to, nhưng cần `$lookup` (tốn chi phí hơn một
  CRUD đơn giản) mỗi khi cần dữ liệu gộp.

## 11. Lỗi thường gặp

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| Container MongoDB không khởi động được, log báo `Wrong mongod version` sau khi đổi tag image | Đã từng chạy với `mongo:latest` (kéo về bản mới hơn, ví dụ 8.2), sau đó đổi lại `docker-compose.yml` sang bản cũ hơn (`mongo:8.0`) trong khi volume dữ liệu cũ vẫn còn — MongoDB không hỗ trợ hạ cấp (downgrade) storage engine giữa các phiên bản có `featureCompatibilityVersion` khác nhau | Không dùng `mongo:latest` ngay từ đầu (ghim version cố định, ở đây là `mongo:8.0`); nếu đã lỡ đổi version trên volume có dữ liệu, phải export/backup dữ liệu trước, xoá volume (`MongoDb/data/`), rồi khởi động sạch với version mới thay vì chạy đè |
| `docker exec ... /tmp/...` báo "no such file or directory" khi chạy trên Git Bash Windows | Git Bash/MSYS tự dịch path kiểu Unix (`/tmp/...`) sang đường dẫn Windows trước khi truyền vào lệnh `mongoimport`/`mongosh` bên trong container | Chạy `export MSYS_NO_PATHCONV=1` trước các lệnh `docker exec`/`docker cp`, hoặc dùng PowerShell |
| Chưa có script tự động dọn dữ liệu để chạy lại từ đầu | Buổi này chưa đóng gói `reset-lab` như bài HDFS | Dừng container, xoá thủ công thư mục `MongoDb/data/*`, rồi `docker compose up -d` và nạp lại dữ liệu theo mục 4 |

## 12. Dừng và dọn dẹp

```bash
docker compose down
```

Giữ nguyên thư mục `data/` nếu muốn giữ lại dữ liệu đã nạp cho lần chạy sau;
xoá thư mục này (mục 11) nếu muốn chạy lại hoàn toàn từ đầu.

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| **Image / ghim version** | "Bản cài đặt đóng gói sẵn" của một phần mềm (ví dụ `mongo:8.0`). "Ghim version" nghĩa là chỉ rõ đúng phiên bản (`8.0`) thay vì `latest` (bản mới nhất, có thể đổi bất cứ lúc nào) — để lần sau chạy lại vẫn ra kết quả giống hệt. |
| **Document** | Một "bản ghi" trong MongoDB, giống 1 dòng trong bảng Excel nhưng linh hoạt hơn (các document trong cùng 1 nhóm có thể có cột khác nhau). |
| **Collection** | Một nhóm document, giống khái niệm "bảng" trong CSDL quan hệ. |
| **CRUD** | 4 thao tác cơ bản: Create (tạo), Read (đọc), Update (sửa), Delete (xoá). |
| **Embedding** | Gộp dữ liệu liên quan vào chung 1 document (ví dụ nhét luôn danh sách sản phẩm của 1 đơn hàng vào chính document đơn hàng đó). Đọc nhanh, nhưng document có thể phình to. |
| **Referencing** | Tách dữ liệu liên quan ra collection riêng, chỉ lưu "mã liên kết" (giống khoá ngoại trong CSDL quan hệ). Linh hoạt hơn, nhưng khi đọc phải nối (`$lookup`) 2 nơi lại. |
| **Index / explain()** | Index = "mục lục" giúp tìm dữ liệu nhanh hơn thay vì quét toàn bộ. `explain()` = lệnh hỏi hệ thống "bạn đã tìm bằng cách nào", dùng để kiểm tra index có thực sự được dùng không. |
| **Aggregation Pipeline** | Một chuỗi bước xử lý dữ liệu nối tiếp nhau (lọc → nối → nhóm → sắp xếp...) để ra báo cáo, ví dụ "doanh thu theo tháng". |
