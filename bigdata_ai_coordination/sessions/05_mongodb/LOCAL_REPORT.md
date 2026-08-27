# LOCAL VALIDATION REPORT – Buổi 5 (MongoDB)

## 1. Trạng thái

**Validation:** PASS

## 2. Environment

| Thành phần | Version |
|---|---|
| OS | Windows 10 Pro 10.0.19045 |
| Docker | 29.7.2 (build a7dcaa6) |
| Docker Compose | v5.4.0 |
| Python (generator) | 3.11.4 |
| MongoDB (container) | **8.0.29** (image ghim `mongo:8.0`, trước đó là `mongo:latest` → đã sửa) |
| mongosh | bundled trong container mongo:8.0 |

**Lưu ý version:** `docker-compose.yml` ban đầu dùng `mongo:latest`, khi đang chạy đã kéo về **8.2.12**. Vì repo yêu cầu ghim version, đã đổi sang `mongo:8.0`. Khi thử downgrade, container không khởi động được (lỗi `Wrong mongod version` — featureCompatibilityVersion `8.2` không tương thích ngược `8.0`). Do dữ liệu cũ trong `MongoDb/data/` chỉ là system DB rỗng (chưa có dữ liệu nghiệp vụ, đã xác nhận trước khi xoá và được sự đồng ý của giảng viên), đã xoá volume và khởi tạo sạch với `mongo:8.0`. **Bài học cho các buổi sau:** không dùng `latest`; khi đổi version phải kiểm tra tương thích storage engine trước, tránh mất dữ liệu thật.

## 3. Cách khởi động

```bash
cd MongoDb
docker compose up -d
# kiểm tra
docker exec mongodb mongosh --quiet --eval "db.version()"
# -> 8.0.29
```

## 4. Chuẩn bị dữ liệu

Dữ liệu lấy từ gói dữ liệu chung `00_shared_data/` (xem `00_shared_data/README.md`),
sinh bằng `generate_retailstream.py --seed 42 --scale sample` (60 products, 150 orders,
380 order_items). Không tạo dataset riêng cho buổi này, đúng nguyên tắc RetailStream.

```bash
cd MongoDb/scripts
python prepare_import_data.py
# -> tạo data_import/products.json, orders_embedded.json, orders_ref.json, order_items.json
```

`prepare_import_data.py` sinh **2 biến thể** để phục vụ đúng yêu cầu bài học
(so sánh embedding vs referencing):

- **embedding**: collection `orders_embedded` — mỗi order có mảng `items` nhúng sẵn.
- **referencing**: collection `orders` + `order_items` riêng, liên kết qua `order_id`.

Import vào MongoDB:

```bash
docker cp data_import/products.json mongodb:/tmp/products.json
docker cp data_import/orders_embedded.json mongodb:/tmp/orders_embedded.json
docker cp data_import/orders_ref.json mongodb:/tmp/orders_ref.json
docker cp data_import/order_items.json mongodb:/tmp/order_items.json

docker exec mongodb mongoimport --db retailstream --collection products --file /tmp/products.json --jsonArray
docker exec mongodb mongoimport --db retailstream --collection orders_embedded --file /tmp/orders_embedded.json --jsonArray
docker exec mongodb mongoimport --db retailstream --collection orders --file /tmp/orders_ref.json --jsonArray
docker exec mongodb mongoimport --db retailstream --collection order_items --file /tmp/order_items.json --jsonArray
```

Kết quả thực tế: `60 / 150 / 150 / 380` document import thành công, `0` lỗi.

**Lưu ý môi trường Windows + Git Bash:** `docker exec` với đường dẫn kiểu `/tmp/...`
bị MSYS/Git Bash tự dịch sang đường dẫn Windows (`C:/Users/.../Temp/...`) và làm
`mongoimport`/`mongosh` báo "no such file or directory". Khắc phục bằng
`export MSYS_NO_PATHCONV=1` trước khi chạy `docker exec`. Cần ghi chú này vào
tài liệu hướng dẫn nếu sinh viên dùng Git Bash trên Windows.

Toàn bộ script chạy demo: `MongoDb/demo.mongodb.js`. Output đầy đủ đã lưu tại
`MongoDb/demo_output.txt`.

## 5. Validation Results

### V01 – CRUD cơ bản (Create/Read/Update/Delete trên `products`)

**Result:** PASS

Command: xem khối "V01" trong `demo.mongodb.js`.

Actual:
```text
Read  -> document PROD99999 tìm thấy đúng.
Update -> { acknowledged: true, matchedCount: 1, modifiedCount: 1 }
Delete -> { acknowledged: true, deletedCount: 1 }
```

### V02 – Tạo index

**Result:** PASS

Command:
```js
db.orders.createIndex({ order_time: 1, status: 1 });
```

Actual: index `order_time_1_status_1` được tạo, xác nhận qua `db.orders.getIndexes()`.

### V03 – Aggregation Pipeline: doanh thu theo danh mục và tháng

**Result:** PASS

Command: xem khối "V03" trong `demo.mongodb.js` (`$lookup` orders + products, `$group` theo category+month).

Actual (trích, đầy đủ trong `demo_output.txt` dòng 31–359):
```text
{ category: 'Thuc pham', month: '2026-02' } revenue: 155,063,000  (4 items)
{ category: 'My pham',   month: '2026-02' } revenue: 84,634,000   (4 items)
...
{ category: 'Thuc pham', month: '2026-04' } revenue: 586,942,000  (17 items)
```
Kết quả hợp lý, tổng khớp số order_items đã import (380 dòng được nhóm đúng theo tháng 2026-02 → 2026-08, khớp khoảng thời gian sinh dữ liệu `--days-back 180`).

### V04 – explain() cho truy vấn dùng index

**Result:** PASS

Command:
```js
db.orders.find({ status: "PAID" }).sort({ order_time: 1 }).explain("executionStats");
```

Actual: `winningPlan.stage = FETCH`, `inputStage.stage = IXSCAN` dùng đúng index
`order_time_1_status_1`; `totalKeysExamined = 150`, `totalDocsExamined = 150`,
`nReturned = 48`. Xác nhận optimizer dùng index thay vì COLLSCAN.

### V05 – So sánh embedding vs referencing

**Result:** PASS

Actual: `orders_embedded.findOne(...)` trả về 1 document có mảng `items` nhúng sẵn
(không cần `$lookup`). `order_items.find({order_id: "ORD000001"})` (referencing) trả
về document riêng, liên kết qua `order_id`.

**Đánh đổi rút ra (để Content AI dùng viết tài liệu đọc):**
- Embedding: đọc nhanh (1 round-trip), phù hợp khi item hiếm khi thay đổi độc lập và đơn hàng không quá nhiều mặt hàng (tránh document quá lớn, giới hạn BSON 16MB).
- Referencing: linh hoạt hơn khi cần truy vấn/cập nhật `order_items` độc lập (vd. đổi giá tại thời điểm mua), tránh document phình to, nhưng cần `$lookup` (tốn chi phí hơn CRUD đơn giản) khi cần dữ liệu gộp.

## 6. Issues Found

### ISSUE-01

**Severity:** Major

**Hiện tượng:** `docker-compose.yml` gốc dùng `image: mongo:latest`, vi phạm quy tắc
ghim version của dự án (`00_MASTER_PLAN.md` mục 7, Khung nội dung mục 2.8).

**Nguyên nhân:** chưa ghim version khi tạo file ban đầu.

**Cách sửa:** đã đổi thành `image: mongo:8.0`. Đã áp dụng.

**File ảnh hưởng:** `MongoDb/docker-compose.yml`.

### ISSUE-02

**Severity:** Minor

**Hiện tượng:** Downgrade MongoDB (8.2 → 8.0) trên volume dữ liệu cũ làm container
crash với lỗi `Wrong mongod version`.

**Nguyên nhân:** MongoDB không hỗ trợ downgrade storage engine giữa các minor version
khác featureCompatibilityVersion.

**Cách sửa:** không áp dụng cho môi trường có dữ liệu thật — phải backup/export trước
khi đổi version. Trong lab của sinh viên, nên nêu rõ trong README: đổi version MongoDB
yêu cầu môi trường sạch hoặc dump/restore dữ liệu.

**File ảnh hưởng:** không có file cần sửa, chỉ là lưu ý vận hành — nên đưa vào
`05_mongodb_practice.md` (Content AI) phần "lỗi thường gặp".

## 7. Mismatch với tài liệu Content AI

| Vị trí | Nội dung hiện tại | Thực tế | Đề xuất |
|---|---|---|---|
| `sessions/05_mongodb/CONTENT_REPORT.md` | "Chưa cập nhật" | Chưa có nội dung slide/tài liệu đọc nào được soạn | Content AI cần đọc `LOCAL_REPORT.md` này để viết `05_mongodb_practice.md` khớp đúng lệnh/kết quả đã kiểm chứng ở trên, đặc biệt các con số aggregation thực tế và explain() thực tế — không tự bịa số liệu mẫu khác |

## 8. Khả năng chạy lại

- [x] chạy từ clean state (đã test: xoá `MongoDb/data`, `docker compose up -d` lại từ đầu, import lại — thành công);
- [x] version được ghim (`mongo:8.0`);
- [x] dữ liệu có đường dẫn tương đối (`00_shared_data/sample/`, `MongoDb/data_import/`);
- [x] container truy cập được dữ liệu (qua `docker cp` + `mongoimport`);
- [x] expected output được lưu (`MongoDb/demo_output.txt`);
- [ ] reset script hoạt động — **chưa có script reset tự động** (`reset-lab.sh/.ps1`); hiện phải xoá thủ công `MongoDb/data/*`. Đề xuất bổ sung nếu buổi này cần phát cho sinh viên tự chạy.

## 9. Kết luận cho giảng viên

Có thể dùng để dạy: **YES**

Các điểm cần đọc trước khi duyệt:

1. Toàn bộ 5 validation item (CRUD, index, aggregation doanh thu theo danh mục/tháng,
   explain, embedding-vs-referencing) đã chạy thật và PASS trên MongoDB 8.0.29 —
   số liệu trong mục 5 là số liệu thật, có thể dùng trực tiếp làm ví dụ trong slide/tài
   liệu đọc.
2. Dữ liệu dùng bộ RetailStream chung (`00_shared_data/`, seed=42) — các buổi sau
   (HDFS, Spark...) nên dùng lại cùng bộ này để nhất quán số liệu xuyên môn.
3. Cần Content AI bổ sung `05_mongodb_practice.md` và `CONTENT_REPORT.md` dựa trên
   report này (hiện đang trống).
4. Chưa có script reset tự động cho sinh viên — quyết định xem có cần thiết cho buổi
   này không (buổi 5 không bắt buộc gói Docker phức tạp như buổi 6 HDFS).
