# RetailStream Sample Data (Gói 0 – Dữ liệu chung)

Bộ dữ liệu mẫu dùng chung cho toàn bộ 15 buổi học phần Nhập môn dữ liệu lớn.
Tuân thủ `bigdata_ai_coordination/00_DATA_CONTRACT.md`. Mọi buổi (MongoDB, HDFS,
MapReduce, Spark, Kafka...) phải dùng lại các file trong `sample/`, không tự
tạo dataset nghiệp vụ riêng.

## Cách sinh lại dữ liệu

```bash
cd 00_shared_data/generators
python generate_retailstream.py --seed 42 --scale sample --output-dir ../sample
python generate_retailstream.py --seed 42 --scale lab --output-dir ../lab
```

`--seed` cố định để tái tạo được cùng một bộ dữ liệu. Hỗ trợ hai mức:
`sample` (20–200 bản ghi/bảng, đã dùng cho MongoDB buổi 5) và `lab`
(10.000–200.000 bản ghi/bảng, dùng cho bài thực hành cá nhân trên
Colab/máy cá nhân, kể cả Spark). Mức `cluster` chưa sinh — để dành khi có
buổi HDFS/Spark cluster thật sự cần quan sát partition, tránh dữ liệu thừa.

## Danh sách file (`sample/`)

| File | Định dạng | Trường chính | Ghi chú |
|---|---|---|---|
| `customers_sample.csv` | CSV | customer_id, customer_segment, city, created_at | có 1 bản ghi trùng cố ý |
| `products_sample.json` | JSON array | product_id, category_id, category_name, product_name, brand, price, attributes, updated_at | `brand=null` ở một số bản ghi cố ý |
| `orders_sample.csv` | CSV | order_id, customer_id, order_time, status, payment_method, total_amount | bản ghi cuối `total_amount=-1.0` cố ý (bài làm sạch dữ liệu) |
| `order_items_sample.csv` | CSV | order_id, product_id, quantity, unit_price | 1–4 dòng/đơn |
| `web_logs_sample.jsonl` | JSON Lines | event_id, event_time, customer_id, session_id, path, product_id, status_code, response_time_ms | có status_code 404/500, customer_id null |
| `clickstream_sample.jsonl` | JSON Lines | event_id, event_time, customer_id, session_id, product_id, event_type | event_type: VIEW/ADD_TO_CART/PURCHASE |
| `product_events_sample.jsonl` | JSON Lines | event_id, event_time, product_id, event_type, old_value, new_value | event_type: PRICE_CHANGED/STOCK_CHANGED |
| `manifest.json` | JSON | seed, record_counts, sha256 từng file, danh sách lỗi cố ý | dùng để kiểm tra tính toàn vẹn khi phân phối lại |

## Danh sách file (`lab/`)

Sinh bằng `--scale lab --seed 42`, quy mô 10.000–200.000 bản ghi/bảng theo
mục 2.8 khung nội dung. Tên file không có hậu tố `_sample` (khác `sample/`).

| File | Định dạng | Số bản ghi | Ghi chú |
|---|---|---:|---|
| `customers.csv` | CSV | 20.000 (+1 trùng cố ý) | cùng schema `customers_sample.csv` |
| `products.json` | JSON array | 5.000 | cùng schema `products_sample.json` |
| `orders.csv` | CSV | 50.000 | cùng schema `orders_sample.csv` |
| `orders.parquet` | Parquet | 50.000 | bản Parquet cho bài Spark — chỉ sinh nếu môi trường có sẵn pandas+pyarrow |
| `order_items.csv` | CSV | ~124.000–125.000 (1–4 dòng/đơn) | cùng schema `order_items_sample.csv` |
| `order_items.parquet` | Parquet | như trên | bản Parquet cho bài Spark — điều kiện như trên |
| `web_logs.jsonl` | JSON Lines | 100.000 | cùng schema `web_logs_sample.jsonl` |
| `clickstream.jsonl` | JSON Lines | 100.000 | cùng schema `clickstream_sample.jsonl` |
| `product_events.jsonl` | JSON Lines | 20.000 | cùng schema `product_events_sample.jsonl` |
| `manifest.json` | JSON | — | seed, record_counts, sha256 từng file, danh sách lỗi cố ý |

Ghi chú Parquet: script kiểm tra `pandas`/`pyarrow` trước khi ghi; nếu môi
trường sinh dữ liệu không có sẵn hai thư viện này, script bỏ qua bước ghi
Parquet (in cảnh báo) và **CSV vẫn là nguồn chính** cho mức `lab`. Máy đã
chạy generator lần này có sẵn `pandas 2.2.1` + `pyarrow 24.0.0` nên
`orders.parquet` và `order_items.parquet` đã được sinh kèm. Nếu môi trường
Spark khác không thấy 2 file Parquet này, hãy chạy lại script trên máy có
pyspark/pandas cài sẵn — không tự cài thêm thư viện chỉ để phục vụ việc này.

## Dữ liệu lỗi/thiếu được tạo có chủ ý

Xem `manifest.json` → `intentional_data_issues`. Mục đích: phục vụ bài tập
làm sạch dữ liệu (data quality) ở các buổi thực hành tích hợp, không phải
lỗi generator.

## Tương quan giả lập giữa segment/payment_method và CANCELLED (chỉ mức `lab`)

Theo yêu cầu giảng viên (Buổi 12 MLlib cho AUC ~0.51 vì dữ liệu ngẫu nhiên
hoàn toàn, không có tín hiệu để mô hình học), generator đã được sửa để CẤY
tương quan giả lập có chủ đích vào `status` của `orders` — **chỉ khi
`--scale lab`** (cờ nội bộ `correlated_labels`, tự bật khi `scale == "lab"`,
mặc định TẮT cho `sample`):

- `customer_segment`: `CHURN_RISK` có xác suất `CANCELLED` cao hơn ~2.6 lần
  so với cơ sở, `VIP` thấp hơn ~0.55 lần, `REGULAR` ~0.9 lần, `NEW` ~1.15 lần.
- `payment_method`: `COD` cao hơn ~1.35 lần, `CREDIT_CARD` thấp hơn ~0.7 lần,
  `E_WALLET` ~0.85 lần, `BANK_TRANSFER` ~1.0 lần (cơ sở).
- Hai yếu tố nhân với nhau (không phải 1 rule if-else lộ liễu duy nhất), có
  cắp trần 0.55 để tránh cực đoan phi thực tế.

Kết quả đo thực tế trên `lab/orders.csv` (seed=42): tỷ lệ hủy tổng thể
~12.6%; `CHURN_RISK` ~25.1% vs `VIP` ~5.2%; `COD` ~17.4% vs `CREDIT_CARD`
~8.9%. Xem chi tiết AUC trước/sau ở
`bigdata_ai_coordination/sessions/12_mllib/LOCAL_REPORT.md`.

**Mức `sample` (seed=42) KHÔNG có tương quan này** — vẫn sinh ngẫu nhiên
hoàn toàn như trước, đã xác nhận `--scale sample --seed 42` cho kết quả
giống hệt 100% (record_counts + sha256 từng file) so với `manifest.json`
đã phát hành trước đó, để không phá vỡ số liệu đã dùng ở
`bigdata_ai_coordination/sessions/05_mongodb/LOCAL_REPORT.md`.

## Trạng thái

- [x] sample (seed=42) — đã sinh, đã kiểm tra thủ công đúng Data Contract.
      Không đổi kể từ lần sinh đầu (giữ nguyên ngẫu nhiên, không tương quan).
- [x] lab (seed=42) — đã sinh lại 2026-08-20 với tương quan giả lập giữa
      segment/payment_method và CANCELLED (xem mục trên), có kèm Parquet cho
      `orders`/`order_items`; đã xác nhận `--scale sample` chạy lại cho kết
      quả giống hệt manifest cũ (không phá vỡ tương thích ngược Buổi 5).
- [ ] cluster — chưa cần, sẽ bổ sung khi buổi HDFS/Spark cluster yêu cầu quan sát partition.
