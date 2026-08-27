# 00 – RETAILSTREAM DATA CONTRACT

Tài liệu này là nguồn chuẩn duy nhất cho tên dataset và trường dữ liệu.

AI không được tự đổi tên trường. Nếu cần thay đổi schema phải đề xuất và được duyệt.

## 1. Quy ước chung

- ID: chuỗi.
- Timestamp: ISO 8601, có timezone khi sinh dữ liệu.
- Không dùng dữ liệu cá nhân thật.
- Tên trường dùng `snake_case`.
- Tiền tệ dùng số thập phân hoặc integer minor-unit theo quyết định triển khai; phải thống nhất trong generator.
- JSON Lines dùng một object trên mỗi dòng.

---

## 2. customers

| Field | Type | Ý nghĩa |
|---|---|---|
| customer_id | string | ID khách hàng |
| customer_segment | string | phân khúc khách hàng |
| city | string | thành phố giả lập |
| created_at | timestamp | thời điểm tạo |

---

## 3. products

| Field | Type | Ý nghĩa |
|---|---|---|
| product_id | string | ID sản phẩm |
| category_id | string | ID danh mục |
| category_name | string | tên danh mục |
| product_name | string | tên sản phẩm |
| brand | string | thương hiệu |
| price | decimal | giá hiện tại |
| attributes | object | thuộc tính linh hoạt |
| updated_at | timestamp | thời điểm cập nhật |

`attributes` được dùng để minh họa document schema linh hoạt trong MongoDB.

---

## 4. orders

| Field | Type | Ý nghĩa |
|---|---|---|
| order_id | string | ID đơn |
| customer_id | string | ID khách |
| order_time | timestamp | thời điểm tạo đơn |
| status | enum | trạng thái |
| payment_method | string | phương thức thanh toán |
| total_amount | decimal | tổng tiền |

Giá trị `status` chuẩn:

- `CREATED`
- `PAID`
- `SHIPPED`
- `CANCELLED`

---

## 5. order_items

| Field | Type | Ý nghĩa |
|---|---|---|
| order_id | string | ID đơn |
| product_id | string | ID sản phẩm |
| quantity | integer | số lượng |
| unit_price | decimal | đơn giá tại thời điểm mua |

---

## 6. web_logs

Dữ liệu JSON Lines hoặc log được chuẩn hóa.

| Field | Type |
|---|---|
| event_id | string |
| event_time | timestamp |
| customer_id | string/null |
| session_id | string |
| path | string |
| product_id | string/null |
| status_code | integer |
| response_time_ms | integer |

---

## 7. clickstream

| Field | Type |
|---|---|
| event_id | string |
| event_time | timestamp |
| customer_id | string/null |
| session_id | string |
| product_id | string |
| event_type | enum |

`event_type` chuẩn:

- `VIEW`
- `ADD_TO_CART`
- `PURCHASE`

Có thể bổ sung `REMOVE_FROM_CART` sau khi được duyệt.

---

## 8. product_events

| Field | Type |
|---|---|
| event_id | string |
| event_time | timestamp |
| product_id | string |
| event_type | enum |
| old_value | object/null |
| new_value | object/null |

Ví dụ:

- `PRICE_CHANGED`
- `STOCK_CHANGED`

---

## 9. Mức dữ liệu

### sample
20–200 bản ghi mỗi nguồn.

Mục tiêu:
- đọc bằng mắt;
- theo dấu thuật toán;
- có expected output công khai.

### lab
10.000–200.000 bản ghi tùy nguồn.

Mục tiêu:
- thực hành cá nhân;
- đủ để thấy khác biệt truy vấn/xử lý.

### cluster
Vài trăm MB hoặc mức phù hợp hạ tầng.

Mục tiêu:
- quan sát partition/task/shuffle;
- Spark/HDFS UI.

### stream
Sinh sự kiện liên tục từ cùng schema `clickstream`/`product_events`.

---

## 10. Quy tắc tương thích

MongoDB có thể biểu diễn `orders + order_items` bằng embedding hoặc referencing để phục vụ bài học, nhưng dữ liệu canonical vẫn giữ hai logical entities.

Mọi biến thể document phải được ghi trong tài liệu buổi MongoDB, không được coi là thay đổi Data Contract toàn môn.
