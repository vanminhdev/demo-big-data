# Buổi 12 – Spark MLlib: dự đoán khách hủy chuyến

`b12_cancel_model.py`: nhãn `label = 1` nếu `status = rider_cancelled`. Đặc trưng lúc khách vừa đặt xe: `eta_min, surge, is_raining, distance_km, hour, vehicle_type, pickup_zone`. Pipeline: StringIndexer → OneHotEncoder → VectorAssembler → LogisticRegression.

```bash
bash scripts/run-b12.sh local      # local[*]
bash scripts/run-b12.sh cluster    # 2 worker
```

Image `apache/spark` không có numpy. Script cài numpy một lần vào `Spark/data/pylibs` (container thấy là `/opt/spark-data/pylibs`), rồi đặt `PYTHONPATH` cho cả Driver và Executor. Không dùng `vector_to_array` vì hàm này cần pandas; xác suất lớp 1 được lấy bằng một UDF nhỏ.

## Kết quả (giống hệt nhau ở local và cụm; `evidence/b12_*_run.log`)

| Chỉ số | Giá trị |
|---|---|
| Tỷ lệ nhãn | 83,0% không hủy / 17,0% hủy |
| train / test (seed 42) | 240.043 / 59.957 |
| AUC mô hình "rò rỉ" (dùng việc có cước hay không) | 0,9886 |
| AUC mô hình đúng | 0,7388 |
| Ngưỡng 0,5 | accuracy 0,833 (mô hình luôn đoán "không hủy": 0,830), precision 0,564, recall 0,072 |
| Ngưỡng 0,3 | accuracy 0,797, precision 0,398, recall 0,385 |
| Hệ số | eta_min +0,250; surge +1,346; is_raining +0,359; distance_km ≈ 0; hour ≈ 0 |
| Thời gian `fit` | local 7,1 s; cụm 9,0 s |

Mô hình được lưu tại `Spark/data/output/b12/cancel_model`, sau đó nạp lại để dự đoán 3 yêu cầu mới (0,064 / 0,458 / 0,747).
