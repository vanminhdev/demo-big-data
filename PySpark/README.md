# Buổi 10 – PySpark DataFrame: pipeline CityRide

`b10_cityride_pipeline.py`: đọc → làm sạch → ghép bảng → tổng hợp → ghi Parquet theo tháng. Chạy được ở hai chế độ mà không sửa code.

```bash
bash scripts/run-local.sh      # local[*] trong container spark-master
bash scripts/run-cluster.sh    # spark://spark-master:7077, 2 worker
```

Cần bật cụm của `Spark/` trước (script tự `docker compose up -d`). Kết quả ghi vào `Spark/data/output/b10/`.

## Điều cần quan sát (log mẫu: `evidence/b10_cluster_run.log`, `b10_local_run.log`)

| Mục | Nội dung | Kết quả trên cụm |
|---|---|---|
| [1] | `inferSchema=True` và schema khai báo | 23,29 s và 0,11 s |
| [2] | kiểm tra chất lượng | 300.000 → 239.879 hoàn thành → 239.456 sau làm sạch |
| [3] | inner join với `drivers` | mất 244 chuyến |
| [4] | ghép với `trip_payments` (một – nhiều) | 254.031 dòng; tổng `fare_vnd` 16.724.347.000 (sai) so với 15.762.641.000 (đúng) |
| [5] | tổng hợp theo tháng, quận; `explain()` | có `BroadcastHashJoin` với bảng `zones` |
| [6] | Spark SQL cho cùng kết quả | `True` |
| [7] | Parquet chia thư mục theo tháng | 23,9 MB CSV và 4,9 MB Parquet; `PartitionFilters` khi lọc tháng 8 |
| [8] | Python UDF và hàm có sẵn | 3,62 s và 0,83 s |

Hướng dẫn thực hành chi tiết: `THUC_HANH_PYSPARK.md`.
