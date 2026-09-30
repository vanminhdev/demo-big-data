# Buổi 9 – Apache Spark trên cụm Standalone

Cụm Docker gồm 1 master và 2 worker (mỗi worker 1 lõi, 640 MB). Cụm này dùng chung cho Buổi 9, 10, 11, 12, 13 và 15.

## Chạy

```bash
bash scripts/start-cluster.sh        # bật cụm, chờ master healthy
bash scripts/run-b09.sh 300          # chạy demo, giữ Spark UI thêm 300 giây
bash scripts/stop-cluster.sh
```

- Master UI: http://localhost:8080. Nếu cổng 8080 đang bận, đặt `export SPARK_MASTER_UI_PORT=18080` trước khi chạy.
- Spark UI của ứng dụng: http://localhost:4040 (chỉ có khi ứng dụng đang chạy).
- `run-b09.sh` tự chép dữ liệu `00_shared_data/cityride/lab` vào `Spark/data/cityride/lab` (thư mục mount `/opt/spark-data`).

## Demo `jobs/b09_cityride_spark.py`

| Mục | Quan sát | Kết quả mẫu (`evidence/b09_run.log`) |
|---|---|---|
| [1] | số partition khi đọc tệp 25 MB, mỗi partition ≤ 4 MB | 7 partition |
| [2] | transformation chỉ ghi kế hoạch, action mới chạy | khai báo 0,10 s; `count()` 4,33 s |
| [3] | gom nhóm theo quận cần shuffle → 2 stage | Q01: 37.767 chuyến hoàn thành |
| [4] | cache khi dùng lại dữ liệu | 1,56 / 1,72 s → 0,71 / 0,70 s |
| [5] | RDD `reduceByKey` và DataFrame `groupBy` cho cùng kết quả | `True` |

Hai cấu hình trong `run-b09.sh` giúp Spark UI dễ đọc khi học: `spark.sql.adaptive.enabled=false` (mỗi action ứng với đúng một job) và `spark.sql.files.maxPartitionBytes=4m`.

`evidence/b09_stage3_tasks.json`, `b09_stage4_tasks.json`: thời gian từng task, dùng vẽ biểu đồ "task chạy theo lượt" trong slide.

## Thư mục `jobs/`

Các script chạy demo của buổi khác (`run-b10`, `run-b12`, …) tự chép mã nguồn vào `jobs/`, vì container chỉ mount được `Spark/jobs` và `Spark/data`. Bản gốc nằm trong thư mục của từng buổi.
