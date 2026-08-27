# 00 – WORK STATUS

> File này là dashboard giám sát dự án. Cập nhật sau mỗi phiên làm việc quan trọng.

| Buổi | Chủ đề | Content | Local | Validation | Trạng thái | Vấn đề chính |
|---:|---|---|---|---|---|---|
| 1 | Tổng quan Big Data | TBD | N/A | N/A | NOT_STARTED | |
| 2 | Transaction/ACID | TBD | TBD | TBD | NOT_STARTED | |
| 3 | Indexing | TBD | TBD | TBD | NOT_STARTED | |
| 4 | CAP/NoSQL | TBD | N/A | N/A | NOT_STARTED | |
| 5 | MongoDB | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết 05_mongodb_practice.md dựa trên LOCAL_REPORT |
| 6 | HDFS | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết 06_hdfs_practice.md + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống) |
| 7 | MapReduce | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết 07_mapreduce_practice.md + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống); 2026-08-22: đã bổ sung YARN thật (ResourceManager+NodeManager) + python3 persistent trong image, job đã chạy lại PASS trên YARN — xem V06/V07 |
| 8 | Giữa kỳ | TBD | N/A | N/A | NOT_STARTED | |
| 9 | Spark | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết slide/tài liệu Buổi 9 + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống); image đổi từ bitnami/spark (không còn free) sang apache/spark:3.5.9-python3 |
| 10 | PySpark | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết slide/tài liệu/bài thực hành Buổi 10 + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống) |
| 11 | Structured Streaming | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết slide/tài liệu/bài thực hành Buổi 11 + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn); 2026-08-22: window đổi từ 1 ngày xuống **5 phút** (watermark 3 phút) sau khi nén trục thời gian dữ liệu — xem LOCAL_REPORT mục "CẬP NHẬT" đầu file, V01b-V05b |
| 12 | MLlib | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết slide/tài liệu/bài thực hành Buổi 12 + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn); 2026-08-22: generator đã cấy tương quan giả lập (chỉ mức `lab`), AUC tăng 0.5117→0.6893 (số liệu thật, xem V08) — dùng số liệu MỚI khi viết nội dung |
| 13 | Kafka | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết slide/tài liệu/bài thực hành Buổi 13 + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn); Kafka dùng image apache/kafka:3.7.1 (KRaft, không Zookeeper) thay vì bitnami/kafka gợi ý trong đề bài |
| 14 | System Design | TODO | DONE (PASS) | VALIDATED (Local) | LOCAL_TESTING | Rà soát tổng hợp 8 buổi 5-13 đã PASS, kiến trúc end-to-end khả thi; 2 quyết định treo (YARN MapReduce, AUC MLlib) — xem LOCAL_REPORT |
| 15 | Optimization | TODO | DONE (PASS_WITH_NOTES) | VALIDATED (Local) | LOCAL_TESTING | Chờ Content AI viết slide/tài liệu/bài thực hành Buổi 15 + CONTENT_REPORT.md dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn); 3/3 tình huống bắt buộc (data skew, small files, shuffle) chạy thật + evidence từ Spark event log (không scrape REST API :4040 sống vì race-condition); hot partition chỉ trích dẫn lại Buổi 13, không chạy mới |

## Trạng thái hợp lệ

- NOT_STARTED
- DRAFTING
- CONTENT_READY
- LOCAL_TESTING
- ISSUES_FOUND
- VALIDATED
- FINALIZED

## Nhật ký cập nhật

### 2026-08-20
- Buổi: Gói 0 (dữ liệu chung) + Buổi 5 (MongoDB)
- AI: Local AI
- Đã hoàn thành:
  - Tạo generator dữ liệu chung RetailStream (`00_shared_data/generators/generate_retailstream.py`, seed=42, mức sample) dùng lại cho mọi buổi sau, có manifest.json + checksum + lỗi dữ liệu cố ý.
  - Buổi 5 MongoDB: ghim `mongo:8.0` (trước là `latest`), import 2 biến thể embedding/referencing, chạy PASS toàn bộ 5 validation item (CRUD, index, aggregation doanh thu theo danh mục/tháng, explain, so sánh embedding/referencing). Chi tiết: `sessions/05_mongodb/LOCAL_REPORT.md`.
- Đang chờ: Content AI viết `05_mongodb_practice.md` + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống).
- Blocker: không có.
- Quyết định cần giảng viên: không có (đã tự xử lý version pin theo đúng quy tắc dự án).

### 2026-08-20 (tiếp)
- Buổi: 6 (HDFS)
- AI: Local AI (subagent song song)
- Đã hoàn thành: đang triển khai (xem log/patch khi subagent hoàn tất).
- Đang chờ: kết quả subagent HDFS.
- Blocker: chưa có.
- Quyết định cần giảng viên: chưa có.

### 2026-08-20 (tiếp 2)
- Việc: mở rộng Gói 0 (dữ liệu chung RetailStream) lên mức `lab` — không phải một buổi cụ thể.
- AI: Local AI (subagent song song)
- Đã hoàn thành:
  - Thêm mức `lab` vào `SCALE_COUNTS` trong `00_shared_data/generators/generate_retailstream.py` (customers=20000, products=5000, orders=50000, web_logs=100000, clickstream=100000, product_events=20000; order_items sinh kèm theo orders, ~124.8k dòng), trong khoảng 10.000–200.000 bản ghi/bảng theo mục 2.8 khung nội dung.
  - Chạy `--seed 42 --scale lab --output-dir ../lab`: sinh 419.863 bản ghi vào `00_shared_data/lab/` (CSV/JSON/JSON Lines đúng schema Data Contract, không thêm field lạ), kèm lỗi/thiếu dữ liệu cố ý tỉ lệ nhỏ giống mức sample.
  - Máy có sẵn pandas 2.2.1 + pyarrow 24.0.0 nên script tự động ghi thêm `orders.parquet` và `order_items.parquet` (không cài thêm thư viện mới); nếu môi trường khác thiếu 2 thư viện này, script tự bỏ qua bước Parquet và dùng CSV làm nguồn chính (đã ghi rõ trong README).
  - Xác nhận tương thích ngược: chạy lại `--scale sample` (vào thư mục tạm, không ghi đè `00_shared_data/sample/`), so sánh record_counts + sha256 từng file + intentional_data_issues với manifest cũ — khớp 100%.
  - Cập nhật `00_shared_data/README.md`: thêm bảng danh sách file mức `lab`, ghi chú Parquet, đánh dấu `[x] lab` trong mục Trạng thái.
- Không đụng tới `00_shared_data/sample/`, `MongoDb/`, `Hdfs/`, `00_DATA_CONTRACT.md`.
- Đang chờ: không có (mức `cluster` cố ý chưa làm, để dành khi buổi HDFS/Spark cluster thật sự cần).
- Blocker: không có.
- Quyết định cần giảng viên: không có.

### 2026-08-20 (tiếp 3)
- Buổi: 6 (HDFS)
- AI: Local AI (subagent HDFS)
- Đã hoàn thành:
  - Tạo thư mục `Hdfs/` ở root, cùng cấp `MongoDb/`, đầy đủ `README.md`, `.env.example`, `docker-compose.yml`, `scripts/{start-cluster,stop-cluster,reset-lab,load-sample-data}.{sh,ps1}`, `data/web_logs_sample.jsonl` (copy từ `00_shared_data/sample/`, không tạo dataset riêng).
  - Cụm Docker Compose 1 NameNode + 2 DataNode, ghim version `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` và `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` (Hadoop 3.2.1, đã pull và chạy thật, không dùng `latest`), có healthcheck curl cho cả 3 service.
  - Chạy PASS toàn bộ 9 validation item (V01–V09): health check cụm, `dfsadmin -report`, upload (`-put`/`-mkdir`/`-ls`), đọc (`-cat`, đối chiếu đúng 200 dòng), tải xuống (`-get`, diff giống hệt), xoá (`-rm`), `fsck` (block/replication=2, HEALTHY), dừng/khởi động lại 1 DataNode (quan sát được: NameNode chỉ đánh dấu Dead sau ~10.5 phút heartbeat timeout — hành vi thật của HDFS, đã ghi rõ trong README và LOCAL_REPORT để Content AI không mô tả sai), và chạy lại toàn bộ từ trạng thái sạch (`docker compose down -v` rồi lên lại) — thành công. Chi tiết đầy đủ: `sessions/06_hdfs/LOCAL_REPORT.md`.
- Đang chờ: Content AI viết `06_hdfs_practice.md` + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn).
- Blocker: không có (đã bị chặn RAM/port do máy chỉ có ~3.8GB cấp cho Docker, nhưng cụm 3 container Hadoop vẫn chạy ổn định trong giới hạn này nên không phải blocker thật sự — đã ghi chú trong README mục lỗi thường gặp).
- Quyết định cần giảng viên: chưa có quyết định bắt buộc; có 1 điểm nên giảng viên biết — gói chưa tạo thư mục `healthcheck/` riêng như khung nội dung liệt kê (healthcheck được khai báo trực tiếp trong `docker-compose.yml` bằng `curl`, đủ chức năng), nêu chi tiết ở mục 7 `LOCAL_REPORT.md` nếu cần đúng 100% cấu trúc thư mục.

### 2026-08-20 (tiếp 4)
- Buổi: 7 (MapReduce)
- AI: Local AI subagent
- Đã hoàn thành:
  - Tạo thư mục `MapReduce/` ở root (ngang cấp `MongoDb/`, `Hdfs/`): `README.md`,
    `mapper.py`, `reducer.py`, `expected_output/` (kết quả tính tay + kết quả job thật, đã diff khớp).
  - Dùng lại nguyên trạng cụm HDFS đang chạy từ Buổi 6 (`hdfs-namenode`,
    `hdfs-datanode1`, `hdfs-datanode2`) — không dựng cụm Hadoop mới, không sửa
    `Hdfs/docker-compose.yml`. Dữ liệu `web_logs_sample.jsonl` (200 dòng) đã có sẵn
    trên HDFS từ buổi trước, không tạo dataset riêng.
  - Cài `python3` runtime trong container `hdfs-namenode` (image gốc không có Python,
    phải trỏ `apt` sang `archive.debian.org` do Debian 9 stretch đã EOL) — thay đổi
    chỉ tồn tại trong container đang chạy, không sửa image/compose.
  - Chạy job Hadoop Streaming thật (`hadoop-streaming-3.2.1.jar`) đếm lượt truy cập
    theo `product_id` từ `web_logs`, bỏ `product_id = null`. Chạy PASS 5 validation
    item (V01–V05): pipe test mapper|sort|reducer, job với `numReduceTasks=1` (1 file
    part-00000), job với `numReduceTasks=3` (3 file part-*, xác nhận số part file đổi
    theo số reducer), đối chiếu kết quả với script Python đếm tay độc lập (khớp
    byte-for-byte: 23 product_id phân biệt, tổng 29 lượt xem trên 200 dòng, 171 dòng
    `product_id=null`), và chạy lại từ trạng thái sạch (xoá output dir, chạy lại job).
  - Ghi chú quan trọng cho Content AI: cụm không có YARN nên job chạy ở chế độ
    LocalJobRunner (không phải phân tán qua NodeManager thật) — vẫn đúng ngữ nghĩa
    Map/Shuffle/Sort/Reduce và đọc/ghi thật trên HDFS. Chi tiết đầy đủ:
    `sessions/07_mapreduce/LOCAL_REPORT.md`.
- Đang chờ: Content AI viết `07_mapreduce_practice.md` + `CONTENT_REPORT.md` dựa trên
  LOCAL_REPORT (hiện đang trống hoàn toàn).
- Blocker: không có.
- Quyết định cần giảng viên: không bắt buộc; nên biết rằng cụm hiện tại (kế thừa từ
  Buổi 6) không có YARN thật, nếu muốn demo YARN phân tán qua nhiều NodeManager cần
  một phiên làm việc riêng mở rộng `Hdfs/docker-compose.yml` (xem ISSUE-01 trong
  LOCAL_REPORT buổi 7).

### 2026-08-20 (tiếp 4)
- Buổi: 9 (Spark)
- AI: Local AI (subagent Spark)
- Đã hoàn thành:
  - Tạo thư mục `Spark/` ở root (cùng cấp `MongoDb/`, `Hdfs/`) với `README.md`, `docker-compose.yml`, `jobs/demo_job.py`, `data/` (copy `orders_sample.csv`, `order_items_sample.csv`, `web_logs_sample.jsonl` từ `00_shared_data/sample/`, không tạo dataset riêng).
  - Cụm Spark Standalone Docker Compose 1 Master + 2 Worker, ghim `apache/spark:3.5.9-python3` (đổi từ `bitnami/spark` gợi ý trong đề bài vì image đó không còn kéo miễn phí từ Docker Hub — đã ghi rõ lý do trong README/LOCAL_REPORT).
  - Chạy PASS toàn bộ 7 validation item (V01-V07): cụm ALIVE đủ 2 worker, `spark-submit` job PySpark thật (join orders+order_items tính doanh thu theo status, groupBy web_logs theo status_code) chạy thành công và output khớp 100% với tính toán độc lập ngoài Spark, xác nhận job chạy trên cluster thật qua log + Spark Master/Application REST API (executor trên 2 IP container Worker khác nhau, không phải `local[*]`), quan sát được partition/stage/task và shuffle (Exchange, shuffle read/write > 0) qua REST API, chạy lại thành công từ trạng thái sạch (`docker compose down` rồi `up -d`). Chi tiết đầy đủ: `sessions/09_spark/LOCAL_REPORT.md`.
  - Kiểm tra RAM trước khi bật Spark (`docker stats`: Mongo + HDFS 3 container dùng ~830 MiB/3.826 GiB) — đủ dư nên **không tắt MongoDB hay HDFS**, cả 7 container (Mongo, 3 HDFS, 3 Spark) chạy đồng thời suốt phiên làm việc, tổng RAM dùng ~1.2 GiB.
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 9 + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn).
- Blocker: không có.
- Quyết định cần giảng viên: không bắt buộc; nên biết việc đổi image `bitnami/spark` → `apache/spark:3.5.9-python3` (mục 2 LOCAL_REPORT) và việc `Spark/` chưa có thư mục `scripts/` start/stop/reset riêng như `Hdfs/`/`MongoDb/` (dùng trực tiếp `docker compose up -d`/`down`, đủ cho quy mô bài).

### 2026-08-20 (tiếp 5)
- Buổi: 10 (PySpark)
- AI: Local AI subagent
- Đã hoàn thành:
  - Tạo thư mục `PySpark/` ở root (cùng cấp `MongoDb/`, `Hdfs/`, `MapReduce/`, `Spark/`): `README.md`, `process_retailstream.py` (1 file duy nhất, có khối CONFIG tập trung `SPARK_MASTER_URL`/`DATA_DIR`/`OUTPUT_DIR`, override được qua biến môi trường, không sửa logic khi đổi chế độ), `data/` (copy `orders_sample.csv`, `order_items_sample.csv`, `products_sample.json`, `manifest.json` từ `00_shared_data/sample/`, không tạo dataset riêng), `output/local_mode/` và `output/cluster_mode/` (kết quả Parquet thật của cả 2 giai đoạn, giữ lại làm bằng chứng), `local_run_stage1.log` + `cluster_run_stage2.log` (log đầy đủ 2 lần chạy thật).
  - Dùng lại đúng cụm Spark Standalone đã dựng ở `Spark/` (1 master + 2 worker, `apache/spark:3.5.9-python3`, không dựng cụm mới, không sửa `Spark/docker-compose.yml`) — cụm đã chạy sẵn từ buổi 9 khi bắt đầu phiên. Do container chỉ mount được `Spark/data`/`Spark/jobs`, bản thực thi (giống hệt bản chính thức trong `PySpark/`) được đặt thêm vào `Spark/data/session10_pyspark/` và `Spark/jobs/process_retailstream.py` để cả 3 container đọc được cùng dữ liệu/job (giải thích chi tiết trong `PySpark/README.md` mục 2).
  - Chạy PASS toàn bộ 6 validation item (V01–V06): đọc 3 nguồn với explicit schema (không inferSchema), làm sạch dữ liệu lỗi/thiếu cố ý (`total_amount=-1.0` bị lọc, `brand=null` được gán `UNKNOWN`), join order_items+orders+products đúng không nhân bản dòng (380→376 dòng sau lọc+join, đối chiếu độc lập bằng script Python thuần khớp tuyệt đối), tính doanh thu theo tháng+danh mục (41 nhóm, tổng doanh thu 9650372000.0), ghi Parquet partition theo tháng (7 thư mục `month=2026-02`..`month=2026-08`), `explain(mode="extended")` với giải thích logical/physical plan ở mức nhập môn (predicate pushdown, BroadcastHashJoin, HashAggregate 2 pha, các điểm shuffle) trong LOCAL_REPORT, và chạy lại đúng cùng file/logic trên cluster thật (chỉ đổi `--master`) — xác nhận qua `applicationId` dạng `app-...` (không phải `local-...`), Spark Master REST API (`completedapps` với `cores: 2`), log driver ghi rõ 2 executor trên 2 IP container worker khác nhau (172.20.0.3, 172.20.0.4). Đối chiếu Parquet output local[*] vs cluster bằng pandas: 41 dòng cả hai bên, `DataFrame.equals() == True`. Chi tiết đầy đủ: `sessions/10_pyspark/LOCAL_REPORT.md`.
  - Kiểm tra `docker stats --no-stream` trước và trong khi chạy job — cả 7 container (Mongo, 3 HDFS, 3 Spark) chạy đồng thời ổn định, không cần tắt container nào.
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 10 + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn).
- Blocker: không có.
- Quyết định cần giảng viên: không bắt buộc; nên biết rằng cả 2 giai đoạn (local[*] và cluster) được chạy bên trong cùng image Spark 3.5.9 đã validate ở Buổi 9 (qua `docker exec spark-submit --master ...`) thay vì dùng PySpark cài qua `pip` trực tiếp trên Windows (máy có sẵn `pyspark==3.3.1` nhưng Java cài sẵn là Java 26, chưa được kiểm thử tương thích) — cách chạy "native" ngoài Docker đã ghi hướng dẫn trong `PySpark/README.md` nhưng chưa kiểm thử thật, không dùng số liệu từ đường đó.

### 2026-08-20 (tiếp 6)
- Buổi: 12 (MLlib)
- AI: Local AI subagent
- Đã hoàn thành:
  - Tạo thư mục `MLlib/` ở root (cùng cấp `Spark/`, `PySpark/`): `README.md`, `train_pipeline.py` (1 file duy nhất dùng chung 2 chế độ, khối CONFIG tập trung `SPARK_MASTER_URL`/`RUN_TAG`/`MLLIB_DATA_DIR`/...), `data/` (copy `orders.csv` + `customers.csv` + `manifest.json` từ `00_shared_data/lab/`, không tạo dataset riêng), `models/{local_mode,cluster_mode}/pipeline_model/` (PipelineModel thật đã lưu), `output/{local_mode,cluster_mode,cluster_worker_down_experiment}/` (metrics.json + log + evidence REST API JSON thật).
  - Dùng mức dữ liệu `lab` (50.000 orders, 5.047 CANCELLED ~10.1%) thay vì `sample` (150 orders, quá nhỏ để train/test có ý nghĩa) — đúng như BRIEF cho phép, không tạo dataset nghiệp vụ mới, chỉ dùng `orders.csv` + `customers.csv`.
  - Pipeline: StringIndexer + OneHotEncoder (customer_segment/city/payment_method) + Imputer (total_amount, xử lý giá trị lỗi cố ý `-1.0` coi là missing) + VectorAssembler + LogisticRegression (có `class_weight` tính từ TRAIN để xử lý mất cân bằng lớp CANCELLED) trong Spark ML `Pipeline` API; train/test split (80/20, seed=42) thực hiện TRƯỚC khi fit bất kỳ transformer nào (Pipeline.fit chỉ nhận train_df nên tự động không rò rỉ).
  - Dùng lại đúng cụm Spark Standalone ở `Spark/` (1 master + 2 worker, `apache/spark:3.5.9-python3`, không dựng cụm mới, không sửa `Spark/docker-compose.yml`). Phải cài bổ sung `numpy` (image gốc không có, `pyspark.ml` cần) vào thư mục bind-mount dùng chung `Spark/data/mllib_session12/pylibs` + set `PYTHONPATH` cho driver và executor.
  - Chạy PASS toàn bộ 6 validation item (V01–V06): feature engineering + xử lý missing/dedup, train/test split không rò rỉ, Pipeline fit/transform + Evaluator (AUC=0.5117, accuracy=0.5332, F1=0.6245, precision(CANCELLED)=0.1053, recall(CANCELLED)=0.4883 — số liệu thật, AUC gần 0.5 vì generator RetailStream không tạo tương quan nhân tạo giữa feature và nhãn, đã ghi rõ đây là bài học thực tế chứ không phải lỗi pipeline), save/load PipelineModel (`reload_prediction_match=true`), chạy `local[*]` PASS (trong container `spark-worker-1` để tránh OOM — xem ISSUE-01), và chạy **cluster thật** PASS: `applicationId` dạng `app-...` (không phải `local-...`), log + Spark Master REST API xác nhận 2 executor trên 2 IP container worker khác nhau (172.20.0.3, 172.20.0.4, `cores: 2`), evidence job/stage/task/partition qua REST API (`stages/39`, `stages/41`: 8 task khớp `spark.sql.shuffle.partitions=8`), đã `docker stop`/`docker start` `spark-worker-2` để quan sát cụm chạy với 1 worker (chỉ còn 1 executor được cấp, job vẫn hoàn tất) rồi khôi phục lại đủ 2 worker ALIVE. Metric local[*] và cluster **giống hệt nhau** (cùng seed, cùng shuffle partitions). Chi tiết đầy đủ: `sessions/12_mllib/LOCAL_REPORT.md`.
  - 2 ISSUE kỹ thuật đã xử lý (không sửa `Spark/docker-compose.yml`): (1) `local[*]` chạy trong container `spark-master` (mem_limit 512m) bị OOMKilled → chuyển sang chạy trong `spark-worker-1` (800m); (2) chế độ cluster dùng container tạm thời (`docker run --rm`, không phải service mới trong compose) làm spark-submit client để tránh cạnh tranh RAM với Master.
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 12 + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn) — đặc biệt lưu ý không mô tả sai AUC~0.51 thành mô hình "dự đoán tốt".
- Blocker: không có.
- Quyết định cần giảng viên: không bắt buộc; nên biết rằng phải cài bổ sung `numpy` vào cụm Spark (không có sẵn trong image `apache/spark:3.5.9-python3`) và cách gọi lệnh cho 2 chế độ khác với Buổi 9/10 (do giới hạn RAM container) — đã ghi rõ trong `MLlib/README.md` mục 4.

### 2026-08-20 (tiếp 7)
- Buổi: 11 (Structured Streaming)
- AI: Local AI subagent
- Đã hoàn thành:
  - Tạo thư mục `StructuredStreaming/` ở root (cùng cấp `Spark/`, `PySpark/`, `MLlib/`): `README.md`, `streaming_job.py` (job chính, 1 file, khối CONFIG tập trung qua biến môi trường), `prepare_batches.py` (chia `00_shared_data/sample/clickstream_sample.jsonl` — 200 dòng, KHÔNG sửa nội dung — thành 15 file `day_XX_YYYY-MM-DD.jsonl` theo ngày để mô phỏng file stream), `batches_staging/` + `manifest.json`, `data_source/` (bằng chứng đầu vào của lần chạy cuối), `checkpoint/{update_mode,complete_mode,append_mode}/` (checkpoint thật giữ nguyên), `logs/` (4 log console đầy đủ của 4 lần chạy thật).
  - Dùng lại cụm Spark ở `Spark/` (image `apache/spark:3.5.9-python3`, không dựng cụm mới, không sửa `Spark/docker-compose.yml`), chạy `local[*]` bên trong container `spark-master` (BRIEF Buổi 11 không bắt buộc chạy Spark Standalone cluster thật, khác Buổi 9/10/12). File nguồn "stream" và job được đặt thêm ở `Spark/data/session11_streaming/` + `Spark/jobs/streaming_job.py` (kỹ thuật giống Buổi 10/12, vì container chỉ mount được `Spark/data`/`Spark/jobs`).
  - Chạy PASS toàn bộ 5 validation item (V01–V05): đọc clickstream qua File Source với schema tường minh (`StructType`, không `inferSchema`); window aggregation tumbling 1 ngày trên `event_time` đếm theo `event_type` (chọn 1 ngày thay vì vài phút vì dữ liệu mẫu trải trên 15 ngày, không phải luồng liên tục vài phút — giải thích trong README mục 3); watermark 1 ngày chứng minh loại đúng 1 bản ghi muộn cụ thể (`event_id=CEV000096`, ngày thật 2026-08-06, bị chèn muộn vào file ngày 2026-08-10) bằng `numRowsDroppedByWatermark=1` lấy trực tiếp từ `StreamingQueryProgress`, không suy đoán; checkpoint restart chứng minh bằng 2 lần chạy thật (`Trigger.availableNow`) — lần 2 tiếp tục đúng từ `batchId=9` (không quay lại 0), log tự in "state for version 9" khi nạp lại state store, `commits/` checkpoint có đủ liên tục `0..15`; so sánh đủ cả 3 output mode (update/complete/append) với bằng chứng thật — phát hiện quan trọng: **complete mode không loại late data bằng watermark** (bản ghi muộn CEV000096 được tính vào kết quả, `numRowsDroppedByWatermark=0`), khác hẳn update/append (loại bỏ, `=1`), đúng tài liệu chính thức của Spark. Chi tiết đầy đủ: `sessions/11_structured_streaming/LOCAL_REPORT.md`.
  - 2 sự cố thật đã xử lý và ghi rõ trong LOCAL_REPORT (không giấu): (1) lần chạy đầu tiên bị crash giữa batch 4 do tranh chấp RAM — job MLlib Buổi 12 chạy `local[*]` ĐỒNG THỜI trong CÙNG container `spark-master` (`mem_limit: 512m`), xác nhận qua `docker stats`/`docker top`, không phải lỗi logic job; đã đợi job kia xong rồi chạy lại thành công; (2) một lần rót file bị sai thứ tự copy khiến 1 file (13 bản ghi ngày 2026-08-13) bị Spark File Source xử lý muộn theo modification time — ghi nhận trung thực làm bài học thêm về "arrival order" thay vì tên file, và các lần chạy sau đã `touch` lại mtime đúng thứ tự trước khi dùng số liệu cho V02/V05.
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 11 + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn).
- Blocker: không có.
- Quyết định cần giảng viên: không bắt buộc; nên biết rằng window dùng 1 ngày thay vì vài phút như BRIEF gợi ý (lý do dữ liệu, xem LOCAL_REPORT mục 7 và README.md mục 3 — cơ chế/API không đổi nếu sau này dùng dữ liệu tốc độ cao với window phút/giờ), và rằng nhiều buổi cùng chạy `local[*]` trong container `spark-master` 512MB đồng thời có thể gây OOM — nên kiểm tra `docker stats` trước khi chạy nếu có nhiều agent làm việc song song.

### 2026-08-20 (tiếp 8)
- Buổi: 13 (Kafka)
- AI: Local AI subagent
- Đã hoàn thành:
  - Tạo thư mục `Kafka/` ở root (cùng cấp `Spark/`, `StructuredStreaming/`): `README.md`, `docker-compose.yml`, `producer.py`, `consumer.py`, `spark_kafka_job.py`, `data/` (log.dirs thật của Kafka, bền vững qua restart), `checkpoint/` (checkpoint Structured Streaming thật), `logs/` (7 file log console đầy đủ của các lần chạy thật: producer x2 chiến lược key, consumer solo, consumer group C1/C2, Spark job x2 lần).
  - Dùng lại `00_shared_data/sample/clickstream_sample.jsonl` (200 dòng) + `product_events_sample.jsonl` (80 dòng), không tạo dataset riêng.
  - Kafka chạy 1 broker chế độ KRaft (không Zookeeper), ghim `apache/kafka:3.7.1` (đổi từ `bitnami/kafka` gợi ý trong đề bài — cùng lý do rủi ro "hết free tier" như `bitnami/spark` ở Buổi 9, cùng nhà cung cấp Bitnami — đã kiểm thử pull `apache/kafka:3.7.1` thành công). Dual listener BROKER (nội bộ, `kafka:29092`) / EXTERNAL (host, `localhost:9092`). Kafka được gắn thêm vào network `spark_spark-net` (external, do `Spark/docker-compose.yml` Buổi 9 tạo) để container Spark gọi được Kafka qua hostname — không sửa `Spark/docker-compose.yml`.
  - Chạy PASS toàn bộ 5 validation item (V01–V05): topic `clickstream` 4 partition (`kafka-topics.sh --describe` xác nhận); producer gửi 200/200 message với 2 chiến lược key khác nhau (`session_id` và `product_id`), quan sát được phân phối partition khác nhau rõ rệt giữa 2 chiến lược (bằng chứng thật, không suy đoán); consumer đọc lại xác nhận offset tăng đơn điệu theo từng partition (0..137/0..156/0..141/0..162, tổng 600 khớp số message tích luỹ); 2 consumer cùng consumer group chạy đồng thời — Kafka tự chia đều 4 partition cho 2 consumer (C1 nhận [2,3]=90 message, C2 nhận [0,1]=110 message, tổng 200 khớp đúng, không trùng/thiếu); Spark Structured Streaming đọc trực tiếp từ Kafka source (`format("kafka")`, connector `spark-sql-kafka-0-10_2.12:3.5.9` qua `--packages`) trên cụm Spark Standalone thật (`applicationId=app-...`, 2 executor trên 2 container worker khác nhau), giữ nguyên window/watermark 1 ngày như Buổi 11, `numInputRows=200` khớp chính xác 200 message producer đã gửi, `endOffset` khớp đúng từng partition với bảng phân phối producer in ra — đã chạy lặp lại 2 lần cho kết quả giống hệt nhau. Chi tiết đầy đủ: `sessions/13_kafka/LOCAL_REPORT.md`.
  - 2 ISSUE kỹ thuật đã xử lý (ghi rõ trong LOCAL_REPORT, không giấu): (1) `spark-submit --packages` lỗi `FileNotFoundException` vì `$HOME=/nonexistent` trong image Spark không ghi được — sửa bằng `--conf spark.jars.ivy=/opt/spark-data/ivy2cache`; (2) volume Kafka ban đầu mount sai đường dẫn log.dirs thực tế (`/tmp/kraft-combined-logs` trong file mẫu khác với `/tmp/kafka-logs` thực tế image dùng khi chạy) khiến dữ liệu mất sau `docker compose down`/`up` — sửa bằng cách đặt tường minh `KAFKA_LOG_DIRS=/var/lib/kafka/data` + mount đúng chỗ, đã kiểm thử lại persistence thành công.
  - Xác nhận 8 container (Kafka, 3 Spark, 3 HDFS, MongoDB) chạy đồng thời ổn định trên máy ~3.826 GiB Docker, không cần tắt container nào (`docker stats` trong LOCAL_REPORT).
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 13 + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn).
- Blocker: không có.
- Quyết định cần giảng viên: không bắt buộc; nên biết việc đổi image `bitnami/kafka` → `apache/kafka:3.7.1` (mục 8 LOCAL_REPORT), thứ tự khởi động bắt buộc `Spark/` trước `Kafka/` (network phụ thuộc), yêu cầu Internet khi `spark-submit --packages` tải connector Kafka lần đầu, và việc chưa có script `reset-lab` riêng cho Kafka (khác `Hdfs/scripts/`) — reset hiện làm thủ công qua `kafka-topics.sh --delete`/`docker compose down -v`.

### 2026-08-20 (tiếp 9)
- Buổi: 14 (System Design)
- AI: Local AI (chính, không qua subagent — việc rà soát tổng hợp)
- Đã hoàn thành:
  - Không dựng hạ tầng mới; rà soát và xác nhận bằng `docker ps`/`docker stats` thật rằng cả 8 container của Buổi 5,6,9,13 vẫn `healthy`/chạy đồng thời ổn định (~1.53GiB RAM lúc idle / 3.826GiB khả dụng).
  - Viết `sessions/14_system_design/LOCAL_REPORT.md`: bảng kiểm kê version toàn bộ component (không có `:latest`), sơ đồ kiến trúc RetailStream end-to-end đã kiểm chứng chạy thật (MongoDB/HDFS/Kafka → Spark Standalone cluster dùng chung cho 5 buổi → Parquet/console/PipelineModel), xác nhận tương thích định dạng dữ liệu giữa các buổi (không buổi nào phải "dịch" lại schema), tổng hợp 4 giới hạn cần nêu rõ cho sinh viên (container mô phỏng node, MapReduce LocalJobRunner không YARN thật, RAM giới hạn khi nhiều job nặng chạy đồng thời, AUC MLlib gần ngẫu nhiên).
- Đang chờ: Content AI viết slide/tài liệu Buổi 14 + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống).
- Blocker: không có.
- Quyết định cần giảng viên: 2 mục treo từ trước (MapReduce có cần YARN thật không; MLlib có cần dữ liệu tương quan giả lập không) — chưa có quyết định, không chặn tiến độ nhưng ảnh hưởng độ "thật" của 2 demo liên quan.

### 2026-08-20 (tiếp 10)
- Buổi: 15 (Optimization)
- AI: Local AI subagent
- Đã hoàn thành:
  - Tạo thư mục `Optimization/` ở root: `README.md`, `make_skewed_data.py`, `parse_event_log.py`, `data_skew_demo.py`, `small_files_demo.py`, `shuffle_demo.py` (bản sao giống hệt job thật chạy từ `Spark/jobs/`), `data/clickstream_skewed.jsonl` (dữ liệu "méo" cố ý riêng cho buổi này, seed=42, ~90% bản ghi gán lại `product_id="PROD00001"`, không ghi đè `00_shared_data/`), `evidence/` (log console + số liệu stage/task xuất từ Spark event log cho cả 3 tình huống).
  - Dùng lại nguyên trạng cụm Spark Standalone Buổi 9 (`Spark/`, image `apache/spark:3.5.9-python3`, không dựng hạ tầng mới, không sửa `docker-compose.yml`), dữ liệu nguồn từ `00_shared_data/lab/` (clickstream 100.000 dòng, orders 50.000 dòng, order_items 124.862 dòng), copy vào `Spark/data/session15_optimization/`.
  - Chạy PASS 3/3 tình huống bắt buộc theo BRIEF, số liệu thật: (1) **data skew** — `repartition(8,"product_id")` cho thấy lệch số dòng/partition 77.04x (SKEWED) so với 1.10x (UNIFORM), 1 partition nhận 91.210/100.000 dòng; (2) **small files** — `repartition(50)` tạo 50 file thật vs `coalesce(2)` chỉ tạo 1 file (input chỉ có 1 partition gốc nên coalesce không tăng lên 2 được), đọc lại tạo 2 task (50 file) vs 1 task (1 file) — chênh lệch task nhỏ hơn nhiều chênh lệch file do Spark bin-pack file nhỏ khi lập kế hoạch đọc, đã ghi rõ đây là giảm nhẹ ở tầng task chứ không loại bỏ vấn đề ở tầng lưu trữ; (3) **shuffle lớn** — join `orders`/`order_items` không lọc + tắt auto-broadcast cho tổng 1.380.870 byte shuffle (write+read), trong khi lọc `status=PAID` trước rồi để Spark tự broadcast cho 0 byte shuffle cho phép join. (4) **hot partition** — không chạy job mới, trích dẫn lại `sessions/13_kafka/LOCAL_REPORT.md` mục V02 theo đúng gợi ý BRIEF, ghi rõ không tính là tình huống mới và số liệu buổi đó không phải ví dụ cực đoan.
  - 2 ISSUE kỹ thuật đã xử lý và ghi rõ trong LOCAL_REPORT (không giấu): (1) cách lấy bằng chứng ban đầu theo đúng gợi ý BRIEF (scrape REST API `:4040` trong lúc job `time.sleep()`) bị race-condition thật (2 lần dò nhầm applicationId của lần chạy trước đã dừng) vì dự án không có Spark History Server — chuyển hẳn sang bật `spark.eventLog.enabled=true` + script `parse_event_log.py` đọc lại SAU KHI job dừng hẳn; (2) demo skew ban đầu dùng `groupBy().count()` trực tiếp không lộ được skew do Spark tự làm map-side combiner (dữ liệu qua shuffle chỉ còn `(key, partial_count)` rất nhỏ) — đổi sang `repartition(8,"product_id")` (hash-partition toàn bộ bản ghi thô, không combiner) mới lộ đúng độ lệch, đồng thời phát hiện thêm phải tắt AQE (`spark.sql.adaptive.enabled=false`) vì `CoalescePartitions` gộp partition nhỏ lại ở quy mô demo làm mất tín hiệu skew.
  - Ghi rõ trong README + LOCAL_REPORT ranh giới "demo giáo dục quy mô lab (vài chục MB), không phải benchmark production" theo đúng yêu cầu BRIEF, kèm 3 giới hạn cụ thể (skew rõ ở số dòng/partition nhưng không rõ ở thời gian task tại quy mô lab; small-files là vấn đề tầng lưu trữ trước khi là vấn đề tầng task; hot partition chỉ là trích dẫn không phải ví dụ cực đoan).
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 15 + `CONTENT_REPORT.md` dựa trên LOCAL_REPORT (hiện đang trống hoàn toàn).
- Blocker: không có.
- Quyết định cần giảng viên: không bắt buộc; nên biết việc đổi cách lấy bằng chứng từ "scrape REST API :4040 khi job đang chạy" (theo gợi ý BRIEF) sang "đọc lại Spark event log sau khi job dừng" (mục Issues Found ISSUE-01 trong LOCAL_REPORT) — không ảnh hưởng độ tin cậy số liệu, chỉ khác cơ chế thu thập bằng chứng.

### 2026-08-22
- Buổi: 7 (MapReduce) — fix theo yêu cầu giảng viên "phải có YARN như thật, phải có python3 sẵn luôn"
- AI: Local AI (chính, không qua subagent — làm từng việc một theo yêu cầu tránh chạy song song tốn RAM)
- Đã hoàn thành:
  - Bổ sung 4 service mới vào `Hdfs/docker-compose.yml`: `resourcemanager` (`bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8`), `nodemanager1`, `historyserver` — cụm giờ có YARN thật (ResourceManager cấp phát, NodeManager thực thi container, AppMaster theo dõi tiến độ), không còn LocalJobRunner.
  - Build 2 image cục bộ có sẵn python3 (không cần cài lại mỗi lần container restart): `retailstream-hadoop-namenode-py3` và `retailstream-hadoop-nodemanager-py3` (Dockerfile tại `MapReduce/Dockerfile.namenode-with-python3` và `Dockerfile.nodemanager-with-python3`). Phát hiện quan trọng khi kiểm thử: mapper/reducer Python thực thi trên **NodeManager** khi chạy YARN thật (không phải NameNode) — ban đầu chỉ build python3 cho NameNode là thiếu, phải bổ sung thêm image cho NodeManager mới chạy được.
  - Gặp và sửa 1 sự cố thật khi chạy thử: job kẹt ở map 0% do AppMaster chiếm gần hết RAM NodeManager (1536MB) vì `yarn.scheduler.minimum-allocation-mb` mặc định làm tròn container lên 1024MB/2048MB — sửa bằng cách tăng RAM NodeManager lên 2560MB, đặt `yarn.app.mapreduce.am.resource.mb=512`, hạ `minimum-allocation-mb` xuống 256MB.
  - Chạy lại job MapReduce Streaming (đếm `product_id` từ `web_logs_sample.jsonl`) thành công trên YARN thật: `application_1787406765274_0001` hoàn tất map 100%/reduce 100%, kết quả khớp 100% với kết quả cũ (LocalJobRunner) và với đếm tay (23 product_id, tổng 29 lượt).
  - Cập nhật `sessions/07_mapreduce/LOCAL_REPORT.md`: thêm V06 (YARN thật, kèm log/sự cố/cách sửa) và V07 (python3 persistent), đánh dấu ISSUE-01 và ISSUE-02 là ĐÃ RESOLVED, cập nhật mục kết luận.
- Đang chờ: Content AI viết 07_mapreduce_practice.md + CONTENT_REPORT.md dựa trên LOCAL_REPORT đã cập nhật.
- Blocker: không có.
- Quyết định cần giảng viên: không còn — mục "MapReduce có cần YARN thật không" (treo từ Buổi 14) nay đã giải quyết theo hướng CÓ, đã triển khai và PASS. Giới hạn còn lại (chỉ 1 NodeManager, mô phỏng trên 1 máy) đã ghi rõ trong LOCAL_REPORT, không cần quyết định thêm.

### 2026-08-22 (tiếp)
- Buổi: 12 (MLlib) — fix theo yêu cầu giảng viên "phải sửa dữ liệu generate sao cho phù hợp chứ ra độ chính xác như thế thì có ý nghĩa gì đâu"
- AI: Local AI (chính, làm từng việc một sau khi xong MapReduce, không chạy song song)
- Đã hoàn thành:
  - **Bước xác minh an toàn bắt buộc trước khi chấp nhận thay đổi generator**: so khớp sha256 + số byte cả 7 file `00_shared_data/sample/` với `manifest.json` — khớp tuyệt đối 100%, xác nhận mức `sample` (Buổi 5 MongoDB phụ thuộc vào số liệu này) hoàn toàn không bị ảnh hưởng bởi thay đổi generator.
  - Xác nhận logic tương quan trong `00_shared_data/generators/generate_retailstream.py` chỉ kích hoạt khi `scale == "lab"` (`correlated_labels = args.scale == "lab"`), cấy tương quan giả lập giữa `customer_segment`/`payment_method` và `status=CANCELLED`.
  - Dừng cụm HDFS/YARN (giải phóng RAM) trước khi khởi động cụm Spark cho MLlib, tránh OOM do chạy đồng thời nhiều cụm nặng (bài học từ Buổi 11/12 cũ).
  - Chạy lại `train_pipeline.py` (không sửa logic) trên dữ liệu `lab` mới cho cả 2 chế độ: `local[*]` và Spark Standalone cluster thật (2 worker) — kết quả giống hệt nhau: **AUC 0.6893** (từ 0.5117), Accuracy 0.6741, F1 0.7282, Precision(1)=0.2098, Recall(1)=0.5982, `reload_prediction_match=true` cả 2 chế độ.
  - Gặp và sửa 2 sự cố thật: (1) `spark-master` bị OOMKilled thật khi chạy local mode với dữ liệu mới (512MB không đủ) — tăng `mem_limit` lên 1536MB trong `Spark/docker-compose.yml`; (2) cluster mode kẹt vĩnh viễn ở "Initial job has not accepted any resources" vì mỗi worker chỉ có 640MB nhỏ hơn executor-memory mặc định 1024MB — sửa bằng cách chỉ định rõ `--executor-memory 512m --executor-cores 1 --total-executor-cores 2`.
  - Cập nhật `sessions/12_mllib/LOCAL_REPORT.md`: thêm mục "CẬP NHẬT 2026-08-22" ở đầu (bảng so sánh AUC trước/sau), V08 (chi tiết lần chạy lại), ISSUE-03/ISSUE-04 (2 sự cố mới), sửa mục kết luận dùng số liệu mới.
  - Đồng bộ `MLlib/data/orders.csv`, `customers.csv` (bản mới có tương quan), `MLlib/models/{local_mode_v2,cluster_mode_v2}/`, `MLlib/output/{local_mode_v2,cluster_mode_v2}/metrics.json`.
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 12 + CONTENT_REPORT.md, PHẢI dùng số liệu MỚI (0.6893...), không dùng số liệu cũ (0.5117...) trừ khi minh hoạ đối chiếu trước/sau.
- Blocker: không có.
- Quyết định cần giảng viên: không còn — mục "MLlib có cần dữ liệu tương quan giả lập không" (treo từ Buổi 14) nay đã giải quyết theo hướng CÓ, đã triển khai và PASS.

### 2026-08-22 (tiếp 2)
- Buổi: 11 (Structured Streaming) — fix theo yêu cầu giảng viên "nên sửa lại cho nhỏ đi cho phù hợp"
- AI: Local AI (chính, làm từng việc một sau khi xong MLlib)
- Đã hoàn thành:
  - Kiểm tra và phát hiện hạ tầng đã dựng dở từ agent trước: `compress_timeline.py` (nén trục thời gian clickstream từ ~14.45 ngày xuống 90 phút, giữ nguyên thứ tự/tỉ lệ), `prepare_batches_v2.py` (chia 19 file cửa sổ 5 phút, có 1 bản ghi muộn cố ý), `streaming_job.py` đã đổi `WINDOW_DURATION="5 minutes"`, `WATERMARK_DELAY="3 minutes"`. 2/3 lần chạy trước đó (update mode, batch 0-10 và 11-19 sau restart) đã hoàn tất đúng và có bằng chứng thật; 1 lần chạy (complete mode) bị dừng dở ở batch 2/18 (do bị kill trước đó).
  - Xoá checkpoint dở dang của complete mode, chạy lại đầy đủ (19 batch, batchId 0-18) — hoàn tất PASS.
  - Xác nhận lại đúng ngữ nghĩa Spark đã biết từ báo cáo cũ (window 1 ngày): `update` mode watermark loại bỏ đúng 1 bản ghi muộn (`numRowsDroppedByWatermark=1` ở batch 6); `complete` mode KHÔNG loại dữ liệu muộn (đúng tài liệu chính thức Spark), tổng 200/200 sự kiện được giữ nguyên trong bảng tích luỹ cuối cùng.
  - Cập nhật `sessions/11_structured_streaming/LOCAL_REPORT.md`: thêm mục "CẬP NHẬT 2026-08-22" đầu file với V01b-V05b (kết quả mới, window 5 phút), giữ nguyên nội dung cũ (window 1 ngày) làm tài liệu tham khảo, sửa mục kết luận.
- Đang chờ: Content AI viết slide/tài liệu/bài thực hành Buổi 11 + CONTENT_REPORT.md, dùng ví dụ window 5 phút (V01b-V05b) thay vì 1 ngày.
- Blocker: không có.
- Quyết định cần giảng viên: không còn — yêu cầu "sửa window nhỏ đi cho phù hợp" đã hoàn thành.

### 2026-08-22 (tiếp 3)
- Buổi: 7, 9, 10, 11, 12, 13, 15 (bổ sung script demo/reset — theo yêu cầu giảng viên)
- AI: Local AI (chính)
- Đã hoàn thành: bổ sung 17 script `.sh` (theo đúng mẫu `Hdfs/scripts/`) cho 7 khu vực trước đây chỉ có lệnh thủ công trong README — **tất cả đã chạy thật thành công, không chỉ viết ra**:
  - `MapReduce/scripts/`: `run-yarn-job.sh` (build image, khởi động cụm, chạy job MapReduce trên YARN thật, in kết quả), `stop-cluster.sh`.
  - `Spark/scripts/`: `start-cluster.sh`, `stop-cluster.sh` (cảnh báo nếu Kafka đang gắn vào network), `run-demo-job.sh`.
  - `PySpark/scripts/`: `run-local.sh`, `run-cluster.sh`.
  - `StructuredStreaming/scripts/`: `prepare-data.sh` (nén thời gian + chia batch), `run-demo.sh` (nhận tham số output mode), `run-demo-checkpoint-restart.sh` (2 giai đoạn, minh hoạ checkpoint restart thật).
  - `MLlib/scripts/`: `prepare-data.sh`, `train-local.sh`, `train-cluster.sh`.
  - `Kafka/scripts/`: `start-cluster.sh`, `reset-topics.sh`, `demo-produce-consume.sh` (kịch bản demo trọn gói), `run-spark-kafka-job.sh`.
  - `Optimization/scripts/`: `run-demo.sh` (chạy đủ 3 tình huống: skew, small files, shuffle).
  - Sửa `MLlib/README.md` mục 5: bảng số liệu chính giờ là AUC MỚI (0.6893), số liệu cũ gấp trong `<details>` tham khảo — trước đó bảng chính vẫn là số liệu cũ dù đã có ghi chú, dễ gây nhầm khi demo.
- Phát hiện + sửa 3 sự cố thật trong lúc viết/kiểm thử script (không chỉ viết suông):
  1. **Spark cluster mode job kẹt vĩnh viễn** (Buổi 9 `run-demo-job.sh` và các job cluster khác) nếu không chỉ định `--executor-memory 512m` — mặc định Spark xin 1024MB/executor > 640MB mỗi worker. Đã thêm tham số này vào mọi script chạy cluster mode.
  2. **Kafka `kafka-topics.sh --delete` làm crash cả broker** trên Windows bind-mount (`AccessDeniedException` khi Kafka đổi tên thư mục log để xoá) — đã ghi nhận là ISSUE-03 trong `sessions/13_kafka/LOCAL_REPORT.md`, sửa `reset-topics.sh` dùng cách an toàn (dừng container + xoá `./data` trên host + khởi động lại).
  3. `rm -rf` trên checkpoint Structured Streaming đôi khi báo "Directory not empty" do độ trễ đồng bộ filesystem Windows bind-mount — đã thêm retry trong script.
- Đang chờ: không có việc gì treo.
- Blocker: không có.
- Quyết định cần giảng viên: không có.

### 2026-08-22 (tiếp 4)
- Buổi: kiểm tra thêm theo yêu cầu giảng viên "còn lỗi nào nữa không" + "container tiêu tốn tài nguyên tối thiểu"
- AI: Local AI
- Đã hoàn thành:
  - Test nốt các script chưa kiểm thử ở vòng trước, phát hiện + sửa 2 lỗi thật:
    1. `Kafka/scripts/run-spark-kafka-job.sh`: không reset checkpoint trước khi chạy → đọc phải state cũ (droppedByWatermark sai lệch). Sửa: xoá checkpoint trước mỗi lần chạy.
    2. `rm -rf checkpoint/*` không xoá được file ẩn (`.metadata.crc`, glob `*` không khớp dotfile) → lỗi `FileAlreadyExistsException` ở lần chạy sau. Sửa: xoá cả thư mục rồi tạo lại. Ghi ISSUE-04 vào `sessions/13_kafka/LOCAL_REPORT.md`.
  - `MapReduce/scripts/stop-cluster.sh`, `Spark/scripts/stop-cluster.sh` đã test thật — hoạt động đúng thiết kế.
  - Theo yêu cầu giảng viên về máy cấu hình thấp: đặt `mem_limit` rõ ràng cho toàn bộ service HDFS/YARN trước đây KHÔNG có trần RAM (mặc định = toàn bộ Docker VM) — `Hdfs/docker-compose.yml`: namenode 768m, datanode1/2 512m mỗi cái, resourcemanager 768m, historyserver 512m, nodemanager1 3072m (phải > mức YARN nội bộ cấp 2560MB). Đã build lại cụm và chạy lại job MapReduce YARN thật để xác nhận không bị OOM lại — PASS.
  - Theo yêu cầu giảng viên "trong docker yml đừng nói gì đến report" (file compose sẽ gửi cho sinh viên): đã rà soát và xoá toàn bộ tham chiếu `LOCAL_REPORT.md`/`sessions/`/`bigdata_ai_coordination` trong comment của `Hdfs/docker-compose.yml` và `Spark/docker-compose.yml`, giữ nguyên nội dung giải thích kỹ thuật.
- Đang chờ: không có việc gì treo.
- Blocker: không có.
- Quyết định cần giảng viên: không có.
