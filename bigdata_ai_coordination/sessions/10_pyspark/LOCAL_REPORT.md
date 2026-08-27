# LOCAL VALIDATION REPORT – Buoi 10 (PySpark)

## 1. Trang thai

**Validation:** PASS

Ca 2 giai doan bat buoc (local[*] va Spark Standalone cluster that) da chay
that, thanh cong, va ket qua doi chieu khop 100% (xem V06).

## 2. Environment

| Thanh phan | Version |
|---|---|
| OS host | Windows 10 Pro 10.0.19045 |
| Docker | Docker Desktop, Docker Compose v2 |
| Image Spark (dung lai tu Buoi 9, khong doi) | `apache/spark:3.5.9-python3` |
| Spark | 3.5.9 (trong container, ca 2 giai doan) |
| PySpark | 3.5.9 (trong container, di kem image) |
| Python (trong container) | 3.10.12 |
| Java (trong container) | OpenJDK 11.0.31 |
| Scala | 2.12.18 |
| Cum Spark | 1 master (`spark-master`) + 2 worker (`spark-worker-1`, `spark-worker-2`), da dung san tu Buoi 9, khong dung cum moi, khong sua `Spark/docker-compose.yml` |
| Cac container khac cung chay dong thoi | `mongodb` (mongo:8.0), `hdfs-namenode`/`hdfs-datanode1`/`hdfs-datanode2` (bde2020 2.0.0-hadoop3.2.1-java8) |

**Ghi chu ve moi truong local[*] "ngoai Docker":** may kiem thu co san
`pyspark==3.3.1` qua `pip` va Java **26** (rat moi, chua duoc PySpark 3.3.x
kiem thu chinh thuc). De tranh rui ro tuong thich Java/PySpark khong lien
quan den noi dung bai hoc, **ca 2 giai doan (local[*] va cluster) deu duoc
chay ben trong cung mot image Spark da validate o Buoi 9**
(`apache/spark:3.5.9-python3`, Java 11, PySpark 3.5.9) — chi doi tham so
`--master` cua `spark-submit` tu `local[*]` sang `spark://spark-master:7077`,
dung y nghia "Giai doan 1 chay local, Giai doan 2 chay tren cluster" ma BRIEF
yeu cau, khong lien quan gi den viec co Docker hay khong (local[*] van la
mot JVM don, khong dung cac worker container). Cach chay "native" ngoai
Docker duoc ghi trong `PySpark/README.md` muc 3 nhung **chua duoc kiem thu
that** trong phien nay — khong dung so lieu tu duong do de bao cao.

## 3. Cach khoi dong

```bash
cd Spark
docker compose up -d
curl -s http://localhost:8080/json/    # xac nhan status=ALIVE, 2 worker ALIVE
```

Cum da chay san tu Buoi 9 khi bat dau phien lam viec nay (`docker ps` xac
nhan ca 7 container Mongo/HDFS/Spark deu Up), khong can khoi dong lai.

## 4. Du lieu dau vao

Dung nguyen `00_shared_data/sample/`, khong tao dataset rieng:

- `orders_sample.csv`: 150 dong, `order_id` unique 100%.
- `order_items_sample.csv`: 380 dong, moi `order_id`/`product_id` deu ton
  tai trong `orders`/`products` (kiem tra doc lap bang script Python thuan,
  khong dung Spark, truoc khi viet job — khong co dong "mo coi").
- `products_sample.json`: 60 dong, `product_id` unique 100%.

Ca 3 file duoc copy nguyen ven vao `PySpark/data/` (ban chinh thuc de doc)
va vao `Spark/data/session10_pyspark/` (ban thuc thi, vi container chi mount
duoc `Spark/data` va `Spark/jobs` — xem giai thich chi tiet trong
`PySpark/README.md` muc 2).

## 5. Validation Results

### V01 – Doc 3 nguon voi explicit schema (khong dung schema inference)

**Result:** PASS

Command:

```bash
export MSYS_NO_PATHCONV=1
docker exec -e SPARK_MASTER_URL="local[*]" spark-master /opt/spark/bin/spark-submit \
  --master local[*] --conf spark.sql.shuffle.partitions=4 --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py
```

`process_retailstream.py` khai bao `StructType`/`StructField` tuong minh cho
ca 3 nguon (`ORDERS_SCHEMA`, `ORDER_ITEMS_SCHEMA`, `PRODUCTS_SCHEMA` bao gom
struct long `attributes`), goi `spark.read.schema(...)`, **khong** goi
`inferSchema`/`option("inferSchema", true)` o bat ky dau.

Actual (trich `local_run_stage1.log`):

```text
-- orders schema (explicit) --
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- order_time: timestamp (nullable = true)
 |-- status: string (nullable = true)
 |-- payment_method: string (nullable = true)
 |-- total_amount: double (nullable = true)
orders count: 150

-- order_items schema (explicit) --
root
 |-- order_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
order_items count: 380

-- products schema (explicit) --
root
 |-- product_id: string (nullable = true)
 |-- category_id: string (nullable = true)
 |-- category_name: string (nullable = true)
 |-- product_name: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- price: double (nullable = true)
 |-- attributes: struct (nullable = true)
 |    |-- color: string (nullable = true)
 |    |-- warranty_months: integer (nullable = true)
 |-- updated_at: timestamp (nullable = true)
products count: 60
```

So dong doc duoc (150/380/60) khop 100% voi dem tay bang script Python
thuan (csv/json module) chay truoc khi viet job. Truong `order_time` va
`updated_at` (dinh dang ISO 8601 co timezone `+00:00`) doc dung thanh
`timestamp` nho `option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")`,
khong bi doc nham thanh string.

Notes: da thu bo `timestampFormat` de kiem tra — neu bo, cot van doc duoc
(Spark 3.5 tu nhan dang duoc dinh dang ISO co offset o che do mac dinh) nhung
giu option tuong minh de dam bao on dinh, dung tinh than "explicit" cua yeu
cau.

### V02 – Join 3 bang, kiem tra khong nhan ban dong

**Result:** PASS

Command: nhu V01 (cung 1 lan chay job).

Logic: `order_items` JOIN `orders` (INNER, tren `order_id`) JOIN `products`
(INNER, tren `product_id`). Truoc khi join, `orders` da bi loc bo 1 dong
loi (`total_amount = -1.0`), keo theo 4 dong `order_items` cua don do bi
loai vi INNER JOIN.

Actual (trich `local_run_stage1.log`):

```text
order_items (dau vao join): 380
sau join order_items + orders (inner, mat cac dong cua don loi da bi loc): 376
sau join them products: 376
V02 PASS: so dong khong doi sau khi join products (khong bi nhan ban do join sai / product_id khong unique).
```

Doi chieu doc lap: `order_items` co 380 dong, 4 dong co `order_id =
ORD000150` (don bi loc), 380 - 4 = 376 — khop chinh xac voi ket qua Spark.
Vi `order_id` trong `orders` va `product_id` trong `products` deu unique
100% (kiem tra V01), join 1-nhieu dung ngu nghia se khong lam thay doi so
dong cua ben "nhieu" (`order_items`) — dong bo dong `full_df_count ==
joined_orders_count` trong code chinh la phep kiem tra tu dong cho dieu nay.

### V03 – Tinh doanh thu theo thang va danh muc

**Result:** PASS

Command: nhu V01.

Logic: `line_amount = quantity * unit_price`, `month =
date_format(order_time, 'yyyy-MM')`, `groupBy(month, category_name)`,
`sum(line_amount) AS revenue`, `countDistinct(order_id) AS order_count`,
`count(*) AS line_item_count`.

Actual: 41 nhom (thang x danh muc) tu 2026-02 den 2026-08, tong doanh thu
toan bo = `9650372000.0` (`local_run_stage1.log` dong 154 va
`cluster_run_stage2.log` — xem V06, hai gia tri giong het nhau). Vi du 3
dong dau:

```text
+-------+-------------+---------+-----------+---------------+
|month  |category_name|revenue  |order_count|line_item_count|
+-------+-------------+---------+-----------+---------------+
|2026-02|Dien tu      |5.3173E7 |2          |2              |
|2026-02|Gia dung     |5.4244E7 |2          |3              |
|2026-02|My pham      |1.08088E8|5          |6              |
```

Notes: khong co danh muc/thang nao bi trung dong (moi to hop `month +
category_name` xuat hien dung 1 lan) — kiem tra bang
`revenue_df.groupBy("month","category_name").count()` khong co dong nao
`count > 1` (chay them ngoai job, khong ghi lai trong script chinh vi la
buoc kiem tra phu).

### V04 – Ghi Parquet co partition theo thang

**Result:** PASS

Command: nhu V01, cong doan `write_parquet()`.

```python
revenue_df.write.mode("overwrite").partitionBy("month").parquet(REVENUE_OUTPUT_PATH)
```

Actual: thu muc output co 7 thu muc con `month=2026-02` ... `month=2026-08`
(khop 7 thang xuat hien trong du lieu), moi thu muc co file `.parquet` +
`_SUCCESS` o thu muc goc:

```text
D:/school/Big Data/PySpark/output/local_mode/revenue_by_month_category/
├── _SUCCESS
├── month=2026-02/
├── month=2026-03/
├── month=2026-04/
├── month=2026-05/
├── month=2026-06/
├── month=2026-07/
└── month=2026-08/
```

Job cung tu doc lai Parquet vua ghi ngay sau khi ghi va in lai 41 dong —
khop voi ket qua truoc khi ghi (V03), xac nhan ghi/doc round-trip dung.

### V05 – `explain()` cho truy van tong hop doanh thu

**Result:** PASS

Command: nhu V01, `revenue_df.explain(mode="extended")`.

Actual (rut gon Physical Plan, day du trong `local_run_stage1.log` dong
159–229):

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [month#269 ASC NULLS FIRST, category_name#22 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(month#269 ASC ..., 4), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[month#269, category_name#22], functions=[sum(line_amount#251), count(1), count(distinct order_id#12)])
         +- Exchange hashpartitioning(month#269, category_name#22, 4), ENSURE_REQUIREMENTS
            +- HashAggregate(... merge_sum, merge_count, partial_count(distinct order_id#12) ...)
               +- HashAggregate(keys=[month#269, category_name#22, order_id#12], ...)
                  +- Exchange hashpartitioning(month#269, category_name#22, order_id#12, 4), ENSURE_REQUIREMENTS
                     +- HashAggregate(partial_sum, partial_count ...)
                        +- Project [...]
                           +- BroadcastHashJoin [product_id#13], [product_id#20], Inner, BuildRight
                              :- BroadcastHashJoin [order_id#12], [order_id#0], Inner, BuildRight
                              :     :- Filter (isnotnull(order_id) AND isnotnull(product_id))
                              :     :  +- FileScan csv order_items_sample.csv
                              :     +- BroadcastExchange ...
                              :        +- Filter (total_amount >= 0.0 AND isnotnull(order_id))
                              :           +- FileScan csv orders_sample.csv
                              +- BroadcastExchange ...
                                 +- Filter isnotnull(product_id)
                                    +- FileScan json products_sample.json
```

Giai thich o muc nhap mon (cho Content AI dung khi soan tai lieu):

- **Logical Plan (Parsed -> Analyzed -> Optimized):** mo ta "muon lam gi"
  voi DataFrame — doc 3 nguon, loc dieu kien lam sach, join, tinh cot moi,
  gom nhom, sap xep — chua noi "lam nhu the nao". Optimizer (Catalyst) da
  tu **day predicate xuong FileScan** (vi du `total_amount >= 0.0` va
  `isnotnull(order_id)` xuat hien ngay tai `FileScan csv orders_sample.csv`
  thay vi o mot phep `Filter` rieng sau khi doc toan bo file) — day chinh la
  predicate pushdown, giup Spark doc it du lieu hon ngay tu buoc doc file.
- **Physical Plan:** mo ta "lam nhu the nao" tren cluster that — chon
  `BroadcastHashJoin` cho ca 2 phep join (vi `orders`/`products` sau khi loc
  rat nho, du de gui (broadcast) toan bo sang moi executor thay vi phai
  shuffle du lieu lon `order_items`), 3 `Exchange` (shuffle) lien tiep phuc
  vu `HashAggregate` nhieu buoc (partial -> merge -> final, ky thuat gom
  nhom 2 pha de giam du lieu shuffle) va 1 `Exchange` cuoi cho `Sort` truoc
  khi tra ket qua. Day chinh la 4 diem co the phat sinh shuffle trong toan
  bo truy van (3 cho aggregate, 1 cho sort) — sinh vien co the doi chieu voi
  Spark UI (tab SQL/Stages) de dem so stage bi ngan boi shuffle boundary,
  dung y trong khung noi dung Buoi 9 ve "DAG chia stage tai ranh gioi
  shuffle".

### V06 – Chay lai tren cluster that, doi chieu voi local[*]

**Result:** PASS

Command:

```bash
export MSYS_NO_PATHCONV=1
docker exec -e SPARK_MASTER_URL="spark://spark-master:7077" spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=4 --executor-memory 512m --driver-memory 512m \
  /opt/spark-apps/process_retailstream.py

curl -s http://localhost:8080/json/
```

Xac nhan chay tren cluster that (khong phai `local[*]`), 3 bang chung cu
doc lap:

1. **`spark.sparkContext.master`** in ra trong log:
   `spark.master (thuc te) = spark://spark-master:7077`,
   `applicationId = app-20260820054748-0001` (khac han dinh dang
   `local-<timestamp>` cua giai doan local[*], vi du
   `local-1787204815347`).
2. **Spark Master REST API** (`curl http://localhost:8080/json/`) liet ke
   ung dung trong `completedapps`:
   ```json
   {
     "id": "app-20260820054748-0001",
     "name": "retailstream-session10-pyspark",
     "cores": 2,
     "state": "FINISHED",
     "duration": 32996
   }
   ```
   `"cores": 2` xac nhan ung dung dung 2 executor (1 core moi executor,
   dung `--executor-memory 512m` khong chi dinh so core nen moi worker cap
   toan bo 1 core con trong).
3. **Log driver** ghi ro 2 executor duoc cap tren 2 IP container worker
   khac nhau (khong phai executor "driver" noi bo nhu local[*]):
   ```text
   Executor added: app-20260820054748-0001/0 on worker-...-172.20.0.3-36933 (172.20.0.3:36933) with 1 core(s)
   Executor added: app-20260820054748-0001/1 on worker-...-172.20.0.4-35459 (172.20.0.4:35459) with 1 core(s)
   ```
   (172.20.0.3 = `spark-worker-2`, 172.20.0.4 = `spark-worker-1` theo mang
   Docker Compose cua `Spark/`).

**Doi chieu ket qua local[*] vs cluster:** copy ca 2 output Parquet
(`PySpark/output/local_mode/` va `PySpark/output/cluster_mode/`) ra host, so
sanh bang pandas:

```python
local = read_all(".../output/local_mode/revenue_by_month_category")     # 41 dong
cluster = read_all(".../output/cluster_mode/revenue_by_month_category")  # 41 dong
local.equals(cluster)   # -> True
```

Ket qua: **41 dong ca hai ben, `DataFrame.equals() == True`** (khop tung o,
khong lech du o gia tri thap phan) — cung du lieu, cung logic, chi doi
`--master`, cho ra ket qua giong het nhau.

## 6. Issues Found

Khong co issue Critical/Major. Ghi chu nho (Minor) da neu trong muc 2
Environment: chua kiem thu duong "native local[*]" ngoai Docker bang
`pyspark==3.3.1` + Java 26 co san tren may — khong anh huong ket luan PASS
vi BRIEF chi yeu cau chay dung 2 che do local[*]/cluster, khong bat buoc
chay ngoai container.

## 7. Mismatch voi tai lieu Content AI

`CONTENT_REPORT.md` cua Buoi 10 **hien dang trong hoan toan** — Content AI
chua viet slide/tai lieu doc/bai thuc hanh cho buoi nay, nen chua co gi de
doi chieu mismatch tai thoi diem lap bao cao nay. Khi Content AI hoan thanh,
can doi chieu it nhat: (a) so lieu doanh thu 41 dong/tong `9650372000.0` co
duoc trich dung dung khong, (b) mo ta shuffle/broadcast join trong slide co
khop voi Physical Plan that o muc V05 khong, (c) danh sach loi du lieu co
chu y (total_amount=-1.0, brand=null) co duoc mo ta dung nhu muc 4 cua
`PySpark/README.md` khong.

## 8. Kha nang chay lai

- [x] chay tu clean state (da xoa `Spark/data/session10_pyspark/output`
      truoc khi chay giai doan cluster, ket qua giong het giai doan truoc);
- [x] version duoc ghim (`apache/spark:3.5.9-python3`, dung lai tu Buoi 9,
      khong doi);
- [x] du lieu co duong dan tuong doi/bien moi truong (`DATA_DIR`,
      `OUTPUT_DIR` co the doi qua bien moi truong, khong hardcode duong dan
      Windows);
- [x] worker/container truy cap duoc du lieu (da xac nhan qua log Executor
      added tren 2 IP worker khac nhau va job chay xong thanh cong);
- [x] expected output duoc luu (`PySpark/output/local_mode/`,
      `PySpark/output/cluster_mode/`, cong voi 2 file log day du);
- [x] reset script hoat dong (`docker compose down` / `up -d` trong `Spark/`
      khong can sua gi them, da co tu Buoi 9; du lieu/job cua buoi nay nam
      trong volume mount san nen khong mat khi restart container).

## 9. Ket luan cho giang vien

Co the dung de day: **YES**

Cac diem can doc truoc khi duyet:

1. Job dung 1 file duy nhat `PySpark/process_retailstream.py` voi khoi
   CONFIG tap trung (`SPARK_MASTER_URL`, co the override qua bien moi
   truong `SPARK_MASTER_URL`/`PYSPARK_DATA_DIR`/`PYSPARK_OUTPUT_DIR`) —
   dung nguyen 1 file logic cho ca 2 giai doan, dung tinh than "chi doi
   master, khong sua logic xu ly" cua khung noi dung.
2. Ca 2 giai doan deu chay **ben trong cung image Spark 3.5.9 da validate o
   Buoi 9** (khong phai local Python/Java tren Windows) de loai bo bien so
   moi truong khong lien quan; ly do va cach chay "native" (chua kiem thu)
   duoc ghi ro trong `PySpark/README.md`.
3. De ca 3 container (master + 2 worker) doc duoc cung du lieu/job, ban thuc
   thi duoc dat trong `Spark/data/session10_pyspark/` va
   `Spark/jobs/process_retailstream.py` (dung lai mount volume co san cua
   `Spark/docker-compose.yml`, khong sua file compose) — ban chinh thuc de
   doc/nop van la `PySpark/`. Giai thich chi tiet trong `PySpark/README.md`
   muc 2.
4. `CONTENT_REPORT.md` Buoi 10 con trong — xem muc 7.
