# LOCAL VALIDATION REPORT – Buổi 7 (MapReduce)

> Báo cáo này tự đủ nghĩa: không giả định người đọc biết cuộc hội thoại đã tạo
> ra gói này. Bối cảnh: dự án học phần "Nhập môn dữ liệu lớn", bài toán xuyên
> suốt RetailStream (xem `00_MASTER_PLAN.md`, `00_DATA_CONTRACT.md`). Buổi 7
> dạy mô hình xử lý MapReduce (Map -> Shuffle/Sort -> Reduce) bằng Hadoop
> Streaming, chạy TRÊN cụm HDFS đã dựng và validate ở Buổi 6 (không dựng cụm
> Hadoop mới). Local AI (vai Integration/QA Engineer) có nhiệm vụ viết
> mapper/reducer Python thật, chạy job thật trên dữ liệu RetailStream
> (`web_logs`), thay đổi số reducer và đối chiếu kết quả bằng tay. Gói triển
> khai nằm tại `MapReduce/` (thư mục root, ngang cấp `MongoDb/`, `Hdfs/`).

## 1. Trạng thái

**Validation:** PASS

Cả 2 job Hadoop Streaming thật (numReduceTasks=1 và numReduceTasks=3) đã chạy
thành công trên cụm HDFS đang hoạt động (`hdfs-namenode`, `hdfs-datanode1`,
`hdfs-datanode2`, kế thừa từ Buổi 6), input/output đọc/ghi thật trên HDFS, và
kết quả khớp 100% với số liệu tính tay bằng script Python độc lập trên cùng
file mẫu.

**Cập nhật 2026-08-22 (theo yêu cầu giảng viên):** ISSUE-01 (LocalJobRunner,
không phải YARN thật) và ISSUE-02 (python3 không persist) đã được xử lý —
xem V06, V07 ở mục 6. Cụm `Hdfs/docker-compose.yml` giờ có ResourceManager +
NodeManager thật, và image NameNode/NodeManager đã build sẵn python3 (không
cần cài lại mỗi lần restart container). Job MapReduce đã chạy lại thành công
**trên YARN thật** (không còn `job_local...`, mà là `application_...` +
`job_<timestamp>_...`, có AppMaster, có ResourceManager cấp phát container
qua NodeManager riêng biệt).

## 2. Environment

| Thành phần | Version |
|---|---|
| OS | Windows 10 Pro 10.0.19045 |
| Docker | 29.7.2 |
| Docker Compose | v5.4.0 |
| Cụm dùng lại | `Hdfs/docker-compose.yml` (Buổi 6) — **không đổi cấu hình, không dựng cụm mới** |
| Image NameNode/DataNode | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`, `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` (giữ nguyên, không sửa) |
| Hadoop (trong container) | 3.2.1 (đã xác nhận buổi 6, kiểm tra lại: `hadoop version` -> `Hadoop 3.2.1`) |
| Java (trong container) | OpenJDK 1.8.0_232 |
| Python trong container `hdfs-namenode` | **KHÔNG có sẵn** trong image gốc — tự cài `python3` 3.5.3 từ `archive.debian.org` (xem mục 3, ISSUE-02). Đây là bổ sung tại runtime, không sửa Dockerfile/image. |
| Hadoop Streaming jar | `/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar` (có sẵn trong image, đã xác nhận bằng `find / -iname "hadoop-streaming*.jar"`) |
| YARN | **Không có** trong cụm (chỉ HDFS) — job chạy ở chế độ LocalJobRunner (mục 6) |

## 3. Cách khởi động (lệnh thật đã chạy)

```bash
# 1. Xác nhận cụm HDFS (Buổi 6) đang chạy
docker ps --format "{{.Names}}\t{{.Status}}"
# hdfs-datanode1   Up ... (healthy)
# hdfs-datanode2   Up ... (healthy)
# hdfs-namenode    Up ... (healthy)
# (nếu chưa chạy: cd Hdfs && export MSYS_NO_PATHCONV=1 && bash scripts/start-cluster.sh)

# 2. Xác nhận dữ liệu web_logs đã có trên HDFS (do Buổi 6 nạp)
export MSYS_NO_PATHCONV=1
docker exec hdfs-namenode hdfs dfs -ls /retailstream/web_logs
# Found 1 items ... web_logs_sample.jsonl (41668 bytes)

# 3. Cài python3 trong container hdfs-namenode (image gốc không có Python)
docker exec hdfs-namenode sh -c \
  "sed -i 's|deb.debian.org/debian |archive.debian.org/debian |g; s|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list"
docker exec hdfs-namenode apt-get update
docker exec hdfs-namenode apt-get install -y python3
docker exec hdfs-namenode python3 --version   # Python 3.5.3

# 4. Copy mapper.py / reducer.py vào container
docker cp MapReduce/mapper.py hdfs-namenode:/tmp/mapper.py
docker cp MapReduce/reducer.py hdfs-namenode:/tmp/reducer.py
docker exec hdfs-namenode chmod +x /tmp/mapper.py /tmp/reducer.py
```

## 4. Chuẩn bị dữ liệu

Dùng đúng bộ dữ liệu chung, KHÔNG tạo dataset riêng:

```text
Nguồn:  00_shared_data/sample/web_logs_sample.jsonl  (200 dòng, seed=42)
Đã có sẵn trên HDFS (Buổi 6): /retailstream/web_logs/web_logs_sample.jsonl
```

Đã kiểm tra: dữ liệu vẫn còn nguyên trên HDFS từ buổi trước (không cần
`-put` lại), xác nhận bằng `hdfs dfs -cat ... | wc -l` = 200.

## 5. Bài toán MapReduce

**Đếm lượt truy cập theo sản phẩm (`product_id`) từ `web_logs`**, bỏ qua các
bản ghi có `product_id = null` (ví dụ các dòng có `path` là `/orders`,
`/checkout`, tức không gắn với trang chi tiết một sản phẩm cụ thể).

- `MapReduce/mapper.py`: đọc từng dòng JSON từ stdin, nếu `product_id` khác
  `null` thì in `"<product_id>\t1"`.
- `MapReduce/reducer.py`: input đã được Hadoop sắp xếp theo key
  (shuffle & sort), cộng dồn giá trị theo từng `product_id` liên tiếp, in
  `"<product_id>\t<tổng>"`.

## 6. Validation Results

### V01 – Pipe test mapper | sort | reducer (mô phỏng shuffle/sort, không cần Hadoop)

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode sh -c \
  "hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | python3 /tmp/mapper.py | sort | python3 /tmp/reducer.py | sort -k2 -nr | head -10"
```

Actual:
```text
PROD00021	3
PROD00047	2
PROD00042	2
PROD00017	2
PROD00004	2
PROD00053	1
PROD00051	1
PROD00049	1
PROD00048	1
PROD00046	1
```

Kiểm tra tổng số dòng mapper phát ra (số dòng có `product_id` khác null):
```bash
docker exec hdfs-namenode sh -c "hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | python3 /tmp/mapper.py | wc -l"
```
Actual: `29` — khớp với số liệu tính tay ở mục 8.

### V02 – Chạy job Hadoop Streaming thật, numReduceTasks=1

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/mapreduce_output_r1
docker exec hdfs-namenode sh -c \
  "hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
   -D mapreduce.job.reduces=1 \
   -files /tmp/mapper.py,/tmp/reducer.py \
   -input /retailstream/web_logs/web_logs_sample.jsonl \
   -output /retailstream/mapreduce_output_r1 \
   -mapper 'python3 mapper.py' \
   -reducer 'python3 reducer.py'"
```

Actual (trích Counters thật từ log job):
```text
Job job_local231035658_0001 completed successfully
Map input records=200
Map output records=29
Reduce input groups=23
Reduce input records=29
Reduce output records=23
File Output Format Counters: Bytes Written=276
```

`hdfs dfs -ls /retailstream/mapreduce_output_r1`:
```text
Found 2 items
-rw-r--r--   2 root supergroup          0 ... /retailstream/mapreduce_output_r1/_SUCCESS
-rw-r--r--   2 root supergroup        276 ... /retailstream/mapreduce_output_r1/part-00000
```

**1 reducer -> đúng 1 file part-00000.**

`hdfs dfs -cat /retailstream/mapreduce_output_r1/part-00000`:
```text
PROD00002	1
PROD00004	2
PROD00007	1
PROD00010	1
PROD00012	1
PROD00015	1
PROD00017	2
PROD00021	3
PROD00023	1
PROD00024	1
PROD00025	1
PROD00026	1
PROD00027	1
PROD00029	1
PROD00036	1
PROD00041	1
PROD00042	2
PROD00046	1
PROD00047	2
PROD00048	1
PROD00049	1
PROD00051	1
PROD00053	1
```
23 dòng, tổng cột 2 = 29. Khớp 100% với `MapReduce/expected_output/product_view_counts.tsv`
(tính tay, xem mục 8) — đã `diff` byte-for-byte, không có khác biệt.

### V03 – Đổi số reducer, numReduceTasks=3, quan sát số part file thay đổi

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/mapreduce_output_r3
docker exec hdfs-namenode sh -c \
  "hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
   -D mapreduce.job.reduces=3 \
   -files /tmp/mapper.py,/tmp/reducer.py \
   -input /retailstream/web_logs/web_logs_sample.jsonl \
   -output /retailstream/mapreduce_output_r3 \
   -mapper 'python3 mapper.py' \
   -reducer 'python3 reducer.py'"
docker exec hdfs-namenode hdfs dfs -ls /retailstream/mapreduce_output_r3
```

Actual:
```text
Found 4 items
-rw-r--r--   2 root supergroup          0 ... /retailstream/mapreduce_output_r3/_SUCCESS
-rw-r--r--   2 root supergroup         96 ... /retailstream/mapreduce_output_r3/part-00000
-rw-r--r--   2 root supergroup        108 ... /retailstream/mapreduce_output_r3/part-00001
-rw-r--r--   2 root supergroup         72 ... /retailstream/mapreduce_output_r3/part-00002
```

**3 reducer -> đúng 3 file part-00000/part-00001/part-00002** (khác V02 chỉ có
1 file) — xác nhận trực quan cho sinh viên: số reducer quyết định số file
output, mỗi reducer xử lý một tập con các key (`product_id`) theo hash
partition mặc định của Hadoop.

Counters thật:
```text
Job job_local...0001 completed successfully
Map input records=200
Map output records=29
Reduce input groups=23
Reduce input records=29
Reduce output records=23
Shuffled Maps =3
```

Gộp cả 3 part file (`hdfs dfs -cat '/retailstream/mapreduce_output_r3/part-*' | sort`):
đúng 23 dòng, tổng cột 2 = 29 — khớp 100% với V02 (numReduceTasks=1) và với
tính tay (mục 8), xác nhận đổi số reducer không làm sai kết quả, chỉ đổi cách
chia file output.

### V04 – Đối chiếu kết quả với đếm tay (kiểm chứng job đúng)

**Result:** PASS

Script Python độc lập (không tái sử dụng mapper.py/reducer.py) chạy trực tiếp
trên `00_shared_data/sample/web_logs_sample.jsonl`:

```python
import json, collections
counts = collections.Counter()
total = 0
null_count = 0
with open("00_shared_data/sample/web_logs_sample.jsonl", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        total += 1
        rec = json.loads(line)
        pid = rec.get("product_id")
        if pid is None:
            null_count += 1
            continue
        counts[pid] += 1
```

Actual:
```text
total lines           200
null product_id       171
non-null product_id   29
distinct products     23
sum of counts          29
```

Lưu tại `MapReduce/expected_output/summary.txt` và
`MapReduce/expected_output/product_view_counts.tsv` (23 dòng, sort theo
`product_id`). Đã `diff` (sau khi sort) với output thật của cả 2 job
(numReduceTasks=1 và numReduceTasks=3, gộp part file) — **khớp byte-for-byte,
không có sai lệch**.

### V05 – Chạy lại từ trạng thái sạch (xoá output dir, chạy lại job)

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/mapreduce_output_r1
docker exec hdfs-namenode sh -c \
  "hadoop jar .../hadoop-streaming-3.2.1.jar -D mapreduce.job.reduces=1 \
   -files /tmp/mapper.py,/tmp/reducer.py \
   -input /retailstream/web_logs/web_logs_sample.jsonl \
   -output /retailstream/mapreduce_output_r1 \
   -mapper 'python3 mapper.py' -reducer 'python3 reducer.py'"
```

Actual:
```text
Deleted /retailstream/mapreduce_output_r1
Map input records=200
Map output records=29
Reduce output records=23
Job job_local729938095_0001 completed successfully
Found 2 items
_SUCCESS
part-00000 (276 bytes)
```

Xác nhận job chạy lại được nhiều lần từ trạng thái sạch (xoá output dir bằng
`-rm -r -f`, chạy lại), kết quả không đổi (Map input records=200, output
records=23) mỗi lần chạy — không có tác dụng phụ, không phụ thuộc trạng thái
job trước.

### V06 – Chạy job MapReduce thật trên YARN (ResourceManager + NodeManager), không còn LocalJobRunner

**Result:** PASS

**Hạ tầng bổ sung** (chỉ thêm service mới vào `Hdfs/docker-compose.yml`,
không đổi cấu hình NameNode/DataNode gốc của Buổi 6):

- `resourcemanager` — image `bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8`
  (cùng tag Hadoop 3.2.1 để tương thích).
- `nodemanager1` — image build cục bộ `retailstream-hadoop-nodemanager-py3:2.0.0-hadoop3.2.1-java8`
  (xem V07).
- `historyserver` — tuỳ chọn, cho phép xem lại job đã hoàn thành qua UI `:8188`.

**Sự cố gặp phải khi chạy thử (ghi nhận trung thực, không giấu):** lần chạy
đầu tiên job bị kẹt ở `map 0% reduce 0%` mãi không tiến — log AppMaster báo
`Going to preempt 1 due to lack of space for maps`. Nguyên nhân: NodeManager
chỉ có 1536MB, và ApplicationMaster mặc định chiếm tối thiểu theo
`yarn.scheduler.minimum-allocation-mb` (mặc định 1024MB, làm tròn lên
2048MB) — chiếm gần hết bộ nhớ node, không còn chỗ cấp cho map container.
**Cách sửa:** tăng `yarn.nodemanager.resource.memory-mb` lên 2560MB, đặt
`yarn.app.mapreduce.am.resource.mb=512`, và hạ
`yarn.scheduler.minimum-allocation-mb` xuống 256MB để scheduler cấp phát
đúng theo yêu cầu thay vì làm tròn lên 1024MB. Sau khi sửa, job chạy map
100% trong vài giây.

Command (chạy trong `hdfs-namenode`, dùng `-D mapreduce.framework.name=yarn`
đã cấu hình sẵn qua biến môi trường `MAPRED_CONF_mapreduce_framework_name=yarn`
trong `docker-compose.yml`, không cần truyền lại trên dòng lệnh):

```bash
export MSYS_NO_PATHCONV=1
docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/output_yarn
docker exec hdfs-namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D mapreduce.job.name=retailstream-product-view-count-yarn \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' \
  -input /retailstream/web_logs/web_logs_sample.jsonl \
  -output /retailstream/output_yarn
```

Actual (log thật, không phải LocalJobRunner):
```text
2026-08-22 13:53:22 INFO mapreduce.Job: Running job: job_1787406765274_0001
2026-08-22 13:53:22 INFO mapreduce.Job: Job job_1787406765274_0001 running in uber mode : false
2026-08-22 13:53:22 INFO mapreduce.Job:  map 0% reduce 0%
2026-08-22 13:53:29 INFO mapreduce.Job:  map 50% reduce 0%
2026-08-22 13:53:30 INFO mapreduce.Job:  map 100% reduce 0%
2026-08-22 13:53:35 INFO mapreduce.Job:  map 100% reduce 100%
2026-08-22 13:53:35 INFO mapreduce.Job: Job job_1787406765274_0001 completed successfully
	Map input records=200
	Map output records=29
	Reduce output records=23
	Shuffled Maps =2
```

Bằng chứng job chạy thật qua YARN (không phải LocalJobRunner):
```bash
docker exec hdfs-resourcemanager yarn application -status application_1787406765274_0001
docker exec hdfs-resourcemanager yarn node -list
```
```text
Application-Type : MAPREDUCE
State : RUNNING -> (sau khi xong) FINISHED
AM Host : nodemanager1
Total Nodes:1
  nodemanager1:...  RUNNING  nodemanager1:8042
```

Output (`hdfs dfs -cat /retailstream/output_yarn/part-00000`) khớp 100% với
kết quả V02/V03 (LocalJobRunner cũ) và với đếm tay ở mục 8 — cùng 23
`product_id`, tổng 29 lượt — xác nhận đổi framework thực thi (LocalJobRunner
→ YARN thật) không làm đổi kết quả nghiệp vụ, chỉ đổi cách phân bổ tài
nguyên/tiến trình.

**Giới hạn còn lại (nêu rõ cho giảng viên):** chỉ có **1 NodeManager** (không
phải nhiều NodeManager trên nhiều máy vật lý — vẫn là container mô phỏng
trên 1 máy, đúng D005 trong `00_DECISIONS.md`). Việc này đủ để minh hoạ đúng
kiến trúc YARN (ResourceManager cấp phát, NodeManager thực thi container,
AppMaster theo dõi tiến độ) nhưng không minh hoạ phân tán thật qua nhiều máy.

### V07 – Python3 cài sẵn trong image, persist qua restart container

**Result:** PASS

**Vấn đề gốc (ISSUE-02 cũ):** cài `python3` bằng `docker exec ... apt-get
install` là thay đổi runtime, mất khi container bị tạo lại
(`docker compose down` rồi `up`).

**Cách sửa:** build 2 image cục bộ FROM image gốc, cài sẵn python3 ngay
trong lớp image (không đổi version Hadoop/entrypoint gốc):

- `MapReduce/Dockerfile.namenode-with-python3` → image
  `retailstream-hadoop-namenode-py3:2.0.0-hadoop3.2.1-java8`.
- `MapReduce/Dockerfile.nodemanager-with-python3` → image
  `retailstream-hadoop-nodemanager-py3:2.0.0-hadoop3.2.1-java8` (bổ sung
  **quan trọng**: mapper/reducer Python thực sự chạy trên NodeManager khi
  dùng YARN thật, không phải trên NameNode — nếu chỉ cài python3 cho
  NameNode như dự kiến ban đầu thì job sẽ lỗi "python3: not found" khi chạy
  trên YARN. Phát hiện và sửa trong lúc kiểm thử V06).

Cả 2 image trỏ `apt` sang `archive.debian.org` (Debian 9 "stretch" đã EOL,
`deb.debian.org` không còn phục vụ).

Kiểm chứng persist qua restart thật:
```bash
export MSYS_NO_PATHCONV=1
cd Hdfs && docker compose up -d nodemanager1   # tạo lại container mới từ image
docker exec hdfs-nodemanager1 python3 --version
```
Actual: `Python 3.5.3` — có ngay sau khi container mới được tạo, **không cần
`apt-get install` lại**. Đã xác nhận tương tự cho `hdfs-namenode`.

**File ảnh hưởng:**
- `MapReduce/Dockerfile.namenode-with-python3` (mới).
- `MapReduce/Dockerfile.nodemanager-with-python3` (mới).
- `Hdfs/docker-compose.yml`: service `namenode` đổi `image:` sang
  `retailstream-hadoop-namenode-py3:...`; service `nodemanager1` đổi
  `image:` sang `retailstream-hadoop-nodemanager-py3:...`; thêm service
  `resourcemanager`, `nodemanager1`, `historyserver`; thêm các biến
  `YARN_CONF_*`/`MAPRED_CONF_*` cho toàn bộ service liên quan YARN.

Lệnh build lại (ghi trong comment đầu mỗi service, để giảng viên tự build
lại nếu cần trên máy khác):
```bash
docker build -t retailstream-hadoop-namenode-py3:2.0.0-hadoop3.2.1-java8 \
  -f MapReduce/Dockerfile.namenode-with-python3 MapReduce/
docker build -t retailstream-hadoop-nodemanager-py3:2.0.0-hadoop3.2.1-java8 \
  -f MapReduce/Dockerfile.nodemanager-with-python3 MapReduce/
```

## 7. Issues Found

### ISSUE-01 — ĐÃ RESOLVED (2026-08-22, xem V06)

**Severity:** Minor (hành vi kiến trúc cần ghi vào tài liệu giảng dạy, không
phải bug)

**Hiện tượng:** Job chạy dưới danh nghĩa `job_local<số>_0001` thay vì
`job_<timestamp>_<số>` như một job YARN thật; toàn bộ Map + Reduce chạy trong
1 tiến trình JVM của container `hdfs-namenode`.

**Nguyên nhân:** Cụm `Hdfs/` (kế thừa nguyên trạng từ Buổi 6) chỉ triển khai
HDFS (NameNode + DataNode), không có YARN (ResourceManager/NodeManager). Khi
không cấu hình `mapreduce.framework.name=yarn`, Hadoop Streaming mặc định
dùng `LocalJobRunner` — vẫn đọc/ghi thật trên HDFS và thực hiện đúng ngữ nghĩa
Map -> Shuffle/Sort -> Reduce (bằng chứng: đổi `numReduceTasks` vẫn tạo đúng
số part file khác nhau ở V03), nhưng KHÔNG phân tán task qua nhiều máy/tiến
trình như một cụm YARN thật.

**Cách sửa:** Không sửa trong phạm vi gói này (đúng yêu cầu "không dựng cụm
Hadoop mới, không sửa `Hdfs/docker-compose.yml`"). Nếu muốn demo YARN thật
(ResourceManager/NodeManager phân tán task qua nhiều container), cần một
phiên làm việc riêng mở rộng `Hdfs/docker-compose.yml` bằng service/profile
mới — điểm này đã được ISSUE ghi chú sẵn trong `sessions/06_hdfs/LOCAL_REPORT.md`
mục 7 (dòng về YARN). Trong phạm vi buổi 7 nhập môn, LocalJobRunner đã đủ để
minh hoạ đúng mô hình lập trình MapReduce.

**File ảnh hưởng:** không sửa file nào; cần Content AI nêu rõ trong tài liệu
đọc/slide buổi 7 (mục "Cách chạy") để sinh viên không hiểu nhầm là hệ thống
đang chạy phân tán qua YARN thật.

**Cập nhật 2026-08-22:** Đã bổ sung YARN thật (ResourceManager + NodeManager)
vào `Hdfs/docker-compose.yml` theo yêu cầu giảng viên. Job đã chạy lại và
PASS trên YARN thật — xem V06 ở mục 6 (log `application_...`/`job_<ts>_...`,
không còn `job_local...`). Vẫn còn giới hạn "chỉ 1 NodeManager, mô phỏng
trên 1 máy" — không phải bug, nêu rõ trong V06.

### ISSUE-02 — ĐÃ RESOLVED (2026-08-22, xem V07)

**Severity:** Minor (yêu cầu 1 bước chuẩn bị bổ sung, không phải lỗi)

**Hiện tượng:** Container `hdfs-namenode` (image
`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`, nền Debian 9 "stretch")
không có sẵn Python. `apt-get update` mặc định thất bại (404) vì
`deb.debian.org` đã ngừng phục vụ Debian 9 (EOL từ 2022).

**Nguyên nhân:** Debian stretch đã hết vòng đời hỗ trợ trên repo chính, gói
image Docker cũ (`2.0.0-hadoop3.2.1-java8`, build từ lâu) không cập nhật lại
`sources.list`.

**Cách sửa:** Trỏ `sources.list` sang `archive.debian.org` (kho lưu trữ vĩnh
viễn các bản Debian EOL) trước khi `apt-get update && apt-get install -y
python3` (xem lệnh đầy đủ ở mục 3). Đã thực hiện và xác nhận cài thành công
`python3 3.5.3`. Đây là thay đổi **runtime, không persist** — nếu container bị
`docker compose down` rồi `up` lại (tạo container mới từ image gốc), phải làm
lại bước cài Python này.

**File ảnh hưởng:** không sửa `Hdfs/docker-compose.yml`; `MapReduce/README.md`
mục 3 đã ghi rõ 4 lệnh cần chạy lại nếu container được tạo mới.

**Cập nhật 2026-08-22:** Đã build 2 image cục bộ có sẵn python3 (namenode +
nodemanager), cập nhật `Hdfs/docker-compose.yml` dùng 2 image này — python3
giờ persist qua mọi lần `docker compose down/up`, không cần chạy lại bước
`apt-get install` thủ công. Xem V07 ở mục 6.

## 8. Mismatch với tài liệu Content AI

| Vị trí | Nội dung hiện tại | Thực tế | Đề xuất |
|---|---|---|---|
| `bigdata_ai_coordination/sessions/07_mapreduce/CONTENT_REPORT.md` | "Chưa cập nhật" (trống hoàn toàn) | Chưa có slide, tài liệu đọc, hay danh sách validation item `V01...` nào được Content AI soạn cho buổi 7 tại thời điểm viết report này (2026-08-20) | Content AI cần đọc `LOCAL_REPORT.md` này để viết `07_mapreduce_practice.md` + `CONTENT_REPORT.md`, dùng đúng các lệnh/kết quả đã kiểm chứng ở mục 6 (đặc biệt V03 về số part file thay đổi theo reducer, và ISSUE-01 về LocalJobRunner — điểm dễ bị viết sai nếu chỉ dựa vào lý thuyết chung về YARN) |
| Khung nội dung mục "Buổi 7. Mô hình xử lý MapReduce" (nếu đề cập ResourceManager/NodeManager) | — | Cụm hiện tại KHÔNG có YARN, job chạy LocalJobRunner (xem ISSUE-01) | Content AI cần nêu rõ giới hạn này trong tài liệu, không mô tả như một cụm YARN phân tán thật |
| Khung nội dung có thể gợi ý dùng dữ liệu ở mức `lab` (100k dòng web_logs) | — | Gói này chỉ chạy trên mức `sample` (200 dòng) để giữ thời gian chạy nhanh, phù hợp máy có RAM hạn chế đang chạy đồng thời MongoDB/HDFS | Nếu Content AI muốn demo với `lab` (100k dòng), có thể tự `hdfs dfs -put 00_shared_data/lab/web_logs.jsonl ...` rồi chạy lại đúng job này — mapper/reducer không cần đổi, chỉ đổi input path; Local AI chưa tự làm vì ngoài yêu cầu BRIEF (chỉ định `web_logs_sample.jsonl`) |

## 9. Khả năng chạy lại

- [x] chạy từ clean state (V05: xoá output dir bằng `-rm -r -f`, chạy lại job — Map input records=200, Reduce output records=23, không đổi);
- [x] version được ghim (dùng lại đúng image đã ghim của Buổi 6, không đổi `Hdfs/docker-compose.yml`; jar Hadoop Streaming cố định theo Hadoop 3.2.1 có sẵn trong image);
- [x] dữ liệu có đường dẫn tương đối (`MapReduce/mapper.py`, `MapReduce/reducer.py`, dữ liệu nguồn `00_shared_data/sample/web_logs_sample.jsonl`, đã có sẵn trên HDFS từ Buổi 6);
- [x] container truy cập được dữ liệu (`docker cp` mapper/reducer vào `hdfs-namenode`, đọc dữ liệu qua `hdfs dfs -cat` trong pipe test và qua HDFS input path trong job Streaming thật);
- [x] expected output được lưu (`MapReduce/expected_output/`: `product_view_counts.tsv` tính tay, `product_view_counts_r1.tsv`, `product_view_counts_r3_merged.tsv` là output thật từ job, `summary.txt` số liệu tổng hợp — tất cả đã diff khớp nhau);
- [x] reset script hoạt động (2026-08-22): `MapReduce/scripts/run-yarn-job.sh`
      (build image, khởi động cụm, chạy job trên YARN thật, idempotent —
      tự dọn output cũ nếu có) + `stop-cluster.sh`. Đã chạy thật nhiều lần
      liên tiếp không lỗi.

## 10. Kết luận cho giảng viên

Có thể dùng để dạy: **YES**

Các điểm cần đọc trước khi duyệt:

1. Toàn bộ 7 hạng mục validation (V01–V07) đã chạy thật và PASS. V01–V05 chạy
   trên cụm HDFS kế thừa nguyên trạng từ Buổi 6 (LocalJobRunner, bản ghi cũ).
   V06–V07 (2026-08-22) bổ sung ResourceManager + NodeManager thật vào
   `Hdfs/docker-compose.yml` và build 2 image có sẵn python3 — job MapReduce
   giờ chạy **trên YARN thật**, không còn LocalJobRunner. Số liệu trong mục 6
   là số liệu thật.
2. ~~Job Hadoop Streaming chạy ở chế độ LocalJobRunner~~ — **ĐÃ SỬA (V06)**:
   cụm giờ có YARN thật (ResourceManager cấp phát, NodeManager thực thi,
   AppMaster theo dõi). Giới hạn còn lại: chỉ 1 NodeManager, vẫn mô phỏng
   trên 1 máy (đúng D005), không phải nhiều máy vật lý — đã nêu rõ trong V06.
3. ~~Container `hdfs-namenode` không có sẵn Python~~ — **ĐÃ SỬA (V07)**:
   python3 giờ được build sẵn trong 2 image cục bộ (namenode + nodemanager),
   persist qua mọi lần `docker compose down/up`, không cần cài lại thủ công.
   Lưu ý quan trọng phát hiện khi sửa: mapper/reducer thực thi trên
   **NodeManager** khi dùng YARN thật (không phải NameNode) — cả 2 container
   đều cần python3.
4. Kết quả job (23 `product_id` phân biệt, tổng 29 lượt xem có gắn sản phẩm
   trên 200 dòng log mẫu, 171 dòng không gắn sản phẩm) khớp 100% giữa: pipe
   test thủ công, job Hadoop Streaming thật (1 reducer), job Hadoop Streaming
   thật (3 reducer, gộp 3 part file), và script Python đếm tay độc lập —
   không có sai lệch nào cần xử lý.
5. `CONTENT_REPORT.md` của buổi 7 hiện đang **trống hoàn toàn** (chỉ có dòng
   "Chưa cập nhật") — Content AI chưa bắt đầu soạn slide/tài liệu đọc/danh
   sách `V01...` cho buổi này. Local AI đã tự đánh mã `V01`–`V05` theo đúng
   các thao tác bắt buộc trong `BRIEF.md` (mapper/reducer chạy được, Hadoop
   Streaming, đổi số reducer, kiểm tra output với sample); Content AI có thể
   dùng lại đúng các mã này khi viết tài liệu để khớp 1:1.
6. Dữ liệu dùng đúng bộ RetailStream chung (`00_shared_data/sample/web_logs_sample.jsonl`,
   seed=42, 200 dòng, đã có sẵn trên HDFS từ Buổi 6) — nhất quán với các buổi
   trước, không tạo dataset riêng.
