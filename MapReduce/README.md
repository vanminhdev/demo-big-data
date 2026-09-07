# MapReduce — Đếm lượt truy cập theo sản phẩm (Hadoop Streaming trên YARN)

Khi dữ liệu (`web_logs`) đã lớn tới mức không xử lý được trên một máy trong
thời gian hợp lý, cần một mô hình chia nhỏ việc tính toán ra nhiều máy và
chạy song song — đó là MapReduce: chia bài toán thành bước **Map** (xử lý
từng bản ghi độc lập), **Shuffle/Sort** (gom nhóm kết quả trung gian theo
key), và **Reduce** (tổng hợp theo từng nhóm key). Vì các bước này chạy phân
tán trên nhiều máy, cần một hệ thống điều phối tài nguyên (CPU, RAM) và
tiến trình trên từng máy — đó là vai trò của **YARN** (Yet Another Resource
Negotiator), gồm **ResourceManager** (điều phối tài nguyên toàn cụm) và
**NodeManager** (quản lý tài nguyên trên từng máy, thực thi task thật).

Bài toán RetailStream: **đếm lượt truy cập theo sản phẩm** (`product_id`) từ
`web_logs`, dùng Hadoop Streaming (Python, không cần biên dịch Java), chạy
**trên YARN thật** (ResourceManager + NodeManager) trong cụm Hadoop đã dựng ở
buổi HDFS (`Hdfs/docker-compose.yml`).

## 1. Kiến trúc và điều kiện tiên quyết

Cụm gồm các container sau (đều nằm trong `Hdfs/docker-compose.yml`):

| Container | Vai trò |
|---|---|
| `hdfs-namenode` | NameNode HDFS, đồng thời là nơi nộp job (`hadoop jar ...`) |
| `hdfs-datanode1`, `hdfs-datanode2` | DataNode HDFS, lưu dữ liệu |
| `hdfs-resourcemanager` | ResourceManager YARN, cấp phát container tài nguyên |
| `hdfs-nodemanager1` | NodeManager YARN, thực thi Mapper/Reducer thật |
| `hdfs-historyserver` | Xem lại job đã hoàn tất qua UI cổng `8188` (tuỳ chọn) |

Lưu ý: trong bảng trên, "container" xuất hiện với 2 nghĩa khác nhau — Docker
container (mỗi dòng của bảng là 1 Docker container, giống các buổi trước) và
"container tài nguyên" của YARN (đơn vị cấp phát CPU/RAM cho một Map/Reduce
task cụ thể, nhắc tới ở mục sau, ví dụ trong thông báo lỗi ở mục "Lỗi thường
gặp") — đây là 2 khái niệm hoàn toàn khác nhau dù trùng tên.

NameNode và NodeManager dùng 2 image build cục bộ có sẵn Python3
(`Dockerfile.namenode-with-python3`, `Dockerfile.nodemanager-with-python3`
trong thư mục này) — không cần cài Python thủ công sau mỗi lần khởi động lại
container.

Kiểm tra cụm đang chạy:

```bash
docker ps --format "{{.Names}}\t{{.Status}}"
# hdfs-namenode          Up ... (healthy)
# hdfs-datanode1          Up ... (healthy)
# hdfs-datanode2          Up ... (healthy)
# hdfs-resourcemanager    Up ...
# hdfs-nodemanager1       Up ...
```

Nếu cụm chưa chạy:

```bash
cd Hdfs
export MSYS_NO_PATHCONV=1
docker compose up -d
```

## 2. Cấu trúc thư mục

```text
MapReduce/
├── README.md
├── mapper.py                             # doc web_logs (JSONL), phat "product_id\t1"
├── reducer.py                            # cong don theo product_id
├── Dockerfile.namenode-with-python3      # image NameNode co san python3
├── Dockerfile.nodemanager-with-python3   # image NodeManager co san python3
├── scripts/
│   ├── run-yarn-job.sh                   # chay job tren YARN, idempotent
│   └── stop-cluster.sh
└── expected_output/
    ├── product_view_counts.tsv           # tinh tay bang Python tren file mau (23 dong)
    ├── product_view_counts_r1.tsv        # output that cua job voi numReduceTasks=1
    ├── product_view_counts_r3_merged.tsv # output that cua job voi numReduceTasks=3 (gop 3 part-*)
    └── summary.txt                       # so lieu tong hop
```

## 3. Bài toán MapReduce

Đếm lượt truy cập theo sản phẩm (`product_id`) từ `web_logs`, bỏ qua các
bản ghi có `product_id = null` (ví dụ các dòng có `path` là `/orders`,
`/checkout`, không gắn với trang chi tiết một sản phẩm cụ thể).

- `mapper.py`: đọc từng dòng JSON từ stdin; nếu `product_id` khác `null` thì
  in `"<product_id>\t1"`.
- `reducer.py`: input đã được Hadoop sắp xếp theo key (**shuffle & sort** —
  bước phân loại lại và chuyển dữ liệu giữa các máy theo key, thường là bước
  tốn tài nguyên nhất của một job MapReduce), cộng dồn giá trị theo từng
  `product_id` liên tiếp, in `"<product_id>\t<tổng>"`.

## 4. Nạp mapper/reducer và dữ liệu vào cụm

```bash
export MSYS_NO_PATHCONV=1
docker cp MapReduce/mapper.py hdfs-namenode:/tmp/mapper.py
docker cp MapReduce/reducer.py hdfs-namenode:/tmp/reducer.py
docker exec hdfs-namenode chmod +x /tmp/mapper.py /tmp/reducer.py

# Xác nhận dữ liệu đã có trên HDFS (nạp từ buổi HDFS); nếu thiếu thì tự put lại:
docker exec hdfs-namenode hdfs dfs -ls /retailstream/web_logs
# nếu thiếu (lấy từ nguồn dữ liệu dùng chung 00_shared_data):
# docker cp 00_shared_data/sample/web_logs_sample.jsonl hdfs-namenode:/tmp/web_logs_sample.jsonl
# docker exec hdfs-namenode hdfs dfs -mkdir -p /retailstream/web_logs
# docker exec hdfs-namenode hdfs dfs -put -f /tmp/web_logs_sample.jsonl /retailstream/web_logs/web_logs_sample.jsonl
```

## 5. Kiểm tra nhanh logic mapper/reducer bằng pipe (không cần Hadoop)

Cách nhanh nhất để debug logic trước khi chạy job thật, mô phỏng
Map → Shuffle/Sort → Reduce bằng lệnh `sort` của Unix:

```bash
docker exec hdfs-namenode sh -c \
  "hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | python3 /tmp/mapper.py | sort | python3 /tmp/reducer.py | sort -k2 -nr | head -10"
```

Kết quả thật:

```text
PROD00021    3
PROD00047    2
PROD00042    2
PROD00017    2
PROD00004    2
...
```

## 6. Chạy job Hadoop Streaming trên YARN thật

```bash
export MSYS_NO_PATHCONV=1
docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/output_yarn

docker exec hdfs-namenode hadoop jar \
  /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D mapreduce.job.name=retailstream-product-view-count-yarn \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' \
  -input /retailstream/web_logs/web_logs_sample.jsonl \
  -output /retailstream/output_yarn

docker exec hdfs-namenode hdfs dfs -ls /retailstream/output_yarn
docker exec hdfs-namenode hdfs dfs -cat /retailstream/output_yarn/part-00000
```

Ý nghĩa các flag chính trong lệnh trên:

| Flag | Ý nghĩa |
|---|---|
| `-files` | Danh sách tệp cần gửi kèm tới mọi máy (NodeManager) sẽ chạy task, để `mapper`/`reducer` có mặt cục bộ khi thực thi |
| `-mapper` | Lệnh chạy cho giai đoạn Map (ở đây là `python3 mapper.py`) |
| `-reducer` | Lệnh chạy cho giai đoạn Reduce (ở đây là `python3 reducer.py`) |
| `-input` | Đường dẫn HDFS chứa dữ liệu đầu vào |
| `-output` | Đường dẫn HDFS sẽ ghi kết quả ra (phải chưa tồn tại trước khi chạy) |

`docker-compose.yml` đã cấu hình sẵn biến môi trường
`MAPRED_CONF_mapreduce_framework_name=yarn` cho các service liên quan, nên
không cần truyền `-D mapreduce.framework.name=yarn` trên dòng lệnh.

Log thật của job (không phải LocalJobRunner — có định danh
`application_...`/`job_<timestamp>_...`):

```text
Running job: job_1787406765274_0001
Job job_1787406765274_0001 running in uber mode : false
 map 0% reduce 0%
 map 50% reduce 0%
 map 100% reduce 0%
 map 100% reduce 100%
Job job_1787406765274_0001 completed successfully
    Map input records=200
    Map output records=29
    Reduce output records=23
    Shuffled Maps =2
```

Ý nghĩa các số chính trong log: `Map input records=200` — tổng số dòng log
mà các Mapper đọc vào; `Map output records=29` — số dòng Mapper phát ra sau
khi lọc bỏ bản ghi `product_id = null` (200 - 171 = 29); `Reduce output
records=23` — số `product_id` phân biệt sau khi Reduce cộng dồn; `Shuffled
Maps=2` — số Mapper đã hoàn tất và chuyển dữ liệu trung gian sang bước
Shuffle/Sort (ở đây job chia input thành 2 phần, chạy 2 Map task).

Xác nhận job chạy qua YARN thật (ResourceManager cấp phát, NodeManager thực thi):

```bash
docker exec hdfs-resourcemanager yarn application -status application_1787406765274_0001
docker exec hdfs-resourcemanager yarn node -list
```

```text
Application-Type : MAPREDUCE
State : FINISHED
AM Host : nodemanager1
Total Nodes: 1
  nodemanager1:...  RUNNING  nodemanager1:8042
```

`AM Host` là máy đang chạy **AM** (ApplicationMaster) — tiến trình do YARN
cấp phát riêng cho job này, chịu trách nhiệm xin thêm container tài nguyên từ
ResourceManager và theo dõi tiến độ Map/Reduce task.

**Giới hạn của cụm minh hoạ:** chỉ có 1 NodeManager (mô phỏng trên một máy,
không phải nhiều máy vật lý) — đủ để minh hoạ đúng vai trò ResourceManager /
NodeManager / ApplicationMaster nhưng không minh hoạ phân tán thật qua nhiều
máy.

## 7. Đổi số Reducer, quan sát số file output thay đổi

```bash
docker exec hdfs-namenode hdfs dfs -rm -r -f /retailstream/mapreduce_output_r3

docker exec hdfs-namenode hadoop jar \
  /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D mapreduce.job.reduces=3 \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -input /retailstream/web_logs/web_logs_sample.jsonl \
  -output /retailstream/mapreduce_output_r3 \
  -mapper 'python3 mapper.py' \
  -reducer 'python3 reducer.py'

docker exec hdfs-namenode hdfs dfs -ls /retailstream/mapreduce_output_r3
```

Kết quả thật:

```text
Found 4 items
-rw-r--r--   2 root supergroup          0 ... /retailstream/mapreduce_output_r3/_SUCCESS
-rw-r--r--   2 root supergroup         96 ... /retailstream/mapreduce_output_r3/part-00000
-rw-r--r--   2 root supergroup        108 ... /retailstream/mapreduce_output_r3/part-00001
-rw-r--r--   2 root supergroup         72 ... /retailstream/mapreduce_output_r3/part-00002
```

Với 1 reducer, output chỉ có **1 file** `part-00000` chứa cả 23 dòng kết
quả. Với 3 reducer, output có **3 file** — mỗi reducer phụ trách một tập hợp
key khác nhau theo hash-partition mặc định của Hadoop (partition là bước gán
mỗi key trung gian cho một reducer cụ thể; mặc định Hadoop tính giá trị băm
(hash) của key rồi lấy phần dư chia cho số reducer để quyết định key đó đi
tới reducer nào). Gộp cả 3 file lại vẫn ra đúng 23 dòng, tổng `count = 29`,
khớp với trường hợp 1 reducer.

## 8. Đối chiếu kết quả (kiểm chứng job đúng)

File mẫu `web_logs_sample.jsonl` có 200 dòng. Đếm bằng script Python độc lập
(không dùng lại mapper.py/reducer.py, xem `expected_output/summary.txt`):

| Chỉ số | Giá trị |
|---|---|
| Tổng số dòng | 200 |
| Số dòng `product_id = null` | 171 |
| Số dòng `product_id` khác null | 29 |
| Số `product_id` phân biệt | 23 |
| Tổng `count` sau reduce | 29 |

`expected_output/product_view_counts.tsv` (tính tay) khớp **byte-for-byte**
(sau khi sort) với cả `product_view_counts_r1.tsv` (job 1 reducer) và
`product_view_counts_r3_merged.tsv` (job 3 reducer, gộp 3 part file) — xác
nhận job MapReduce cho kết quả đúng, không phụ thuộc số reducer hay
framework thực thi (YARN thật cho cùng kết quả với LocalJobRunner đã kiểm
thử trước đó).

## 9. Ví dụ minh hoạ WordCount (tham khảo, không chạy trong gói này)

Trước khi vào bài chính, có thể minh hoạ khái niệm Map/Reduce bằng WordCount
kinh điển (đếm từ trong văn bản):

```text
Map:          "the cat sat" -> (the,1) (cat,1) (sat,1)
Shuffle/Sort: gom theo key
Reduce:       (the,[1,1,...]) -> (the, tổng)
```

Bài chính bắt buộc dùng dữ liệu RetailStream thật (mục 6–8 ở trên), không
dừng lại ở lý thuyết WordCount.

## 10. Lỗi thường gặp

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| `mapper.py`/`reducer.py` báo lỗi "Permission denied" hoặc "cannot execute" khi job chạy | File Python được sao chép/tạo trên Windows rồi `docker cp` vào container thường mất bit quyền thực thi (`+x`); container Linux từ chối chạy file không có quyền này | Chạy `docker exec hdfs-namenode chmod +x /tmp/mapper.py /tmp/reducer.py` sau mỗi lần `docker cp` (đã có sẵn trong mục 4), trước khi chạy job |
| Job báo lỗi `org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory ... already exists` | Hadoop Streaming không cho ghi đè thư mục output đã tồn tại từ lần chạy trước, để tránh vô tình mất kết quả cũ | Xoá thư mục output cũ bằng `hdfs dfs -rm -r -f <output>` trước khi chạy lại (đã có sẵn ở đầu mục 6 và mục 7) |
| Job kẹt ở `map 0% reduce 0%` không tiến, log AppMaster báo "Going to preempt 1 due to lack of space for maps" | NodeManager không đủ RAM: ApplicationMaster mặc định chiếm bộ nhớ theo `yarn.scheduler.minimum-allocation-mb` (mặc định 1024MB, làm tròn lên 2048MB), chiếm gần hết bộ nhớ của NodeManager chỉ có 1536MB, không còn chỗ cấp container cho Map task (sự cố có thật đã gặp khi bật YARN cho cụm này) | Tăng `yarn.nodemanager.resource.memory-mb` (ví dụ lên 2560MB), giảm `yarn.app.mapreduce.am.resource.mb` (ví dụ 512MB) và hạ `yarn.scheduler.minimum-allocation-mb` (ví dụ 256MB) trong cấu hình YARN của cụm |
| Job báo "python3: not found" khi chạy trên YARN dù đã cài python3 trong container NameNode | Mapper/Reducer thực thi trên **NodeManager**, không phải NameNode, khi job chạy qua YARN thật — cài Python chỉ ở NameNode không đủ | Đảm bảo image dùng cho NodeManager cũng có sẵn python3 (xem `Dockerfile.nodemanager-with-python3` trong thư mục này) |

## 11. Dọn dẹp (tuỳ chọn)

```bash
docker exec hdfs-namenode hdfs dfs -rm -r -f \
  /retailstream/mapreduce_output_r1 \
  /retailstream/mapreduce_output_r3 \
  /retailstream/output_yarn
```

Không cần dừng cụm HDFS/YARN — cụm này còn được dùng chung cho các buổi
khác.

## 12. Hướng dẫn chạy nhanh bằng Script (Khuyến nghị)

Thay vì phải gõ thủ công từng lệnh ở các mục trên (dễ gặp lỗi thiếu Docker image Python3, quyền file hoặc lệch thư mục), toàn bộ quy trình đã được tự động hóa trọn gói trong thư mục `scripts/`.

### Môi trường khuyến nghị:
- **Git Bash** (trên Windows) hoặc Terminal Linux/macOS.
- Nếu dùng **PowerShell**: hãy gọi qua Git Bash bằng `bash scripts/<tên_script>.sh`.

### Thư mục làm việc (Working Directory):
Mở terminal và chuyển vào thư mục `MapReduce`:
```bash
cd "d:/school/Big Data/MapReduce"
```

### Thứ tự thực hiện:

#### Bước 1: Khởi chạy toàn bộ quy trình tự động
Chạy script chính để tự động hóa từ đầu đến cuối:
```bash
bash scripts/run-yarn-job.sh
```
*Lệnh này làm gì:*
1. **Tự động build 2 image NameNode & NodeManager** có sẵn Python 3 từ `Dockerfile.namenode-with-python3` và `Dockerfile.nodemanager-with-python3` (bỏ qua nếu đã build trước đó).
2. **Khởi động cụm HDFS + YARN** (`Hdfs/docker-compose.yml`) gồm NameNode, 2 DataNode, ResourceManager, NodeManager, HistoryServer.
3. **Thăm dò sức khỏe (Polling healthcheck)** chờ `hdfs-nodemanager1` chuyển sang trạng thái `healthy` (tối đa 150s) trước khi nộp job.
4. **Nạp dữ liệu chuẩn**: Tự động copy dữ liệu mẫu từ `00_shared_data/sample/web_logs_sample.jsonl` đưa lên HDFS tại `/retailstream/web_logs/` (nếu chưa có).
5. **Copy mapper & reducer**: Đưa `mapper.py` và `reducer.py` vào NameNode.
6. **Xóa output cũ và nộp job Hadoop Streaming trên YARN**: Chạy job đếm lượt truy cập theo sản phẩm.
7. **In kết quả**: Tự động hiển thị nội dung `part-00000` ra màn hình terminal.

#### Bước 2: Xem trạng thái và trực quan hóa trên Web UI
Sau khi job hoàn thành (hoặc trong khi job đang chạy), mở trình duyệt web:
- **YARN ResourceManager UI**: [http://localhost:8088/cluster](http://localhost:8088/cluster) (xem ApplicationMaster, danh sách node, dung lượng RAM/vCores đã cấp phát).
- **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870) (duyệt file hệ thống tại Utilities -> Browse the file system).

#### Bước 3: Dừng cụm khi kết thúc học phần
Khi không còn sử dụng cụm HDFS/YARN:
```bash
bash scripts/stop-cluster.sh
```
*Lệnh này làm gì:* Chuyển về thư mục `Hdfs` và thực thi `docker compose down` để hạ các container, giải phóng RAM mà vẫn giữ nguyên dữ liệu trong các volume.

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| **Container** | Một "hộp" chạy phần mềm biệt lập, giống một máy ảo thu nhỏ. Một cụm (Hadoop) trong dự án này gồm nhiều container chạy trên CÙNG một máy thật, giả lập nhiều máy. Lưu ý: trong tài liệu này, "container" còn xuất hiện với nghĩa thứ hai — "container tài nguyên" của YARN (đơn vị cấp phát CPU/RAM cho một Map/Reduce task cụ thể), là khái niệm khác dù trùng tên. |
| **Cluster** (cụm) | Một nhóm nhiều container/máy phối hợp làm việc cùng nhau, thay vì 1 máy làm hết. |
| **Docker Compose** | Công cụ mô tả "cần bao nhiêu container, cấu hình ra sao" trong 1 file (`docker-compose.yml`), rồi bật/tắt tất cả cùng lúc bằng 1 lệnh. |
| **HDFS** | Hệ thống lưu trữ file phân tán — một file lớn được chia nhỏ và lưu rải trên nhiều máy (ở đây là nhiều container), có sao lưu để không mất dữ liệu nếu 1 máy hỏng. |
| **NameNode** | "Người quản lý mục lục" của HDFS — biết file nào gồm những mảnh nào, nằm ở đâu. Chỉ có 1 (hoặc dự phòng). |
| **DataNode** | Nơi thực sự lưu trữ các mảnh file. Có nhiều DataNode. |
| **MapReduce** | Cách xử lý dữ liệu lớn theo 2 bước: Map (mỗi máy xử lý một phần dữ liệu riêng) → Reduce (gộp kết quả từ các máy lại). |
| **Mapper / Reducer** | Đoạn code xử lý bước Map / bước Reduce. |
| **Shuffle** | Bước "phân loại lại và chuyển dữ liệu giữa các máy" nằm giữa Map và Reduce — thường là bước tốn tài nguyên nhất. |
| **YARN / ResourceManager / NodeManager** | YARN là "người điều phối tài nguyên" cho cả cụm — quyết định job nào chạy ở đâu, dùng bao nhiêu CPU/RAM. ResourceManager là bộ não trung tâm, NodeManager là "cánh tay" chạy trên từng máy để thực thi. Có YARN thì job mới thực sự chạy phân tán qua nhiều máy; không có YARN, job chạy gói gọn trong 1 tiến trình (gọi là **LocalJobRunner**) — vẫn đúng kết quả nhưng không phải chạy phân tán thật. |
| **ApplicationMaster (AM/AppMaster)** | "Người quản lý" của MỘT job cụ thể khi chạy trên YARN — do ResourceManager cấp phát, có nhiệm vụ xin thêm tài nguyên (container) từ ResourceManager và theo dõi tiến độ các Mapper/Reducer của job đó. Mỗi job có 1 ApplicationMaster riêng. |
| **numReduceTasks** | Số lượng "luồng Reduce" chạy song song — càng nhiều thì kết quả bị chia thành càng nhiều file đầu ra nhỏ hơn. |
