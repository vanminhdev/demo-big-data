# LOCAL VALIDATION REPORT – Buổi 6 (HDFS)

> Báo cáo này tự đủ nghĩa: không giả định người đọc biết cuộc hội thoại đã tạo ra
> gói này. Bối cảnh: dự án học phần "Nhập môn dữ liệu lớn", bài toán xuyên suốt
> RetailStream (xem `00_MASTER_PLAN.md`, `00_DATA_CONTRACT.md`). Buổi 6 dạy HDFS;
> Local AI (vai Integration/QA Engineer) có nhiệm vụ dựng cụm Docker HDFS thật,
> chạy thử thật các thao tác cơ bản, và xác nhận PASS/FAIL cho từng hạng mục.
> Gói triển khai nằm tại `Hdfs/` (thư mục root, ngang cấp `MongoDb/`).

## 1. Trạng thái

**Validation:** PASS

Tất cả 9 hạng mục kiểm thử (V01–V09) đã chạy thật và PASS. Không có hạng mục
FAIL. Có một quan sát hành vi thật cần lưu ý cho giảng dạy (xem mục 5, V08 và
mục 6 Issues Found ISSUE-01) — không phải lỗi cấu hình, mà là hành vi mặc định
của HDFS (heartbeat timeout ~10.5 phút trước khi DataNode dừng bị đánh dấu Dead).

## 2. Environment

| Thành phần | Version |
|---|---|
| OS | Windows 10 Pro 10.0.19045 |
| Docker | 29.7.2 (build a7dcaa6) |
| Docker Compose | v5.4.0 |
| Docker Desktop VM resources (lúc test) | 2 CPU, 3.826 GiB RAM |
| Image NameNode | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` |
| Image DataNode | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` |
| Hadoop (trong container) | 3.2.1 (xác nhận bằng `hadoop version`) |
| Java (trong container) | OpenJDK 1.8.0_232 |
| Kiến trúc | linux/amd64 (Docker Desktop, WSL2 backend) |

Cả hai image đều được **ghim version cụ thể** (không dùng `latest`), đúng quy tắc
dự án (`00_MASTER_PLAN.md` mục 7, Khung nội dung mục "Gói cài đặt Docker bắt buộc").

## 3. Cách khởi động (lệnh thật đã chạy)

```bash
cd Hdfs
cp .env.example .env
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
docker compose up -d
```

Sau đó dùng script chính thức:

```bash
export MSYS_NO_PATHCONV=1
bash scripts/start-cluster.sh
```

Kết quả: cả 3 container (`hdfs-namenode`, `hdfs-datanode1`, `hdfs-datanode2`)
đạt trạng thái `healthy` trong `docker compose ps` (dựa trên healthcheck curl vào
Web UI của từng service).

## 4. Chuẩn bị dữ liệu

Dùng đúng bộ dữ liệu chung, KHÔNG tạo dataset riêng cho buổi này:

```text
Nguồn:  00_shared_data/sample/web_logs_sample.jsonl  (200 dòng, seed=42)
Copy:   Hdfs/data/web_logs_sample.jsonl
```

Upload lên HDFS bằng script:

```bash
bash scripts/load-sample-data.sh
```

Script thực hiện: `docker cp data/web_logs_sample.jsonl hdfs-namenode:/tmp/...`
→ `hdfs dfs -mkdir -p /retailstream/web_logs` → `hdfs dfs -put -f ...`.

**Lưu ý Windows/Git Bash (giống buổi 5 MongoDB):** `docker exec`/`docker cp` với
path kiểu `/tmp/...` hoặc `/retailstream/...` bị Git Bash/MSYS tự dịch sang path
Windows, gây lỗi "No such file or directory". Khắc phục bằng
`export MSYS_NO_PATHCONV=1` trước khi chạy các lệnh này (đã áp dụng trong mọi
script `.sh` của gói này).

## 5. Validation Results

### V01 – Cluster khởi động, health check (1 NameNode + 2 DataNode)

**Result:** PASS

Command:
```bash
docker compose up -d
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Actual:
```text
NAMES            STATUS                    PORTS
hdfs-datanode2   Up 26 seconds (healthy)   0.0.0.0:9865->9864/tcp
hdfs-datanode1   Up 26 seconds (healthy)   0.0.0.0:9864->9864/tcp
hdfs-namenode    Up 31 seconds (healthy)   0.0.0.0:9000->9000/tcp, 0.0.0.0:9870->9870/tcp
```

Cả 3 container `healthy` trong vòng dưới 1 phút.

### V02 – `hdfs dfsadmin -report` xác nhận 2 DataNode sống

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs dfsadmin -report
```

Actual (trích):
```text
Live datanodes (2):
Name: 172.19.0.3:9866 (hdfs-datanode2...)  Num of Blocks: 0
Name: 172.19.0.4:9866 (hdfs-datanode1...)  Num of Blocks: 0
```

### V03 – Upload file lên HDFS (`-put`) + `-mkdir` + `-ls`

**Result:** PASS

Command:
```bash
docker cp data/web_logs_sample.jsonl hdfs-namenode:/tmp/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -mkdir -p /retailstream/web_logs
docker exec hdfs-namenode hdfs dfs -put /tmp/web_logs_sample.jsonl /retailstream/web_logs/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -ls /retailstream/web_logs
```

Actual:
```text
Found 1 items
-rw-r--r--   2 root supergroup      41668 2026-08-20 05:06 /retailstream/web_logs/web_logs_sample.jsonl
```
Cột `2` xác nhận replication factor = 2 áp dụng cho file (đúng `HDFS_REPLICATION`
trong `.env`, khớp với số DataNode có trong cụm).

### V04 – Đọc nội dung (`-cat`) và đối chiếu số dòng

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | head -3
docker exec hdfs-namenode hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | wc -l
```

Actual:
```text
{"event_id": "WEV000001", "event_time": "2026-08-17T04:43:27+00:00", "customer_id": "CUST00043", ...}
{"event_id": "WEV000002", "event_time": "2026-07-30T05:05:20+00:00", "customer_id": "CUST00007", ...}
{"event_id": "WEV000003", "event_time": "2026-08-13T06:56:37+00:00", "customer_id": "CUST00086", ...}
200
```
200 dòng khớp đúng số dòng file nguồn `00_shared_data/sample/web_logs_sample.jsonl`.

### V05 – Tải file về (`-get`) và so khớp nội dung

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs dfs -get /retailstream/web_logs/web_logs_sample.jsonl /tmp/web_logs_get.jsonl
docker exec hdfs-namenode sh -c "diff /tmp/web_logs_sample.jsonl /tmp/web_logs_get.jsonl && echo DIFF_OK_IDENTICAL"
```

Actual:
```text
DIFF_OK_IDENTICAL
```

### V06 – Xoá file (`-rm`)

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs dfs -rm /retailstream/web_logs/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -ls /retailstream/web_logs
```

Actual:
```text
Deleted /retailstream/web_logs/web_logs_sample.jsonl
(ls trả về rỗng — không còn item nào)
```

### V07 – `hdfs fsck` xem block/replication

**Result:** PASS

Command:
```bash
docker exec hdfs-namenode hdfs fsck /retailstream/web_logs/web_logs_sample.jsonl -files -blocks -locations
```

Actual (trích, chạy khi file còn tồn tại, trước V06):
```text
/retailstream/web_logs/web_logs_sample.jsonl 41668 bytes, replicated: replication=2, 1 block(s):  OK
0. BP-1966078123-172.19.0.2-...:blk_1073741825_1001 len=41668 Live_repl=2
   [DatanodeInfoWithStorage[172.19.0.3:9866,...], DatanodeInfoWithStorage[172.19.0.4:9866,...]]

Status: HEALTHY
 Number of data-nodes: 2
 Total blocks (validated): 1 (avg. block size 41668 B)
 Minimally replicated blocks: 1 (100.0 %)
 Under-replicated blocks: 0 (0.0 %)
 Average block replication: 2.0
 Missing blocks: 0
 Corrupt blocks: 0
The filesystem under path '...' is HEALTHY
```
Xác nhận: 1 block, 2 bản sao đặt trên đúng 2 DataNode khác nhau (`172.19.0.3` và
`172.19.0.4`), replication = 2, trạng thái HEALTHY.

### V08 – Dừng một DataNode, quan sát, khởi động lại

**Result:** PASS

Command:
```bash
docker stop hdfs-datanode1
# ngay sau khi dừng (~15s):
docker exec hdfs-namenode hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
```

Actual (ngay sau khi dừng, ~15s):
```text
Live datanodes (2):
```
**Quan sát quan trọng:** NameNode KHÔNG lập tức đánh dấu DataNode1 là Dead —
`dfsadmin -report` vẫn hiện "Live datanodes (2)" ngay sau khi container bị dừng
(do container tắt đột ngột, không gửi tín hiệu shutdown sạch, NameNode chỉ dựa
vào việc thiếu heartbeat). Sau khi đợi đủ thời gian `heartbeat.recheck-interval`
mặc định của Hadoop (~10.5 phút):

```bash
docker exec hdfs-namenode hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
```
```text
Live datanodes (1):
Dead datanodes (1):
```

Report chi tiết xác nhận `hdfs-datanode1` chuyển sang mục "Dead datanodes",
`Last contact` dừng lại đúng thời điểm container bị `docker stop`. Khởi động lại:

```bash
docker start hdfs-datanode1
# ~20s sau:
docker exec hdfs-namenode hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
```
```text
Live datanodes (2):
```
DataNode1 tái gia nhập cụm thành công, quay lại "Live" trong ~20 giây sau khi
container khởi động lại (đăng ký lại + gửi heartbeat).

### V09 – Chạy lại từ trạng thái sạch (clean state)

**Result:** PASS

Command:
```bash
docker compose down -v          # xoá container + volume (namenode/datanode1/datanode2 data)
bash scripts/start-cluster.sh   # dựng lại từ đầu, tự chờ health
bash scripts/load-sample-data.sh
docker exec hdfs-namenode hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | wc -l
docker exec hdfs-namenode hdfs dfsadmin -report | grep "Live datanodes"
```

Actual:
```text
[start-cluster] all containers healthy.
[load-sample-data] uploaded. Listing:
Found 1 items ... web_logs_sample.jsonl
200
Live datanodes (2)
```
Xác nhận toàn bộ gói (`docker-compose.yml` + `.env.example` + `scripts/*`) chạy
lại được từ trạng thái sạch (down -v rồi up lại), không cần can thiệp thủ công.

## 6. Issues Found

### ISSUE-01

**Severity:** Minor (hành vi thật cần ghi vào tài liệu giảng dạy, không phải bug)

**Hiện tượng:** Sau khi `docker stop` một DataNode, `hdfs dfsadmin -report` và
NameNode Web UI vẫn hiển thị DataNode đó là "Live" trong khoảng ~10 phút, thay vì
phát hiện ngay lập tức.

**Nguyên nhân:** Hành vi mặc định của HDFS — NameNode chỉ đánh dấu DataNode "Dead"
sau khi hết `dfs.namenode.heartbeat.recheck-interval` (mặc định 300000ms) kết hợp
`dfs.heartbeat.interval` (mặc định 3s), tổng cộng ~10.5 phút không nhận heartbeat.
Đây không phải lỗi cấu hình của gói Docker này.

**Cách sửa:** Không cần "sửa" — cần Content AI ghi rõ trong tài liệu đọc/slide để
sinh viên không hiểu nhầm là hệ thống phát hiện lỗi DataNode tức thời. Nếu muốn
demo trên lớp có phản hồi nhanh hơn cho mục đích sư phạm, có thể cân nhắc set
`HDFS_CONF_dfs_namenode_heartbeat_recheck___interval` nhỏ hơn trong
`docker-compose.yml` — **hiện KHÔNG áp dụng thay đổi này** để giữ hành vi mặc định
thật của HDFS (đúng nguyên tắc "không tối ưu quá mức nhập môn" và để sinh viên
thấy đúng hành vi production-like). Đề xuất: nêu rõ trong tài liệu, không sửa cấu hình.

**File ảnh hưởng:** không có file cần sửa trong `Hdfs/`; đây là nội dung cần đưa
vào `06_hdfs_practice.md` (Content AI), mục "lỗi thường gặp" / phần thực hành fault-tolerance.

## 7. Mismatch với tài liệu Content AI

| Vị trí | Nội dung hiện tại | Thực tế | Đề xuất |
|---|---|---|---|
| `bigdata_ai_coordination/sessions/06_hdfs/CONTENT_REPORT.md` | "Chưa cập nhật" (trống hoàn toàn) | Chưa có slide, tài liệu đọc, hay danh sách validation item `V01...` nào được Content AI soạn cho buổi 6 tại thời điểm viết report này (2026-08-20) | Content AI cần đọc `LOCAL_REPORT.md` này để viết `06_hdfs_practice.md` + `CONTENT_REPORT.md`, dùng đúng các lệnh/kết quả đã kiểm chứng ở mục 5 (đặc biệt V08 về heartbeat timeout — điểm dễ bị viết sai nếu chỉ dựa vào lý thuyết chung) |
| Khung nội dung mục "Gói cài đặt Docker bắt buộc" yêu cầu thư mục `healthcheck/` riêng | — | Gói này định nghĩa healthcheck trực tiếp trong `docker-compose.yml` (mục `healthcheck:` của từng service dùng `curl`), không tạo thư mục `healthcheck/` riêng vì không cần script phức tạp hơn | Có thể chấp nhận được vì mục tiêu (health check hoạt động) đã đạt; nếu giảng viên muốn đúng cấu trúc thư mục 100% như khung, có thể tách ra `healthcheck/namenode.sh` — hiện chưa làm vì không cần thiết về mặt chức năng |
| Khung nội dung yêu cầu "Nếu Buổi 7 chạy MapReduce trên YARN, cùng gói hoặc profile mở rộng phải có ResourceManager..." | — | **ĐÃ LÀM (2026-08-22)**: Buổi 7 đã mở rộng đúng `Hdfs/docker-compose.yml` thêm ResourceManager/NodeManager/HistoryServer, đúng như khung nội dung yêu cầu | Không còn hành động cần làm — Content AI có thể trích dẫn `sessions/07_mapreduce/LOCAL_REPORT.md` V06/V07 khi viết nội dung liên quan đến YARN |

## 8. Khả năng chạy lại

- [x] chạy từ clean state (V09: `docker compose down -v` rồi `start-cluster.sh` + `load-sample-data.sh` — thành công, 200 dòng, 2 live datanodes);
- [x] version được ghim (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`, `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8`);
- [x] dữ liệu có đường dẫn tương đối (`Hdfs/data/web_logs_sample.jsonl`, copy từ `00_shared_data/sample/`);
- [x] container truy cập được dữ liệu (`docker cp` + `hdfs dfs -put`);
- [x] expected output được lưu (mục 5 của report này — số liệu thật, không suy đoán);
- [x] reset script hoạt động (`scripts/reset-lab.sh` — `docker compose down -v`, đã test qua V09 với lệnh tương đương thủ công).

## 9. Kết luận cho giảng viên

Có thể dùng để dạy: **YES**

Các điểm cần đọc trước khi duyệt:

1. Toàn bộ 9 hạng mục validation (V01–V09: health check, upload, ls, cat, get,
   rm, fsck, dừng/khởi động lại DataNode, chạy lại từ sạch) đã chạy thật và PASS
   trên cụm `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` +
   `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` (Hadoop 3.2.1). Số liệu
   trong mục 5 là số liệu thật, dùng trực tiếp làm ví dụ trong slide/tài liệu đọc.
2. Điểm sư phạm quan trọng nhất rút ra từ thực hành: HDFS **không** phát hiện
   DataNode lỗi tức thời — có độ trễ ~10.5 phút mặc định (V08, ISSUE-01). Đây là
   nội dung cần Content AI trình bày rõ, tránh sinh viên hiểu nhầm.
3. `CONTENT_REPORT.md` của buổi 6 hiện đang **trống hoàn toàn** — Content AI
   chưa bắt đầu soạn slide/tài liệu đọc/danh sách `V01...` cho buổi này. Local AI
   đã tự đánh mã `V01`–`V09` theo đúng các thao tác bắt buộc trong `BRIEF.md` và
   Khung nội dung (mục "Thực hành tích hợp trên lớp"), Content AI có thể dùng lại
   đúng các mã này khi viết tài liệu để khớp 1:1.
4. ~~Gói KHÔNG bao gồm YARN/MapReduce~~ — **ĐÃ BỔ SUNG (2026-08-22)**: Buổi 7
   (MapReduce) đã mở rộng `Hdfs/docker-compose.yml` (dùng chung file này)
   thêm ResourceManager, NodeManager, HistoryServer — đúng như dự kiến ở
   dòng này. Cụm HDFS của Buổi 6 không đổi cấu hình, chỉ được bổ sung thêm
   service YARN. Xem `sessions/07_mapreduce/LOCAL_REPORT.md` V06/V07.
5. Dữ liệu dùng đúng bộ RetailStream chung (`00_shared_data/sample/web_logs_sample.jsonl`,
   seed=42, 200 dòng) — nhất quán với buổi 5 (MongoDB) đã dùng cùng bộ dữ liệu.
