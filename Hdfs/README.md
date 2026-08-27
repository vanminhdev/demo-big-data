# Thực hành HDFS — Cụm Docker RetailStream

Hướng dẫn này dựng một cụm HDFS tối thiểu (1 NameNode + 2 DataNode) bằng Docker
Compose và chạy các thao tác cơ bản: upload, list, đọc, tải xuống, xóa tệp,
kiểm tra block/replication bằng `fsck`, và quan sát hành vi của cụm khi một
DataNode gặp sự cố. Toàn bộ lệnh trong tài liệu này đã được chạy thật trên môi
trường Windows 10 + Docker Desktop (WSL2 backend) và kết quả ghi lại bên dưới
là kết quả thật, dùng để đối chiếu khi tự chạy lại.

## 1. Yêu cầu môi trường

| Mục | Tối thiểu khuyến nghị | Ghi chú |
|---|---|---|
| Docker Desktop / Docker Engine | hỗ trợ Compose v2 (`docker compose`) | đã kiểm thử với Docker 29.7.2, Docker Compose v5.4.0 |
| RAM cấp cho Docker | ≥ 4 GB | máy kiểm thử dùng Docker Desktop VM với 3,826 GiB RAM, 2 CPU — cụm 3 container chạy ổn định nhưng gần giới hạn nếu chạy song song với các bài thực hành khác |
| Đĩa trống | ≥ 3 GB | image + volume dữ liệu HDFS |
| Cổng cần trống trên host | 9000, 9870, 9864, 9865 | xem mục 8 nếu bị chiếm |

## 2. Phiên bản đã kiểm thử

| Thành phần | Image:tag | Hadoop | Java (trong container) |
|---|---|---|---|
| NameNode | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | 3.2.1 | OpenJDK 1.8.0_232 |
| DataNode | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | 3.2.1 | OpenJDK 1.8.0_232 |

Cả hai image được ghim phiên bản cụ thể, không dùng `latest`. Kiến trúc đã kiểm
thử: `linux/amd64` (Docker Desktop trên Windows, WSL2 backend).

## 3. Khởi động cụm

Sao chép tệp cấu hình mẫu thành `.env`:

```bash
cd Hdfs
cp .env.example .env
```

Biến quan trọng trong `.env`:

- `HDFS_REPLICATION` (mặc định `2`) — replication (số bản sao mỗi khối dữ liệu
  được lưu trên các DataNode khác nhau) phải nhỏ hơn hoặc bằng số DataNode
  đang chạy (cụm này có 2 DataNode).
- `NAMENODE_HTTP_PORT`, `NAMENODE_RPC_PORT`, `DATANODE1_HTTP_PORT`,
  `DATANODE2_HTTP_PORT` — đổi nếu cổng mặc định bị chiếm trên máy bạn.

Kéo image và khởi động cụm:

```bash
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
docker compose up -d
```

Kiểm tra trạng thái container:

```bash
docker compose ps
```

**Kết quả mong đợi** (đã ghi nhận thật, cả 3 container `healthy` trong vòng
dưới 1 phút):

```text
NAMES            STATUS                    PORTS
hdfs-datanode2   Up 26 seconds (healthy)   0.0.0.0:9865->9864/tcp
hdfs-datanode1   Up 26 seconds (healthy)   0.0.0.0:9864->9864/tcp
hdfs-namenode    Up 31 seconds (healthy)   0.0.0.0:9000->9000/tcp, 0.0.0.0:9870->9870/tcp
```

Xem log nếu một container không lên `healthy`:

```bash
docker logs hdfs-namenode --tail 50
docker logs hdfs-datanode1 --tail 50
docker logs hdfs-datanode2 --tail 50
```

## 4. Xác nhận cụm nhận diện đủ DataNode

```bash
docker exec hdfs-namenode hdfs dfsadmin -report
```

**Kết quả mong đợi** (trích, ngay sau khi khởi động, chưa có dữ liệu):

```text
Live datanodes (2):
Name: 172.19.0.3:9866 (hdfs-datanode2...)  Num of Blocks: 0
Name: 172.19.0.4:9866 (hdfs-datanode1...)  Num of Blocks: 0
```

Các địa chỉ `172.19.0.x` là IP nội bộ do mạng ảo Docker (Docker network) tự
động cấp phát cho từng container, không phải địa chỉ cố định — có thể đổi
sang giá trị khác ở mỗi lần `docker compose up` lại.

NameNode Web UI: `http://localhost:9870` (hoặc cổng bạn đổi qua
`NAMENODE_HTTP_PORT`). Vào tab **Datanodes** để xem danh sách Live/Dead, tab
**Utilities → Browse the file system** để duyệt cây thư mục HDFS bằng giao
diện web. DataNode Web UI ít dùng hơn: `http://localhost:9864` (datanode1),
`http://localhost:9865` (datanode2).

## 5. Nạp dữ liệu và thao tác tệp cơ bản

Dữ liệu thực hành là `web_logs_sample.jsonl` — nhật ký truy cập RetailStream,
200 dòng JSON Lines. Sao chép tệp vào container NameNode rồi đưa lên HDFS:

```bash
docker cp data/web_logs_sample.jsonl hdfs-namenode:/tmp/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -mkdir -p /retailstream/web_logs
docker exec hdfs-namenode hdfs dfs -put /tmp/web_logs_sample.jsonl \
  /retailstream/web_logs/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -ls /retailstream/web_logs
```

**Kết quả mong đợi:**

```text
Found 1 items
-rw-r--r--   2 root supergroup      41668 2026-08-20 05:06 /retailstream/web_logs/web_logs_sample.jsonl
```

Cột `2` ngay sau quyền truy cập là **replication factor thực tế của tệp**
(không phải số liên kết cứng như trên hệ thống tệp Unix cục bộ) — khớp với
`HDFS_REPLICATION=2` trong `.env`.

Đọc nội dung và đếm số dòng:

```bash
docker exec hdfs-namenode hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | head -3
docker exec hdfs-namenode hdfs dfs -cat /retailstream/web_logs/web_logs_sample.jsonl | wc -l
```

**Kết quả mong đợi:**

```text
{"event_id": "WEV000001", "event_time": "2026-08-17T04:43:27+00:00", "customer_id": "CUST00043", ...}
{"event_id": "WEV000002", "event_time": "2026-07-30T05:05:20+00:00", "customer_id": "CUST00007", ...}
{"event_id": "WEV000003", "event_time": "2026-08-13T06:56:37+00:00", "customer_id": "CUST00086", ...}
200
```

Tải tệp về và đối chiếu với bản gốc:

```bash
docker exec hdfs-namenode hdfs dfs -get /retailstream/web_logs/web_logs_sample.jsonl /tmp/web_logs_get.jsonl
docker exec hdfs-namenode sh -c "diff /tmp/web_logs_sample.jsonl /tmp/web_logs_get.jsonl && echo DIFF_OK_IDENTICAL"
```

**Kết quả mong đợi:** `DIFF_OK_IDENTICAL` — nội dung tải về giống hệt bản gốc.

Xóa tệp:

```bash
docker exec hdfs-namenode hdfs dfs -rm /retailstream/web_logs/web_logs_sample.jsonl
docker exec hdfs-namenode hdfs dfs -ls /retailstream/web_logs
```

**Kết quả mong đợi:** thông báo `Deleted ...` và `-ls` trả về danh sách rỗng.

## 6. Kiểm tra block và replication bằng `fsck`

Block là khối dữ liệu cố định kích thước mà HDFS chia nhỏ mỗi tệp ra để lưu
phân tán trên các DataNode (kích thước mặc định thường là 128 MB; tệp nhỏ hơn
kích thước này chỉ chiếm 1 block).

Chạy lại bước 5 để tệp tồn tại trở lại (`-put`), rồi:

```bash
docker exec hdfs-namenode hdfs fsck /retailstream/web_logs/web_logs_sample.jsonl \
  -files -blocks -locations
```

**Kết quả mong đợi** (trích):

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

Tệp nhỏ hơn kích thước một block nên chỉ tạo 1 block, với đúng 2 bản sao đặt
trên 2 DataNode khác nhau (`172.19.0.3` và `172.19.0.4`), khớp
`replication = 2`.

## 7. Thử nghiệm chịu lỗi DataNode

Dừng một DataNode và quan sát ngay sau đó:

```bash
docker stop hdfs-datanode1
docker exec hdfs-namenode hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
```

**Kết quả mong đợi ngay sau khi dừng (~15 giây):**

```text
Live datanodes (2):
```

Đây là hành vi mặc định thật của HDFS, không phải lỗi cấu hình: khi một
container bị `docker stop`, nó không gửi tín hiệu shutdown sạch cho NameNode,
NameNode chỉ phát hiện mất kết nối qua việc **thiếu heartbeat**. Ngưỡng
chuyển từ "Live" sang "Dead" được tính theo công thức:

```
2 × dfs.namenode.heartbeat.recheck-interval + 10 × dfs.heartbeat.interval
```

Với giá trị mặc định `dfs.namenode.heartbeat.recheck-interval = 300000 ms`
(5 phút) và `dfs.heartbeat.interval = 3 giây`, thời gian chờ là
2 × 5 + 10 × 3/60 ≈ **10,5 phút** không nhận được heartbeat.

Sau khi đợi đủ thời gian đó:

```bash
docker exec hdfs-namenode hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
```

**Kết quả mong đợi:**

```text
Live datanodes (1):
Dead datanodes (1):
```

Khởi động lại DataNode và kiểm tra sau khoảng 20 giây:

```bash
docker start hdfs-datanode1
docker exec hdfs-namenode hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
```

**Kết quả mong đợi:**

```text
Live datanodes (2):
```

DataNode tái gia nhập cụm sau khi đăng ký lại và gửi heartbeat. Trong suốt
khoảng chờ "Dead", dữ liệu vẫn đọc được bình thường nếu còn ít nhất một bản
sao sống, vì `fsck` và các thao tác đọc dùng metadata block chứ không phụ
thuộc trạng thái heartbeat tức thời.

## 8. Dừng cụm và đặt lại môi trường

Dừng cụm nhưng giữ lại dữ liệu HDFS đã tạo:

```bash
docker compose down
```

Đặt lại hoàn toàn từ trạng thái sạch (xóa volume dữ liệu HDFS, không đụng đến
`data/web_logs_sample.jsonl` hay dữ liệu của các bài thực hành khác):

```bash
docker compose down -v
docker compose up -d
```

Chạy lại từ đầu (kéo image → khởi động → nạp dữ liệu → kiểm tra) đã được xác
nhận hoạt động ổn định nhiều lần liên tiếp trên cùng một máy, cho kết quả
giống hệt mục 5–6 ở trên (200 dòng, 2 DataNode Live).

## 9. Lỗi thường gặp

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| `docker exec ... /tmp/...` báo "No such file or directory" trong Git Bash | Git Bash/MSYS tự dịch path kiểu Unix (`/tmp/...`) sang path Windows trước khi truyền vào container | Chạy `export MSYS_NO_PATHCONV=1` trước các lệnh `docker exec`/`docker cp`, hoặc dùng PowerShell |
| Cổng `9870`/`9000`/`9864`/`9865` báo "port is already allocated" | Một ứng dụng khác, hoặc một cụm HDFS cũ chưa `docker compose down`, đang giữ cổng | Đổi giá trị `*_PORT` trong `.env` rồi `docker compose up -d` lại; hoặc tìm và dừng container đang giữ cổng bằng `docker ps` |
| Container không lên trạng thái `healthy`, RAM máy tụt thấp | Docker Desktop không được cấp đủ RAM | Vào Docker Desktop → Settings → Resources, tăng RAM tối thiểu 4 GB; đóng bớt cụm Docker khác đang chạy song song |
| `docker compose up` báo lỗi quyền ghi volume, hoặc NameNode không format được | Volume cũ ở trạng thái không nhất quán từ lần chạy trước | Chạy `docker compose down -v` để xóa volume rồi khởi động lại từ sạch (mục 8) |
| Sau khi dừng DataNode, `dfsadmin -report` vẫn hiện "Live datanodes (2)" ngay lập tức | Hành vi mặc định của HDFS, chưa hết thời gian chờ heartbeat timeout | Đợi khoảng 10–11 phút hoặc theo dõi qua Web UI; đây không phải lỗi, xem mục 7 |
| Kiến trúc CPU không khớp (ví dụ Apple Silicon chạy image build cho `amd64`) | Image `bde2020/hadoop-*` được kiểm thử trên `linux/amd64` | Bật giả lập x86 trong Docker Desktop hoặc dùng máy/host có kiến trúc phù hợp |

## 10. Ghi chú phạm vi

Cụm Docker Compose này **mô phỏng nhiều node trên một máy**, không phải nhiều
máy vật lý thật. Mục tiêu là sử dụng và quan sát cụm HDFS (block, replication,
heartbeat, `fsck`), không phải tự cấu hình NameNode/DataNode từ đầu — toàn bộ
cấu hình XML được sinh sẵn từ các biến môi trường (`CORE_CONF_*`,
`HDFS_CONF_*`) khai báo trong `docker-compose.yml`.

## 11. Chạy nhanh bằng script có sẵn

Thư mục `scripts/` cung cấp các script tiện dụng thực hiện đúng chuỗi lệnh ở
mục 3–5 (`start-cluster`, `stop-cluster`, `reset-lab`, `load-sample-data`, có
cả bản `.sh` và `.ps1`) nếu muốn khởi động nhanh mà không gõ từng lệnh. Toàn
bộ lệnh HDFS ở các mục trên đều gõ trực tiếp được, không bắt buộc phải dùng
script.

## Phụ lục: Bảng thuật ngữ

| Thuật ngữ | Giải thích |
|---|---|
| **Container** | Một "hộp" chạy phần mềm biệt lập, giống một máy ảo thu nhỏ. Một cụm (Hadoop/Spark/Kafka) trong dự án này gồm nhiều container chạy trên CÙNG một máy thật, giả lập nhiều máy. |
| **Cluster** (cụm) | Một nhóm nhiều container/máy phối hợp làm việc cùng nhau, thay vì 1 máy làm hết. |
| **Docker Compose** | Công cụ mô tả "cần bao nhiêu container, cấu hình ra sao" trong 1 file (`docker-compose.yml`), rồi bật/tắt tất cả cùng lúc bằng 1 lệnh. |
| **Image / ghim version** | "Bản cài đặt đóng gói sẵn" của một phần mềm (ví dụ `mongo:8.0`). "Ghim version" nghĩa là chỉ rõ đúng phiên bản thay vì `latest` (bản mới nhất, có thể đổi bất cứ lúc nào) — để lần sau chạy lại vẫn ra kết quả giống hệt. |
| **HDFS** | Hệ thống lưu trữ file phân tán — một file lớn được chia nhỏ và lưu rải trên nhiều máy (ở đây là nhiều container), có sao lưu để không mất dữ liệu nếu 1 máy hỏng. |
| **NameNode** | "Người quản lý mục lục" của HDFS — biết file nào gồm những mảnh nào, nằm ở đâu. Chỉ có 1 (hoặc dự phòng). |
| **DataNode** | Nơi thực sự lưu trữ các mảnh file. Có nhiều DataNode. |
| **Block / Replication** | Block = 1 mảnh của file lớn. Replication = số bản sao của mỗi mảnh (ví dụ 2 nghĩa là mỗi mảnh được lưu ở 2 DataNode khác nhau, để nếu 1 cái hỏng vẫn còn bản kia). |
| **Heartbeat** | Tín hiệu "tôi vẫn sống" mà DataNode gửi định kỳ cho NameNode. Nếu NameNode không nhận được tín hiệu này trong một khoảng thời gian (mặc định ~10.5 phút), mới coi DataNode đó là "chết". |
| **fsck** | Lệnh kiểm tra "sức khoẻ" của file/hệ thống lưu trữ (file có đủ bản sao không, có bị hỏng không). |
