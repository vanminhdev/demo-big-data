# 00 – GIẢI THÍCH THUẬT NGỮ (dành cho giảng viên đọc LOCAL_REPORT)

Các báo cáo kỹ thuật (`LOCAL_REPORT.md`) và README của dự án dùng nhiều thuật
ngữ tiếng Anh chuyên ngành (thường không dịch trong ngành phần mềm vì dịch ra
tiếng Việt dễ gây hiểu nhầm hơn). File này giải thích chúng bằng tiếng Việt đơn
giản để **giảng viên đọc report không bị vướng do không quen thuật ngữ**. Đây
KHÔNG phải tài liệu dạy cho sinh viên (tài liệu đọc cho sinh viên do Content AI
soạn theo `00_SHARED_CONTEXT.md` đã có quy tắc giải thích thuật ngữ riêng, mức
độ sư phạm hơn).

Sắp xếp theo nhóm chủ đề, không theo alphabet, để tra theo buổi đang đọc.

## Chung — mọi buổi

| Thuật ngữ | Giải thích đơn giản |
|---|---|
| **Container** | Một "hộp" chạy phần mềm biệt lập, giống một máy ảo thu nhỏ. Một cụm (Hadoop/Spark/Kafka) trong dự án này gồm nhiều container chạy trên CÙNG một máy thật, giả lập nhiều máy. |
| **Cluster** (cụm) | Một nhóm nhiều container/máy phối hợp làm việc cùng nhau, thay vì 1 máy làm hết. |
| **Docker Compose** | Công cụ mô tả "cần bao nhiêu container, cấu hình ra sao" trong 1 file (`docker-compose.yml`), rồi bật/tắt tất cả cùng lúc bằng 1 lệnh. |
| **Image / ghim version** | "Bản cài đặt đóng gói sẵn" của một phần mềm (ví dụ `mongo:8.0`). "Ghim version" nghĩa là chỉ rõ đúng phiên bản (`8.0`) thay vì `latest` (bản mới nhất, có thể đổi bất cứ lúc nào) — để lần sau chạy lại vẫn ra kết quả giống hệt. |
| **PASS / FAIL / PASS_WITH_NOTES** | PASS = đã chạy thật, đúng kết quả. FAIL = chạy nhưng sai/không được. PASS_WITH_NOTES = chạy được, có kết quả thật, nhưng có điểm cần giảng viên lưu ý (không phải lỗi nghiêm trọng). |
| **Explicit schema / schema tường minh** | Khai báo rõ tên cột và kiểu dữ liệu (ví dụ "cột `price` là số thập phân") thay vì để phần mềm tự đoán — tránh đoán sai. |
| **Evidence** | Bằng chứng — số liệu/log thật lấy từ hệ thống, không phải suy đoán. |
| **OOM / OOMKilled** | "Hết bộ nhớ" (Out Of Memory) — khi 1 container/tiến trình dùng RAM vượt quá giới hạn được cấp (`mem_limit`), hệ điều hành sẽ tự tắt (kill) nó để bảo vệ máy. Trong report, đây là sự cố thật đã gặp và đã ghi lại cách sửa (thường là tăng `mem_limit` hoặc giảm tải chạy đồng thời), không phải lỗi logic của job. |
| **mem_limit** | Giới hạn RAM tối đa cấp cho 1 container, khai báo trong `docker-compose.yml`. Nếu container cần nhiều RAM hơn mức này sẽ bị OOM (xem trên). |

## MongoDB (Buổi 5)

| Thuật ngữ | Giải thích |
|---|---|
| **Document** | Một "bản ghi" trong MongoDB, giống 1 dòng trong bảng Excel nhưng linh hoạt hơn (các document trong cùng 1 nhóm có thể có cột khác nhau). |
| **Collection** | Một nhóm document, giống khái niệm "bảng" trong CSDL quan hệ. |
| **CRUD** | 4 thao tác cơ bản: Create (tạo), Read (đọc), Update (sửa), Delete (xoá). |
| **Embedding** | Gộp dữ liệu liên quan vào chung 1 document (ví dụ nhét luôn danh sách sản phẩm của 1 đơn hàng vào chính document đơn hàng đó). Đọc nhanh, nhưng document có thể phình to. |
| **Referencing** | Tách dữ liệu liên quan ra collection riêng, chỉ lưu "mã liên kết" (giống khoá ngoại trong CSDL quan hệ). Linh hoạt hơn, nhưng khi đọc phải nối (`$lookup`) 2 nơi lại. |
| **Index / explain()** | Index = "mục lục" giúp tìm dữ liệu nhanh hơn thay vì quét toàn bộ. `explain()` = lệnh hỏi hệ thống "bạn đã tìm bằng cách nào", dùng để kiểm tra index có thực sự được dùng không. |
| **Aggregation Pipeline** | Một chuỗi bước xử lý dữ liệu nối tiếp nhau (lọc → nối → nhóm → sắp xếp...) để ra báo cáo, ví dụ "doanh thu theo tháng". |

## HDFS (Buổi 6) và MapReduce (Buổi 7)

| Thuật ngữ | Giải thích |
|---|---|
| **HDFS** | Hệ thống lưu trữ file phân tán — một file lớn được chia nhỏ và lưu rải trên nhiều máy (ở đây là nhiều container), có sao lưu để không mất dữ liệu nếu 1 máy hỏng. |
| **NameNode** | "Người quản lý mục lục" của HDFS — biết file nào gồm những mảnh nào, nằm ở đâu. Chỉ có 1 (hoặc dự phòng). |
| **DataNode** | Nơi thực sự lưu trữ các mảnh file. Có nhiều DataNode. |
| **Block / Replication** | Block = 1 mảnh của file lớn. Replication = số bản sao của mỗi mảnh (ví dụ 2 nghĩa là mỗi mảnh được lưu ở 2 DataNode khác nhau, để nếu 1 cái hỏng vẫn còn bản kia). |
| **Heartbeat** | Tín hiệu "tôi vẫn sống" mà DataNode gửi định kỳ cho NameNode. Nếu NameNode không nhận được tín hiệu này trong một khoảng thời gian (mặc định ~10.5 phút), mới coi DataNode đó là "chết". |
| **fsck** | Lệnh kiểm tra "sức khoẻ" của file/hệ thống lưu trữ (file có đủ bản sao không, có bị hỏng không). |
| **MapReduce** | Cách xử lý dữ liệu lớn theo 2 bước: Map (mỗi máy xử lý một phần dữ liệu riêng) → Reduce (gộp kết quả từ các máy lại). |
| **Mapper / Reducer** | Đoạn code xử lý bước Map / bước Reduce. |
| **Shuffle** | Bước "phân loại lại và chuyển dữ liệu giữa các máy" nằm giữa Map và Reduce — thường là bước tốn tài nguyên nhất. |
| **YARN / ResourceManager / NodeManager** | YARN là "người điều phối tài nguyên" cho cả cụm — quyết định job nào chạy ở đâu, dùng bao nhiêu CPU/RAM. ResourceManager là bộ não trung tâm, NodeManager là "cánh tay" chạy trên từng máy để thực thi. Có YARN thì job mới thực sự chạy phân tán qua nhiều máy; không có YARN, job chạy gói gọn trong 1 tiến trình (gọi là **LocalJobRunner**) — vẫn đúng kết quả nhưng không phải chạy phân tán thật. (Cập nhật: từ 2026-08-22, dự án đã có YARN thật cho MapReduce Buổi 7, xem `sessions/07_mapreduce/LOCAL_REPORT.md` mục V06.) |
| **ApplicationMaster (AM/AppMaster)** | "Người quản lý" của MỘT job cụ thể khi chạy trên YARN — do ResourceManager cấp phát, có nhiệm vụ xin thêm tài nguyên (container) từ ResourceManager và theo dõi tiến độ các Mapper/Reducer của job đó. Mỗi job có 1 ApplicationMaster riêng. |
| **numReduceTasks** | Số lượng "luồng Reduce" chạy song song — càng nhiều thì kết quả bị chia thành càng nhiều file đầu ra nhỏ hơn. |

## Spark / PySpark (Buổi 9, 10)

| Thuật ngữ | Giải thích |
|---|---|
| **Spark Standalone Cluster** | Cụm Spark tự quản lý (không cần YARN/Kubernetes), gồm 1 Master (điều phối) + nhiều Worker (thực thi việc). |
| **Driver** | Chương trình chính điều khiển toàn bộ job Spark (chạy trên máy nộp job). |
| **Executor** | Tiến trình thực sự chạy trên từng Worker để xử lý dữ liệu — nếu thấy có executor chạy trên nhiều Worker khác nhau, nghĩa là job thật sự chạy phân tán (không phải chạy 1 mình trên máy local). |
| **local[\*]** | Chế độ chạy Spark ngay trên 1 máy (không dùng cluster), dùng để học/thử nhanh. `local[*]` nghĩa là dùng tất cả CPU core có sẵn của máy đó. |
| **Job / Stage / Task** | 1 hành động (ví dụ "tính tổng doanh thu") tạo ra 1 Job. Job được chia thành nhiều Stage (mỗi lần cần "xáo trộn" dữ liệu giữa các máy = 1 ranh giới Stage mới). Mỗi Stage lại chia nhỏ thành nhiều Task chạy song song. |
| **Partition** | Một "phần" của dữ liệu được xử lý độc lập — dữ liệu càng được chia thành nhiều partition thì càng chạy song song được nhiều. |
| **DAG** | Sơ đồ các bước xử lý nối tiếp nhau mà Spark lên kế hoạch trước khi thực sự chạy (Directed Acyclic Graph — "đồ thị có hướng không quay vòng", không cần nhớ tên đầy đủ, chỉ cần hiểu là "kế hoạch thực thi"). |
| **Lazy evaluation** | Spark không xử lý ngay khi bạn viết lệnh — nó chỉ "ghi nhớ kế hoạch" và đợi tới khi thực sự cần kết quả (gọi 1 "action") mới chạy thật. |
| **Shuffle** (trong Spark) | Giống MapReduce — bước tốn kém khi phải trộn/chuyển dữ liệu giữa các máy (ví dụ khi `join` hoặc `groupBy` dữ liệu nằm rải rác). |
| **explain()** | Lệnh xem "Spark định làm gì" (kế hoạch thực thi) trước/sau khi tối ưu, giúp hiểu vì sao 1 câu lệnh chạy nhanh/chậm. |
| **spark-submit** | Lệnh dùng để "nộp" 1 chương trình Spark cho cluster chạy. |

## Structured Streaming (Buổi 11) và Kafka (Buổi 13)

| Thuật ngữ | Giải thích |
|---|---|
| **Streaming (xử lý luồng)** | Xử lý dữ liệu liên tục, mới đến đâu xử lý đến đó — khác với "batch" (xử lý theo lô, gom đủ dữ liệu rồi chạy 1 lần). |
| **Window (cửa sổ thời gian)** | Gom các sự kiện lại theo từng khoảng thời gian cố định để tính toán (ví dụ "đếm số lượt xem mỗi 5 phút"). |
| **Watermark** | Một "hạn chót" cho phép dữ liệu đến muộn bao lâu vẫn được tính. Sự kiện đến sau hạn này sẽ bị bỏ qua (tuỳ chế độ). Có watermark giúp hệ thống biết khi nào "chốt" một cửa sổ thời gian thay vì chờ mãi. |
| **Late data (dữ liệu đến muộn)** | Sự kiện có thời gian xảy ra (event time) sớm nhưng vì lý do mạng/hệ thống lại "đến nơi" trễ hơn các sự kiện khác. |
| **Checkpoint** | "Điểm lưu tạm" ghi lại job đã xử lý tới đâu — nếu job bị dừng và chạy lại, nó đọc checkpoint để tiếp tục đúng chỗ, không phải xử lý lại từ đầu. |
| **Output mode (complete / append / update)** | Cách kết quả được ghi ra mỗi lần cập nhật: `complete` = ghi lại toàn bộ kết quả từ đầu mỗi lần; `append` = chỉ ghi thêm dòng mới; `update` = chỉ ghi những dòng có thay đổi. |
| **Kafka broker** | 1 "máy chủ" Kafka nhận và lưu tạm các sự kiện gửi tới. |
| **Topic** | Một "kênh" để gửi/nhận sự kiện trong Kafka (giống 1 hàng đợi có tên). |
| **Partition (trong Kafka)** | 1 topic được chia thành nhiều partition để nhiều máy có thể đọc/ghi song song — sự kiện trong CÙNG 1 partition mới được đảm bảo đúng thứ tự. |
| **Producer / Consumer** | Producer = bên gửi sự kiện vào Kafka. Consumer = bên đọc sự kiện ra. |
| **Consumer group** | Một nhóm consumer cùng đọc 1 topic — Kafka tự chia các partition cho từng consumer trong nhóm để không ai đọc trùng nhau. |
| **Offset** | "Số thứ tự" của 1 sự kiện trong 1 partition — dùng để biết đã đọc tới đâu. |
| **KRaft** | Cách Kafka tự quản lý nội bộ mà KHÔNG cần một phần mềm phụ trợ tên là Zookeeper (cách cũ) — đơn giản hoá triển khai, không ảnh hưởng khái niệm topic/partition/consumer group. |

## MLlib — học máy (Buổi 12)

| Thuật ngữ | Giải thích |
|---|---|
| **Feature (đặc trưng)** | Các thông tin đầu vào dùng để dự đoán (ví dụ: khách hàng thuộc phân khúc nào, thanh toán bằng gì). |
| **Label (nhãn)** | Kết quả cần dự đoán (ví dụ: đơn hàng có bị hủy hay không). |
| **Train / Test split** | Chia dữ liệu thành 2 phần: phần "Train" để mô hình học, phần "Test" để kiểm tra xem mô hình học tốt tới đâu trên dữ liệu nó CHƯA từng thấy. |
| **Pipeline (MLlib)** | Một chuỗi bước xử lý dữ liệu + huấn luyện mô hình được đóng gói lại để chạy lại dễ dàng, đúng thứ tự mỗi lần. |
| **VectorAssembler** | Công cụ gộp nhiều cột feature riêng lẻ thành 1 "vector" (danh sách số) để mô hình học máy có thể xử lý. |
| **StringIndexer / OneHotEncoder** | Cách chuyển dữ liệu dạng chữ (ví dụ "Hà Nội", "TP.HCM") thành dạng số để mô hình hiểu được. |
| **Evaluator** | Công cụ chấm điểm xem mô hình dự đoán tốt tới đâu. |
| **AUC** | Một điểm số (0 đến 1) đo mức độ mô hình phân biệt đúng 2 nhóm (ví dụ "sẽ hủy" và "không hủy"). AUC = 0.5 nghĩa là mô hình đoán chẳng khác gì tung đồng xu; AUC càng gần 1 thì càng phân biệt tốt. |
| **Precision / Recall / F1** | 3 cách đo độ chính xác khác nhau khi 2 nhóm cần dự đoán bị lệch số lượng (ví dụ ít đơn bị hủy hơn nhiều so với đơn thành công) — dùng khi chỉ nhìn "% đoán đúng chung" (accuracy) dễ gây hiểu lầm. |
| **Data leakage (rò rỉ dữ liệu)** | Lỗi để mô hình "nhìn trộm" thông tin từ tập Test trong lúc học ở tập Train — làm điểm số trông tốt giả tạo, không phản ánh đúng thực tế. |
| **PipelineModel (save/load)** | Sau khi huấn luyện xong, mô hình được lưu lại thành file để lần sau dùng lại ngay, không phải huấn luyện lại từ đầu. |

## Buổi 15 – Tối ưu

| Thuật ngữ | Giải thích |
|---|---|
| **Data skew (lệch dữ liệu)** | Tình trạng dữ liệu bị dồn không đều — ví dụ 1 sản phẩm chiếm 80% lượt xem — khiến máy xử lý phần đó bị quá tải trong khi các máy khác rảnh, làm chậm cả job. |
| **Small files problem** | Có quá nhiều file nhỏ thay vì ít file lớn — làm hệ thống tốn công quản lý/mở từng file, chậm hơn dù tổng dung lượng dữ liệu không đổi. |
| **Hot partition** | Giống data skew nhưng ở cấp độ Kafka — 1 partition nhận quá nhiều sự kiện so với các partition khác (thường do chọn "key" phân vùng không tốt). |
| **Broadcast join** | Một cách tối ưu khi join 2 bảng mà 1 bảng rất nhỏ — thay vì "xáo trộn" (shuffle) cả 2 bảng lớn qua lại giữa các máy, hệ thống gửi hẳn 1 bản sao bảng nhỏ tới TẤT CẢ các máy, tránh phải shuffle bảng lớn. |
