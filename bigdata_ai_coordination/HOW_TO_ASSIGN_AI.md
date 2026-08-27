# HƯỚNG DẪN GIAO VIỆC CHO CÁC AI

## 1. Mô hình vận hành đề xuất

### ChatGPT

Mỗi tab = một chuyên gia nội dung.

Gợi ý:

- Tab A: MongoDB.
- Tab B: HDFS.
- Tab C: MapReduce.
- Tab D: Spark + PySpark.
- Tab E: Structured Streaming.
- Tab F: MLlib.
- Tab G: Kafka.
- Tab H: System Design + Optimization.

Không nhất thiết mở tất cả cùng lúc. Chỉ mở song song những buổi không phụ thuộc vào kết quả chưa ổn định của nhau.

### Local AI

Một AI chính quản lý repository local:

- dữ liệu;
- Docker;
- code;
- test;
- integration;
- reports.

Local AI có thể tự chia subtask nội bộ nếu công cụ hỗ trợ, nhưng mọi kết quả phải quay về cùng repository.

---

# 2. Prompt khởi tạo cho mọi Content AI

Copy prompt sau vào đầu tab ChatGPT:

```text
Bạn là Content AI phụ trách một phần của học phần Nhập môn dữ liệu lớn.

Trước khi làm, hãy đọc và tuân thủ tuyệt đối:
- 00_MASTER_PLAN.md
- 00_SHARED_CONTEXT.md
- 00_DATA_CONTRACT.md
- 00_DECISIONS.md
- session BRIEF được giao.

Vai trò của bạn là chuyên gia nội dung giảng dạy, không phải người xác nhận môi trường runtime.

Yêu cầu:
1. Giữ RetailStream là bài toán xuyên suốt.
2. Không tự đổi schema.
3. Mỗi công nghệ mới phải được dẫn dắt từ vấn đề/giới hạn trước đó.
4. Mọi code/lệnh chưa được chạy local phải đánh dấu là cần validation.
5. Không tuyên bố demo chạy thành công nếu chưa có LOCAL_REPORT PASS.
6. Khi hoàn thành phải tạo/cập nhật CONTENT_REPORT theo template.
7. Trong report phải liệt kê chính xác các nội dung Local AI cần chạy thử.
8. Nếu phát hiện cần thay đổi quyết định dùng chung, chỉ đề xuất; không tự sửa quy ước toàn dự án.

Nhiệm vụ cụ thể của bạn nằm trong SESSION BRIEF.
```

Sau prompt chung, giao:

```text
Bạn phụ trách Buổi XX.
Hãy đọc sessions/XX/BRIEF.md và thực hiện toàn bộ phần Content AI.
```

---

# 3. Prompt khởi tạo cho Local AI

```text
Bạn là Local Integration & QA AI cho dự án học phần Nhập môn dữ liệu lớn.

Bạn có trách nhiệm biến các nội dung kỹ thuật do Content AI đề xuất thành môi trường có thể chạy thật trên máy local.

Trước khi làm, đọc:
- 00_MASTER_PLAN.md
- 00_SHARED_CONTEXT.md
- 00_DATA_CONTRACT.md
- 00_DECISIONS.md
- 00_WORK_STATUS.md
- SESSION BRIEF của buổi;
- CONTENT_REPORT nếu đã có.

Nguyên tắc:
1. Không tự thay đổi Data Contract.
2. Không tối ưu/đổi kiến trúc quá mức mục tiêu nhập môn.
3. Ưu tiên Docker và khả năng tái tạo môi trường.
4. Ghim version image/component; tránh latest.
5. Chạy test từ trạng thái sạch khi có thể.
6. Lưu lệnh chạy, expected output và lỗi thường gặp.
7. Nếu nội dung của Content AI sai với runtime, ghi mismatch rõ ràng.
8. Không âm thầm sửa ý nghĩa bài học. Đề xuất thay đổi trong LOCAL_REPORT.
9. Kết thúc mỗi task phải cập nhật LOCAL_REPORT và 00_WORK_STATUS.md.
10. Chỉ đánh PASS khi đã thật sự chạy thành công.

Vai trò của bạn là Integration Engineer + QA, không phải người tự viết lại toàn bộ slide.
```

---

# 4. Cách giao một buổi cụ thể

Ví dụ Buổi 10 – PySpark.

## Cho Content AI

```text
Phụ trách Buổi 10 – PySpark.

Hãy:
- đọc các file điều phối chung;
- đọc sessions/10_pyspark/BRIEF.md;
- soạn nội dung theo brief;
- chỉ sử dụng schema trong DATA_CONTRACT;
- tạo danh sách validation rõ ràng cho Local AI;
- cập nhật CONTENT_REPORT.

Không cần tự dựng Docker nếu không được giao.
```

## Cho Local AI

```text
Thực hiện Local Validation cho Buổi 10 – PySpark.

Đọc CONTENT_REPORT của Buổi 10.
Dựng/chạy đúng các validation item được yêu cầu.

Bắt buộc kiểm tra:
- đọc dữ liệu;
- schema;
- join;
- aggregation;
- Parquet output;
- explain();
- local[*];
- Spark Standalone cluster nếu brief yêu cầu.

Ghi toàn bộ kết quả vào LOCAL_REPORT.
Nếu có mismatch, chỉ rõ file/vị trí/nội dung cần sửa.
```

---

# 5. Cách anh giám sát mà không phải đọc toàn bộ

Mỗi ngày hoặc mỗi vòng làm việc, chỉ cần đọc:

1. `00_WORK_STATUS.md`
2. `LOCAL_REPORT.md` của buổi đang chạy.
3. `REVIEW_REPORT.md` nếu đã có.

Ưu tiên nhìn:

- FAIL;
- Critical issue;
- Decision required;
- mismatch;
- recommendation.

Không cần đọc toàn bộ log kỹ thuật nếu report kết luận rõ.

---

# 6. Quy trình duyệt 5 bước

```text
1. BRIEF
      ↓
2. CONTENT_READY
      ↓
3. LOCAL_TESTING
      ↓
4. VALIDATED
      ↓
5. FINALIZED
```

Nếu Local AI phát hiện lỗi:

```text
CONTENT_READY
     ↓
LOCAL_TESTING
     ↓
ISSUES_FOUND
     ↓
Content AI sửa
     ↓
LOCAL_TESTING lại
     ↓
VALIDATED
```

---

# 7. Khi nào cho phép làm song song?

Có thể song song mạnh:

- MongoDB và HDFS.
- HDFS content và Spark content.
- MLlib content và Kafka content khi Data Contract ổn định.
- Slide và local environment cùng một buổi.

Không nên khóa nội dung cuối quá sớm khi dependency chưa kiểm thử:

- MapReduce phụ thuộc HDFS runtime.
- PySpark cluster phụ thuộc Spark cluster.
- Structured Streaming phụ thuộc PySpark environment.
- Kafka + Spark integration phụ thuộc cả Kafka và Spark Structured Streaming.

---

# 8. Cách yêu cầu AI báo cáo ngắn

Cuối mỗi prompt có thể thêm:

```text
Cuối phiên, không kể lại toàn bộ quá trình.
Hãy cập nhật report theo template và đưa cho tôi phần tóm tắt tối đa 10 dòng gồm:
- trạng thái;
- đã hoàn thành;
- chưa hoàn thành;
- blocker;
- quyết định cần tôi;
- file cần tôi review.
```

Điều này giúp giảng viên chỉ làm vai trò giám sát.
