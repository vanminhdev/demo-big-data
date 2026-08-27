# Bộ điều phối AI – Học phần Nhập môn dữ liệu lớn

Bộ tài liệu này dùng để điều phối đồng thời hai luồng công việc:

1. **Content AI trên ChatGPT**: soạn slide, tài liệu đọc, bài thực hành, câu hỏi, ví dụ và nội dung giảng dạy.
2. **Local AI trên máy giảng viên**: chuẩn bị dữ liệu, Docker, mã demo, chạy thử, kiểm tra tương thích và xác nhận nội dung kỹ thuật thực sự chạy được.

Giảng viên đóng vai trò **Project Owner / Reviewer**: giao việc lớn, đọc báo cáo, ra quyết định và duyệt kết quả.

## Nguyên tắc sử dụng

Mọi AI tham gia dự án phải đọc trước:

1. `00_MASTER_PLAN.md`
2. `00_SHARED_CONTEXT.md`
3. `00_DATA_CONTRACT.md`
4. `00_DECISIONS.md`
5. `00_WORK_STATUS.md`
6. `sessions/<buoi>/BRIEF.md` hoặc brief được giao.

Giảng viên đọc `00_THUAT_NGU.md` khi cần tra nghĩa các thuật ngữ tiếng Anh xuất
hiện trong `LOCAL_REPORT.md`/README kỹ thuật (shuffle, watermark, partition,
executor, AUC, v.v.) — không phải tài liệu bắt buộc cho AI, chỉ để giảng viên
đọc report dễ hơn.

Không AI nào được tự ý thay đổi schema dữ liệu, công nghệ nền, quy ước tên tệp hoặc hướng bài toán xuyên suốt. Nếu cần thay đổi, phải đề xuất trong report; giảng viên duyệt rồi mới ghi quyết định vào `00_DECISIONS.md`.

## Quy tắc hoàn thành

Một nội dung kỹ thuật **không được coi là hoàn thành chỉ vì code đã được sinh ra**.

Các demo liên quan MongoDB, HDFS, MapReduce, Spark, PySpark, Structured Streaming, MLlib và Kafka chỉ được chuyển sang trạng thái `VALIDATED` khi Local AI đã chạy thử và ghi kết quả vào `LOCAL_REPORT.md`.

## Luồng công việc chuẩn

```text
MASTER PLAN
    |
SHARED CONTEXT
    |
DATA CONTRACT
    |
SESSION BRIEF
   /       \
  /         \
Content AI   Local AI
   |           |
CONTENT       LOCAL
REPORT        REPORT
   \           /
    \         /
      REVIEW
        |
   FINAL PACKAGE
```

## Cách giao việc nhanh

- Mỗi tab ChatGPT nên phụ trách **một buổi hoặc một cụm phụ thuộc gần nhau**.
- Local AI nên phụ trách **toàn bộ integration + QA** để tránh nhiều môi trường Docker khác nhau.
- Không đồng bộ bằng cách copy lịch sử chat giữa các AI. Đồng bộ qua các file trong repository này.
