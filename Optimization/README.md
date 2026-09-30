# Buổi 15 – Tối ưu pipeline Spark

`b15_optimization.py` chạy 4 thí nghiệm trên cụm 2 worker (AQE tắt để thấy rõ từng thay đổi). Thời gian từng stage và task được lấy từ REST API của Spark UI.

```bash
bash scripts/run-b15.sh
```

## Kết quả (`evidence/b15_run.log`)

| Thí nghiệm | Trước | Sau |
|---|---|---|
| [1] Ghép 2 triệu chuyến (80% Q01) với bảng 12 quận | shuffle: 8,3 s; task nóng 1.635.685/2.000.012 dòng, lâu nhất 2,27 s, trung vị 0,25 s | broadcast: 2,7 s |
| [2] Đọc cùng dữ liệu | 400 tệp: 16,3 s | 2 tệp: 1,0 s |
| [3] Tổng doanh thu theo quận | CSV: 1,8 s | Parquet: 1,1 s |
| [4] Gom nhóm theo tài xế | 200 partition: 4,5 s | 4 partition: 0,6 s |

Ở thí nghiệm [1], 12 quận được băm vào 12 partition nhưng 3 partition trống và 2 partition nhận 2 quận: băm không bảo đảm mỗi khóa một partition.

Thời gian thay đổi đôi chút giữa các lần chạy; tỷ lệ trước/sau thì ổn định.
