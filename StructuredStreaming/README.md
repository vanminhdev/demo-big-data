# Buổi 11 – Structured Streaming: đếm yêu cầu đặt xe theo cửa sổ 5 phút

Tình huống: 18:00–18:30, đếm số yêu cầu đặt xe của 3 quận (Q01, Q03, Q05) mỗi 5 phút để bật giá cao điểm. Nguồn là thư mục JSON: mỗi tệp là một đợt sự kiện, có cài sẵn sự kiện đến muộn.

```bash
python make_booking_stream.py          # sinh booking_batches/batch_01..06.jsonl (612 sự kiện)
bash scripts/run-b11.sh update         # hoặc append, complete
bash scripts/run-b11.sh restart        # 3 đợt đầu, dừng, chạy tiếp 3 đợt với cùng checkpoint
```

Truy vấn (`b11_booking_stream.py`): `withWatermark("event_time", "10 minutes")` + `groupBy(window(5 minutes), zone_id).count()`, `trigger(availableNow=True)`, `maxFilesPerTrigger=1`. Chạy `local[2]` trong container `spark-master`.

## Sự kiện muộn cài sẵn

| Batch Spark | Tệp | Sự kiện muộn | Kết quả |
|---|---|---|---|
| 1 | batch_02 | 3 sự kiện 18:03, Q01 | được cộng: Q01 18:00–18:05 từ 38 lên 41 |
| 3 | batch_04 | 2 sự kiện 18:12, Q03 | được cộng: Q03 18:10–18:15 từ 34 lên 36 |
| 5 | batch_06 | 2 sự kiện 18:04, Q05 | **bị bỏ** (`numRowsDroppedByWatermark = 1`) ở `update`/`append`; vẫn được cộng ở `complete` (Q05 = 22) |

Log mẫu: `evidence/b11_update.log`, `b11_append.log`, `b11_complete.log`, `b11_restart.log`. Cuối mỗi log có bảng tóm tắt từng micro-batch: số dòng vào, watermark, số dòng trạng thái, số dòng bị bỏ.

Lưu ý: Spark so thời điểm kết thúc cửa sổ với watermark của micro-batch **trước**, và tài liệu Spark chỉ bảo đảm "dữ liệu trong ngưỡng không bị bỏ". Vì vậy sự kiện rất muộn được đặt ở batch_06 để chắc chắn cửa sổ đã đóng.
