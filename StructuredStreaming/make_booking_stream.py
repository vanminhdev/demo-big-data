"""
Buổi 11: sinh các "đợt" sự kiện đặt xe CityRide cho demo Structured Streaming.

Tình huống: 18:00–18:30 tối, đếm số yêu cầu đặt xe theo quận trong từng cửa
sổ 5 phút để bật giá cao điểm. Dùng 3 quận: Q01 Hoàn Kiếm, Q03 Đống Đa,
Q05 Cầu Giấy.

Mỗi tệp batch_XX.jsonl là những sự kiện "đến hệ thống" trong một khoảng
thời gian xử lý. Phần lớn sự kiện đến đúng lúc, nhưng có vài sự kiện đến
muộn (điện thoại mất sóng rồi gửi lại):

  batch_01  sự kiện 18:00–18:05
  batch_02  sự kiện 18:05–18:10  + 3 sự kiện muộn của 18:03 (Q01)
  batch_03  sự kiện 18:10–18:15
  batch_04  sự kiện 18:15–18:20  + 2 sự kiện muộn của 18:12 (Q03)
  batch_05  sự kiện 18:20–18:25
  batch_06  sự kiện 18:25–18:30  + 2 sự kiện RẤT muộn của 18:04 (Q05)

Watermark = thời điểm sự kiện lớn nhất đã thấy − 10 phút, cập nhật sau mỗi
micro-batch. Tới batch_06, watermark đã qua 18:09, cửa sổ 18:00–18:05 đã
đóng nên 2 sự kiện 18:04 bị bỏ. Sự kiện muộn 18:03 ở batch_02 và 18:12 ở
batch_04 vẫn trong ngưỡng 10 phút nên được cộng.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path(__file__).parent / "booking_batches"
ZONES = {"Q01": 9, "Q03": 7, "Q05": 5}      # số yêu cầu trung bình mỗi phút
START = datetime(2026, 9, 18, 18, 0, 0)
rng = random.Random(11)
seq = 0


def event(ts, zone):
    global seq
    seq += 1
    return {"event_id": f"E{seq:05d}", "event_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "zone_id": zone, "event_type": "ride_requested"}


def minute_events(minute_start):
    evs = []
    for zone, rate in ZONES.items():
        for _ in range(max(0, int(rng.gauss(rate, 1.5)))):
            evs.append(event(minute_start + timedelta(seconds=rng.randint(0, 59)), zone))
    return evs


def late(hh, mm, ss, zone, n):
    return [event(START.replace(hour=hh, minute=mm, second=ss + i), zone) for i in range(n)]


def main():
    OUT.mkdir(exist_ok=True)
    for f in OUT.glob("*.jsonl"):
        f.unlink()
    extra = {2: late(18, 3, 10, "Q01", 3), 4: late(18, 12, 20, "Q03", 2),
             6: late(18, 4, 30, "Q05", 2)}
    summary = []
    for b in range(1, 7):
        evs = []
        for m in range(5):
            evs += minute_events(START + timedelta(minutes=5 * (b - 1) + m))
        evs.sort(key=lambda e: e["event_time"])
        evs += extra.get(b, [])
        with open(OUT / f"batch_{b:02d}.jsonl", "w", encoding="utf-8") as f:
            for e in evs:
                f.write(json.dumps(e) + "\n")
        summary.append((b, len(evs), len(extra.get(b, []))))
    for b, n, k in summary:
        print(f"batch_{b:02d}: {n} su kien ({k} den muon)")


if __name__ == "__main__":
    main()
