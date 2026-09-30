"""
Sinh bộ dữ liệu CityRide (ứng dụng gọi xe hư cấu tại Hà Nội) cho Buổi 9–15.

Bảng sinh ra (CSV có header, UTF-8):
  zones.csv          12 quận: zone_id, zone_name, is_center
  drivers.csv        tài xế: driver_id, vehicle_type, join_date, rating
  trips.csv          chuyến xe (bảng lớn, xem cột bên dưới)
  trip_payments.csv  các phần thanh toán của chuyến hoàn thành
                     (khoảng 6% chuyến trả làm 2 phần: ví + tiền mặt)

Cột trips.csv:
  trip_id, request_time, pickup_zone, dropoff_zone, driver_id, vehicle_type,
  distance_km, duration_min, eta_min, surge, is_raining, fare_vnd,
  payment_method, status

  status: completed | rider_cancelled | driver_cancelled
  eta_min: thời gian chờ tài xế tới đón mà ứng dụng báo cho khách
  Tỷ lệ khách hủy phụ thuộc eta_min, surge, is_raining, giờ cao điểm
  (dùng cho Buổi 12 MLlib).

Lỗi dữ liệu cài cố ý (dùng cho Buổi 10 làm sạch và kiểm tra ghép bảng):
  - khoảng 1% chuyến hoàn thành có payment_method rỗng;
  - khoảng 0,2% chuyến hoàn thành có fare_vnd = 0 (lỗi ghi nhận);
  - khoảng 0,1% chuyến có driver_id không có trong drivers.csv.

Cách chạy:
  python generate_cityride.py --scale sample --out sample
  python generate_cityride.py --scale lab    --out lab
"""

import argparse
import csv
import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

ZONES = [
    # zone_id, tên quận, trung tâm?, trọng số nhu cầu đặt xe
    ("Q01", "Hoàn Kiếm", 1, 16),
    ("Q02", "Ba Đình", 1, 10),
    ("Q03", "Đống Đa", 1, 14),
    ("Q04", "Hai Bà Trưng", 1, 10),
    ("Q05", "Cầu Giấy", 0, 12),
    ("Q06", "Thanh Xuân", 0, 8),
    ("Q07", "Hoàng Mai", 0, 6),
    ("Q08", "Long Biên", 0, 5),
    ("Q09", "Tây Hồ", 0, 5),
    ("Q10", "Nam Từ Liêm", 0, 6),
    ("Q11", "Bắc Từ Liêm", 0, 4),
    ("Q12", "Hà Đông", 0, 4),
]

# Hệ số nhu cầu theo giờ trong ngày (0h..23h): cao điểm sáng và chiều
HOUR_WEIGHT = [2, 1, 1, 1, 1, 2, 5, 10, 12, 8, 6, 7,
               8, 6, 6, 7, 9, 12, 13, 10, 8, 6, 4, 3]
PEAK_HOURS = {7, 8, 17, 18}

SCALES = {
    # số tài xế, số chuyến, ngày bắt đầu, số ngày
    "sample": (40, 200, datetime(2026, 9, 1), 3),
    "lab": (2000, 300_000, datetime(2026, 7, 1), 92),
}


def weighted_choice(rng, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


def gen_drivers(rng, n):
    rows = []
    for i in range(1, n + 1):
        vt = "bike" if rng.random() < 0.7 else "car"
        join = datetime(2023, 1, 1) + timedelta(days=rng.randint(0, 1200))
        rating = round(min(5.0, max(3.5, rng.gauss(4.7, 0.2))), 2)
        rows.append((f"D{i:05d}", vt, join.date().isoformat(), rating))
    return rows


def fare(vehicle_type, km, surge):
    if vehicle_type == "bike":
        base, per_km = 12_000, 4_500
    else:
        base, per_km = 25_000, 11_000
    raw = (base + per_km * km) * surge
    return int(round(raw / 1000.0)) * 1000


def cancel_probability(eta, surge, raining, hour):
    # Mô hình logistic giả lập: chờ lâu, giá cao, mưa, cao điểm -> dễ hủy
    z = -3.4 + 0.22 * eta + 1.1 * (surge - 1.0) + 0.5 * raining
    z += 0.35 if hour in PEAK_HOURS else 0.0
    return 1.0 / (1.0 + math.exp(-z))


def gen_trips(rng, drivers, n_trips, start, n_days):
    zone_ids = [z[0] for z in ZONES]
    zone_w = [z[3] for z in ZONES]
    center = {z[0] for z in ZONES if z[2] == 1}
    drivers_by_type = {"bike": [], "car": []}
    for d in drivers:
        drivers_by_type[d[1]].append(d[0])

    # Mỗi ngày có mưa hay không (mùa hè Hà Nội: khoảng 35% số ngày)
    rain_day = [rng.random() < 0.35 for _ in range(n_days)]
    day_weight = [1.15 if (start + timedelta(days=d)).weekday() >= 4 else 1.0
                  for d in range(n_days)]

    trips, payments = [], []
    for i in range(1, n_trips + 1):
        d = weighted_choice(rng, range(n_days), day_weight)
        h = weighted_choice(rng, range(24), HOUR_WEIGHT)
        t = start + timedelta(days=d, hours=h, seconds=rng.randint(0, 3599))
        pz = weighted_choice(rng, zone_ids, zone_w)
        dz = weighted_choice(rng, zone_ids, zone_w)
        vt = "bike" if rng.random() < 0.7 else "car"
        km = round(min(35.0, max(0.8, rng.lognormvariate(1.45, 0.55))), 1)
        if pz != dz:
            km = round(km + rng.uniform(1.0, 4.0), 1)
        raining = 1 if (rain_day[d] and rng.random() < 0.5) else 0
        busy = h in PEAK_HOURS or raining
        surge = 1.0
        if busy:
            surge = weighted_choice(rng, [1.0, 1.2, 1.5], [5, 3, 2])
        eta = max(1, int(rng.gauss(4 + (4 if busy else 0) + (1 if pz in center else 0), 2.2)))
        speed = rng.uniform(14, 20) if busy else rng.uniform(20, 30)
        duration = max(3, int(round(km / speed * 60 + rng.uniform(0, 4))))

        p_cancel = cancel_probability(eta, surge, raining, h)
        u = rng.random()
        if u < p_cancel:
            status = "rider_cancelled"
        elif u < p_cancel + 0.03:
            status = "driver_cancelled"
        else:
            status = "completed"

        driver_id = ""
        if status != "rider_cancelled" or rng.random() < 0.4:
            driver_id = rng.choice(drivers_by_type[vt])
            if rng.random() < 0.001:
                driver_id = f"D9{rng.randint(0, 9999):04d}"  # không có trong drivers

        trip_id = f"T{i:07d}"
        fare_vnd, pay_method = "", ""
        if status == "completed":
            fare_vnd = fare(vt, km, surge)
            pay_method = weighted_choice(rng, ["cash", "wallet", "card"], [45, 40, 15])
            if rng.random() < 0.002:
                fare_vnd = 0
            if rng.random() < 0.06 and fare_vnd > 0:
                wallet_part = int(fare_vnd * rng.choice([0.3, 0.5, 0.7]) / 1000) * 1000
                payments.append((trip_id, "wallet", wallet_part))
                payments.append((trip_id, "cash", fare_vnd - wallet_part))
                pay_method = "split"
            elif fare_vnd > 0:
                payments.append((trip_id, pay_method, fare_vnd))
            if rng.random() < 0.01:
                pay_method = ""
        else:
            duration = ""
        trips.append((trip_id, t.strftime("%Y-%m-%d %H:%M:%S"), pz, dz, driver_id, vt,
                      km, duration, eta, surge, raining, fare_vnd, pay_method, status))
    # Đánh lại mã chuyến theo thứ tự thời gian đặt xe
    trips.sort(key=lambda r: r[1])
    new_id = {r[0]: f"T{k:07d}" for k, r in enumerate(trips, start=1)}
    trips = [(new_id[r[0]],) + tuple(r[1:]) for r in trips]
    payments = sorted(((new_id[p[0]], p[1], p[2]) for p in payments), key=lambda p: p[0])
    return trips, payments


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", choices=SCALES, default="sample")
    ap.add_argument("--out", default=None)
    ap.add_argument("--seed", type=int, default=2026)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    n_drivers, n_trips, start, n_days = SCALES[args.scale]
    out = Path(args.out or Path(__file__).parent / args.scale)
    out.mkdir(parents=True, exist_ok=True)

    drivers = gen_drivers(rng, n_drivers)
    trips, payments = gen_trips(rng, drivers, n_trips, start, n_days)

    write_csv(out / "zones.csv", ["zone_id", "zone_name", "is_center"],
              [(z[0], z[1], z[2]) for z in ZONES])
    write_csv(out / "drivers.csv", ["driver_id", "vehicle_type", "join_date", "rating"], drivers)
    write_csv(out / "trips.csv",
              ["trip_id", "request_time", "pickup_zone", "dropoff_zone", "driver_id",
               "vehicle_type", "distance_km", "duration_min", "eta_min", "surge",
               "is_raining", "fare_vnd", "payment_method", "status"], trips)
    write_csv(out / "trip_payments.csv", ["trip_id", "method", "amount_vnd"], payments)

    status_count = {}
    for r in trips:
        status_count[r[-1]] = status_count.get(r[-1], 0) + 1
    manifest = {
        "scale": args.scale, "seed": args.seed,
        "rows": {"zones": len(ZONES), "drivers": len(drivers), "trips": len(trips),
                 "trip_payments": len(payments)},
        "trip_status": status_count,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
