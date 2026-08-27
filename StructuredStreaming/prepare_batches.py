"""
prepare_batches.py
Buoi 11 - Spark Structured Streaming (RetailStream / clickstream)

Chia 00_shared_data/sample/clickstream_sample.jsonl (200 dong, KHONG sua noi
dung tung ban ghi) thanh cac file batch theo NGAY cua event_time, de "rot"
dan vao thu muc data_source/ mo phong file stream toi qua tung dot
(micro-batch). Day KHONG phai tao dataset moi - chi la phan chia lai thu tu
ARRIVAL (thu tu file xuat hien trong thu muc theo doi) cua du lieu goc, moi
ban ghi giu nguyen 100% cac truong theo Data Contract.

De chung minh watermark loai bo du lieu den muon (late data): MOT ban ghi cu
the cua ngay 2026-08-06 (ngay som, "LATE_DAY") duoc CO Y giu lai khoi file
batch ngay 08-06, va duoc CHEN vao file batch cua ngay 2026-08-10
("INJECT_DAY") - tuc la duoc rot vao stream SAU KHI cac ngay 08-07, 08-08,
08-09 da duoc xu ly va watermark (tumbling 1 ngay, watermark delay 1 ngay)
da vuot qua, dong han cua so [2026-08-06, 2026-08-07). Ban ghi nay KHONG bi
sua bat ky truong nao, chi bi "tra ve muon" trong thu tu cac file duoc rot.

Output trong batches_staging/:
  - day_01_2026-08-05.jsonl ... day_14_2026-08-19.jsonl (14 file, moi ngay
    1 file, rieng ngay INJECT_DAY co them 1 dong la late record)
  - manifest.json: mo ta day du tung file + ban ghi muon la ban ghi nao
"""
import json
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "..", "00_shared_data", "sample", "clickstream_sample.jsonl")
OUT_DIR = os.path.join(ROOT, "batches_staging")

os.makedirs(OUT_DIR, exist_ok=True)
for f in os.listdir(OUT_DIR):
    os.remove(os.path.join(OUT_DIR, f))

records = []
with open(SRC, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))

records.sort(key=lambda r: r["event_time"])

by_day = {}
for r in records:
    day = r["event_time"][:10]
    by_day.setdefault(day, []).append(r)

days = sorted(by_day.keys())
print("Cac ngay co du lieu:", days)

LATE_DAY = "2026-08-06"
INJECT_DAY = "2026-08-10"

late_record = by_day[LATE_DAY].pop()  # giu lai 1 ban ghi cuoi cung cua ngay 08-06
print("Late record (giu lai, se rot muon vao batch ngay", INJECT_DAY, "):")
print(" ", late_record)

manifest = []
for idx, day in enumerate(days, start=1):
    recs = list(by_day[day])
    is_inject_day = day == INJECT_DAY
    if is_inject_day:
        recs = recs + [late_record]
    fname = f"day_{idx:02d}_{day}.jsonl"
    path = os.path.join(OUT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    entry = {"file": fname, "day": day, "count": len(recs)}
    if is_inject_day:
        entry["contains_late_record"] = late_record["event_id"]
        entry["late_record_actual_day"] = LATE_DAY
    manifest.append(entry)
    tag = "  <-- CHUA BAN GHI MUON " + late_record["event_id"] if is_inject_day else ""
    print(f"{fname}: {len(recs)} ban ghi{tag}")

with open(os.path.join(OUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
    json.dump(
        {
            "late_record": late_record,
            "late_day": LATE_DAY,
            "inject_day": INJECT_DAY,
            "batches_in_arrival_order": manifest,
        },
        f,
        indent=2,
    )

print("\nTong so file batch:", len(manifest))
print("Da ghi manifest.json vao", OUT_DIR)
