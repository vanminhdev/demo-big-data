"""
prepare_batches_v2.py
Buoi 11 - Spark Structured Streaming (RetailStream / clickstream) - BAN SUA

Phien ban v2: dung `data_source_v2/clickstream_compressed.jsonl` (200 dong,
sinh boi compress_timeline.py, event_time da duoc NEN LAI xuong ~90 phut,
KHONG dung 00_shared_data/sample/clickstream_sample.jsonl truc tiep va KHONG
sua file do) thay vi chia theo NGAY, chia 200 dong thanh cac file batch theo
CUA SO 5 PHUT cua event_time (khop voi STREAM_WINDOW_DURATION="5 minutes"
trong streaming_job.py) de rot dan vao thu muc batches_staging_v2/ mo phong
file stream toi qua tung dot (micro-batch), giong ky thuat cua
prepare_batches.py (v1) nhung don vi la PHUT thay vi NGAY.

De chung minh watermark loai bo du lieu den muon (late data): MOT ban ghi cu
the cua cua so SOM (LATE_WINDOW_INDEX = 2, tuc [09:10, 09:15)) duoc CO Y giu
lai khoi file batch cua so 2, va duoc CHEN vao file batch cua cua so
INJECT_WINDOW_INDEX = 6 ([09:30, 09:35)) - tuc la duoc rot vao stream SAU KHI
4 cua so tiep theo (3,4,5) da duoc xu ly va watermark (delay 3 phut) da vuot
qua xa diem dong cua cua so 2. Ban ghi nay KHONG bi sua bat ky truong nao.

Output trong batches_staging_v2/:
  - win_00_0900.jsonl .. win_18_1025.jsonl (19 file, moi cua so 5 phut 1 file,
    rieng cua so INJECT_WINDOW_INDEX co them 1 dong la late record)
  - manifest.json: mo ta day du tung file + ban ghi muon la ban ghi nao
"""
import json
import os
from collections import defaultdict
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "data_source_v2", "clickstream_compressed.jsonl")
OUT_DIR = os.path.join(ROOT, "batches_staging_v2")
WINDOW_MINUTES = 5

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

assert len(records) == 200, f"Ky vong 200 dong (tu clickstream_compressed.jsonl), doc duoc {len(records)}"

records.sort(key=lambda r: r["event_time"])

parsed = [(r, datetime.fromisoformat(r["event_time"])) for r in records]
base = min(t for _, t in parsed)
base = base.replace(second=0, microsecond=0, minute=(base.minute // WINDOW_MINUTES) * WINDOW_MINUTES)

by_win = defaultdict(list)
for r, t in parsed:
    idx = int((t - base).total_seconds() // (WINDOW_MINUTES * 60))
    by_win[idx].append(r)

win_indices = sorted(by_win.keys())
print("Tong so cua so 5 phut co du lieu:", len(win_indices), "-> chi so:", win_indices)

LATE_WINDOW_INDEX = 2
INJECT_WINDOW_INDEX = 6
assert LATE_WINDOW_INDEX in by_win and INJECT_WINDOW_INDEX in by_win
assert INJECT_WINDOW_INDEX - LATE_WINDOW_INDEX >= 3, "can it nhat 3 cua so cach biet de watermark chac chan da vuot qua"

late_record = by_win[LATE_WINDOW_INDEX].pop()  # giu lai 1 ban ghi cuoi cung cua cua so LATE
print("Late record (giu lai, se rot muon vao window index", INJECT_WINDOW_INDEX, "):")
print(" ", late_record)


def window_label(idx):
    start = base.timestamp() + idx * WINDOW_MINUTES * 60
    dt = datetime.fromtimestamp(start, tz=base.tzinfo)
    return dt.strftime("%H%M")


manifest = []
for order, idx in enumerate(win_indices, start=1):
    recs = list(by_win[idx])
    is_inject = idx == INJECT_WINDOW_INDEX
    if is_inject:
        recs = recs + [late_record]
    fname = f"win_{order:02d}_{window_label(idx)}.jsonl"
    path = os.path.join(OUT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    entry = {
        "file": fname,
        "window_index": idx,
        "window_start": datetime.fromtimestamp(
            base.timestamp() + idx * WINDOW_MINUTES * 60, tz=base.tzinfo
        ).isoformat(),
        "count": len(recs),
    }
    if is_inject:
        entry["contains_late_record"] = late_record["event_id"]
        entry["late_record_actual_window_index"] = LATE_WINDOW_INDEX
    manifest.append(entry)
    tag = "  <-- CHUA BAN GHI MUON " + late_record["event_id"] if is_inject else ""
    print(f"{fname}: {len(recs)} ban ghi{tag}")

with open(os.path.join(OUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
    json.dump(
        {
            "source": "data_source_v2/clickstream_compressed.jsonl (sinh boi compress_timeline.py)",
            "window_minutes": WINDOW_MINUTES,
            "base_window_start": base.isoformat(),
            "late_record": late_record,
            "late_window_index": LATE_WINDOW_INDEX,
            "inject_window_index": INJECT_WINDOW_INDEX,
            "batches_in_arrival_order": manifest,
        },
        f,
        indent=2,
    )

print("\nTong so file batch:", len(manifest))
print("Tong so dong:", sum(e["count"] for e in manifest), "(ky vong 200)")
print("Da ghi manifest.json vao", OUT_DIR)
