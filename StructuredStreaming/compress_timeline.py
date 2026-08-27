"""
compress_timeline.py
Buoi 11 - Spark Structured Streaming (RetailStream / clickstream)

Sinh mot bo du lieu clickstream RIENG cho buoi nay (KHONG dung/khong sua
00_shared_data/sample/clickstream_sample.jsonl - file do dung chung voi
Buoi 13 Kafka da co bao cao PASS dua tren dung 200 dong do).

Muc dich: du lieu goc (200 dong) co event_time trai tren ~14.45 ngay
(2026-08-05T05:57:18Z .. 2026-08-19T16:42:52Z), qua thua de minh hoa tumbling
window "vai phut" nhu de bai goc goi y (BRIEF Buoi 11). Script nay NEN LAI
truc thoi gian: giu nguyen THU TU va TI LE khoang cach tuong doi giua cac su
kien, chi co lai toan bo khung thoi gian xuong con ~90 phut (1.5 gio, nam
trong khoang "1-2 gio" theo yeu cau giang vien).

CONG THUC NEN (tuyen tinh, giu nguyen thu tu va ti le khoang cach):

    span_goc_giay   = (max(event_time) - min(event_time)).total_seconds()
                     = 1,248,334 giay (~14.4483 ngay)
    span_dich_giay  = 90 * 60 = 5,400 giay  (muc tieu 90 phut)
    SCALE           = span_dich_giay / span_goc_giay  = 0.0043257653800986
                     (nghia la 1 giay du lieu goc -> ~0.004326 giay du lieu
                     moi; tuong duong co lai ~231.17 lan)

    new_event_time(r) = ANCHOR + (event_time(r) - min(event_time)) * SCALE

trong do ANCHOR la moc thoi gian bat dau moi (mac dinh: thoi diem chay script,
lam tron xuong phut gan nhat, UTC) - CO THE override qua bien moi truong
COMPRESS_ANCHOR_UTC (ISO 8601, vd "2026-08-20T09:00:00+00:00") de ket qua
lap lai duoc (deterministic) giua cac lan chay.

KHONG doi: event_id, customer_id, session_id, product_id, event_type cua
tung dong - CHI doi event_time. Khong them/bot dong nao, khong sua schema
(00_DATA_CONTRACT.md muc 7 - clickstream).

Output: StructuredStreaming/data_source_v2/clickstream_compressed.jsonl
        StructuredStreaming/data_source_v2/compress_manifest.json (cong thuc
        + anchor + bang doi chieu 5 dong dau/cuoi de kiem tra bang mat)
"""
import json
import os
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "..", "00_shared_data", "sample", "clickstream_sample.jsonl")
OUT_DIR = os.path.join(ROOT, "data_source_v2")
os.makedirs(OUT_DIR, exist_ok=True)

TARGET_SPAN_SECONDS = 90 * 60  # 90 phut, nam trong khoang "1-2 gio" yeu cau

records = []
with open(SRC, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))

assert len(records) == 200, f"Ky vong dung 200 dong goc, doc duoc {len(records)}"

records.sort(key=lambda r: r["event_time"])

parsed_times = [datetime.fromisoformat(r["event_time"]) for r in records]
t_min = min(parsed_times)
t_max = max(parsed_times)
span_seconds = (t_max - t_min).total_seconds()
scale = TARGET_SPAN_SECONDS / span_seconds

anchor_env = os.environ.get("COMPRESS_ANCHOR_UTC")
if anchor_env:
    anchor = datetime.fromisoformat(anchor_env)
else:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    anchor = now

print("So dong goc:", len(records))
print("event_time goc: min =", t_min.isoformat(), " max =", t_max.isoformat())
print("span_seconds goc =", span_seconds, f"(~{span_seconds/86400:.4f} ngay)")
print("TARGET_SPAN_SECONDS =", TARGET_SPAN_SECONDS, "(90 phut)")
print("SCALE =", scale, f" (co lai ~{1/scale:.2f} lan)")
print("ANCHOR (moc bat dau moi, UTC) =", anchor.isoformat())

compressed = []
for r in records:
    orig_t = datetime.fromisoformat(r["event_time"])
    delta = (orig_t - t_min).total_seconds() * scale
    new_t = anchor.fromtimestamp(anchor.timestamp() + delta, tz=timezone.utc)
    new_r = dict(r)  # giu nguyen tat ca truong khac, khong doi schema
    new_r["event_time"] = new_t.isoformat()
    compressed.append(new_r)

out_path = os.path.join(OUT_DIR, "clickstream_compressed.jsonl")
with open(out_path, "w", encoding="utf-8") as f:
    for r in compressed:
        f.write(json.dumps(r) + "\n")

new_min = datetime.fromisoformat(compressed[0]["event_time"])
new_max = datetime.fromisoformat(compressed[-1]["event_time"])
print("event_time moi: min =", new_min.isoformat(), " max =", new_max.isoformat())
print("span moi (giay) =", (new_max - new_min).total_seconds())

manifest = {
    "source_file": "00_shared_data/sample/clickstream_sample.jsonl (KHONG sua, chi doc)",
    "record_count": len(records),
    "formula": (
        "new_event_time = ANCHOR + (event_time - min(event_time)) * SCALE, "
        "SCALE = target_span_seconds / original_span_seconds"
    ),
    "original_span": {
        "min_event_time": t_min.isoformat(),
        "max_event_time": t_max.isoformat(),
        "span_seconds": span_seconds,
        "span_days": span_seconds / 86400,
    },
    "target_span_seconds": TARGET_SPAN_SECONDS,
    "scale_factor": scale,
    "compression_ratio": 1 / scale,
    "anchor_utc": anchor.isoformat(),
    "compressed_span": {
        "min_event_time": new_min.isoformat(),
        "max_event_time": new_max.isoformat(),
        "span_seconds": (new_max - new_min).total_seconds(),
    },
    "unchanged_fields": [
        "event_id",
        "customer_id",
        "session_id",
        "product_id",
        "event_type",
    ],
    "changed_fields": ["event_time"],
    "sample_before_after": [
        {
            "event_id": records[i]["event_id"],
            "event_time_original": records[i]["event_time"],
            "event_time_compressed": compressed[i]["event_time"],
        }
        for i in (0, 1, 2, 100, 197, 198, 199)
    ],
}
with open(os.path.join(OUT_DIR, "compress_manifest.json"), "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2)

print("\nDa ghi:", out_path)
print("Da ghi manifest:", os.path.join(OUT_DIR, "compress_manifest.json"))
