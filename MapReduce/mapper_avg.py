#!/usr/bin/env python3
"""
Mapper cho bai toan: thoi gian phan hoi TRUNG BINH theo status_code tu web_logs.

Output (stdout): "status_code<TAB>response_time_ms,1" cho moi dong.
Value la cap (gia tri, 1), KHONG phai trung binh cuc bo: trung binh cua cac
trung binh se sai vi mat so luong ban ghi cua tung nhom (xem slide Buoi 7).

Chay thu: cat web_logs_sample.jsonl | python3 mapper_avg.py | head
"""
import json
import sys

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    try:
        rec = json.loads(line)
        status, ms = rec["status_code"], rec["response_time_ms"]
    except (ValueError, KeyError):
        continue
    if status is None or ms is None:
        continue
    print("%s\t%s,1" % (status, ms))
