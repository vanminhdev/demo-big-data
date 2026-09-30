#!/usr/bin/env python3
"""
Reducer cho bai toan trung binh: cong tong gia tri va tong so luong cua tung key,
ROI MOI chia. Input da duoc Hadoop sap xep theo key.

Output: "status_code<TAB>so_luong<TAB>tong_ms<TAB>trung_binh_ms"
"""
import sys

key, total, count = None, 0, 0


def flush():
    if key is not None and count:
        print("%s\t%d\t%d\t%.2f" % (key, count, total, total / count))


for raw in sys.stdin:
    k, v = raw.rstrip("\n").split("\t", 1)
    ms, n = v.split(",")
    if k != key:
        flush()
        key, total, count = k, 0, 0
    total += int(ms)
    count += int(n)
flush()
