#!/usr/bin/env python3
"""
Reducer cho bai toan dem luot truy cap theo san pham (product_id).

Input (stdin): cac dong "product_id\tcount_tam_thoi" DA duoc Hadoop sap xep
theo key (shuffle & sort) - day la dam bao cua Hadoop Streaming, khong phai
do reducer tu sap xep.

Output (stdout): "product_id\ttong_so_luot_xem" - moi product_id xuat hien
dung 1 dong, la tong tat ca cac gia tri "1" ma mapper da phat ra cho key do.

Chay thu ngoai Hadoop de kiem tra nhanh (can sort truoc, giong Hadoop lam):
    cat web_logs_sample.jsonl | python3 mapper.py | sort | python3 reducer.py
"""
import sys


def main():
    current_key = None
    current_count = 0

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            key, value = line.split("\t", 1)
        except ValueError:
            continue

        try:
            value = int(value)
        except ValueError:
            continue

        if current_key == key:
            current_count += value
        else:
            if current_key is not None:
                print("%s\t%d" % (current_key, current_count))
            current_key = key
            current_count = value

    # Xuat key cuoi cung con tich luy trong bo nho.
    if current_key is not None:
        print("%s\t%d" % (current_key, current_count))


if __name__ == "__main__":
    main()
