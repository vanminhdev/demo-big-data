#!/usr/bin/env python3
"""
Mapper cho bai toan: dem luot truy cap theo san pham (product_id) tu web_logs.

Input (stdin): moi dong la 1 JSON object cua web_logs, vi du:
{"event_id": "WEV000002", ..., "product_id": "PROD00025", ...}

Output (stdout): "product_id\t1" cho moi dong co product_id khac null.
Cac dong co product_id = null (vi du path = "/orders", "/checkout") bi bo qua,
vi khong gan voi mot san pham cu the nao.

Chay thu ngoai Hadoop (khong can Hadoop Streaming) de kiem tra nhanh:
    cat web_logs_sample.jsonl | python3 mapper.py | head
"""
import json
import sys


def main():
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            # Dong JSON bi loi dinh dang -> bo qua, khong lam job chet.
            continue

        product_id = record.get("product_id")
        if product_id is None:
            continue

        # Dinh dang output chuan cua Hadoop Streaming: key TAB value
        print("%s\t1" % product_id)


if __name__ == "__main__":
    main()
