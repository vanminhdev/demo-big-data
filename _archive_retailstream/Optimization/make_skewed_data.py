"""
Buoi 15 - Optimization: sinh du lieu clickstream "meo" (skewed) CHI DE MINH HOA data skew.

Dau vao: 00_shared_data/lab/clickstream.jsonl (100.000 dong, KHONG sua noi dung goc).
Dau ra: Optimization/data/clickstream_skewed.jsonl - BAN SAO cua dau vao nhung ~90% ban ghi bi
gan lai product_id = "PROD00001" (mot product_id co that trong RetailStream, chi bi TAI SU
DUNG lam "hot key"); 10% ban ghi con lai giu nguyen product_id goc.

Day la du lieu rieng cho buoi 15 (khong ghi de 00_shared_data/), deterministic voi seed=42 de
chay lai cho ra ket qua giong het (ty le hot-key thuc te dat duoc: 89.92%, in ra khi chay).

Cach chay lai (tu thu muc goc repo):
    python Optimization/make_skewed_data.py
"""
import json
import random

SEED = 42
SRC = "00_shared_data/lab/clickstream.jsonl"
DST = "Optimization/data/clickstream_skewed.jsonl"
HOT_PRODUCT_ID = "PROD00001"
HOT_PROBABILITY = 0.90


def main():
    random.seed(SEED)
    n = 0
    hot = 0
    with open(SRC, encoding="utf-8") as f, open(DST, "w", encoding="utf-8") as out:
        for line in f:
            n += 1
            d = json.loads(line)
            if random.random() < HOT_PROBABILITY:
                d["product_id"] = HOT_PRODUCT_ID
                hot += 1
            out.write(json.dumps(d, ensure_ascii=False) + "\n")
    print(f"total={n} hot_assigned={hot} pct={round(hot / n * 100, 2)}%")


if __name__ == "__main__":
    main()
