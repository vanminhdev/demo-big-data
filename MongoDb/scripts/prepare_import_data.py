"""Chuan bi du lieu MongoDB tu 00_shared_data/sample.

Sinh 2 bien the theo dung yeu cau BRIEF (Buoi 5 - MongoDB):
- embedding: orders_embedded.json (orders + order_items long vao mang "items")
- referencing: orders_ref.json + order_items.json (hai collection rieng, lien ket qua order_id)

Chay: python prepare_import_data.py
"""
import csv
import json
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent.parent / "00_shared_data" / "sample"
OUT = Path(__file__).resolve().parent.parent / "data_import"
OUT.mkdir(exist_ok=True)


def read_csv(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    orders = read_csv(SRC / "orders_sample.csv")
    items = read_csv(SRC / "order_items_sample.csv")

    for o in orders:
        o["total_amount"] = float(o["total_amount"])
    for it in items:
        it["quantity"] = int(it["quantity"])
        it["unit_price"] = float(it["unit_price"])

    items_by_order = {}
    for it in items:
        items_by_order.setdefault(it["order_id"], []).append({
            "product_id": it["product_id"],
            "quantity": it["quantity"],
            "unit_price": it["unit_price"],
        })

    # bien the embedding
    embedded = []
    for o in orders:
        doc = dict(o)
        doc["items"] = items_by_order.get(o["order_id"], [])
        embedded.append(doc)

    with open(OUT / "orders_embedded.json", "w", encoding="utf-8") as f:
        json.dump(embedded, f, ensure_ascii=False, indent=2)

    # bien the referencing: 2 collection rieng
    with open(OUT / "orders_ref.json", "w", encoding="utf-8") as f:
        json.dump(orders, f, ensure_ascii=False, indent=2)

    with open(OUT / "order_items.json", "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    print(f"OK: {len(orders)} orders, {len(items)} order_items -> {OUT}")


if __name__ == "__main__":
    main()
