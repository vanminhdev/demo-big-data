"""Sinh du lieu mau RetailStream dung chung cho toan hoc phan.

Tuan thu 00_DATA_CONTRACT.md. Khong tu doi ten truong / schema.

Usage:
    python generate_retailstream.py --seed 42 --scale sample --output-dir ../sample
"""
import argparse
import csv
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA_VERSION = "1.0"
DATA_VERSION = "retailstream-data-v1"

SCALE_COUNTS = {
    "sample": dict(customers=100, products=60, orders=150, web_logs=200, clickstream=200, product_events=80),
    # Muc lab: 10.000-200.000 ban ghi/bang, theo khuyen nghi muc 2.8 khung noi dung.
    # order_items khong khai bao rieng: sinh tu orders (1-4 dong/don) nen ~125k dong.
    "lab": dict(customers=20000, products=5000, orders=50000, web_logs=100000, clickstream=100000, product_events=20000),
}

# Ten file khong co hau to "_sample" chi dung cho muc lab (theo cau truc muc 2.8:
# lab/orders.csv, lab/order_items.csv, lab/products.json, ...). Muc sample giu
# nguyen hau to "_sample" de tuong thich nguoc voi du lieu da phat hanh.
FILENAME_SUFFIX = {"sample": "_sample", "lab": ""}

try:
    import pandas as _pd  # dung de ghi Parquet cho muc lab neu co san
    _HAS_PANDAS = True
except ImportError:  # khong tu cai them thu vien moi
    _HAS_PANDAS = False

CITIES = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Hue"]
SEGMENTS = ["NEW", "REGULAR", "VIP", "CHURN_RISK"]
CATEGORIES = [
    ("CAT01", "Dien tu"), ("CAT02", "Thoi trang"), ("CAT03", "Gia dung"),
    ("CAT04", "Sach"), ("CAT05", "Thuc pham"), ("CAT06", "My pham"),
]
BRANDS = ["Aurora", "Nimbus", "Vertex", "Solace", "Kestrel", "Marlow"]
PAYMENT_METHODS = ["COD", "CREDIT_CARD", "E_WALLET", "BANK_TRANSFER"]
ORDER_STATUSES = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]
CLICK_EVENT_TYPES = ["VIEW", "ADD_TO_CART", "PURCHASE"]
PRODUCT_EVENT_TYPES = ["PRICE_CHANGED", "STOCK_CHANGED"]
PATHS = ["/home", "/search", "/product", "/cart", "/checkout", "/orders"]


def iso_now_offset(base, days_back, rng):
    dt = base - timedelta(days=rng.uniform(0, days_back), hours=rng.uniform(0, 23))
    return dt.replace(microsecond=0).isoformat()


def gen_customers(n, rng, base):
    rows = []
    for i in range(1, n + 1):
        cid = f"CUST{i:05d}"
        row = {
            "customer_id": cid,
            "customer_segment": rng.choice(SEGMENTS),
            "city": rng.choice(CITIES),
            "created_at": iso_now_offset(base, 400, rng),
        }
        rows.append(row)
    # ban ghi trung co chu y (data quality lab)
    if rows:
        dup = dict(rows[0])
        rows.append(dup)
    return rows


def gen_products(n, rng, base):
    rows = []
    for i in range(1, n + 1):
        cat_id, cat_name = rng.choice(CATEGORIES)
        price = round(rng.uniform(20000, 25000000), -3)
        row = {
            "product_id": f"PROD{i:05d}",
            "category_id": cat_id,
            "category_name": cat_name,
            "product_name": f"{cat_name} {BRANDS[i % len(BRANDS)]} #{i}",
            "brand": rng.choice(BRANDS),
            "price": price,
            "attributes": {
                "color": rng.choice(["den", "trang", "xanh", "do", None]),
                "warranty_months": rng.choice([6, 12, 24, None]),
            },
            "updated_at": iso_now_offset(base, 60, rng),
        }
        # gia tri thieu co chu y
        if i % 17 == 0:
            row["brand"] = None
        rows.append(row)
    return rows


BASE_STATUS_WEIGHTS = {"CREATED": 0.15, "PAID": 0.35, "SHIPPED": 0.4, "CANCELLED": 0.10}

# He so tuong quan gia lap (chi dung khi correlated=True, tuc muc "lab") giua
# customer_segment / payment_method va xac suat CANCELLED. Muc dich: de mo
# hinh MLlib (Buoi 12) hoc duoc tin hieu that thay vi du lieu hoan toan ngau
# nhien (xem yeu cau giang vien, khong ap dung cho muc "sample" de khong pha
# vo tuong thich nguoc voi du lieu da dung o Buoi 5 MongoDB).
SEGMENT_CANCEL_MULT = {"CHURN_RISK": 2.6, "NEW": 1.15, "REGULAR": 0.9, "VIP": 0.55}
PAYMENT_CANCEL_MULT = {"COD": 1.35, "BANK_TRANSFER": 1.0, "E_WALLET": 0.85, "CREDIT_CARD": 0.7}


def compute_correlated_status_weights(customer_segment, payment_method):
    """Tra ve list trong so theo dung thu tu ORDER_STATUSES, voi xac suat
    CANCELLED bi keo len/xuong theo segment + payment_method (2 yeu to nho
    ket hop, khong phai 1 rule if-else lo lieu duy nhat)."""
    mult = SEGMENT_CANCEL_MULT.get(customer_segment, 1.0) * PAYMENT_CANCEL_MULT.get(payment_method, 1.0)
    cancelled_w = min(BASE_STATUS_WEIGHTS["CANCELLED"] * mult, 0.55)
    remaining = 1.0 - cancelled_w
    other_total = BASE_STATUS_WEIGHTS["CREATED"] + BASE_STATUS_WEIGHTS["PAID"] + BASE_STATUS_WEIGHTS["SHIPPED"]
    created_w = BASE_STATUS_WEIGHTS["CREATED"] / other_total * remaining
    paid_w = BASE_STATUS_WEIGHTS["PAID"] / other_total * remaining
    shipped_w = BASE_STATUS_WEIGHTS["SHIPPED"] / other_total * remaining
    return [created_w, paid_w, shipped_w, cancelled_w]  # khop ORDER_STATUSES


def gen_orders_and_items(n, customers, products, rng, base, correlated=False):
    """correlated=False (mac dinh, dung cho muc "sample"): GIU NGUYEN 100%
    logic/thu tu goi rng.* nhu ban goc de --scale sample --seed 42 tai tao
    dung tung byte so voi manifest.json da phat hanh (Buoi 5 MongoDB phu
    thuoc du lieu nay). correlated=True CHI dung cho muc "lab": cay tuong
    quan gia lap co chu y giua customer_segment/payment_method va status
    CANCELLED de phuc vu bai MLlib (Buoi 12)."""
    orders, items = [], []
    for i in range(1, n + 1):
        oid = f"ORD{i:06d}"
        cust_row = rng.choice(customers)
        cust = cust_row["customer_id"]

        if correlated:
            # payment_method duoc chon SOM HON (khac thu tu goc) chi trong
            # nhanh nay, de dung lam yeu to tinh trong so cho status.
            payment_method = rng.choice(PAYMENT_METHODS)
            weights = compute_correlated_status_weights(cust_row.get("customer_segment"), payment_method)
            status = rng.choices(ORDER_STATUSES, weights=weights)[0]
        else:
            status = rng.choices(ORDER_STATUSES, weights=[0.15, 0.35, 0.4, 0.10])[0]
            payment_method = None  # se chon dung vi tri nhu ban goc, ben duoi

        order_time = iso_now_offset(base, 180, rng)
        n_items = rng.randint(1, 4)
        chosen = rng.sample(products, n_items)
        total = 0.0
        for p in chosen:
            qty = rng.randint(1, 3)
            unit_price = p["price"]
            total += qty * unit_price
            items.append({
                "order_id": oid,
                "product_id": p["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
            })

        if not correlated:
            payment_method = rng.choice(PAYMENT_METHODS)

        orders.append({
            "order_id": oid,
            "customer_id": cust,
            "order_time": order_time,
            "status": status,
            "payment_method": payment_method,
            "total_amount": round(total, 2),
        })
    # 1 don loi co chu y: total_amount am (data quality lab)
    if orders:
        orders[-1]["total_amount"] = -1.0
    return orders, items


def gen_web_logs(n, customers, products, rng, base):
    rows = []
    for i in range(1, n + 1):
        session = f"SESS{rng.randint(1, n // 4 + 1):05d}"
        cust = rng.choice(customers)["customer_id"] if rng.random() > 0.2 else None
        path = rng.choice(PATHS)
        prod = rng.choice(products)["product_id"] if path == "/product" else None
        rows.append({
            "event_id": f"WEV{i:06d}",
            "event_time": iso_now_offset(base, 30, rng),
            "customer_id": cust,
            "session_id": session,
            "path": path,
            "product_id": prod,
            "status_code": rng.choices([200, 200, 200, 404, 500], weights=[5, 5, 5, 1, 1])[0],
            "response_time_ms": rng.randint(15, 1200),
        })
    return rows


def gen_clickstream(n, customers, products, rng, base):
    rows = []
    for i in range(1, n + 1):
        session = f"SESS{rng.randint(1, n // 4 + 1):05d}"
        cust = rng.choice(customers)["customer_id"] if rng.random() > 0.25 else None
        rows.append({
            "event_id": f"CEV{i:06d}",
            "event_time": iso_now_offset(base, 14, rng),
            "customer_id": cust,
            "session_id": session,
            "product_id": rng.choice(products)["product_id"],
            "event_type": rng.choices(CLICK_EVENT_TYPES, weights=[0.6, 0.3, 0.1])[0],
        })
    return rows


def gen_product_events(n, products, rng, base):
    rows = []
    for i in range(1, n + 1):
        p = rng.choice(products)
        etype = rng.choice(PRODUCT_EVENT_TYPES)
        if etype == "PRICE_CHANGED":
            old_v = {"price": p["price"]}
            new_v = {"price": round(p["price"] * rng.uniform(0.8, 1.2), -3)}
        else:
            old_v = {"stock": rng.randint(0, 500)}
            new_v = {"stock": rng.randint(0, 500)}
        rows.append({
            "event_id": f"PEV{i:05d}",
            "event_time": iso_now_offset(base, 45, rng),
            "product_id": p["product_id"],
            "event_type": etype,
            "old_value": old_v,
            "new_value": new_v,
        })
    return rows


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_json(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--scale", choices=list(SCALE_COUNTS.keys()), default="sample")
    ap.add_argument("--output-dir", default="../sample")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    base = datetime(2026, 8, 20, tzinfo=timezone.utc)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    counts = SCALE_COUNTS[args.scale]

    customers = gen_customers(counts["customers"], rng, base)
    products = gen_products(counts["products"], rng, base)
    correlated_labels = args.scale == "lab"
    orders, order_items = gen_orders_and_items(
        counts["orders"], customers, products, rng, base, correlated=correlated_labels
    )
    web_logs = gen_web_logs(counts["web_logs"], customers, products, rng, base)
    clickstream = gen_clickstream(counts["clickstream"], customers, products, rng, base)
    product_events = gen_product_events(counts["product_events"], products, rng, base)

    files = {}
    sfx = FILENAME_SUFFIX[args.scale]
    order_fields = ["order_id", "customer_id", "order_time", "status", "payment_method", "total_amount"]
    item_fields = ["order_id", "product_id", "quantity", "unit_price"]

    p = out / f"customers{sfx}.csv"
    write_csv(p, customers, ["customer_id", "customer_segment", "city", "created_at"])
    files[p.name] = p

    p = out / f"products{sfx}.json"
    write_json(p, products)
    files[p.name] = p

    p = out / f"orders{sfx}.csv"
    write_csv(p, orders, order_fields)
    files[p.name] = p

    p = out / f"order_items{sfx}.csv"
    write_csv(p, order_items, item_fields)
    files[p.name] = p

    p = out / f"web_logs{sfx}.jsonl"
    write_jsonl(p, web_logs)
    files[p.name] = p

    p = out / f"clickstream{sfx}.jsonl"
    write_jsonl(p, clickstream)
    files[p.name] = p

    p = out / f"product_events{sfx}.jsonl"
    write_jsonl(p, product_events)
    files[p.name] = p

    # Muc lab: cung cap them ban Parquet cho orders/order_items (bai Spark),
    # theo quy tac dinh dang muc 2.8. Chi ghi khi pandas+pyarrow da co san,
    # khong tu cai them thu vien moi.
    if args.scale == "lab" and _HAS_PANDAS:
        try:
            p = out / f"orders{sfx}.parquet"
            _pd.DataFrame(orders, columns=order_fields).to_parquet(p, index=False)
            files[p.name] = p

            p = out / f"order_items{sfx}.parquet"
            _pd.DataFrame(order_items, columns=item_fields).to_parquet(p, index=False)
            files[p.name] = p
        except Exception as exc:  # thieu engine parquet (pyarrow/fastparquet) -> bo qua, dung CSV lam nguon chinh
            print(f"CANH BAO: khong ghi duoc Parquet ({exc}); dung CSV lam nguon chinh cho muc lab.")

    manifest = {
        "data_version": DATA_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "seed": args.seed,
        "scale": args.scale,
        "correlated_labels": correlated_labels,
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "orders": len(orders),
            "order_items": len(order_items),
            "web_logs": len(web_logs),
            "clickstream": len(clickstream),
            "product_events": len(product_events),
        },
        "intentional_data_issues": [
            f"customers{sfx}.csv: 1 ban ghi trung id (CUST00001) de phuc vu bai lam sach du lieu",
            f"products{sfx}.json: field 'brand' = null cho cac product_id chia het cho 17",
            f"orders{sfx}.csv: ban ghi cuoi cung co total_amount = -1.0 (du lieu loi co chu y)",
            f"web_logs{sfx}.jsonl: co status_code 404/500 va customer_id null cho khach vang lai",
        ] + (
            [
                f"orders{sfx}.csv: status CANCELLED co tuong quan gia lap co chu y voi "
                f"customer_segment (CHURN_RISK cao hon ~2.6x, VIP thap hon ~0.55x so voi co so) "
                f"va payment_method (COD cao hon ~1.35x, CREDIT_CARD thap hon ~0.7x) - chi ap dung "
                f"muc lab, phuc vu bai MLlib Buoi 12; muc sample KHONG co tuong quan nay (giu nguyen "
                f"ngau nhien, tuong thich nguoc voi Buoi 5 MongoDB)."
            ]
            if correlated_labels
            else []
        ),
        "files": {
            name: {"sha256": sha256_of(path), "bytes": path.stat().st_size}
            for name, path in files.items()
        },
    }
    with open(out / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"OK: da sinh {sum(manifest['record_counts'].values())} ban ghi vao {out.resolve()}")


if __name__ == "__main__":
    main()
