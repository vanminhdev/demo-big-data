#!/usr/bin/env python3
"""Sinh du lieu JSON cho bai thuc hanh MongoDB (chu de quan ly ban hang).

Chay: python generate_lab_data.py
Ket qua: 4 file JSON trong thu muc data/ ke ben (customers, products, orders, reviews).
Seed co dinh (=2026) de moi lan chay ra dung mot bo du lieu giong nhau.
"""
import json
import random
from datetime import datetime, timedelta, timezone

random.seed(2026)

OUT_DIR = __file__.replace("scripts/generate_lab_data.py", "data").replace(
    "scripts\\generate_lab_data.py", "data"
)

N_CUSTOMERS = 100
N_PRODUCTS = 80
N_ORDERS = 250
N_REVIEWS = 200

CITIES = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Hue", "Nha Trang", "Vinh"]
SEGMENTS = ["NEW", "REGULAR", "VIP", "CHURN_RISK"]
SEGMENT_WEIGHTS = [0.30, 0.40, 0.15, 0.15]

FIRST_NAMES = ["Nguyen Van", "Tran Thi", "Le Van", "Pham Thi", "Hoang Van", "Vu Thi",
               "Dang Van", "Bui Thi", "Do Van", "Ngo Thi", "Duong Van", "Ly Thi"]
GIVEN_NAMES = ["An", "Binh", "Chi", "Dung", "Giang", "Hoa", "Khanh", "Linh",
               "Minh", "Ngoc", "Phuong", "Quang", "Son", "Thao", "Uyen", "Yen"]

CATEGORIES = [
    ("CAT01", "Dien tu"), ("CAT02", "Thoi trang"), ("CAT03", "Gia dung"),
    ("CAT04", "My pham"), ("CAT05", "Thuc pham"), ("CAT06", "Van phong pham"),
]
BRANDS = ["Aurora", "Kestrel", "Nimbus", "Vertex", "Solace", "Marlow", "Halcyon", "Orion"]
COLORS = ["den", "trang", "do", "xanh", "vang", "bac"]

PAYMENT_METHODS = ["COD", "BANK_TRANSFER", "E_WALLET", "CREDIT_CARD"]
ORDER_STATUSES = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]
STATUS_WEIGHTS = [0.15, 0.35, 0.35, 0.15]

REVIEW_COMMENTS_BY_RATING = {
    5: ["San pham rat tot, dung nhu mo ta.", "Chat luong vuot mong doi, se mua lai.",
        "Dong goi can than, giao hang nhanh."],
    4: ["San pham on, dung duoc.", "Chat luong tot nhung gia hoi cao.",
        "Giao hang hoi cham nhung san pham ok."],
    3: ["San pham tam on, khong co gi noi bat.", "Dung tam duoc, chua that su an tuong."],
    2: ["San pham khong nhu mo ta.", "Chat luong duoi mong doi."],
    1: ["That vong, khong nen mua.", "San pham loi, da doi tra."],
}

ACCOUNT_START = datetime(2025, 6, 1, tzinfo=timezone.utc)   # cho created_at cua customer/product
ORDER_START = datetime(2026, 1, 1, tzinfo=timezone.utc)     # cho order_time/review created_at


def rand_dt(base=ORDER_START, days_span=230):
    return base + timedelta(
        days=random.uniform(0, days_span),
        hours=random.uniform(0, 23),
        minutes=random.uniform(0, 59),
    )


def iso(dt):
    return dt.isoformat()


def gen_customers():
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        cid = f"CUST{i:04d}"
        name = f"{random.choice(FIRST_NAMES)} {random.choice(GIVEN_NAMES)}"
        city = random.choice(CITIES)
        segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        created = rand_dt(base=ACCOUNT_START, days_span=300)
        customers.append({
            "customer_id": cid,
            "full_name": name,
            "email": f"{cid.lower()}@example.com",
            "phone": f"09{random.randint(10000000, 99999999)}",
            "customer_segment": segment,
            "city": city,
            "address": f"So {random.randint(1, 300)} duong {random.choice(GIVEN_NAMES)}, {city}",
            "created_at": iso(created),
        })
    return customers


def gen_products():
    products = []
    for i in range(1, N_PRODUCTS + 1):
        pid = f"PROD{i:04d}"
        cat_id, cat_name = random.choice(CATEGORIES)
        brand = random.choice(BRANDS)
        price = random.randint(50, 2500) * 10000  # 500,000 - 25,000,000 VND
        products.append({
            "product_id": pid,
            "category_id": cat_id,
            "category_name": cat_name,
            "product_name": f"{cat_name} {brand} #{i}",
            "brand": brand,
            "price": price,
            "stock_quantity": random.randint(0, 200),
            "attributes": {
                "color": random.choice(COLORS),
                "warranty_months": random.choice([6, 12, 24, 36]),
            },
            "tags": random.sample(
                ["ban_chay", "moi_ve", "giam_gia", "cao_cap", "pho_thong"], k=random.randint(1, 3)
            ),
            "created_at": iso(rand_dt(base=ACCOUNT_START, days_span=300)),
        })
    return products


def gen_orders(customers, products):
    orders = []
    for i in range(1, N_ORDERS + 1):
        oid = f"ORD{i:05d}"
        cust = random.choice(customers)
        order_time = rand_dt()
        status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        n_items = random.randint(1, 5)
        chosen = random.sample(products, k=n_items)
        items = []
        total = 0
        for p in chosen:
            qty = random.randint(1, 4)
            line_total = qty * p["price"]
            total += line_total
            items.append({
                "product_id": p["product_id"],
                "product_name": p["product_name"],
                "unit_price": p["price"],
                "quantity": qty,
                "line_total": line_total,
            })
        orders.append({
            "order_id": oid,
            "customer": {
                "customer_id": cust["customer_id"],
                "full_name": cust["full_name"],
                "city": cust["city"],
                "customer_segment": cust["customer_segment"],
            },
            "order_time": iso(order_time),
            "status": status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_address": cust["address"],
            "items": items,
            "total_amount": total,
            "note": "" if random.random() > 0.1 else "Giao trong gio hanh chinh",
        })
    return orders


def gen_reviews(customers, products):
    reviews = []
    for i in range(1, N_REVIEWS + 1):
        rid = f"REV{i:05d}"
        cust = random.choice(customers)
        prod = random.choice(products)
        rating = random.choices([5, 4, 3, 2, 1], weights=[0.35, 0.30, 0.20, 0.10, 0.05], k=1)[0]
        reviews.append({
            "review_id": rid,
            "product_id": prod["product_id"],
            "customer_id": cust["customer_id"],
            "rating": rating,
            "comment": random.choice(REVIEW_COMMENTS_BY_RATING[rating]),
            "created_at": iso(rand_dt()),
        })
    return reviews


def main():
    customers = gen_customers()
    products = gen_products()
    orders = gen_orders(customers, products)
    reviews = gen_reviews(customers, products)

    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    files = {
        "customers.json": customers,
        "products.json": products,
        "orders.json": orders,
        "reviews.json": reviews,
    }
    for name, data in files.items():
        path = os.path.join(OUT_DIR, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"{name}: {len(data)} document")


if __name__ == "__main__":
    main()
