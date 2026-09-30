import sys
import time

import requests

BASE_URL = "http://localhost:8000"


def post(url: str, payload: dict) -> None:
    response = requests.post(url, json=payload, timeout=5)
    print(response.status_code, response.json())
    response.raise_for_status()


if __name__ == "__main__":
    order_id = sys.argv[1] if len(sys.argv) > 1 else "ORD-001"

    print(f"Creating {order_id}")
    post(f"{BASE_URL}/orders", {"orderId": order_id})
    time.sleep(1)

    for status in ["PAID", "SHIPPED"]:
        print(f"Sending {order_id} -> {status}")
        post(f"{BASE_URL}/orders/{order_id}/events", {"status": status})
        time.sleep(1)
