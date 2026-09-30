import json
import os
import time

import requests
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-service")
TOPIC = "order-events"
ORDER_API_BASE_URL = os.getenv("ORDER_API_BASE_URL", "http://api:8000")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])

print(f"[consumer] group={GROUP_ID} topic={TOPIC}")
print("[consumer] Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        key = msg.key().decode("utf-8") if msg.key() else None
        value = json.loads(msg.value().decode("utf-8"))
        order_id = value["orderId"]
        status = value["status"]

        print(
            f"[consumer] key={key} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"orderId={order_id} "
            f"status={status}",
            flush=True,
        )

        # Simulate business processing so students can observe the asynchronous flow.
        time.sleep(2)

        response = requests.put(
            f"{ORDER_API_BASE_URL}/internal/orders/{order_id}/status",
            json={"status": status},
            timeout=5,
        )
        response.raise_for_status()

        # Commit only after processing succeeds.
        consumer.commit(message=msg, asynchronous=False)
        print(
            f"[consumer] processed and committed orderId={order_id} "
            f"offset={msg.offset()}",
            flush=True,
        )
except KeyboardInterrupt:
    print("[consumer] Stopping...")
finally:
    consumer.close()
