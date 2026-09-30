import json
import os
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "order-events"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "order-api",
    "acks": "all",
})


def publish_order_event(order_id: str, status: str) -> str:
    """Create one Kafka event using order_id as the Kafka message key."""
    event_id = str(uuid4())
    event = {
        "eventId": event_id,
        "orderId": order_id,
        "status": status,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    producer.produce(
        topic=TOPIC,
        key=order_id,
        value=json.dumps(event, ensure_ascii=False),
    )
    producer.flush(5)
    return event_id
