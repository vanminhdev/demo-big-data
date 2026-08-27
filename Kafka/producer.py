"""
producer.py
Buoi 13 - Apache Kafka (RetailStream / clickstream)

Gui 200 su kien clickstream (00_shared_data/sample/clickstream_sample.jsonl,
KHONG sua noi dung) vao topic "clickstream" (4 partition, xem
docker-compose.yml). Ho tro 2 CHIEN LUOC KEY khac nhau de minh hoa anh huong
cua key toi phan phoi partition (dung BRIEF muc 4 "key/partition"):

  --key-strategy session_id   (mac dinh) -> key = session_id
  --key-strategy product_id                -> key = product_id
  --key-strategy none                      -> key = None (Kafka round-robin/sticky)

Chay tren HOST (khong phai trong container) - ket noi qua EXTERNAL listener
localhost:9092 (xem docker-compose.yml).

Vi du:
    pip install kafka-python
    python producer.py --key-strategy session_id
    python producer.py --key-strategy product_id
"""
import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict

from kafka import KafkaProducer
from kafka.errors import KafkaError

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "clickstream")
DEFAULT_DATA_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "00_shared_data", "sample", "clickstream_sample.jsonl",
)


def build_key(record, strategy):
    if strategy == "session_id":
        return record.get("session_id")
    if strategy == "product_id":
        return record.get("product_id")
    if strategy == "none":
        return None
    raise ValueError("key-strategy khong hop le: " + strategy)


def main():
    parser = argparse.ArgumentParser(description="RetailStream Kafka producer (clickstream)")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--data-file", default=DEFAULT_DATA_FILE)
    parser.add_argument(
        "--key-strategy", choices=["session_id", "product_id", "none"], default="session_id",
        help="Truong nao dung lam Kafka message key",
    )
    args = parser.parse_args()

    print("=" * 88)
    print("KAFKA PRODUCER - RetailStream clickstream")
    print("bootstrap_servers =", args.bootstrap_servers)
    print("topic             =", args.topic)
    print("data_file         =", os.path.abspath(args.data_file))
    print("key_strategy      =", args.key_strategy)
    print("=" * 88)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )

    sent = 0
    errors = 0
    partition_counts = Counter()
    partition_keys = defaultdict(set)
    futures = []

    with open(args.data_file, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[SKIP] dong {line_no}: JSON loi ({e})")
                errors += 1
                continue

            key = build_key(record, args.key_strategy)
            fut = producer.send(args.topic, key=key, value=record)
            futures.append((fut, key))

    producer.flush(timeout=30)

    for fut, key in futures:
        try:
            meta = fut.get(timeout=10)
            partition_counts[meta.partition] += 1
            if key is not None:
                partition_keys[meta.partition].add(key)
            sent += 1
        except KafkaError as e:
            print("[ERROR] gui that bai:", e)
            errors += 1

    producer.close()

    print("\n--- KET QUA GUI ---")
    print(f"Tong so ban ghi doc duoc : {sent + errors}")
    print(f"Gui thanh cong           : {sent}")
    print(f"Loi                      : {errors}")
    print("\nPhan phoi theo partition (key_strategy=%s):" % args.key_strategy)
    for p in sorted(partition_counts):
        distinct_keys = len(partition_keys[p]) if args.key_strategy != "none" else "-"
        print(f"  partition {p}: {partition_counts[p]:>4} message(s)  (so key phan biet: {distinct_keys})")

    if sent == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
