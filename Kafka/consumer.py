"""
consumer.py
Buoi 13 - Apache Kafka (RetailStream / clickstream)

Consumer don gian de quan sat OFFSET tang dan theo tung PARTITION, va (khi
chay nhieu tien trinh cung --group) quan sat CONSUMER GROUP PARTITION
ASSIGNMENT (Kafka tu chia partition cho tung consumer trong group).

Chay tren HOST, ket noi qua EXTERNAL listener localhost:9092.

Vi du:
    # 1 consumer doc het topic tu dau, in offset tung message
    python consumer.py --group demo-group-solo --from-beginning --max-messages 400

    # 2 consumer CUNG group (chay o 2 terminal khac nhau, hoac --duration ngan
    # de demo trong 1 lan) de xem Kafka chia partition:
    python consumer.py --group demo-group-shared --consumer-name C1 --from-beginning --duration 15
    python consumer.py --group demo-group-shared --consumer-name C2 --from-beginning --duration 15
"""
import argparse
import json
import os
import time
from collections import Counter, defaultdict

from kafka import KafkaConsumer, TopicPartition

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "clickstream")


def main():
    parser = argparse.ArgumentParser(description="RetailStream Kafka consumer (clickstream)")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group", required=True, help="consumer group id")
    parser.add_argument("--consumer-name", default="consumer-1", help="ten hien thi (chi de log, khong gui len broker)")
    parser.add_argument("--from-beginning", action="store_true", help="auto_offset_reset=earliest")
    parser.add_argument("--max-messages", type=int, default=None, help="dung sau khi doc du so message nay")
    parser.add_argument("--duration", type=float, default=20.0, help="so giay toi da lang nghe (poll loop)")
    args = parser.parse_args()

    print("=" * 88)
    print(f"KAFKA CONSUMER [{args.consumer_name}] - group={args.group}")
    print("bootstrap_servers =", args.bootstrap_servers)
    print("topic             =", args.topic)
    print("=" * 88)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        consumer_timeout_ms=2000,  # poll wakes up periodically to check duration/max
    )

    # buoc join group + nhan partition assignment truoc khi doc. Dung poll(0)
    # (timeout_ms=0) de kich hoat rebalance/assignment ma KHONG bo lo message
    # nao (poll co timeout > 0 co the da tra ve message ngay lan goi dau).
    consumer.poll(timeout_ms=0)
    assignment = sorted(tp.partition for tp in consumer.assignment())
    print(f"[{args.consumer_name}] PARTITION ASSIGNMENT (group={args.group}): {assignment}")

    start = time.time()
    count = 0
    per_partition_last_offset = {}
    per_partition_count = Counter()

    try:
        while time.time() - start < args.duration:
            if args.max_messages is not None and count >= args.max_messages:
                break
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                assignment = sorted(tp.partition for tp in consumer.assignment())
                continue
            for tp, messages in batch.items():
                for msg in messages:
                    count += 1
                    per_partition_last_offset[msg.partition] = msg.offset
                    per_partition_count[msg.partition] += 1
                    print(
                        f"[{args.consumer_name}] partition={msg.partition} offset={msg.offset} "
                        f"key={msg.key} event_id={msg.value.get('event_id')} "
                        f"event_type={msg.value.get('event_type')}"
                    )
                    if args.max_messages is not None and count >= args.max_messages:
                        break
    finally:
        assignment_final = sorted(tp.partition for tp in consumer.assignment())
        consumer.close()

    print(f"\n--- TOM TAT [{args.consumer_name}] ---")
    print("partition assignment (cuoi phien) :", assignment_final)
    print("tong so message da doc            :", count)
    print("offset cuoi cung theo partition    :")
    for p in sorted(per_partition_last_offset):
        print(f"  partition {p}: last_offset={per_partition_last_offset[p]}  count_in_this_run={per_partition_count[p]}")


if __name__ == "__main__":
    main()
