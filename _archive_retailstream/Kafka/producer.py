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
    parser.add_argument(
        "--delay", type=float, default=0.0,
        help="Do tre giua cac message (giay). Vi du: --delay 0.1 de quan sat dong su kien chay thoi gian thuc tren consumer!"
    )
    parser.add_argument(
        "--interactive", action="store_true",
        help="Che do tuong tac: go tung su kien tu ban phim de quan sat ngay tren consumer"
    )
    args = parser.parse_args()

    print("=" * 88)
    print("KAFKA PRODUCER - RetailStream clickstream")
    print("bootstrap_servers =", args.bootstrap_servers)
    print("topic             =", args.topic)
    print("key_strategy      =", args.key_strategy)
    if args.interactive:
        print("che do            = TUONG TAC BAN PHIM (Interactive Mode)")
    else:
        print("data_file         =", os.path.abspath(args.data_file))
        if args.delay > 0:
            print(f"toc do gui        = Do tre {args.delay}s / message (Che do Stream mo phong)")
        else:
            print("toc do gui        = Gui toi da toc do (Batch stream)")
    print("=" * 88)

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            linger_ms=10,
        )
    except Exception as e:
        print(f"\n[LOI KET NOI] Khong the ket noi toi Kafka broker tai {args.bootstrap_servers}: {e}")
        print("-> Hay dam bao Docker Kafka da khoi dong (bash scripts/start-cluster.sh)")
        sys.exit(1)

    sent = 0
    errors = 0
    partition_counts = Counter()
    partition_keys = defaultdict(set)

    # =========================================================================
    # CHE DO 1: TUONG TAC GO TAY TU BAN PHIM
    # =========================================================================
    if args.interactive:
        print("\n[CHE DO TUONG TAC] Nhap su kien thu nghiem (Go 'exit' hoac 'quit' de dung):")
        print("Goi y thu nghiem cac hanh vi: VIEW, ADD_TO_CART, PURCHASE")
        idx = 1
        while True:
            try:
                event_type = input(f"\n[Su kien #{idx}] Nhap event_type (hoac enter de chon 'VIEW'): ").strip()
                if event_type.lower() in ("exit", "quit"):
                    break
                if not event_type:
                    event_type = "VIEW"

                session_id = input(f"             Nhap session_id (mac dinh 'SESS00001'): ").strip() or "SESS00001"
                product_id = input(f"             Nhap product_id (mac dinh 'PROD00010'): ").strip() or "PROD00010"

                record = {
                    "event_id": f"CEV_MANUAL_{idx:04d}",
                    "event_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "customer_id": "CUST_STUDENT",
                    "session_id": session_id,
                    "product_id": product_id,
                    "event_type": event_type
                }
                key = build_key(record, args.key_strategy)
                fut = producer.send(args.topic, key=key, value=record)
                meta = fut.get(timeout=10)
                print(f"  ==> DA GUI! Roi vao Partition={meta.partition}, Offset={meta.offset} (Key='{key}')")
                sent += 1
                partition_counts[meta.partition] += 1
                idx += 1
            except KeyboardInterrupt:
                print("\nDung che do tuong tac.")
                break
            except Exception as e:
                print(f"  [LOI] Gui that bai: {e}")
                errors += 1
        producer.close()
        return

    # =========================================================================
    # CHE DO 2: DOC TU TEP CLICKSTREAM_SAMPLE.JSONL
    # =========================================================================
    futures = []
    if not os.path.exists(args.data_file):
        print(f"\n[LOI] Khong tim thay tep du lieu: {args.data_file}")
        sys.exit(1)

    print(f"\nDang doc va gui du lieu tu {os.path.basename(args.data_file)}...")
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
            futures.append((fut, key, record))

            if args.delay > 0:
                producer.flush()
                time.sleep(args.delay)

    producer.flush(timeout=30)

    for fut, key, record in futures:
        try:
            meta = fut.get(timeout=10)
            partition_counts[meta.partition] += 1
            if key is not None:
                partition_keys[meta.partition].add(key)
            sent += 1
            if args.delay > 0:
                print(f"  [SENT #{sent:03d}] Partition={meta.partition} | Offset={meta.offset:>3} | Key={str(key):<12} | Event={record.get('event_type')}")
        except KafkaError as e:
            print("[ERROR] gui that bai:", e)
            errors += 1

    producer.close()

    print("\n" + "=" * 88)
    print("--- KET QUA TONG KET GUI KAFKA ---")
    print(f"Tong so ban ghi doc duoc : {sent + errors}")
    print(f"Gui thanh cong           : {sent}")
    print(f"Loi                      : {errors}")
    print("\nPhan phoi theo partition (key_strategy=%s):" % args.key_strategy)
    for p in sorted(partition_counts):
        distinct_keys = len(partition_keys[p]) if args.key_strategy != "none" else "-"
        print(f"  partition {p}: {partition_counts[p]:>4} message(s)  (so key phan biet: {distinct_keys})")

    print("\n[GIAI THICH NGUYEN LY]:")
    if args.key_strategy == "session_id":
        print("  -> Khi key=session_id: Kafka bam Murmur2(session_id) % 4.")
        print("     Moi su kien cua CUNG MOT session_id deu vao chung 1 partition -> BAO TOAN DUNG THU TU THOI GIAN!")
    elif args.key_strategy == "product_id":
        print("  -> Khi key=product_id: Cac san pham 'hot' (nhieu luot xem) se don vao 1 partition gay LECH TAI (Hot Partition).")
    else:
        print("  -> Khi key=none: Kafka tu dong phan bo deu giua cac partition (Round-robin / Sticky).")
    print("=" * 88)

    if sent == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
