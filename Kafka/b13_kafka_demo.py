"""
Buổi 13 – Apache Kafka: sự kiện chuyến xe CityRide.

Mỗi chuyến sinh 3 sự kiện theo thứ tự: ride_requested → driver_accepted →
trip_completed. Nhiều dịch vụ cùng cần các sự kiện này: tính giá, thông báo,
phân tích.

Chạy lần lượt 5 thí nghiệm (python b13_kafka_demo.py), mỗi thí nghiệm in ra
một điều cần quan sát:
  [1] key = trip_id: mọi sự kiện của một chuyến vào cùng partition, đúng thứ tự
  [2] consumer group "pricing" có 2 consumer: chia nhau 4 partition
  [3] group "analytics" đọc lại toàn bộ, độc lập với "pricing"
  [4] commit offset: group "notify" dừng giữa chừng, chạy lại đọc tiếp đúng chỗ
  [5] key = zone_id: partition nóng (quận trung tâm dồn vào một partition)

Kafka: docker compose trong thư mục này (localhost:9092).
"""

import json
import random
import warnings
import threading
import time
from collections import Counter, defaultdict

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError

warnings.filterwarnings("ignore", category=DeprecationWarning)
BOOT = "localhost:9092"
TOPIC = "ride-events"
TOPIC_ZONE = "ride-events-by-zone"
ZONES = ["Q01"] * 16 + ["Q03"] * 14 + ["Q05"] * 12 + ["Q02"] * 10 + ["Q04"] * 10 + \
        ["Q06"] * 8 + ["Q07"] * 6 + ["Q10"] * 6 + ["Q08"] * 5 + ["Q09"] * 5 + ["Q11"] * 4 + ["Q12"] * 4
rng = random.Random(13)


def section(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70, flush=True)


def create_topics():
    """Tạo 2 topic 4 partition. Chạy trên Kafka trống (scripts/reset-b13.sh)."""
    admin = KafkaAdminClient(bootstrap_servers=BOOT)
    existing = set(admin.list_topics())
    if TOPIC in existing:
        raise SystemExit("Topic da ton tai: chay bash scripts/reset-b13.sh de bat dau tu Kafka trong")
    admin.create_topics([NewTopic(TOPIC, 4, 1), NewTopic(TOPIC_ZONE, 4, 1)])
    admin.close()
    time.sleep(2)


def trip_events(n_trips, start=1):
    evs = []
    for i in range(start, start + n_trips):
        trip, zone = f"T{i:04d}", rng.choice(ZONES)
        for step in ("ride_requested", "driver_accepted", "trip_completed"):
            evs.append({"trip_id": trip, "zone_id": zone, "event_type": step})
    rng.shuffle(evs)                                   # các chuyến xen kẽ nhau...
    evs.sort(key=lambda e: (["ride_requested", "driver_accepted",
                             "trip_completed"].index(e["event_type"])))  # ...nhưng đúng thứ tự từng chuyến
    return evs


def produce(topic, events, key_field):
    p = KafkaProducer(bootstrap_servers=BOOT, acks="all",
                      key_serializer=str.encode,
                      value_serializer=lambda v: json.dumps(v).encode())
    placed = []
    for e in events:
        md = p.send(topic, key=e[key_field], value=e).get(timeout=10)
        placed.append((e, md.partition, md.offset))
    p.flush()
    p.close()
    return placed


def consume(group, n_expected, name="", timeout=20, max_records=None, commit=True):
    c = KafkaConsumer(TOPIC, bootstrap_servers=BOOT, group_id=group,
                      auto_offset_reset="earliest", enable_auto_commit=False,
                      value_deserializer=lambda b: json.loads(b.decode()))
    got, t0 = [], time.time()
    while len(got) < n_expected and time.time() - t0 < timeout:
        for tp, recs in c.poll(timeout_ms=500).items():
            for r in recs:
                if max_records is not None and len(got) >= max_records:
                    break
                got.append((tp.partition, r.offset, r.value))
        if max_records is not None and len(got) >= max_records:
            break
    parts = sorted(tp.partition for tp in c.assignment())
    if commit and got:
        last = {}
        for part, off, _ in got:
            last[part] = max(last.get(part, -1), off)
        from kafka.structs import OffsetAndMetadata
        c.commit({TopicPartition(TOPIC, pt): OffsetAndMetadata(off + 1, None, -1)
                  for pt, off in last.items()})
    c.close()
    return parts, got


def show_lag(group):
    probe = KafkaConsumer(bootstrap_servers=BOOT, group_id=group, enable_auto_commit=False)
    tps = [TopicPartition(TOPIC, p) for p in range(4)]
    end = probe.end_offsets(tps)
    total = 0
    for tp in tps:
        cm = probe.committed(tp) or 0
        total += end[tp] - cm
        print(f"    partition {tp.partition}: offset cuoi = {end[tp]:>2}, {group} da commit = {cm:>2}, lag = {end[tp] - cm}")
    print(f"    tong lag = {total}")
    probe.close()


def main():
    create_topics()

    section("[1] Gui 30 chuyen x 3 su kien, key = trip_id (topic 4 partition)")
    placed = produce(TOPIC, trip_events(30), "trip_id")
    per_part = Counter(p for _, p, _ in placed)
    print("so su kien moi partition:", dict(sorted(per_part.items())))
    for trip in ("T0001", "T0002", "T0003"):
        rows = [(e["event_type"], p, o) for e, p, o in placed if e["trip_id"] == trip]
        print(f"  {trip}: " + ", ".join(f"{t}@p{p}/off{o}" for t, p, o in rows))

    section("[2] Group 'pricing' co 2 consumer chay cung luc")
    mk = lambda: KafkaConsumer(TOPIC, bootstrap_servers=BOOT, group_id="pricing",
                               auto_offset_reset="earliest", enable_auto_commit=False,
                               value_deserializer=lambda b: json.loads(b.decode()))
    cons = {"pricing-A": mk(), "pricing-B": mk()}
    # chờ Kafka chia xong partition cho cả 2 consumer rồi mới đọc từ đầu
    t0 = time.time()
    while time.time() - t0 < 30:
        for c in cons.values():
            c.poll(timeout_ms=300)
        a = [set(tp.partition for tp in c.assignment()) for c in cons.values()]
        if all(a) and not (a[0] & a[1]) and len(a[0] | a[1]) == 4:
            break
    for c in cons.values():
        c.seek_to_beginning(*c.assignment())
    got = {n: [] for n in cons}
    t0 = time.time()
    while sum(len(g) for g in got.values()) < 90 and time.time() - t0 < 20:
        for n, c in cons.items():
            for tp, recs in c.poll(timeout_ms=300).items():
                got[n] += [(tp.partition, r.offset, r.value) for r in recs]
    results = {}
    for n, c in cons.items():
        results[n] = (sorted(tp.partition for tp in c.assignment()), got[n])
        c.close()
    for name, (parts, g) in results.items():
        print(f"  {name}: partition {parts}, doc {len(g)} su kien")
    print("  tong:", sum(len(g) for _, g in results.values()))

    section("[3] Group 'analytics' doc doc lap tu dau topic")
    parts, got = consume("analytics", 90)
    print(f"  analytics: partition {parts}, doc {len(got)} su kien (khong anh huong 'pricing')")
    order_ok = all(
        [v["event_type"] for _, _, v in got if v["trip_id"] == trip] ==
        ["ride_requested", "driver_accepted", "trip_completed"]
        for trip in {v["trip_id"] for _, _, v in got})
    print("  thu tu 3 su kien cua moi chuyen dung:", order_ok)

    section("[4] Commit offset va chay lai (group 'notify')")
    _, got1 = consume("notify", 90, max_records=40)
    print(f"  lan 1: doc {len(got1)} su kien, commit roi dung")
    produce(TOPIC, trip_events(10, start=31), "trip_id")
    print("  producer gui them 10 chuyen = 30 su kien moi (tong 120)")
    print("  lag truoc lan 2:")
    show_lag("notify")
    _, got2 = consume("notify", 80)
    print(f"  lan 2: doc {len(got2)} su kien (50 con lai cua lo cu + 30 moi)")
    print("  lag sau lan 2:")
    show_lag("notify")

    section("[5] Key = zone_id: partition nong")
    evs = [{"trip_id": f"Z{i:04d}", "zone_id": rng.choice(ZONES), "event_type": "ride_requested"}
           for i in range(400)]
    placed = produce(TOPIC_ZONE, evs, "zone_id")
    per_part = Counter(p for _, p, _ in placed)
    zones_in = defaultdict(set)
    for e, p, _ in placed:
        zones_in[p].add(e["zone_id"])
    for p in range(4):
        print(f"  partition {p}: {per_part.get(p, 0):>3} su kien, quan {sorted(zones_in[p])}")
    placed2 = produce(TOPIC_ZONE, [dict(e, trip_id=e["trip_id"] + "b") for e in evs], "trip_id")
    per2 = Counter(p for _, p, _ in placed2)
    print("  so sanh key = trip_id:", dict(sorted(per2.items())))


if __name__ == "__main__":
    main()
