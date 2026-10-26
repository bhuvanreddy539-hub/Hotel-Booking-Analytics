"""Replay hotel_bookings.csv into Kafka as booking and cancellation events."""
import argparse, csv, json, time
from kafka import KafkaProducer
from common import KAFKA, TOPIC_BOOKINGS, TOPIC_CANCELS, build_events

ap = argparse.ArgumentParser()
ap.add_argument("--csv", default="../data/hotel_bookings.csv")
ap.add_argument("--rate", type=float, default=500, help="bookings per second (0 = as fast as possible)")
ap.add_argument("--limit", type=int, default=0, help="stop after N rows (0 = all)")
a = ap.parse_args()

producer = KafkaProducer(bootstrap_servers=KAFKA, linger_ms=20,
                         value_serializer=lambda v: json.dumps(v).encode(),
                         key_serializer=lambda k: k.encode())
sent_b = sent_c = 0
t0 = time.time()
with open(a.csv, newline="", encoding="utf-8") as f:
    for idx, row in enumerate(csv.DictReader(f)):
        if a.limit and idx >= a.limit:
            break
        booking, cancel = build_events(row, idx)
        producer.send(TOPIC_BOOKINGS, key=booking["booking_id"], value=booking); sent_b += 1
        if cancel:
            producer.send(TOPIC_CANCELS, key=cancel["booking_id"], value=cancel); sent_c += 1
        if a.rate and sent_b % max(1, int(a.rate)) == 0:
            time.sleep(1)
        if sent_b % 5000 == 0:
            print(f"sent {sent_b} bookings, {sent_c} cancellations")
producer.flush()
print(f"done: {sent_b} bookings, {sent_c} cancellations in {time.time()-t0:.1f}s")
