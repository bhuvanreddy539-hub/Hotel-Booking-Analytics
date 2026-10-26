"""Stream processor: consume both topics and route each event to the stores modelled for it."""
import json, collections
from kafka import KafkaConsumer
from common import KAFKA, TOPIC_BOOKINGS, TOPIC_CANCELS
from stores import Stores, retry

st = Stores()
consumer = retry(lambda: KafkaConsumer(
    TOPIC_BOOKINGS, TOPIC_CANCELS, bootstrap_servers=KAFKA, group_id="hotel-processor",
    auto_offset_reset="earliest", enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode())), "Kafka")
pending = collections.deque()   # cancellations that arrived before their booking (topics are independent)
n = 0


def handle_cancel(e):
    bid = e["booking_id"]
    if st.seen("cancels", bid):
        return True
    if not st.seen("bookings", bid):
        return False
    st.write_cancellation(e)
    st.mark("cancels", bid)
    return True


def drain():
    for _ in range(len(pending)):
        e = pending.popleft()
        if not handle_cancel(e):
            pending.append(e)


print("stream processor running ...")
while True:
    batches = consumer.poll(timeout_ms=1000, max_records=500)
    for msgs in batches.values():
        for m in msgs:
            e = m.value
            if e["type"] == "booking":
                if not st.seen("bookings", e["booking_id"]):
                    st.write_booking(e)
                    st.mark("bookings", e["booking_id"])
            elif e["type"] == "cancellation":
                if not handle_cancel(e):
                    pending.append(e)
            n += 1
            if n % 2000 == 0:
                print(f"processed {n} events, {len(pending)} cancellations waiting")
    if pending:
        drain()
