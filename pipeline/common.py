"""Shared settings and the CSV-row -> event conversion used by the producer."""
import os, uuid, datetime as dt

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "hotelpass123")

TOPIC_BOOKINGS = "bookings"
TOPIC_CANCELS = "cancellations"

MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], 1)}
NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def _clean(v, default="UNK"):
    return default if v in (None, "", "NULL", "NA") else v


def build_events(row, idx):
    """Return (booking_event, cancellation_event_or_None) for one CSV row.

    booking_id is deterministic (uuid5 of the row number) so replays are idempotent.
    The booking event always starts as not-cancelled; a separate cancellation event
    flips it, as described in the report (Section 2.6).
    """
    nights = int(row["stays_in_weekend_nights"]) + int(row["stays_in_week_nights"])
    arrival = dt.date(int(row["arrival_date_year"]), MONTHS[row["arrival_date_month"]],
                      int(row["arrival_date_day_of_month"]))
    lead = int(row["lead_time"])
    booked_at = dt.datetime.combine(arrival - dt.timedelta(days=lead), dt.time(12, 0))
    bid = str(uuid.uuid5(NS, f"booking-{idx}"))
    adr = float(row["adr"])
    children = row["children"] if row["children"] not in ("", "NA", "NULL") else "0"
    booking = {
        "type": "booking", "booking_id": bid, "hotel": row["hotel"], "is_canceled": 0,
        "reservation_status": "Booked", "lead_time": lead,
        "booked_at": booked_at.isoformat(),
        "arrival": {"year": arrival.year, "month": row["arrival_date_month"],
                    "day": arrival.day, "date": arrival.isoformat(),
                    "ym": f"{arrival.year}-{arrival.month:02d}"},
        "stay": {"weekend_nights": int(row["stays_in_weekend_nights"]),
                 "week_nights": int(row["stays_in_week_nights"]), "nights": nights},
        "guest": {"adults": int(row["adults"]), "children": int(float(children)),
                  "babies": int(row["babies"]), "country": _clean(row["country"]),
                  "repeated": int(row["is_repeated_guest"])},
        "preferences": {"meal": row["meal"], "room": row["reserved_room_type"],
                        "special_requests": int(row["total_of_special_requests"])},
        "channel": {"segment": row["market_segment"], "distribution": row["distribution_channel"]},
        "deposit_type": row["deposit_type"], "customer_type": row["customer_type"],
        "adr": adr,
    }
    cancel = None
    if int(row["is_canceled"]) == 1:
        cancel = {"type": "cancellation", "booking_id": bid, "hotel": row["hotel"],
                  "arrival_date": arrival.isoformat(), "arrival_month": booking["arrival"]["ym"],
                  "lead_time": lead, "adr": adr, "nights": nights,
                  "segment": row["market_segment"],
                  "status": row["reservation_status"],
                  "canceled_at": f"{row['reservation_status_date']}T12:00:00"}
    return booking, cancel
