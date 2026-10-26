# Hotel Booking and Customer Preference Analytics System

Multi-NoSQL real-time analytics for the Hotel Booking Demand dataset (119,390 bookings).
CSV -> Kafka -> stream processor -> MongoDB / Cassandra / Redis / Neo4j -> FastAPI -> React dashboard.

| Data | Database | Used for |
|---|---|---|
| Bookings (documents) | MongoDB | cancellation, segment, meal, hotel aggregations |
| Booking-event log | Cassandra | monthly seasonality (partition: hotel + arrival month) |
| Counters | Redis | totals, revenue, occupancy, latest bookings |
| Preference graph | Neo4j | what guests from a country/segment choose |

## Requirements
Docker (8 GB RAM, 16 GB recommended), Python 3.10+, Node.js 18+.

## Run
```bash
# 1. start the databases and Kafka (Cassandra needs ~1-2 minutes to be ready)
docker compose up -d

# 2. Python environment
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. stream processor (leave running; it waits for the databases)
cd pipeline && python consumer.py

# 4. in a second terminal: replay the CSV into Kafka
cd pipeline && python producer.py --rate 2000      # --limit 5000 for a quick trial

# 5. in a third terminal: API
cd backend && uvicorn main:app --port 8000

# 6. in a fourth terminal: dashboard -> http://localhost:5173
cd dashboard && npm install && npm run dev
```

## Tests (use these results in the report)
```bash
python tests/test_events_offline.py   # no databases needed
python tests/verify_load.py           # compare all four databases with the CSV (after the load finishes)
python tests/smoke_test.py            # every REST endpoint
pip install websockets && python tests/ws_test.py   # WebSocket feed
```

## Design notes
- `booking_id` is a deterministic UUID per CSV row, so replays are idempotent: Mongo uses `$setOnInsert`,
  Cassandra's primary key is unique, Neo4j and Redis counters are guarded by a `seen:*` set.
- A booking event always starts as not-cancelled; a separate cancellation event flips it. The topics are
  independent, so a cancellation that arrives before its booking waits in a small queue until the booking is seen.
- The dataset has no booking time, so `booked_at` = arrival date minus lead time. It has no guest ID, so the
  graph models Country/Segment -> RoomType/Meal preferences, not individual guests.
- Replication factor is 1 (single-node demo). Use NetworkTopologyStrategy with RF >= 3 in production.
