"""Connections and the four database writers (one per data model)."""
import json, time, datetime as dt, uuid
from decimal import Decimal
import redis
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase
from common import *


def retry(fn, what, tries=40, wait=5):
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            print(f"waiting for {what} ({i+1}/{tries}): {e}")
            time.sleep(wait)
    raise RuntimeError(f"could not connect to {what}")


class Stores:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, decode_responses=True)
        self.mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        self.db = self.mongo["hotel"]
        retry(lambda: self.db.command("ping"), "MongoDB")
        self.db.bookings.create_index([("hotel", 1), ("arrival.year", 1), ("arrival.month", 1)])
        self.cluster = Cluster([CASSANDRA_HOST])
        self.cs = retry(lambda: self.cluster.connect(), "Cassandra")
        self.cs.execute("CREATE KEYSPACE IF NOT EXISTS hotel WITH replication = "
                        "{'class':'SimpleStrategy','replication_factor':1}")
        self.cs.set_keyspace("hotel")
        self.cs.execute("""CREATE TABLE IF NOT EXISTS booking_events_by_month (
            hotel text, arrival_month text, booked_at timestamp, event_id uuid,
            is_canceled boolean, lead_time int, adr decimal, segment text,
            PRIMARY KEY ((hotel, arrival_month), booked_at, event_id)
        ) WITH CLUSTERING ORDER BY (booked_at DESC)""")
        self.ins = self.cs.prepare(
            "INSERT INTO booking_events_by_month (hotel, arrival_month, booked_at, event_id, "
            "is_canceled, lead_time, adr, segment) VALUES (?,?,?,?,?,?,?,?)")
        self.neo = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        retry(lambda: self.neo.verify_connectivity(), "Neo4j")
        with self.neo.session() as s:
            for lbl, key in [("Country", "code"), ("RoomType", "code"), ("Meal", "code"), ("Segment", "name")]:
                s.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{lbl}) REQUIRE n.{key} IS UNIQUE")

    # ---- idempotency guard (at-least-once delivery) ----
    def seen(self, kind, bid):
        return self.r.sismember(f"seen:{kind}", bid)

    def mark(self, kind, bid):
        self.r.sadd(f"seen:{kind}", bid)

    # ---- booking: all four stores ----
    def write_booking(self, e):
        doc = {k: v for k, v in e.items() if k != "type"}
        doc["_id"] = doc.pop("booking_id")
        # MongoDB: document stored as received; $setOnInsert keeps replays from undoing a cancellation
        self.db.bookings.update_one({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True)
        # Cassandra: append to the time-ordered log
        self.cs.execute(self.ins, (e["hotel"], e["arrival"]["ym"], dt.datetime.fromisoformat(e["booked_at"]),
                                   uuid.UUID(e["booking_id"]), False, e["lead_time"],
                                   Decimal(str(e["adr"])), e["channel"]["segment"]))
        # Redis: pre-aggregated counters
        pipe = self.r.pipeline()
        pipe.incr("bookings:total")
        pipe.incrbyfloat("revenue:total", e["adr"] * e["stay"]["nights"])
        pipe.incr(f"occupancy:{e['hotel']}:{e['arrival']['date']}")
        pipe.lpush("bookings:recent", json.dumps({
            "booking_id": e["booking_id"], "hotel": e["hotel"], "country": e["guest"]["country"],
            "segment": e["channel"]["segment"], "adr": e["adr"], "arrival": e["arrival"]["date"]}))
        pipe.ltrim("bookings:recent", 0, 19)
        pipe.execute()
        # Neo4j: preference graph
        g, p, c = e["guest"], e["preferences"], e["channel"]
        with self.neo.session() as s:
            s.run("""
              MERGE (c:Country {code:$country}) MERGE (sg:Segment {name:$segment})
              MERGE (r:RoomType {code:$room}) MERGE (m:Meal {code:$meal})
              MERGE (c)-[b:BOOKED]->(r) ON CREATE SET b.count=1 ON MATCH SET b.count=b.count+1
              MERGE (c)-[h:CHOSE]->(m)  ON CREATE SET h.count=1 ON MATCH SET h.count=h.count+1
              MERGE (sg)-[b2:BOOKED]->(r) ON CREATE SET b2.count=1 ON MATCH SET b2.count=b2.count+1
              MERGE (sg)-[h2:CHOSE]->(m)  ON CREATE SET h2.count=1 ON MATCH SET h2.count=h2.count+1
            """, country=g["country"], segment=c["segment"], room=p["room"], meal=p["meal"])

    # ---- cancellation: Mongo status, Cassandra log, Redis counters ----
    def write_cancellation(self, e):
        self.db.bookings.update_one({"_id": e["booking_id"]}, {"$set": {
            "is_canceled": 1, "reservation_status": e["status"], "canceled_at": e["canceled_at"]}})
        self.cs.execute(self.ins, (e["hotel"], e["arrival_month"], dt.datetime.fromisoformat(e["canceled_at"]),
                                   uuid.uuid5(NS, "cancel-" + e["booking_id"]), True, e["lead_time"],
                                   Decimal(str(e["adr"])), e["segment"]))
        pipe = self.r.pipeline()
        pipe.incr("bookings:canceled")
        pipe.incrbyfloat("revenue:total", -e["adr"] * e["nights"])
        pipe.decr(f"occupancy:{e['hotel']}:{e['arrival_date']}")
        pipe.execute()
