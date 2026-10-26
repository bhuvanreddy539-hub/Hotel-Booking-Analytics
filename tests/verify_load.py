"""Compare what the four databases hold with what the CSV says. Run after the consumer has caught up."""
import csv, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import redis
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase
from common import *

total = canc = 0; rev = 0.0
with open(os.path.join(os.path.dirname(__file__), "..", "data", "hotel_bookings.csv"), newline="") as f:
    for row in csv.DictReader(f):
        total += 1; canc += int(row["is_canceled"])
        if row["is_canceled"] == "0":
            rev += float(row["adr"]) * (int(row["stays_in_weekend_nights"]) + int(row["stays_in_week_nights"]))
print(f"CSV      : {total} bookings, {canc} cancelled, revenue {rev:,.2f}")

r = redis.Redis(host=REDIS_HOST, decode_responses=True)
rt, rc, rr = int(r.get("bookings:total") or 0), int(r.get("bookings:canceled") or 0), float(r.get("revenue:total") or 0)
b = MongoClient(MONGO_URI)["hotel"]["bookings"]
mt, mc = b.count_documents({}), b.count_documents({"is_canceled": 1})
cs = Cluster([CASSANDRA_HOST]).connect("hotel")
ct = cc = 0
for hotel in ("City Hotel", "Resort Hotel"):
    for y in (2015, 2016, 2017):
        for m in range(1, 13):
            for x in cs.execute("SELECT is_canceled FROM booking_events_by_month WHERE hotel=%s AND arrival_month=%s", (hotel, f"{y}-{m:02d}")):
                ct += 1; cc += bool(x.is_canceled)
with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS)).session() as s:
    gn = s.run("MATCH (:Country)-[b:BOOKED]->(:RoomType) RETURN sum(b.count) AS n").single()["n"]
print(f"Redis    : {rt} bookings, {rc} cancelled, revenue {rr:,.2f}")
print(f"MongoDB  : {mt} documents, {mc} cancelled")
print(f"Cassandra: {ct - cc} booking rows, {cc} cancellation rows")
print(f"Neo4j    : {gn} BOOKED edge count")
checks = {"Redis total": rt == total, "Redis cancelled": rc == canc, "Redis revenue": abs(rr - rev) < 1,
          "Mongo total": mt == total, "Mongo cancelled": mc == canc,
          "Cassandra bookings": ct - cc == total, "Cassandra cancellations": cc == canc, "Neo4j edges": gn == total}
for k, v in checks.items():
    print(("PASS " if v else "FAIL ") + k)
sys.exit(0 if all(checks.values()) else 1)
