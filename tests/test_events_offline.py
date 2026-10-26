"""Offline check (no databases needed): event conversion reproduces the report's totals."""
import csv, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
from common import build_events
nb = nc = 0; rev = 0.0; ids = set()
with open(os.path.join(os.path.dirname(__file__), "..", "data", "hotel_bookings.csv"), newline="") as f:
    for i, row in enumerate(csv.DictReader(f)):
        b, c = build_events(row, i)
        nb += 1; ids.add(b["booking_id"]); rev += b["adr"] * b["stay"]["nights"]
        if c:
            nc += 1; rev -= c["adr"] * c["nights"]
print(f"bookings={nb} unique_ids={len(ids)} cancellations={nc} net_revenue={rev:,.2f}")
assert nb == 119390 and len(ids) == nb and nc == 44224 and abs(rev - 25996260) < 5, "totals do not match"
print("PASS: events reproduce 119,390 bookings, 44,224 cancellations and ~25.996M revenue")
