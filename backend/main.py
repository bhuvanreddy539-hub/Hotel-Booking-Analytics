"""FastAPI service: the only component that reads from all four databases."""
import asyncio, json, os, sys, time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
import redis
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASS", "hotelpass123"))

app = FastAPI(title="Hotel Booking Analytics API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

r = redis.Redis(host=REDIS_HOST, decode_responses=True)
mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
bookings = mongo["hotel"]["bookings"]
_cass = {}
_neo = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
_cache = {}


def cass():
    if "s" not in _cass:
        s = Cluster([CASSANDRA_HOST]).connect("hotel")
        _cass["s"] = s
        _cass["q"] = s.prepare("SELECT is_canceled, adr FROM booking_events_by_month "
                               "WHERE hotel=? AND arrival_month=?")
    return _cass["s"], _cass["q"]


def cached(key, ttl, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    val = fn()
    _cache[key] = (time.time(), val)
    return val


def summary_data():
    total, canc, rev = r.get("bookings:total"), r.get("bookings:canceled"), r.get("revenue:total")
    source = "redis"
    if total is None:   # fallback when counters are missing (Section 2.9 of the report)
        source = "mongodb"
        agg = list(bookings.aggregate([{"$group": {"_id": None, "n": {"$sum": 1},
              "c": {"$sum": "$is_canceled"},
              "rev": {"$sum": {"$cond": [{"$eq": ["$is_canceled", 0]},
                                          {"$multiply": ["$adr", "$stay.nights"]}, 0]}}}}]))
        a = agg[0] if agg else {"n": 0, "c": 0, "rev": 0}
        total, canc, rev = a["n"], a["c"], a["rev"]
    total, canc, rev = int(total or 0), int(canc or 0), float(rev or 0)
    return {"total_bookings": total, "cancellations": canc, "net_bookings": total - canc,
            "cancellation_rate": round(100 * canc / total, 1) if total else 0.0,
            "revenue": round(rev, 2), "source": source}


@app.get("/api/health")
def health():
    out = {}
    for name, fn in [("redis", lambda: r.ping()), ("mongodb", lambda: mongo.admin.command("ping")),
                     ("cassandra", lambda: cass()[0].execute("SELECT now() FROM system.local")),
                     ("neo4j", lambda: _neo.verify_connectivity())]:
        try:
            fn(); out[name] = "up"
        except Exception:
            out[name] = "down"
    return {"api": "up", "databases": out}


@app.get("/api/summary")
def summary():
    return summary_data()


@app.get("/api/recent")
def recent():
    return [json.loads(x) for x in r.lrange("bookings:recent", 0, 19)]


@app.get("/api/occupancy")
def occupancy(hotel: str = "City Hotel", date: str = Query("2016-08-15")):
    v = r.get(f"occupancy:{hotel}:{date}")
    return {"hotel": hotel, "date": date, "rooms_booked": int(v or 0), "source": "redis"}


@app.get("/api/hotels")
def hotels():
    rows = bookings.aggregate([{"$group": {"_id": "$hotel", "bookings": {"$sum": 1},
        "cancel_rate": {"$avg": "$is_canceled"}, "adr": {"$avg": "$adr"},
        "nights": {"$avg": "$stay.nights"}, "lead_time": {"$avg": "$lead_time"}}}, {"$sort": {"_id": 1}}])
    return [{"hotel": x["_id"], "bookings": x["bookings"], "cancel_rate": round(100 * x["cancel_rate"], 1),
             "adr": round(x["adr"], 2), "nights": round(x["nights"], 2), "lead_time": round(x["lead_time"], 1)}
            for x in rows]


@app.get("/api/cancellation/leadtime")
def by_leadtime():
    bounds = [0, 8, 31, 91, 181, 366, 100000]
    labels = ["0-7", "8-30", "31-90", "91-180", "181-365", ">365"]
    rows = bookings.aggregate([{"$bucket": {"groupBy": "$lead_time", "boundaries": bounds, "default": "other",
        "output": {"bookings": {"$sum": 1}, "rate": {"$avg": "$is_canceled"}}}}])
    d = {x["_id"]: x for x in rows}
    return [{"bucket": labels[i], "bookings": d.get(b, {}).get("bookings", 0),
             "cancel_rate": round(100 * d.get(b, {}).get("rate", 0), 1)} for i, b in enumerate(bounds[:-1])]


@app.get("/api/cancellation/segment")
def by_segment():
    rows = bookings.aggregate([{"$group": {"_id": "$channel.segment", "bookings": {"$sum": 1},
        "rate": {"$avg": "$is_canceled"}, "adr": {"$avg": "$adr"}}}, {"$sort": {"bookings": -1}}])
    return [{"segment": x["_id"], "bookings": x["bookings"], "cancel_rate": round(100 * x["rate"], 1),
             "adr": round(x["adr"], 2)} for x in rows]


@app.get("/api/meals")
def meals():
    rows = bookings.aggregate([{"$group": {"_id": "$preferences.meal", "bookings": {"$sum": 1},
        "adr": {"$avg": "$adr"}}}, {"$sort": {"bookings": -1}}])
    return [{"meal": x["_id"], "bookings": x["bookings"], "adr": round(x["adr"], 2)} for x in rows]


MONTHS = [f"{y}-{m:02d}" for y in (2015, 2016, 2017) for m in range(1, 13)
          if (2015, 7) <= (y, m) <= (2017, 8)]


@app.get("/api/monthly")
def monthly():
    def load():
        s, q = cass()
        out = []
        for ym in MONTHS:
            row = {"month": ym}
            for hotel, key in [("City Hotel", "city"), ("Resort Hotel", "resort")]:
                n = c = 0; adr = 0.0
                for x in s.execute(q, (hotel, ym)):
                    if x.is_canceled:
                        c += 1
                    else:
                        n += 1; adr += float(x.adr)
                row[f"{key}_bookings"] = n
                row[f"{key}_adr"] = round(adr / n, 1) if n else 0
            out.append(row)
        return out
    return cached("monthly", 15, load)   # 15 s cache: cross-partition scan, as in Section 2.9


@app.get("/api/preferences")
def preferences(country: str = "PRT"):
    with _neo.session() as s:
        rooms = s.run("MATCH (:Country {code:$c})-[b:BOOKED]->(r:RoomType) "
                      "RETURN r.code AS room, b.count AS bookings ORDER BY bookings DESC LIMIT 5", c=country)
        rooms = [dict(x) for x in rooms]
        meals = s.run("MATCH (:Country {code:$c})-[h:CHOSE]->(m:Meal) "
                      "RETURN m.code AS meal, h.count AS bookings ORDER BY bookings DESC LIMIT 5", c=country)
        meals = [dict(x) for x in meals]
    return {"country": country, "rooms": rooms, "meals": meals}


@app.get("/api/countries")
def countries():
    with _neo.session() as s:
        rows = s.run("MATCH (c:Country)-[b:BOOKED]->() RETURN c.code AS code, sum(b.count) AS n "
                     "ORDER BY n DESC LIMIT 12")
        return [x["code"] for x in rows]


@app.websocket("/ws")
async def ws(sock: WebSocket):
    await sock.accept()
    try:
        await sock.send_json({"type": "snapshot", **await asyncio.to_thread(summary_data)})
        last = None
        while True:
            await asyncio.sleep(2)
            cur = await asyncio.to_thread(summary_data)
            await sock.send_json({"type": "update" if cur != last else "tick", **cur})
            last = cur
    except (WebSocketDisconnect, RuntimeError):
        pass
