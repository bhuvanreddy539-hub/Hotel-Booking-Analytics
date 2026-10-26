"""Hit every REST endpoint and check the documented keys. Run after the pipeline has loaded data."""
import requests, sys
B = "http://localhost:8000"
checks = [("/api/health", ["api", "databases"]), ("/api/summary", ["total_bookings", "revenue"]),
          ("/api/recent", None), ("/api/occupancy", ["rooms_booked"]), ("/api/hotels", None),
          ("/api/cancellation/leadtime", None), ("/api/cancellation/segment", None),
          ("/api/meals", None), ("/api/monthly", None), ("/api/preferences?country=PRT", ["rooms", "meals"]),
          ("/api/countries", None)]
ok = 0
for path, keys in checks:
    try:
        resp = requests.get(B + path, timeout=30); data = resp.json()
        good = resp.status_code == 200 and (keys is None or all(k in data for k in keys))
    except Exception as e:
        good = False
    print(("PASS " if good else "FAIL ") + path); ok += good
print(f"{ok}/{len(checks)} endpoints passed")
sys.exit(0 if ok == len(checks) else 1)
