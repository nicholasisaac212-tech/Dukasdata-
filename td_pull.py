import os, sys, time, json, csv, datetime as dt
import urllib.request, urllib.parse

KEYS = [k.strip() for k in (os.environ.get("TD_KEYS") or os.environ.get("TD_KEY") or "").split(",") if k.strip()]
if not KEYS:
    sys.exit("TD_KEYS secret is missing")
print("keys loaded:", len(KEYS))
_rot = [0]
_last = {}
PAIRS = ["USD/JPY", "GBP/JPY", "EUR/JPY", "AUD/JPY"]
START = dt.date(2022, 12, 26)
END = dt.date.today() + dt.timedelta(days=1)
STEP = 45            # days per request (45*96 = 4320 rows < 5000 cap)
PAUSE = 9            # min seconds between requests on the SAME key (free plan: 8 per minute)
os.makedirs("out", exist_ok=True)

def next_key():
    if not KEYS:
        sys.exit("all keys are out of credits for today - run again tomorrow (it skips nothing already saved only if you rerun)")
    k = KEYS[_rot[0] % len(KEYS)]; _rot[0] += 1
    wait = PAUSE - (time.time() - _last.get(k, 0))
    if wait > 0: time.sleep(wait)
    _last[k] = time.time()
    return k

def call(sym, a, b):
    for attempt in range(8):
        key = next_key()
        q = urllib.parse.urlencode(dict(symbol=sym, interval="15min", start_date=a.isoformat(),
            end_date=b.isoformat(), outputsize=5000, timezone="UTC", format="JSON", apikey=key))
        url = "https://api.twelvedata.com/time_series?" + q
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                d = json.loads(r.read().decode())
        except Exception as e:
            print("  network error", e, "- retry"); time.sleep(30); continue
        if d.get("status") == "error":
            msg = str(d.get("message"))
            print("  API error:", d.get("code"), msg[:150])
            if "day" in msg.lower() and ("credit" in msg.lower() or "limit" in msg.lower()):
                print("  key out of daily credits, dropping it"); KEYS.remove(key); continue
            if d.get("code") == 429 or "minute" in msg.lower():
                time.sleep(20); continue
            if "No data" in msg or d.get("code") == 400 and "data" in msg.lower():
                return []
            time.sleep(20); continue
        return d.get("values", [])
    raise SystemExit("giving up on " + sym + " " + str(a))

for sym in PAIRS:
    name = sym.replace("/", "")
    rows = {}
    a = START
    while a < END:
        b = min(a + dt.timedelta(days=STEP), END)
        vals = call(sym, a, b)
        for v in vals:
            rows[v["datetime"]] = (v["open"], v["high"], v["low"], v["close"])
        print(f"{name} {a}->{b}: {len(vals)} rows (total {len(rows)})", flush=True)
        a = b
    if not rows:
        sys.exit("no rows for " + name)
    with open(f"out/{name}_TD_M15.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["datetime", "open", "high", "low", "close"])
        for k in sorted(rows): w.writerow((k,) + rows[k])
    print("saved", name, len(rows), "rows", flush=True)
print("DONE")
