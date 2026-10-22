#!/usr/bin/env python3
"""
duka_safe.py - slow, resumable Dukascopy tick downloader -> 15-minute BID / ASK / MID candles.

Why this version: Dukascopy throttles by IP (429 / 503 / empty or 202 replies). This one
  * goes one request at a time with a pause between them (default 0.4 s) and slows itself down on 429/5xx,
  * never treats a reply as "no data" on a normal trading hour until it has been asked 3 times,
  * keeps a ledger (ledger_<PAIR>.csv): a day only counts as done when every hour was fetched; re-running
    resumes where it stopped and re-does only the unfinished days,
  * needs nothing but python3, pandas and numpy (no ledger.py, runs on Windows too).

Usage examples
  python3 duka_safe.py --symbols USDJPY --start 2025-06-01 --end 2025-06-30     # try a month first
  python3 duka_safe.py --symbols USDJPY GBPJPY EURJPY AUDJPY CADJPY --start 2023-01-02
  python3 duka_safe.py --symbols EURGBP --start 2023-01-02 --delay 0.8          # slower if you still get 429
  python3 duka_safe.py --compile-only --symbols USDJPY                          # just rebuild the output CSVs

Output (in ./out): <PAIR>_M15_dukascopy.csv  (time UTC, bid/ask/mid OHLC, spread, tick count)
                   <PAIR>_M15_mid_MT5fmt.csv (same tab format as your MT5 files, but MID prices, UTC)
Times are UTC. Ctrl+C is safe: run the same command again to continue.
"""
import argparse, csv, datetime as dt, lzma, random, sys, time, urllib.request, urllib.error
from pathlib import Path
import numpy as np
import pandas as pd

BASE = "https://datafeed.dukascopy.com/datafeed"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
DIV1000 = {"USDJPY","EURJPY","GBPJPY","AUDJPY","NZDJPY","CADJPY","CHFJPY","XAUUSD","XAGUSD"}
TICK = np.dtype([("ms", ">i4"), ("ask", ">i4"), ("bid", ">i4"), ("av", ">f4"), ("bv", ">f4")])
OUT = Path("out"); PARTS = Path("parts")


def divider(p): return 1000 if p in DIV1000 else 100000


def expected_open(h: dt.datetime) -> bool:
    """Hours where FX normally has ticks (so an empty reply is suspicious)."""
    wd = h.weekday()
    if wd == 5: return False
    if wd == 4: return h.hour < 21          # Friday closes 21:00 (summer) / 22:00 (winter) UTC
    if wd == 6: return h.hour >= 22         # Sunday opens 21:00 / 22:00 UTC (22 = safe in both seasons)
    return True


def hours_to_try(day: dt.date):
    wd = day.weekday()
    if wd == 5: return []
    if wd == 6: return list(range(20, 24))  # try from 20 so winter/summer opens are both covered
    return list(range(24))


class Throttle:
    def __init__(self, delay): self.base = delay; self.delay = delay; self.last = 0.0; self.n429 = 0
    def wait(self):
        gap = self.delay - (time.time() - self.last)
        if gap > 0: time.sleep(gap)
        self.last = time.time()
    def slower(self): self.delay = min(max(self.delay * 1.6, 1.0), 6.0); self.n429 += 1
    def faster(self): self.delay = max(self.base, self.delay * 0.97)


THR = None
def _open(url):                      # separate function so tests can replace it
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read()


def fetch_hour(pair: str, h: dt.datetime, log):
    """Returns (status, bytes). status: data | empty | notfound | failed."""
    url = f"{BASE}/{pair}/{h.year}/{h.month-1:02d}/{h.day:02d}/{h.hour:02d}h_ticks.bi5"
    empties = 0
    for attempt in range(9):
        THR.wait()
        try:
            code, body = _open(url)
            if code == 200 and len(body) > 0:
                THR.faster(); return "data", body
            # 200/202 with empty body: real closed hour OR throttling -> ask again before believing it
            empties += 1
            if not expected_open(h) or empties >= 3:
                return "empty", b""
            time.sleep(4 * empties + random.random())
        except urllib.error.HTTPError as e:
            if e.code == 404: return "notfound", b""
            THR.slower()
            wait = min(120, 5 * 2 ** min(attempt, 4)) + random.random() * 3
            log(f"    HTTP {e.code} on {h:%Y-%m-%d %Hh}; backing off {wait:.0f}s (delay now {THR.delay:.1f}s)")
            time.sleep(wait)
        except Exception as e:
            THR.slower()
            wait = min(90, 4 * 2 ** min(attempt, 4)) + random.random() * 3
            log(f"    {type(e).__name__} on {h:%Y-%m-%d %Hh}; retry in {wait:.0f}s")
            time.sleep(wait)
    return "failed", b""


def decode(body: bytes, h: dt.datetime, div: int):
    try: raw = lzma.decompress(body)
    except lzma.LZMAError: return None
    n = len(raw) // 20
    if n == 0: return pd.DataFrame(columns=["bid", "ask"])
    a = np.frombuffer(raw, dtype=TICK, count=n)
    t = np.datetime64(h.replace(tzinfo=None), "ns") + a["ms"].astype("timedelta64[ms]")
    return pd.DataFrame({"bid": a["bid"].astype(np.float64) / div, "ask": a["ask"].astype(np.float64) / div}, index=pd.DatetimeIndex(t))


def to_m15(ticks: pd.DataFrame) -> pd.DataFrame:
    ticks = ticks.sort_index()
    b = ticks.bid.resample("15min").ohlc().add_prefix("bid_")
    a = ticks.ask.resample("15min").ohlc().add_prefix("ask_")
    out = b.join(a).dropna(subset=["bid_open"])
    out["spread_mean"] = (ticks.ask - ticks.bid).resample("15min").mean()
    out["ticks"] = ticks.bid.resample("15min").count()
    return out


def do_day(pair, day, div, log):
    stats = dict(data=0, empty=0, notfound=0, failed=0, ticks=0); frames = []
    for hr in hours_to_try(day):
        h = dt.datetime(day.year, day.month, day.day, hr)
        st, body = fetch_hour(pair, h, log)
        stats[st] += 1
        if st == "data":
            df = decode(body, h, div)
            if df is None: stats["failed"] += 1; stats["data"] -= 1
            else: frames.append(df); stats["ticks"] += len(df)
    if frames:
        PARTS.joinpath(pair).mkdir(parents=True, exist_ok=True)
        to_m15(pd.concat(frames)).to_csv(PARTS / pair / f"{day}.csv", index_label="time")
    return stats


def load_ledger(pair):
    p = Path(f"ledger_{pair}.csv"); done = {}
    if p.exists():
        for r in csv.DictReader(p.open()): done[r["date"]] = int(r["failed"])
    return done


def compile_pair(pair):
    files = sorted((PARTS / pair).glob("*.csv")) if (PARTS / pair).exists() else []
    if not files: print(f"[{pair}] nothing to compile"); return
    df = pd.concat([pd.read_csv(f, parse_dates=["time"]) for f in files]).drop_duplicates("time").sort_values("time")
    for k in ("open", "high", "low", "close"):
        df["mid_" + k] = (df["bid_" + k] + df["ask_" + k]) / 2
    OUT.mkdir(exist_ok=True)
    df.to_csv(OUT / f"{pair}_M15_dukascopy.csv", index=False)
    m = pd.DataFrame({"<DATE>": df.time.dt.strftime("%Y.%m.%d"), "<TIME>": df.time.dt.strftime("%H:%M:%S"),
                      "<OPEN>": df.mid_open, "<HIGH>": df.mid_high, "<LOW>": df.mid_low, "<CLOSE>": df.mid_close,
                      "<TICKVOL>": df.ticks, "<VOL>": 0, "<SPREAD>": (df.spread_mean * div_pts(pair)).round(0).fillna(0).astype(int)})
    m.to_csv(OUT / f"{pair}_M15_mid_MT5fmt.csv", sep="\t", index=False, float_format="%.5f" if divider(pair) == 100000 else "%.3f")
    print(f"[{pair}] wrote {len(df):,} M15 bars {df.time.min()} -> {df.time.max()}  ->  {OUT}/")


def div_pts(pair): return divider(pair)


def main(argv=None):
    global THR
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbols", nargs="+", default=["USDJPY", "GBPJPY", "EURJPY", "AUDJPY", "CADJPY"])
    ap.add_argument("--start", default="2023-01-02"); ap.add_argument("--end", default=None, help="inclusive, default yesterday")
    ap.add_argument("--delay", type=float, default=0.4, help="seconds between requests (raise it if you still see 429s)")
    ap.add_argument("--compile-only", action="store_true")
    a = ap.parse_args(argv)
    THR = Throttle(a.delay)
    start = dt.date.fromisoformat(a.start); end = dt.date.fromisoformat(a.end) if a.end else dt.date.today() - dt.timedelta(days=1)
    log = lambda s: print(s, flush=True)
    for pair in [s.upper() for s in a.symbols]:
        if not a.compile_only:
            led = load_ledger(pair); lp = Path(f"ledger_{pair}.csv"); new = not lp.exists()
            days = [start + dt.timedelta(d) for d in range((end - start).days + 1)]
            days = [d for d in days if d.weekday() != 5 and not (str(d) in led and led[str(d)] == 0)]
            log(f"[{pair}] {len(days)} days to fetch ({start} -> {end}); delay {THR.delay:.1f}s; Ctrl+C is safe, re-run to resume")
            t0 = time.time(); fails = 0
            with lp.open("a", newline="") as f:
                w = csv.writer(f)
                if new: w.writerow(["date", "data", "empty", "notfound", "failed", "ticks"]); f.flush()
                for i, d in enumerate(days, 1):
                    s = do_day(pair, d, divider(pair), log)
                    w.writerow([d, s["data"], s["empty"], s["notfound"], s["failed"], s["ticks"]]); f.flush()
                    fails += s["failed"] > 0
                    if i % 10 == 0 or i == len(days):
                        rate = i / (time.time() - t0); eta = (len(days) - i) / rate / 60
                        log(f"[{pair}] {i}/{len(days)} days | last {d} ticks {s['ticks']:,} | days with failed hours {fails} | 429s seen {THR.n429} | delay {THR.delay:.1f}s | ETA {eta:.0f} min")
            if fails: log(f"[{pair}] {fails} day(s) had hours that still failed: just run the same command again to redo them.")
        compile_pair(pair)


if __name__ == "__main__":
    main()
