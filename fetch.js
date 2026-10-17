// One-off Dukascopy M15 pull (bid + ask) for 4 JPY pairs, polite/slow version.
// Waits until Dukascopy stops throttling (HTTP 429), then pulls slowly. Serves the CSVs over HTTP.
// Delete this Railway service when you have the files.
const fs = require("fs");
const http = require("http");
const path = require("path");
const { getHistoricalRates } = require("dukascopy-node");

const PAIRS = ["usdjpy", "gbpjpy", "eurjpy", "audjpy"];
const FROM = process.env.FROM || "2023-01-01";
const TO = process.env.TO || new Date().toISOString().slice(0, 10);
const BATCH = parseInt(process.env.BATCH || "2", 10);
const PAUSE = parseInt(process.env.PAUSE || "1500", 10);
const WAIT_MINUTES = parseInt(process.env.WAIT_MINUTES || "30", 10);
const OUT = path.join(__dirname, "out");
const TOKEN = process.env.DL_TOKEN || "change-me-" + Math.random().toString(36).slice(2, 10);
fs.mkdirSync(OUT, { recursive: true });

let status = "starting";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitUntilClear() {
  const url = "https://datafeed.dukascopy.com/datafeed/USDJPY/2025/05/12/12h_ticks.bi5";
  const tries = WAIT_MINUTES;
  for (let i = 1; i <= tries; i++) {
    try {
      const r = await fetch(url);
      await r.arrayBuffer();
      console.log(`PREFLIGHT try ${i}/${tries}: HTTP ${r.status}`);
      status = `preflight HTTP ${r.status} (try ${i}/${tries})`;
      if (r.status === 200) return true;
    } catch (e) {
      console.log(`PREFLIGHT try ${i}/${tries} FAILED`, String(e && e.cause ? e.cause : e));
      status = `preflight failed (try ${i}/${tries})`;
    }
    await sleep(60000);
  }
  return false;
}

async function pullOne(pair, side, from, to) {
  for (let attempt = 1; attempt <= 6; attempt++) {
    try {
      return await getHistoricalRates({
        instrument: pair,
        dates: { from: new Date(from + "T00:00:00Z"), to: new Date(to + "T00:00:00Z") },
        timeframe: "m15",
        priceType: side,
        utcOffset: 0,
        format: "json",
        volumes: false,
        ignoreFlats: false,
        useCache: false,
        batchSize: BATCH,
        pauseBetweenBatchesMs: PAUSE,
        retryCount: 5,
        pauseBetweenRetriesMs: 8000,
        failAfterRetryCount: true,
      });
    } catch (e) {
      console.log(`  ${pair} ${side} ${from}->${to} attempt ${attempt} failed: ${e && e.message}. Waiting 2 min...`);
      status = `throttled, retrying ${pair} ${side} (attempt ${attempt})`;
      await sleep(120000);
    }
  }
  throw new Error(`giving up on ${pair} ${side} ${from}->${to}`);
}

// pull one calendar year at a time so progress is saved and a failure only costs that chunk
function yearChunks(from, to) {
  const out = [];
  let y = parseInt(from.slice(0, 4), 10);
  const endY = parseInt(to.slice(0, 4), 10);
  for (; y <= endY; y++) {
    const a = y === parseInt(from.slice(0, 4), 10) ? from : `${y}-01-01`;
    const b = y === endY ? to : `${y + 1}-01-01`;
    if (a < b) out.push([a, b]);
  }
  return out;
}

async function pull() {
  const ok = await waitUntilClear();
  if (!ok) { status = "BLOCKED: Dukascopy still answering 429/timeouts after waiting"; console.log(status); return; }
  for (const pair of PAIRS) {
    for (const side of ["bid", "ask"]) {
      const file = path.join(OUT, `${pair.toUpperCase()}_${side}_M15.csv`);
      if (fs.existsSync(file)) { console.log("skip (exists)", file); continue; }
      const lines = ["datetime,open,high,low,close"];
      let total = 0;
      for (const [a, b] of yearChunks(FROM, TO)) {
        status = `pulling ${pair} ${side} ${a}->${b}`;
        console.log(status);
        const data = await pullOne(pair, side, a, b);
        console.log(`  ${pair} ${side} ${a}->${b}: ${data.length} rows`);
        total += data.length;
        for (const r of data) lines.push(`${new Date(r.timestamp).toISOString()},${r.open},${r.high},${r.low},${r.close}`);
      }
      if (!total) throw new Error("0 rows for " + pair + " " + side);
      fs.writeFileSync(file, lines.join("\n"));
      console.log("saved", file, total, "rows");
    }
  }
  status = "done";
  console.log("DONE. Open /" + TOKEN + "/ for the file list");
}

http.createServer((req, res) => {
  const base = "/" + TOKEN + "/";
  if (req.url === base + "status") {
    res.end(status + "\n");
  } else if (req.url === base) {
    const files = fs.readdirSync(OUT);
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end("<h3>" + status + "</h3>" + files.map((f) => `<p><a href="${base}${f}">${f}</a></p>`).join(""));
  } else if (req.url.startsWith(base) && /^[A-Z]+_(bid|ask)_M15\.csv$/.test(req.url.slice(base.length))) {
    const f = path.join(OUT, req.url.slice(base.length));
    if (!fs.existsSync(f)) { res.writeHead(404); return res.end("not ready\n"); }
    res.writeHead(200, { "Content-Type": "text/csv" });
    fs.createReadStream(f).pipe(res);
  } else {
    res.writeHead(404); res.end("not found\n");
  }
}).listen(process.env.PORT || 3000, () => console.log("server up; token path:", TOKEN));

pull().catch((e) => { status = "ERROR: " + e.message; console.error(e); });
