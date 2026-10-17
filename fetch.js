// One-off Dukascopy M15 pull (bid + ask) for 4 JPY pairs, then serves a zip over HTTP so you can download it.
// Delete this Railway service when you have the files.
const fs = require("fs");
const http = require("http");
const path = require("path");
const { getHistoricalRates } = require("dukascopy-node");

const PAIRS = ["usdjpy", "gbpjpy", "eurjpy", "audjpy"];
const FROM = process.env.FROM || "2023-01-01";
const TO = process.env.TO || new Date().toISOString().slice(0, 10);
const OUT = path.join(__dirname, "out");
const TOKEN = process.env.DL_TOKEN || "change-me-" + Math.random().toString(36).slice(2, 10);
fs.mkdirSync(OUT, { recursive: true });

let status = "starting";
let ready = false;

async function preflight() {
  // 1) raw connectivity test to Dukascopy's data server (month is 0-based in their URLs: 09 = October)
  const urls = [
    "https://datafeed.dukascopy.com/datafeed/USDJPY/2026/09/12/12h_ticks.bi5",
    "https://datafeed.dukascopy.com/datafeed/USDJPY/2025/05/12/12h_ticks.bi5",
  ];
  for (const u of urls) {
    try {
      const r = await fetch(u);
      const b = await r.arrayBuffer();
      console.log("PREFLIGHT raw", u, "HTTP", r.status, "bytes", b.byteLength);
    } catch (e) {
      console.log("PREFLIGHT raw FAILED", u, String(e && e.cause ? e.cause : e));
    }
  }
  // 2) tiny library test with strict errors
  try {
    const d = await getHistoricalRates({
      instrument: "usdjpy",
      dates: { from: new Date("2025-05-12T00:00:00Z"), to: new Date("2025-05-14T00:00:00Z") },
      timeframe: "m15", priceType: "bid", utcOffset: 0, format: "json",
      ignoreFlats: false, useCache: false, retryCount: 2, failAfterRetryCount: true,
    });
    console.log("PREFLIGHT library test rows:", d.length, d.length ? JSON.stringify(d[0]) : "");
  } catch (e) {
    console.log("PREFLIGHT library test ERROR:", e && e.message ? e.message : e);
    if (e && e.stack) console.log(String(e.stack).split("\n").slice(0, 4).join(" | "));
  }
}

async function pull() {
  await preflight();
  for (const pair of PAIRS) {
    for (const side of ["bid", "ask"]) {
      const file = path.join(OUT, `${pair.toUpperCase()}_${side}_M15.csv`);
      if (fs.existsSync(file)) { console.log("skip (exists)", file); continue; }
      status = `pulling ${pair} ${side}`;
      console.log(status);
      const data = await getHistoricalRates({
        instrument: pair,
        dates: { from: new Date(FROM + "T00:00:00Z"), to: new Date(TO + "T00:00:00Z") },
        timeframe: "m15",
        priceType: side,
        utcOffset: 0,
        format: "json",
        volumes: false,
        ignoreFlats: false,
        useCache: false,
        retryCount: 3,
        failAfterRetryCount: true,
        batchSize: 10,
        pauseBetweenBatchesMs: 500,
      });
      if (!data.length) throw new Error("0 rows for " + pair + " " + side + " - aborting");
      const lines = ["datetime,open,high,low,close"];
      for (const r of data) lines.push(`${new Date(r.timestamp).toISOString()},${r.open},${r.high},${r.low},${r.close}`);
      fs.writeFileSync(file, lines.join("\n"));
      console.log("saved", file, data.length, "rows");
    }
  }
  status = "done";
  ready = true;
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
