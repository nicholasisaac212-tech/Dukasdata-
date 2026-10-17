// One-off Dukascopy M15 pull (bid + ask) for 4 JPY pairs, then serves a zip over HTTP so you can download it.
// Delete this Railway service when you have the files.
const fs = require("fs");
const http = require("http");
const path = require("path");
const { getHistoricalRates } = require("dukascopy-node");

const PAIRS = ["usdjpy", "gbpjpy", "eurjpy", "audjpy"];
const FROM = process.env.FROM || "2023-01-01";
const TO = process.env.TO || "2026-10-18";
const OUT = path.join(__dirname, "out");
const TOKEN = process.env.DL_TOKEN || "change-me-" + Math.random().toString(36).slice(2, 10);
fs.mkdirSync(OUT, { recursive: true });

let status = "starting";
let ready = false;

async function pull() {
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
        batchSize: 10,
        pauseBetweenBatchesMs: 500,
      });
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
