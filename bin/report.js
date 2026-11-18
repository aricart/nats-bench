#!/usr/bin/env node
// turns a history run (bin/history.js) into tables and a chart page.
//
//   node bin/report.js [run-dir] [--html history/<run-id>/report.html]
//
// with no run-dir it reports the newest run under history/. the go client
// (nats bench) is the 100% baseline: every value is also shown as a
// percentage of go for the same workload and transport. the tables have a row
// per client version and a column per runtime (with its version); the chart
// page has one chart per workload and transport, with the client versions on
// the x axis, a line per runtime, and go at 100%.

import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";
import { root } from "./versions.js";

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: { html: { type: "string" } },
});

function newestRun() {
  const dir = join(root, "history");
  const runs = existsSync(dir)
    ? readdirSync(dir).filter((d) => existsSync(join(dir, d, "run.json")))
      .sort()
    : [];
  if (runs.length === 0) {
    throw new Error("no history yet, run: node bin/history.js");
  }
  return join(dir, runs.at(-1));
}

function compare(a, b) {
  const x = a.split(".").map(Number);
  const y = b.split(".").map(Number);
  for (let i = 0; i < 3; i++) if (x[i] !== y[i]) return x[i] - y[i];
  return 0;
}

const runDir = positionals[0] ? resolve(positionals[0]) : newestRun();
const run = JSON.parse(readFileSync(join(runDir, "run.json"), "utf8"));
const records = readdirSync(runDir)
  .filter((f) => f.endsWith(".json") && f !== "run.json")
  .map((f) => JSON.parse(readFileSync(join(runDir, f), "utf8")));

// a chart per "<workload> <size>B <transport>"; a pubsub record has pub and
// sub sides. older runs have one size per workload, so size may be missing
const sizeOf = (r) => r.workload.size ?? 128;
const sideKeys = (r) => {
  const sides = r.error ? { [r.workload.name]: null } : r.summary;
  return Object.entries(sides).map(([side, s]) => [
    `${
      r.workload.name === "pubsub" ? `pubsub (${side})` : r.workload.name
    } ${sizeOf(r)}B ${r.transport}`,
    s ? s.median : null,
  ]);
};
// "<workload> <size>B", the part of a key the zats reference is matched on
const zatsKey = (key) => key.split(" ").slice(0, 2).join(" ");

const charts = new Map(); // key -> Map(runtime -> Map(client -> median))
const go = new Map(); // key -> median of the go baseline
const zats = new Map(); // "<workload> <size>B" -> median of zats on both sides
const runtimeVersions = new Map();
for (const r of records) {
  if (r.runtime.version) runtimeVersions.set(r.runtime.name, r.runtime.version);
  if (r.runtime.name === "go") {
    for (const [key, v] of sideKeys(r)) go.set(key, v);
    continue;
  }
  if (r.runtime.name === "zats") {
    zats.set(
      `${r.workload.name} ${sizeOf(r)}B`,
      r.summary?.[r.workload.name]?.median ?? null,
    );
    continue;
  }
  for (const [key, v] of sideKeys(r)) {
    if (!charts.has(key)) charts.set(key, new Map());
    const series = charts.get(key);
    if (!series.has(r.runtime.name)) series.set(r.runtime.name, new Map());
    series.get(r.runtime.name).set(r.client.version, v);
  }
}
// charts in workload, transport, size order
{
  const parse = (k) => {
    const m = k.match(/^(.*) (\d+)B (\w+)$/);
    return [m[1], m[3], Number(m[2])];
  };
  const sorted = [...charts.entries()].sort(([a], [b]) => {
    const [x, y] = [parse(a), parse(b)];
    return x[0].localeCompare(y[0]) || x[1].localeCompare(y[1]) || x[2] - y[2];
  });
  charts.clear();
  for (const [k, v] of sorted) charts.set(k, v);
}
const clients = [
  ...new Set(records.map((r) => r.client.version).filter((v) => v !== null)),
].sort(compare);
const label = (rt) => `${rt} ${runtimeVersions.get(rt) ?? ""}`.trim();
const pct = (v, base) => (base ? Math.round((100 * v) / base) : null);
const fmt = (v, base) => {
  if (v === undefined) return "";
  if (v === null) return "error";
  const p = pct(v, base);
  return p === null ? v.toLocaleString() : `${v.toLocaleString()} (${p}%)`;
};

// tables
const goLabel = go.size
  ? `go (nats.go ${runtimeVersions.get("go") ?? "?"})`
  : null;
console.log(`run ${run.runId}`);
console.log(
  `server ${run.server.version}, ${run.host.cpu}, ${run.host.cores} cores, ${run.host.platform} ${run.host.arch}`,
);
console.log(
  `median msgs/sec of ${run.iterations} iterations` +
    (goLabel ? `; percentages are of the ${goLabel} baseline` : ""),
);
console.log("");
for (const [key, series] of charts) {
  const base = go.get(key);
  const rts = [...series.keys()];
  const head = ["client", ...rts.map(label)];
  const rows = clients.map((
    c,
  ) => [c, ...rts.map((rt) => fmt(series.get(rt).get(c), base))]);
  if (base) {
    rows.unshift([
      goLabel,
      ...rts.map(() => `${base.toLocaleString()} (100%)`),
    ]);
  }
  const z = zats.get(zatsKey(key));
  if (z) {
    rows.push([
      `zats ${runtimeVersions.get("zats") ?? ""} both sides`.trim(),
      ...rts.map(() => fmt(z, base)),
    ]);
  }
  const w = head.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => r[i].length))
  );
  const line = (cells) =>
    "| " +
    cells.map((c, i) => (i === 0 ? c.padEnd(w[i]) : c.padStart(w[i]))).join(
      " | ",
    ) + " |";
  console.log(`### ${key}\n`);
  console.log(line(head));
  console.log(
    "|" +
      w.map((n, i) => (i === 0 ? "-".repeat(n + 2) : "-".repeat(n + 1) + ":"))
        .join("|") +
      "|",
  );
  for (const r of rows) console.log(line(r));
  console.log("");
}

// chart page: inline svg, no dependencies. y is the percentage of go when
// there is a go baseline for the chart, msgs/sec otherwise
const palette = ["#2a78d6", "#d6602a", "#2aa86b", "#a33fbf", "#c9a227"];
function chart(key, series) {
  const base = go.get(key);
  const val = (v) => (base ? (100 * v) / base : v);
  const unit = (
    v,
  ) => (base ? `${Math.round(v)}%` : Math.round(v).toLocaleString());
  const W = 560, H = 260, L = 70, R = 16, T = 28, B = 40;
  const all = [...series.values()].flatMap((m) => [...m.values()])
    .filter((v) => typeof v === "number").map(val);
  const z = zats.get(zatsKey(key));
  if (base) all.push(100);
  if (z) all.push(val(z));
  const max = Math.max(1, ...all) * 1.1;
  const x = (i) =>
    L + (clients.length === 1 ? 0 : (i * (W - L - R)) / (clients.length - 1));
  const y = (v) => T + (H - T - B) * (1 - v / max);
  let svg = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${key}">`;
  svg += `<text x="${L}" y="18" class="t">${key}${
    base ? " — % of go" : " — msgs/sec"
  }</text>`;
  for (let k = 0; k <= 4; k++) {
    const v = (max / 4) * k;
    svg += `<line x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${
      y(v)
    }" class="g"/>`;
    svg += `<text x="${L - 6}" y="${y(v) + 4}" class="a" text-anchor="end">${
      unit(v)
    }</text>`;
  }
  clients.forEach((c, i) => {
    svg += `<text x="${x(i)}" y="${
      H - B + 18
    }" class="a" text-anchor="middle">${c}</text>`;
  });
  const hline = (v, cls, text) => {
    svg += `<line x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${
      y(v)
    }" class="${cls}"/>`;
    svg += `<text x="${W - R}" y="${
      y(v) - 4
    }" class="a" text-anchor="end">${text}</text>`;
  };
  if (base) hline(100, "base", `${goLabel}: ${base.toLocaleString()} msgs/sec`);
  if (z) {
    hline(
      val(z),
      "ref",
      `zats both sides: ${z.toLocaleString()}${
        base ? ` (${pct(z, base)}%)` : ""
      }`,
    );
  }
  [...series.entries()].forEach(([rt, m], s) => {
    const color = palette[s % palette.length];
    const pts = clients.map((c, i) => [i, m.get(c)]).filter(([, v]) =>
      typeof v === "number"
    );
    svg += `<polyline fill="none" stroke="${color}" stroke-width="2" points="${
      pts.map(([i, v]) => `${x(i)},${y(val(v))}`).join(" ")
    }"/>`;
    for (const [i, v] of pts) {
      svg += `<circle cx="${x(i)}" cy="${
        y(val(v))
      }" r="3" fill="${color}"><title>${label(rt)} ${clients[i]}: ${
        fmt(v, base)
      } msgs/sec</title></circle>`;
    }
  });
  svg += `</svg>`;
  const legend = [...series.keys()].map((rt, s) =>
    `<span><i style="background:${palette[s % palette.length]}"></i>${
      label(rt)
    }</span>`
  ).join("");
  return `<figure>${svg}<figcaption>${legend}</figcaption></figure>`;
}

const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>nats.js bench history</title>
<style>
:root { --fg: #1d1d1f; --muted: #6e6e73; --grid: #e5e5ea; --bg: #ffffff; }
@media (prefers-color-scheme: dark) { :root { --fg: #f2f2f7; --muted: #a1a1a6; --grid: #3a3a3c; --bg: #1c1c1e; } }
body { font: 14px/1.5 system-ui, sans-serif; color: var(--fg); background: var(--bg); margin: 24px auto; max-width: 1200px; padding: 0 16px; }
p { color: var(--muted); }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(min(100%, 520px), 1fr)); gap: 24px; }
figure { margin: 0; } svg { width: 100%; height: auto; }
.t { fill: var(--fg); font-weight: 600; font-size: 13px; } .a { fill: var(--muted); font-size: 11px; }
.g { stroke: var(--grid); }
.base { stroke: var(--fg); stroke-dasharray: 6 3; }
.ref { stroke: var(--muted); stroke-dasharray: 2 3; }
figcaption { display: flex; flex-wrap: wrap; gap: 12px; font-size: 12px; color: var(--muted); }
figcaption i { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 4px; vertical-align: middle; }
</style></head><body>
<h1>nats.js bench history</h1>
<p>run ${run.runId} · nats-server ${run.server.version} · ${run.host.cpu}, ${run.host.cores} cores, ${run.host.platform} ${run.host.arch} · median msgs/sec of ${run.iterations} iterations${
  goLabel ? ` · 100% = ${goLabel} with the same parameters` : ""
} · runtimes: ${
  [...runtimeVersions.keys()].filter((r) => r !== "go" && r !== "zats").map(
    label,
  ).join(", ")
}</p>
<div class="grid">${
  [...charts.entries()].map(([k, s]) => chart(k, s)).join("\n")
}</div>
</body></html>`;

const htmlFile = values.html
  ? resolve(values.html)
  : join(runDir, "report.html");
writeFileSync(htmlFile, html);
console.log(`chart page: ${htmlFile}`);
