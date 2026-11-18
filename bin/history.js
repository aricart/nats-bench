#!/usr/bin/env node
// runs a small set of workloads for every client version x runtime and saves
// one json record per combination under history/<run-id>/. the point is to
// spot regressions between releases, not to measure peak throughput, so the
// workloads are small.
//
//   node bin/history.js [--clients all|latest|3.4.0,3.5.0]
//     [--runtimes node,deno,bun,browser] [--iterations 3] [--out history]
//     [--workloads core|js|all] [--sizes 0,8,32,64,128]
//
// --clients all (the default) is every released version: run
// `node bin/versions.js install all` first. each record has the host, the
// server version, the client version (checked against the server's /connz),
// the runtime and its version, the transport and the workload, so the records
// can be sliced by any of them. bin/report.js turns them into tables and
// charts.
//
// internal comparison, not in the readme: when zats (aricart/zats, a native
// zig client; or $ZATS) is on the PATH, the sub-zats and pub-zats workloads
// pair the js side with zats on the other side, and zats on both sides runs
// once as a reference. without zats they are skipped.

import { execFileSync, spawn, spawnSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:net";
import {
  arch,
  cpus,
  hostname,
  platform,
  release,
  tmpdir,
  totalmem,
} from "node:os";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";
import { runBrowser } from "./browser.js";
import { installed, latest, root, sync, versionsDir } from "./versions.js";

const { values } = parseArgs({
  options: {
    clients: { type: "string", default: "all" },
    runtimes: { type: "string", default: "node,deno,bun,browser" },
    iterations: { type: "string", default: "3" },
    out: { type: "string", default: "history" },
    // core (the default), js (jetstream and kv) or all
    workloads: { type: "string", default: "core" },
    // message sizes in bytes; every workload with a payload runs at each
    sizes: { type: "string", default: "0,8,32,64,128" },
  },
});
const iterations = Number(values.iterations);
const runtimes = values.runtimes.split(",");
const sizes = values.sizes.split(",").map(Number);

// small workloads: enough to spot trouble
const STREAM = "benchstream";
const BUCKET = "benchbucket";
const allWorkloads = [
  {
    name: "pub",
    cli: ["pub", "bench.core"],
    msgs: 50_000,
    size: 128,
    transports: ["tcp", "ws"],
  },
  {
    name: "pubsub",
    cli: ["pubsub", "bench.ps"],
    msgs: 50_000,
    size: 128,
    transports: ["tcp", "ws"],
  },
  {
    name: "jspub",
    group: "js",
    cli: ["js", "pub", "bench.js"],
    msgs: 5_000,
    size: 128,
    transports: ["tcp", "ws"],
    stream: true,
  },
  {
    name: "kvput",
    group: "js",
    cli: ["kv", "put"],
    msgs: 1_000,
    size: 128,
    transports: ["tcp", "ws"],
  },
  {
    name: "kvget",
    group: "js",
    cli: ["kv", "get"],
    msgs: 1_000,
    size: 128,
    transports: ["tcp", "ws"],
  },
  // zats is a fast native client, so on the other side it doesn't limit the
  // js side being measured: sub-zats measures the js subscriber while zats
  // publishes, pub-zats the js publisher while zats subscribes
  {
    name: "sub-zats",
    zats: "pub",
    msgs: 100_000,
    size: 128,
    transports: ["tcp", "ws"],
  },
  {
    name: "pub-zats",
    zats: "sub",
    msgs: 100_000,
    size: 128,
    transports: ["tcp", "ws"],
  },
];
// one workload per size; kvget reads what kvput wrote, so it has no size
const workloads = allWorkloads.filter((w) =>
  values.workloads === "all" || (w.group ?? "core") === values.workloads
).flatMap((w) =>
  w.name === "kvget" ? [w] : sizes.map((size) => ({ ...w, size }))
);
// jetstream is only enabled on the server when a workload needs it
const needsJetStream = workloads.some((w) => w.group === "js");

// zats (https://github.com/aricart/zats) on the PATH, or $ZATS
const ZATS = process.env.ZATS ?? "zats";
const hasZats = spawnSync(ZATS, ["--help"], { stdio: "ignore" }).status === 0;

// the go client (nats bench, from the nats cli) is the 100% baseline: every
// core workload also runs with it, with the same parameters
const NATS = process.env.NATS_CLI ?? "nats";
const hasGo = spawnSync(NATS, ["--version"], { stdio: "ignore" }).status === 0;
const goWorkloads = new Set(["pub", "pubsub", "sub-zats", "pub-zats"]);

function freePort() {
  return new Promise((resolve) => {
    const s = createServer().listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

async function startServer() {
  const dir = mkdtempSync(join(tmpdir(), "nats-bench-history-"));
  const [port, ws, http] = [
    await freePort(),
    await freePort(),
    await freePort(),
  ];
  const conf = join(dir, "server.conf");
  writeFileSync(
    conf,
    `port: ${port}\nhttp_port: ${http}\nwebsocket { port: ${ws}, no_tls: true }\n` +
      (needsJetStream
        ? `jetstream { store_dir: "${join(dir, "js")}" }\n`
        : "") +
      `max_payload: 1048576\n`,
  );

  const proc = spawn("nats-server", ["-c", conf], { stdio: "ignore" });
  const monitor = `http://127.0.0.1:${http}`;
  for (let i = 0; i < 50; i++) {
    try {
      const varz = await (await fetch(`${monitor}/varz`)).json();
      return {
        proc,
        dir,
        tcp: `127.0.0.1:${port}`,
        ws: `ws://127.0.0.1:${ws}`,
        monitor,
        version: varz.version,
        gitCommit: varz.git_commit,
      };
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  throw new Error("nats-server didn't start");
}

// the newest closed connections first, so the window always covers this run
async function maxCid(monitor) {
  const c =
    await (await fetch(`${monitor}/connz?state=closed&sort=stop&limit=256`))
      .json();
  return Math.max(0, ...(c.connections ?? []).map((x) => x.cid));
}

// the client versions the server saw since cid, e.g. { "nats.js 3.4.0": 3 }
async function clientsSince(monitor, cid) {
  const c =
    await (await fetch(`${monitor}/connz?state=closed&sort=stop&limit=256`))
      .json();
  const seen = {};
  for (const x of c.connections ?? []) {
    if (x.cid > cid) {
      seen[`${x.lang} ${x.version}`] = (seen[`${x.lang} ${x.version}`] ?? 0) +
        1;
    }
  }
  return seen;
}

function nats(server, ...args) {
  execFileSync("nats", ["--server", server.tcp, ...args], { stdio: "ignore" });
}

function prepare(server, w) {
  if (!w.stream) return;
  try {
    nats(server, "stream", "info", STREAM);
    nats(server, "stream", "purge", STREAM, "-f");
  } catch {
    nats(
      server,
      "stream",
      "add",
      STREAM,
      "--subjects",
      "bench.js",
      "--storage",
      "memory",
      "--defaults",
    );
  }
}

// summary of one iteration group from `bench.js --json`
function summarize(group) {
  const rates = group.iterations
    ? group.iterations.map((i) => i.results.throughputMsgsPerSec)
    : [group.results.throughputMsgsPerSec];
  const sorted = [...rates].sort((a, b) => a - b);
  return {
    rates,
    median: sorted[Math.floor(sorted.length / 2)],
    min: sorted[0],
    max: sorted[sorted.length - 1],
  };
}

function runCli(runtime, client, w, transport, server, out) {
  const dir = client === "latest" ? root : sync(client);
  const bench = join(dir, "bench.js");
  const args = [
    ...w.cli,
    "--msgs",
    String(w.msgs),
    "--iterations",
    String(iterations),
    "--json",
    out,
    "--server",
    transport === "ws" ? server.ws.replace("ws://", "") : server.tcp,
  ];
  if (w.name !== "kvget") args.push("--size", String(w.size));
  if (w.name === "kvput" || w.name === "kvget") args.push("--bucket", BUCKET);
  if (w.name === "jspub") args.push("--stream", STREAM);
  if (transport === "ws") args.push("--ws");

  let cmd;
  if (runtime === "deno") {
    // deno runs the root bench.js with the version's import map
    const map = client === "latest"
      ? []
      : ["--import-map", join(versionsDir, client, "import_map.json")];
    cmd = ["deno", ["run", "-A", ...map, join(root, "bench.js"), ...args]];
  } else {
    cmd = [runtime, [bench, ...args]];
  }
  const r = spawnSync(cmd[0], cmd[1], {
    encoding: "utf8",
    cwd: root,
    timeout: 300_000,
  });
  if (r.status !== 0) {
    return { error: (r.stderr || r.stdout || `exit ${r.status}`).slice(-2000) };
  }
  return JSON.parse(readFileSync(out, "utf8"));
}

async function runBrowserWorkload(client, w, server) {
  const command = w.name;
  return await runBrowser(command, {
    client,
    server: server.ws,
    subject: w.cli.at(-1).startsWith("bench.") ? w.cli.at(-1) : "bench",
    msgs: w.msgs,
    size: w.size,
    iterations,
    bucket: BUCKET,
    stream: STREAM,
  });
}

// waits until the server has a subscription on subject
async function waitForInterest(monitor, subject, timeout = 30_000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const c = await (await fetch(`${monitor}/connz?subs=1&limit=1024`)).json();
    if (
      (c.connections ?? []).some((x) =>
        (x.subscriptions_list ?? []).includes(subject)
      )
    ) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`no subscription on ${subject} after ${timeout}ms`);
}

function spawnAsync(cmd, args, opts = {}) {
  return new Promise((resolve) => {
    const p = spawn(cmd, args, { cwd: root, ...opts });
    let out = "";
    p.stdout?.on("data", (d) => (out += d));
    p.stderr?.on("data", (d) => (out += d));
    p.on("close", (status) => resolve({ status, out }));
  });
}

// "Sub stats: 3.36M msgs/sec" -> 3360000
function zatsRate(out, side) {
  const m = out.match(new RegExp(`${side} stats: ([0-9.]+)([KMG]?) msgs/sec`));
  if (!m) return null;
  return Math.round(Number(m[1]) * ({ K: 1e3, M: 1e6, G: 1e9 }[m[2]] ?? 1));
}

function zatsArgs(role, subject, w, server) {
  return [
    "bench",
    role,
    subject,
    "--msgs",
    String(w.msgs),
    "--size",
    String(w.size),
    "-s",
    `nats://${server.tcp}`,
  ];
}

function cliCommand(runtime, client, args) {
  if (runtime === "deno") {
    const map = client === "latest"
      ? []
      : ["--import-map", join(versionsDir, client, "import_map.json")];
    return ["deno", ["run", "-A", ...map, join(root, "bench.js"), ...args]];
  }
  const dir = client === "latest" ? root : sync(client);
  return [runtime, [join(dir, "bench.js"), ...args]];
}

// one js side of a zats-paired workload, as a promise of the bench json
function jsSide(runtime, client, command, subject, w, transport, server, out) {
  if (runtime === "browser") {
    return runBrowser(command, {
      client,
      server: server.ws,
      subject,
      msgs: w.msgs,
      size: w.size,
      iterations: 1,
    }).then((r) => (r.error ? { error: r.error } : r.iterations[0]));
  }
  const args = [
    command,
    subject,
    "--msgs",
    String(w.msgs),
    "--size",
    String(w.size),
    "--json",
    out,
    "--server",
    transport === "ws" ? server.ws.replace("ws://", "") : server.tcp,
  ];
  if (transport === "ws") args.push("--ws");
  const [cmd, a] = cliCommand(runtime, client, args);
  return spawnAsync(cmd, a).then((r) =>
    r.status === 0
      ? JSON.parse(readFileSync(out, "utf8"))
      : { error: r.out.slice(-2000) }
  );
}

// runs a zats-paired workload: a fresh subject and processes per iteration,
// so publisher and subscriber can be lined up each time
async function runZatsPaired(runtime, client, w, transport, server, tmp) {
  const iters = [];
  for (let i = 0; i < iterations; i++) {
    const subject = `bench.z.${w.name}.${Date.now()}.${i}`;
    const out = join(tmp, `z${i}.json`);
    rmSync(out, { force: true });
    let r;
    if (runtime === "zats") {
      // reference: zats on both sides, the measured side as in the workload
      const measured = w.zats === "pub" ? "sub" : "pub";
      if (measured === "sub") {
        const sub = spawnAsync(ZATS, zatsArgs("sub", subject, w, server));
        await waitForInterest(server.monitor, subject);
        await spawnAsync(ZATS, zatsArgs("pub", subject, w, server));
        const s = await sub;
        r = { rate: zatsRate(s.out, "Sub") };
      } else {
        const sub = spawnAsync(ZATS, zatsArgs("sub", subject, w, server));
        await waitForInterest(server.monitor, subject);
        const p = await spawnAsync(ZATS, zatsArgs("pub", subject, w, server));
        await sub;
        r = { rate: zatsRate(p.out, "Pub") };
      }
      if (r.rate === null) return { error: "couldn't parse zats bench output" };
      iters.push({ results: { throughputMsgsPerSec: r.rate } });
      continue;
    }
    if (w.zats === "pub") {
      // the js subscriber is measured
      const js = jsSide(
        runtime,
        client,
        "sub",
        subject,
        w,
        transport,
        server,
        out,
      );
      await waitForInterest(server.monitor, subject);
      const z = await spawnAsync(ZATS, zatsArgs("pub", subject, w, server));
      if (z.status !== 0) return { error: `zats pub: ${z.out.slice(-500)}` };
      r = await js;
    } else {
      // the js publisher is measured
      const z = spawnAsync(ZATS, zatsArgs("sub", subject, w, server));
      await waitForInterest(server.monitor, subject);
      r = await jsSide(
        runtime,
        client,
        "pub",
        subject,
        w,
        transport,
        server,
        out,
      );
      await z;
    }
    if (r.error) return { error: r.error };
    iters.push(r);
  }
  return { metadata: iters[0].metadata ?? { runtime }, iterations: iters };
}

// msgs/sec from a `nats bench --csv` file: the first data row
function goRate(file) {
  const rows = readFileSync(file, "utf8").split("\n").filter((l) =>
    l && !l.startsWith("#")
  );
  const head = readFileSync(file, "utf8").split("\n").find((l) =>
    l.startsWith("#")
  );
  const col = head.slice(1).split(",").indexOf("MsgsPerSec");
  return Math.round(Number(rows[0].split(",")[col]));
}

// nats bench rejects --size 0, but publishes a --payload file as is, so 0
// bytes is an empty payload file. --size only sets the reported bytes then.
let emptyPayload;
function sizeArgs(role, w) {
  if (w.size > 0) return ["--size", `${w.size}B`];
  if (role !== "pub") return [];
  if (!emptyPayload) {
    emptyPayload = join(mkdtempSync(join(tmpdir(), "nats-bench-")), "empty");
    writeFileSync(emptyPayload, "");
  }
  return ["--payload", emptyPayload];
}

function goArgs(role, subject, w, transport, server, csv) {
  return [
    "bench",
    role,
    subject,
    "--msgs",
    String(w.msgs),
    ...sizeArgs(role, w),
    "--clients",
    "1",
    "--no-progress",
    "--csv",
    csv,
    "-s",
    transport === "ws" ? server.ws : `nats://${server.tcp}`,
  ];
}

// the go baseline for a core workload, with the same parameters as the js
// runs: pub alone, go pub + go sub, go sub with zats publishing, go pub with
// zats subscribing. a fresh subject and processes per iteration.
async function runGo(w, transport, server, tmp) {
  const sides = w.name === "pubsub" ? ["pub", "sub"] : [w.name];
  const iters = Object.fromEntries(sides.map((s) => [s, []]));
  for (let i = 0; i < iterations; i++) {
    const subject = `bench.go.${w.name}.${Date.now()}.${i}`;
    const csv = (n) => join(tmp, `go-${n}-${i}.csv`);
    for (const n of ["pub", "sub"]) rmSync(csv(n), { force: true });
    const fail = (r, what) => ({ error: `${what}: ${r.out.slice(-500)}` });

    if (w.name === "pub") {
      const p = await spawnAsync(
        NATS,
        goArgs("pub", subject, w, transport, server, csv("pub")),
      );
      if (p.status !== 0) return fail(p, "nats bench pub");
      iters.pub.push(goRate(csv("pub")));
    } else if (w.name === "pubsub") {
      const s = spawnAsync(
        NATS,
        goArgs("sub", subject, w, transport, server, csv("sub")),
      );
      await waitForInterest(server.monitor, subject);
      const p = await spawnAsync(
        NATS,
        goArgs("pub", subject, w, transport, server, csv("pub")),
      );
      const sr = await s;
      if (p.status !== 0) return fail(p, "nats bench pub");
      if (sr.status !== 0) return fail(sr, "nats bench sub");
      iters.pub.push(goRate(csv("pub")));
      iters.sub.push(goRate(csv("sub")));
    } else if (w.name === "sub-zats") {
      const s = spawnAsync(
        NATS,
        goArgs("sub", subject, w, transport, server, csv("sub")),
      );
      await waitForInterest(server.monitor, subject);
      const z = await spawnAsync(ZATS, zatsArgs("pub", subject, w, server));
      const sr = await s;
      if (z.status !== 0) return fail(z, "zats pub");
      if (sr.status !== 0) return fail(sr, "nats bench sub");
      iters[w.name].push(goRate(csv("sub")));
    } else if (w.name === "pub-zats") {
      const z = spawnAsync(ZATS, zatsArgs("sub", subject, w, server));
      await waitForInterest(server.monitor, subject);
      const p = await spawnAsync(
        NATS,
        goArgs("pub", subject, w, transport, server, csv("pub")),
      );
      await z;
      if (p.status !== 0) return fail(p, "nats bench pub");
      iters[w.name].push(goRate(csv("pub")));
    }
  }
  const group = (rates) => ({
    metadata: { runtime: "go" },
    iterations: rates.map((r) => ({ results: { throughputMsgsPerSec: r } })),
  });
  return w.name === "pubsub"
    ? { pub: group(iters.pub), sub: group(iters.sub) }
    : group(iters[w.name]);
}

function versionOf(client) {
  return client === "latest" ? latest() : client;
}

async function main() {
  let clients;
  if (values.clients === "all") clients = installed();
  else if (values.clients === "latest") clients = ["latest"];
  else clients = values.clients.split(",");
  if (clients.length === 0) {
    console.error(
      "no client versions installed, run: node bin/versions.js install all",
    );
    process.exit(1);
  }

  const server = await startServer();
  const runId = new Date().toISOString().replace(/[:.]/g, "-");
  const outDir = join(resolve(root, values.out), runId);
  mkdirSync(outDir, { recursive: true });
  const host = {
    hostname: hostname(),
    platform: platform(),
    osRelease: release(),
    arch: arch(),
    cpu: cpus()[0]?.model,
    cores: cpus().length,
    memGB: Math.round(totalmem() / 2 ** 30),
  };
  writeFileSync(
    join(outDir, "run.json"),
    JSON.stringify(
      {
        runId,
        host,
        server: { version: server.version, gitCommit: server.gitCommit },
        clients: clients.map(versionOf),
        runtimes,
        iterations,
        workloads,
      },
      null,
      2,
    ),
  );
  console.log(
    `run ${runId}: server ${server.version}, clients ${
      clients.join(" ")
    }, runtimes ${runtimes.join(" ")}`,
  );

  if (!hasZats) {
    console.log(`${ZATS} not found: skipping the zats-paired workloads`);
  }
  const zatsVersion = hasZats
    // zats prints its version on stderr
    ? (({ stdout, stderr }) =>
      (stdout || stderr).trim().replace(/^zats\s+/, ""))(
        spawnSync(ZATS, ["--version"], { encoding: "utf8" }),
      )
    : null;
  // the references don't involve a nats.js client, so they run once: the go
  // client (the 100% baseline) and zats on both sides
  const plan = [];
  if (hasGo) plan.push([null, "go"]);
  else console.log(`${NATS} not found: no go baseline`);
  for (const client of clients) {
    for (const runtime of runtimes) plan.push([client, runtime]);
  }
  if (hasZats) plan.push([null, "zats"]);

  const tmp = mkdtempSync(join(tmpdir(), "nats-bench-out-"));
  try {
    for (const [client, runtime] of plan) {
      for (const w of workloads) {
        if (w.zats && !hasZats) continue;
        if (runtime === "zats" && !w.zats) continue;
        if (runtime === "go" && !goWorkloads.has(w.name)) continue;
        const transports = runtime === "browser"
          ? ["ws"]
          : runtime === "zats"
          ? ["tcp"]
          : w.transports;
        for (const transport of transports) {
          prepare(server, w);
          const cid = await maxCid(server.monitor);
          const out = join(tmp, "out.json");
          rmSync(out, { force: true });
          const result = runtime === "go"
            ? await runGo(w, transport, server, tmp)
            : w.zats
            ? await runZatsPaired(runtime, client, w, transport, server, tmp)
            : runtime === "browser"
            ? await runBrowserWorkload(client, w, server)
            : runCli(runtime, client, w, transport, server, out);
          const seen = await clientsSince(server.monitor, cid);

          const groups = result.error
            ? {}
            : (w.name === "pubsub"
              ? { pub: result.pub, sub: result.sub }
              : { [w.name]: result });
          const first = Object.values(groups)[0];
          const record = {
            schema: 1,
            runId,
            timestamp: new Date().toISOString(),
            host,
            server: { version: server.version },
            // the zats reference has no nats.js client
            client: {
              version: client === null ? null : versionOf(client),
              seen,
            },
            runtime: {
              name: runtime,
              // the go baseline's version is nats.go's, as the server saw it
              version: runtime === "zats"
                ? zatsVersion
                : runtime === "go"
                ? (Object.keys(seen).find((k) => k.startsWith("go "))?.slice(
                  3,
                ) ?? null)
                : first?.metadata?.runtimeVersion ?? null,
            },
            transport,
            workload: { name: w.name, msgs: w.msgs, size: w.size, iterations },
            summary: Object.fromEntries(
              Object.entries(groups).map(([k, g]) => [k, summarize(g)]),
            ),
            error: result.error ?? null,
            result: result.error ? null : result,
          };
          const label = record.client.version ?? "ref";
          const file =
            `${label}_${runtime}_${transport}_${w.name}_${w.size}B.json`;
          writeFileSync(join(outDir, file), JSON.stringify(record, null, 2));

          const line = result.error
            ? `ERROR ${
              String(result.error).split("\n").find((l) => l.trim()) ?? ""
            }`
            : Object.entries(record.summary).map(([k, s]) =>
              `${k} ${s.median.toLocaleString()}`
            ).join("  ");
          console.log(
            `${label.padEnd(6)} ${runtime.padEnd(7)} ${transport.padEnd(3)} ${
              w.name.padEnd(8)
            } ${`${w.size}B`.padStart(4)} ${line}  [${Object.keys(seen).join(", ")}]`,
          );
        }
      }
    }
  } finally {
    server.proc.kill();
    rmSync(tmp, { recursive: true, force: true });
    if (emptyPayload) {
      rmSync(join(emptyPayload, ".."), { recursive: true, force: true });
    }
    await new Promise((r) => setTimeout(r, 300));
    rmSync(server.dir, { recursive: true, force: true });
  }
  console.log(`saved to ${outDir}`);
}

await main();
