#!/usr/bin/env node
// runs one bench job in headless chrome over websockets, using the same
// workload code as the cli (browser/entry.js -> workloads/job.js).
//
//   node bin/browser.js <command> [--client <v>] [--server ws://localhost:8080]
//     [--subject s] [--msgs n] [--size n] [--iterations n] [--json file]
//     [--bucket b] [--stream s]
//
// commands: pub, pubsub, jspub, kvput, kvget. --client picks an installed
// version from .versions (default: the latest, from the root deno.json). the
// result is the same json shape as `bench.js --json`, with runtime "browser".

import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { root, versionsDir } from "./versions.js";

const chromePaths = {
  darwin: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  linux: "google-chrome",
};

export async function runBrowser(command, o) {
  const config = {
    server: o.server ?? "ws://localhost:8080",
    subject: o.subject ?? "bench",
    msgSize: o.size ?? 128,
    multiSubject: false,
    payload: null,
    bucket: o.bucket ?? "benchbucket",
    stream: o.stream ?? "benchstream",
  };
  const job = {
    command,
    iterations: o.iterations ?? 1,
    config,
    options: {
      server: config.server,
      workers: 1,
      msgs: o.msgs ?? 100000,
      size: config.msgSize,
    },
  };

  // bundle the entry against the chosen client version
  const tmp = mkdtempSync(join(tmpdir(), "nats-bench-browser-"));
  const bundle = join(tmp, "bundle.js");
  const map = o.client && o.client !== "latest"
    ? ["--import-map", join(versionsDir, o.client, "import_map.json")]
    : ["--config", join(root, "deno.json")];
  execFileSync(
    "deno",
    [
      "bundle",
      "--quiet",
      "--platform",
      "browser",
      ...map,
      "-o",
      bundle,
      join(root, "browser/entry.js"),
    ],
    { stdio: ["ignore", "ignore", "inherit"] },
  );
  const page =
    `<!doctype html><meta charset="utf-8"><script type="module" src="/bundle.js"></script>`;

  let done;
  const result = new Promise((resolve) => (done = resolve));
  const server = createServer((req, res) => {
    if (req.url === "/") {
      res.writeHead(200, { "content-type": "text/html" }).end(page);
    } else if (req.url === "/bundle.js") {
      res.writeHead(200, { "content-type": "text/javascript" }).end(
        readFileSync(bundle),
      );
    } else if (req.url === "/job") {
      res.writeHead(200, { "content-type": "application/json" }).end(
        JSON.stringify(job),
      );
    } else if (req.url === "/result" && req.method === "POST") {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        res.writeHead(204).end();
        done(JSON.parse(body));
      });
    } else {
      res.writeHead(404).end();
    }
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${server.address().port}/`;

  const profile = join(tmp, "profile");
  const chrome = spawn(o.chrome ?? chromePaths[process.platform], [
    "--headless=new",
    `--user-data-dir=${profile}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    url,
  ], { stdio: "ignore" });
  const exited = new Promise((r) => chrome.once("exit", r));

  const timeout = setTimeout(
    () => done({ error: "timed out waiting for the browser" }),
    o.timeout ?? 300_000,
  );
  try {
    return await result;
  } finally {
    clearTimeout(timeout);
    chrome.kill();
    server.close();
    // chrome writes to its profile until it exits
    await exited;
    rmSync(tmp, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 200,
    });
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      client: { type: "string" },
      server: { type: "string" },
      subject: { type: "string" },
      msgs: { type: "string" },
      size: { type: "string" },
      iterations: { type: "string" },
      bucket: { type: "string" },
      stream: { type: "string" },
      json: { type: "string" },
    },
  });
  const [command] = positionals;
  if (!command) {
    console.error(
      "usage: node bin/browser.js <pub|pubsub|jspub|kvput|kvget> [options]",
    );
    process.exit(1);
  }
  const num = (v) => (v === undefined ? undefined : Number(v));
  const r = await runBrowser(command, {
    ...values,
    msgs: num(values.msgs),
    size: num(values.size),
    iterations: num(values.iterations),
  });
  if (values.json) writeFileSync(values.json, JSON.stringify(r, null, 2));
  if (r.error) {
    console.error(r.error);
    process.exit(1);
  }
  const groups = command === "pubsub"
    ? { pub: r.pub, sub: r.sub }
    : { [command]: r };
  for (const [k, g] of Object.entries(groups)) {
    const rates = g.iterations.map((i) => i.results.throughputMsgsPerSec);
    console.log(
      `${g.metadata.runtimeVersion} ${k}: ${
        rates.map((x) => x.toLocaleString()).join(", ")
      } msgs/sec`,
    );
  }
}
