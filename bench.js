#!/usr/bin/env node

// unified nats bench cli - works with node, deno, bun
// usage:
//   node bench.js pub test --msgs 100000
//   deno run --allow-net bench.js pub test --msgs 100000
//   bun bench.js pub test --msgs 100000

import { cli } from "@aricart/cobra";

// detect runtime
function detectRuntime() {
  if (typeof Deno !== "undefined") return "deno";
  if (typeof Bun !== "undefined") return "bun";
  if (typeof process !== "undefined" && process.versions?.node) return "node";
  return "unknown";
}

// get runtime version
function getRuntimeVersion() {
  const runtime = detectRuntime();
  if (runtime === "deno") return Deno.version.deno;
  if (runtime === "bun") return Bun.version;
  if (runtime === "node") return process.version;
  return "unknown";
}

// import connection - works for all runtimes via package.json/deno.json
async function createConnection(options) {
  const { ws, transportNode, progress } = options;
  const servers = options.server || "localhost:4222";
  const runtime = detectRuntime();

  // connection options with no reconnect
  const opts = {
    servers,
    maxReconnectAttempts: 0,
  };

  let nc;
  if (ws) {
    // use websocket transport from nats-core
    const core = await import("@nats-io/nats-core");
    const { wsconnect } = core;
    const url = servers.startsWith("ws://") ? servers : `ws://${servers}`;
    nc = await wsconnect({ ...opts, servers: url });
  } else {
    // use runtime-specific transport
    if (transportNode || runtime === "node" || runtime === "bun") {
      // use node transport (default for node/bun, optional for deno)
      const mod = await import("@nats-io/transport-node");
      const { connect } = mod;
      nc = await connect(opts);
    } else {
      // use deno transport (default for deno)
      const mod = await import("@nats-io/transport-deno");
      const { connect } = mod;
      nc = await connect(opts);
    }
  }

  // listen for connection closed
  nc.closed().then((err) => {
    const msg = err ? `Connection closed with error: ${err.message}` : "Connection closed cleanly";
    if (progress) {
      progress.log(msg).catch(() => console.error(`\n${msg}`));
    } else {
      console.error(`\n${msg}`);
    }
  });

  // add status listeners for errors and disconnects
  const statusListener = (async () => {
    try {
      for await (const s of nc.status()) {
        const msg = `Connection status: ${s.type}${s.data ? ` - ${s.data}` : ""}`;
        if (progress) {
          await progress.log(msg);
        } else {
          console.error(`\n${msg}`);
        }
      }
    } catch (err) {
      const msg = `Status listener error: ${err.message}\n${err.stack}`;
      if (progress) {
        await progress.log(msg).catch(() => console.error(`\n${msg}`));
      } else {
        console.error(`\n${msg}`);
      }
    }
  })();

  // attach the status listener promise to the connection for cleanup
  nc._statusListener = statusListener;

  return nc;
}

// import bench functions
async function importBenchFunctions() {
  // use relative import that works for all runtimes
  const mod = await import("./workloads/mod.js");
  return mod;
}

// distribute messages across clients as evenly as possible
function distributeMsgs(total, clients) {
  const distribution = [];
  const base = Math.floor(total / clients);
  const remainder = total % clients;

  for (let i = 0; i < clients; i++) {
    distribution.push(base + (i < remainder ? 1 : 0));
  }

  return distribution;
}

// run benchmark with workers
// one set of progress bars on the terminal; pubsub shares one between its
// pub and sub workers, so the bars don't redraw over each other
async function createMultibar() {
  const cliProgress = await import("cli-progress");
  return new cliProgress.default.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: " {label} [{bar}] {percentage}% | {value}/{total} | {duration_formatted}",
  }, cliProgress.default.Presets.shades_classic);
}

// one bar per worker. cli-progress doesn't redraw well when bars are added to
// a running multibar, so callers sharing one create all the bars up front
function createBars(multibar, command, numClients, numMsgs) {
  return distributeMsgs(numMsgs, numClients).map((n, i) =>
    multibar.create(n, 0, { label: `${command} ${i + 1}`.padEnd(6) })
  );
}

// builds job description and distributes to workers. with shared bars the
// caller stops their multibar, otherwise the bars are made and stopped here
async function runWithWorkers(command, jobConfig, jobOpts, sharedBars = null) {
  const runtime = detectRuntime();
  const { Benchmark, BenchSample } = await importBenchFunctions();
  const benchmark = new Benchmark("NATS", command.toUpperCase(), jobOpts.numClients);

  // distribute messages across workers
  const msgsPerClient = distributeMsgs(jobOpts.numMsgs, jobOpts.numClients);

  const multibar = sharedBars ? null : await createMultibar();
  const progressBars = sharedBars ??
    createBars(multibar, command, jobOpts.numClients, jobOpts.numMsgs);

  // create workers and distribute job
  const workers = [];
  const workerPromises = [];

  for (let i = 0; i < jobOpts.numClients; i++) {
    let worker;
    let promise;

    // build worker-specific job config
    const workerJobConfig = {
      ...jobConfig,
      msgsPerClient: msgsPerClient[i],
    };

    if (runtime === "deno") {
      // deno uses web workers
      worker = new Worker(new URL("./workloads/worker.js", import.meta.url).href, {
        type: "module",
      });

      promise = new Promise((resolve, reject) => {
        worker.onmessage = (e) => {
          const data = e.data;

          if (data.type === 'progress') {
            // update progress bar
            progressBars[data.workerId].update(data.value);
          } else if (data.type === 'result') {
            if (data.success) {
              const s = data.sample;
              const sample = new BenchSample();
              sample.jobMsgCnt = s.jobMsgCnt;
              sample.msgCnt = s.msgCnt;
              sample.msgBytes = s.msgBytes;
              sample.ioBytes = s.ioBytes;
              sample.start = s.start;
              sample.end = s.end;
              resolve(sample);
            } else {
              reject(new Error(data.error));
            }
            worker.terminate();
          }
        };

        worker.onerror = (err) => {
          reject(err);
          worker.terminate();
        };

        worker.postMessage({ command, config: workerJobConfig, workerId: i });
      });
    } else {
      // node and bun use worker_threads
      const { Worker } = await import("node:worker_threads");
      worker = new Worker(new URL("./workloads/worker.js", import.meta.url).pathname, {
        type: "module",
      });

      promise = new Promise((resolve, reject) => {
        worker.on("message", (data) => {
          if (data.type === 'progress') {
            // update progress bar
            progressBars[data.workerId].update(data.value);
          } else if (data.type === 'result') {
            if (data.success) {
              const s = data.sample;
              const sample = new BenchSample();
              sample.jobMsgCnt = s.jobMsgCnt;
              sample.msgCnt = s.msgCnt;
              sample.msgBytes = s.msgBytes;
              sample.ioBytes = s.ioBytes;
              sample.start = s.start;
              sample.end = s.end;
              resolve(sample);
            } else {
              reject(new Error(data.error));
            }
            worker.terminate();
          }
        });

        worker.on("error", (err) => {
          reject(err);
          worker.terminate();
        });

        worker.postMessage({ command, config: workerJobConfig, workerId: i });
      });
    }

    workers.push(worker);
    workerPromises.push(promise);
  }

  // wait for all workers to return
  const samples = await Promise.all(workerPromises);

  if (multibar) multibar.stop();

  // aggregate results
  samples.forEach((s) => benchmark.addSample(s));

  return benchmark;
}

// create root command
const root = cli({
  use: "bench",
  short: "NATS benchmarking tool (JavaScript)",
  long: `Benchmark utility for NATS messaging

Available runtimes:
  node    - Run with Node.js
  deno    - Run with Deno (deno run --allow-net bench.js)
  bun     - Run with Bun

Examples:
  bench pub test --msgs 1000000
  bench sub test --workers 5
  bench pub test --ws --json results.json`,
});

// add global flags
root.addFlag({
  short: "s",
  name: "server",
  type: "string",
  usage: "NATS server URL",
  default: "localhost:4222",
  persistent: true,
});

root.addFlag({
  name: "ws",
  type: "boolean",
  usage: "Use WebSocket transport",
  default: false,
  persistent: true,
});

root.addFlag({
  name: "transport-node",
  type: "boolean",
  usage: "Use the Node.js transport under Deno (Node.js and Bun always use it)",
  default: false,
  persistent: true,
});

root.addFlag({
  name: "csv",
  type: "string",
  usage: "Export results to CSV file (optional)",
  default: "",
  persistent: true,
});

root.addFlag({
  name: "json",
  type: "string",
  usage: "Export results to JSON file (optional)",
  default: "",
  persistent: true,
});

// add pub subcommand
const pub = root.addCommand({
  use: "pub <subject>",
  short: "Publish Core NATS messages",
  long: `Publish messages to a NATS subject for benchmarking.

Examples:
  bench pub test --msgs 1000000
  bench pub benchmark --size 256 --multisubject
  bench pub test --ws --json results.json`,
  run: async (cmd, args, flags) => {
    if (args.length === 0) {
      cmd.stderr("Error: subject is required\n");
      cmd.stderr("Usage: bench pub <subject> [flags]\n");
      return 1;
    }

    const subject = args[0];
    const runtime = detectRuntime();
    const runtimeVersion = getRuntimeVersion();

    const server = flags.value("server");
    const workers = flags.value("workers");
    const msgs = flags.value("msgs");
    // check changed for size since 0 is a valid value
    const sizeFlag = flags.getFlag("size");
    const size = sizeFlag?.changed ? flags.value("size") : 128;
    const multisubject = flags.value("multisubject");
    const ws = flags.value("ws");
    const transportNode = flags.value("transport-node");
    const csvFile = flags.value("csv");
    const jsonFile = flags.value("json");
    // check changed for payload since empty string could be intentional
    const payloadFlag = flags.getFlag("payload");
    const payload = payloadFlag?.changed ? flags.value("payload") : null;
    const warmup = flags.value("warmup");
    const iterations = flags.value("iterations");

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: pub ${subject}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    if (warmup) cmd.stdout(`Warmup: enabled\n`);
    if (iterations > 1) cmd.stdout(`Iterations: ${iterations}\n`);
    cmd.stdout("\n");

    try {
      // build job description
      const jobConfig = {
        server,
        subject,
        msgSize: size,
        multiSubject: multisubject,
        payload: payload !== null ? new TextEncoder().encode(payload) : null,
        ws,
        transportNode,
        warmup,
      };

      const jobOpts = {
        numClients: workers,
        numMsgs: msgs,
      };

      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      const metadata = {
        runtime, runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "pub", subject,
        options: { server, workers, msgs, size, multisubject, iterations },
      };

      await runIterated({
        command: "pub", jobConfig, jobOpts, metadata, iterations,
        benchType: "PUB", cmd, jsonFile, csvFile,
      });
      return 0;
    } catch (err) {
      cmd.stderr(`\nError: ${err.message}\n`);
      if (err.stack) {
        cmd.stderr(err.stack + "\n");
      }
      return 1;
    }
  },
});

// add pub flags
pub.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers",
  default: 1,
});

pub.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages to publish",
  default: 100000,
});

pub.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the test messages",
  default: 128,
});

pub.addFlag({
  name: "multisubject",
  type: "boolean",
  usage: "Multi-subject mode, publish to subject.<seq>",
  default: false,
});

pub.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom message payload",
  default: "",
});

pub.addFlag({
  name: "warmup",
  type: "boolean",
  usage: "Run warmup before benchmark",
  default: false,
});

pub.addFlag({
  name: "iterations",
  type: "number",
  usage: "Number of bench iterations; reports min/median/mean/max/stddev",
  default: 1,
});

// add sub subcommand
const sub = root.addCommand({
  use: "sub <subject>",
  short: "Subscribe to Core NATS messages",
  long: `Subscribe to messages from a NATS subject for benchmarking.

Examples:
  bench sub test --msgs 100000
  bench sub events --workers 3
  bench sub test --ws --csv results.csv`,
  run: async (cmd, args, flags) => {
    if (args.length === 0) {
      cmd.stderr("Error: subject is required\n");
      cmd.stderr("Usage: bench sub <subject> [flags]\n");
      return 1;
    }

    const subject = args[0];
    const runtime = detectRuntime();
    const runtimeVersion = getRuntimeVersion();

    const server = flags.value("server");
    const workers = flags.value("workers");
    const msgs = flags.value("msgs");
    // check changed for size since 0 is a valid value
    const size = flags.getFlag("size")?.changed ? flags.value("size") : 128;
    const ws = flags.value("ws");
    const transportNode = flags.value("transport-node");
    const csvFile = flags.value("csv");
    const jsonFile = flags.value("json");
    const iterations = flags.value("iterations");

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: sub ${subject}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    if (iterations > 1) cmd.stdout(`Iterations: ${iterations}\n`);
    cmd.stdout("\n");

    try {
      // build job description
      const jobConfig = {
        server,
        subject,
        msgSize: size,
        multiSubject: false,
        payload: null,
        ws,
        transportNode,
      };

      const jobOpts = {
        numClients: workers,
        numMsgs: msgs,
      };

      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      const metadata = {
        runtime, runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "sub", subject,
        options: { server, workers, msgs, size, multisubject: false, iterations },
      };

      await runIterated({
        command: "sub", jobConfig, jobOpts, metadata, iterations,
        benchType: "SUB", cmd, jsonFile, csvFile,
      });
      return 0;
    } catch (err) {
      cmd.stderr(`\nError: ${err.message}\n`);
      if (err.stack) {
        cmd.stderr(err.stack + "\n");
      }
      return 1;
    }
  },
});

// add sub flags
sub.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers",
  default: 1,
});

sub.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages to subscribe to",
  default: 100000,
});

sub.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the test messages",
  default: 128,
});

sub.addFlag({
  name: "iterations",
  type: "number",
  usage: "Number of bench iterations; reports min/median/mean/max/stddev",
  default: 1,
});

// add pubsub subcommand
const pubsub = root.addCommand({
  use: "pubsub <subject>",
  short: "Publish and Subscribe simultaneously",
  long: `Run publishers and subscribers simultaneously for benchmarking.

Examples:
  bench pubsub test --msgs 100000 --pubworkers 2 --subworkers 2
  bench pubsub test --msgs 1000000 --pubworkers 3 --subworkers 3 --multisubject`,
  run: async (cmd, args, flags) => {
    if (args.length === 0) {
      cmd.stderr("Error: subject is required\n");
      cmd.stderr("Usage: bench pubsub <subject> [flags]\n");
      return 1;
    }

    const subject = args[0];
    const runtime = detectRuntime();
    const runtimeVersion = getRuntimeVersion();

    const server = flags.value("server");
    const pubworkers = flags.value("pubworkers");
    const subworkers = flags.value("subworkers");
    const msgs = flags.value("msgs");
    const sizeFlag = flags.getFlag("size");
    const size = sizeFlag?.changed ? flags.value("size") : 128;
    const multisubject = flags.value("multisubject");
    const ws = flags.value("ws");
    const transportNode = flags.value("transport-node");
    const csvFile = flags.value("csv");
    const jsonFile = flags.value("json");
    const payloadFlag = flags.getFlag("payload");
    const payload = payloadFlag?.changed ? flags.value("payload") : null;
    const iterations = flags.value("iterations");

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: pubsub ${subject}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Pub Workers: ${pubworkers}\n`);
    cmd.stdout(`Sub Workers: ${subworkers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    if (iterations > 1) cmd.stdout(`Iterations: ${iterations}\n`);
    cmd.stdout("\n");

    try {
      // build job descriptions for pub and sub
      const pubJobConfig = {
        server,
        subject,
        msgSize: size,
        multiSubject: multisubject,
        payload: payload !== null ? new TextEncoder().encode(payload) : null,
        ws,
        transportNode,
      };

      const subJobConfig = {
        server,
        subject: multisubject ? `${subject}.*` : subject,
        msgSize: size,
        multiSubject: false,
        payload: null,
        ws,
        transportNode,
      };

      const pubJobOpts = { numClients: pubworkers, numMsgs: msgs };
      const subJobOpts = { numClients: subworkers, numMsgs: msgs };

      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      const pubMeta = {
        runtime, runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "pub", subject,
        options: { server, workers: pubworkers, msgs, size, multisubject, iterations },
      };
      const subMeta = {
        runtime, runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "sub", subject,
        options: { server, workers: subworkers, msgs, size, multisubject: false, iterations },
      };

      const { IterationGroup } = await importBenchFunctions();
      const pubGroup = new IterationGroup("NATS", "PUB");
      const subGroup = new IterationGroup("NATS", "SUB");
      pubGroup.setMetadata(pubMeta);
      subGroup.setMetadata(subMeta);

      for (let i = 0; i < iterations; i++) {
        if (iterations > 1) cmd.stdout(`\n>>> iteration ${i + 1}/${iterations}\n`);
        // all the bars exist before any worker starts
        const multibar = await createMultibar();
        const subBars = createBars(multibar, "sub", subJobOpts.numClients, subJobOpts.numMsgs);
        const pubBars = createBars(multibar, "pub", pubJobOpts.numClients, pubJobOpts.numMsgs);
        const subPromise = runWithWorkers("sub", subJobConfig, subJobOpts, subBars);
        await new Promise(resolve => setTimeout(resolve, 1000));
        const pubPromise = runWithWorkers("pub", pubJobConfig, pubJobOpts, pubBars);
        const [subBm, pubBm] = await Promise.all([subPromise, pubPromise])
          .finally(() => multibar.stop());
        pubBm.setMetadata(pubMeta);
        subBm.setMetadata(subMeta);
        pubGroup.add(pubBm);
        subGroup.add(subBm);
        if (iterations > 1) {
          cmd.stdout(`    pub rate: ${pubBm.sampleGroup.rate().toLocaleString()} msgs/sec\n`);
          cmd.stdout(`    sub rate: ${subBm.sampleGroup.rate().toLocaleString()} msgs/sec\n`);
        }
      }

      // output results
      cmd.stdout("\n=== Publisher Results ===\n");
      cmd.stdout(iterations > 1 ? pubGroup.report() : pubGroup.iterations[0].report());
      cmd.stdout("\n");
      cmd.stdout("\n=== Subscriber Results ===\n");
      cmd.stdout(iterations > 1 ? subGroup.report() : subGroup.iterations[0].report());
      cmd.stdout("\n");

      if (jsonFile) {
        const combined = {
          pub: iterations > 1 ? pubGroup.toJSON() : pubGroup.iterations[0].toJSON(),
          sub: iterations > 1 ? subGroup.toJSON() : subGroup.iterations[0].toJSON(),
        };
        await writeOutputFile(jsonFile, JSON.stringify(combined, null, 2));
        cmd.stdout(`JSON results written to ${jsonFile}\n`);
      }

      if (csvFile) {
        const pubCsv = iterations > 1 ? pubGroup.csv() : pubGroup.iterations[0].csv();
        const subCsv = iterations > 1 ? subGroup.csv() : subGroup.iterations[0].csv();
        const combined = `# Publisher Results\n${pubCsv}\n\n# Subscriber Results\n${subCsv}`;
        await writeOutputFile(csvFile, combined);
        cmd.stdout(`CSV results written to ${csvFile}\n`);
      }

      return 0;
    } catch (err) {
      cmd.stderr(`\nError: ${err.message}\n`);
      if (err.stack) {
        cmd.stderr(err.stack + "\n");
      }
      return 1;
    }
  },
});

// add pubsub flags
pubsub.addFlag({
  name: "pubworkers",
  type: "number",
  usage: "Number of publisher workers",
  default: 1,
});

pubsub.addFlag({
  name: "subworkers",
  type: "number",
  usage: "Number of subscriber workers",
  default: 1,
});

pubsub.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages to publish/subscribe",
  default: 100000,
});

pubsub.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the test messages",
  default: 128,
});

pubsub.addFlag({
  name: "multisubject",
  type: "boolean",
  usage: "Multi-subject mode, publish to subject.<seq>",
  default: false,
});

pubsub.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom message payload",
  default: "",
});

pubsub.addFlag({
  name: "iterations",
  type: "number",
  usage: "Number of bench iterations; reports min/median/mean/max/stddev",
  default: 1,
});

// add kv command with put and get subcommands
const kv = root.addCommand({
  use: "kv",
  short: "NATS KV bucket operations",
  long: "Benchmark NATS KV bucket operations (put and get)",
});

// add common kv flags at parent level
kv.addFlag({
  name: "bucket",
  type: "string",
  usage: "KV bucket name",
  default: "benchbucket",
  persistent: true,
});

kv.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers",
  default: 1,
  persistent: true,
});

kv.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of key-value operations",
  default: 100000,
  persistent: true,
});

kv.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the values for put",
  default: 128,
  persistent: true,
});

kv.addFlag({
  name: "keyprefix",
  type: "string",
  usage: "Key prefix",
  default: "",
  persistent: true,
});

kv.addFlag({
  name: "sleep",
  type: "number",
  usage: "Sleep between operations in ms",
  default: 0,
  persistent: true,
});

kv.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom value payload for put",
  default: "",
  persistent: true,
});

kv.addFlag({
  name: "randomize",
  type: "number",
  usage: "Get random keys from 0 to n-1; 0 gets them in order",
  default: 0,
  persistent: true,
});

kv.addFlag({
  name: "iterations",
  type: "number",
  usage: "Number of bench iterations; reports min/median/mean/max/stddev",
  default: 1,
  persistent: true,
});

// kv put subcommand
const kvput = kv.addCommand({
  use: "put",
  short: "Put values to NATS KV bucket",
  long: `Benchmark putting key-value pairs to a NATS KV bucket.

Examples:
  bench kv put --bucket mybucket --msgs 10000
  bench kv put --bucket mybucket --workers 3 --msgs 100000 --size 256`,
  run: async (cmd, args, flags) => {
    const bucket = flags.value("bucket");
    if (!bucket) {
      cmd.stderr("Error: --bucket flag is required\n");
      cmd.stderr("Usage: bench kv put --bucket <name> [flags]\n");
      return 1;
    }
    const runtime = detectRuntime();
    const runtimeVersion = getRuntimeVersion();

    const server = flags.value("server");
    const workers = flags.value("workers");
    const msgs = flags.value("msgs");
    const sizeFlag = flags.getFlag("size");
    const size = sizeFlag?.changed ? flags.value("size") : 128;
    const keyprefix = flags.value("keyprefix");
    const sleep = flags.value("sleep");
    const ws = flags.value("ws");
    const transportNode = flags.value("transport-node");
    const csvFile = flags.value("csv");
    const jsonFile = flags.value("json");
    const payloadFlag = flags.getFlag("payload");
    const payload = payloadFlag?.changed ? flags.value("payload") : null;
    const iterations = flags.value("iterations");

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: kv put ${bucket}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (keyprefix) cmd.stdout(`Key Prefix: ${keyprefix}\n`);
    if (sleep > 0) cmd.stdout(`Sleep: ${sleep}ms\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    if (iterations > 1) cmd.stdout(`Iterations: ${iterations}\n`);
    cmd.stdout("\n");

    try {
      const jobConfig = {
        server, bucket, msgSize: size,
        payload: payload !== null ? new TextEncoder().encode(payload) : null,
        keyprefix, sleep, ws, transportNode,
      };
      const jobOpts = { numClients: workers, numMsgs: msgs };
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      const metadata = {
        runtime, runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "kv put", bucket,
        options: { server, workers, msgs, size, keyprefix, sleep, iterations },
      };

      await runIterated({
        command: "kvput", jobConfig, jobOpts, metadata, iterations,
        benchType: "KV_PUT", cmd, jsonFile, csvFile,
      });
      return 0;
    } catch (err) {
      cmd.stderr(`\nError: ${err.message}\n`);
      if (err.stack) {
        cmd.stderr(err.stack + "\n");
      }
      return 1;
    }
  },
});

// kv get subcommand
const kvget = kv.addCommand({
  use: "get",
  short: "Get values from NATS KV bucket",
  long: `Benchmark getting key-value pairs from a NATS KV bucket.

Examples:
  bench kv get --bucket mybucket --msgs 10000
  bench kv get --bucket mybucket --workers 3 --msgs 100000 --randomize 10000`,
  run: async (cmd, args, flags) => {
    const bucket = flags.value("bucket");
    const runtime = detectRuntime();
    const runtimeVersion = getRuntimeVersion();

    const server = flags.value("server");
    const workers = flags.value("workers");
    const msgs = flags.value("msgs");
    const keyprefix = flags.value("keyprefix");
    const randomize = flags.value("randomize");
    const sleep = flags.value("sleep");
    const ws = flags.value("ws");
    const transportNode = flags.value("transport-node");
    const csvFile = flags.value("csv");
    const jsonFile = flags.value("json");
    const iterations = flags.value("iterations");

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: kv get ${bucket}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    if (keyprefix) cmd.stdout(`Key Prefix: ${keyprefix}\n`);
    if (randomize > 0) cmd.stdout(`Randomize: 0-${randomize - 1}\n`);
    if (sleep > 0) cmd.stdout(`Sleep: ${sleep}ms\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    if (iterations > 1) cmd.stdout(`Iterations: ${iterations}\n`);
    cmd.stdout("\n");

    try {
      const jobConfig = { server, bucket, keyprefix, randomize, sleep, ws, transportNode };
      const jobOpts = { numClients: workers, numMsgs: msgs };
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      const metadata = {
        runtime, runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "kv get", bucket,
        options: { server, workers, msgs, keyprefix, randomize, sleep, iterations },
      };

      await runIterated({
        command: "kvget", jobConfig, jobOpts, metadata, iterations,
        benchType: "KV_GET", cmd, jsonFile, csvFile,
      });
      return 0;
    } catch (err) {
      cmd.stderr(`\nError: ${err.message}\n`);
      if (err.stack) {
        cmd.stderr(err.stack + "\n");
      }
      return 1;
    }
  },
});

// add js command with pub/ordered/consume/fetch subcommands
const js = root.addCommand({
  use: "js",
  short: "JetStream benchmark operations",
  long: "Benchmark JetStream operations (pub, ordered, consume, fetch)",
});

// add common js flags at parent level
js.addFlag({
  name: "stream",
  type: "string",
  usage: "Stream name",
  default: "benchstream",
  persistent: true,
});

js.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers",
  default: 1,
  persistent: true,
});

js.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages",
  default: 100000,
  persistent: true,
});

js.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the messages",
  default: 128,
  persistent: true,
});

js.addFlag({
  name: "sleep",
  type: "number",
  usage: "Sleep between operations in ms",
  default: 0,
  persistent: true,
});

js.addFlag({
  name: "purge",
  type: "boolean",
  usage: "Purge the stream before running",
  default: false,
  persistent: true,
});

js.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom message payload",
  default: "",
  persistent: true,
});

js.addFlag({
  name: "iterations",
  type: "number",
  usage: "Number of bench iterations; reports min/median/mean/max/stddev",
  default: 1,
  persistent: true,
});

// js pub subcommand
const jspub = js.addCommand({
  use: "pub <subject>",
  short: "Publish JetStream messages",
  long: `Benchmark publishing messages to JetStream.

Examples:
  bench js pub bench --msgs 100000
  bench js pub bench --workers 3 --batch 1000`,
  run: async (cmd, args, flags) => {
    if (args.length === 0) {
      cmd.stderr("Error: subject is required\n");
      cmd.stderr("Usage: bench js pub <subject> [flags]\n");
      return 1;
    }

    const subject = args[0];
    const runtime = detectRuntime();
    const runtimeVersion = getRuntimeVersion();

    const server = flags.value("server");
    const workers = flags.value("workers");
    const msgs = flags.value("msgs");
    const sizeFlag = flags.getFlag("size");
    const size = sizeFlag?.changed ? flags.value("size") : 128;
    const stream = flags.value("stream");
    const sleep = flags.value("sleep");
    const purge = flags.value("purge");
    const multisubject = flags.value("multisubject");
    const multisubjectmax = flags.value("multisubjectmax");
    const batch = flags.value("batch");
    const ws = flags.value("ws");
    const transportNode = flags.value("transport-node");
    const csvFile = flags.value("csv");
    const jsonFile = flags.value("json");
    const payloadFlag = flags.getFlag("payload");
    const payload = payloadFlag?.changed ? flags.value("payload") : null;
    const iterations = flags.value("iterations");

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: js pub ${subject}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Stream: ${stream}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    cmd.stdout(`Batch: ${batch}\n`);
    if (multisubject) cmd.stdout(`Multi-subject: enabled (max: ${multisubjectmax})\n`);
    if (sleep > 0) cmd.stdout(`Sleep: ${sleep}ms\n`);
    if (purge) cmd.stdout(`Purge: enabled\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    if (iterations > 1) cmd.stdout(`Iterations: ${iterations}\n`);
    cmd.stdout("\n");

    try {
      const jobConfig = {
        server, subject, stream, msgSize: size,
        payload: payload !== null ? new TextEncoder().encode(payload) : null,
        multiSubject: multisubject,
        multiSubjectMax: multisubjectmax,
        batch, sleep, purge, ws, transportNode,
      };
      const jobOpts = { numClients: workers, numMsgs: msgs };
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      const metadata = {
        runtime, runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "js pub", subject, stream,
        options: { server, workers, msgs, size, batch, multisubject, multisubjectmax, sleep, purge, iterations },
      };

      await runIterated({
        command: "jspub", jobConfig, jobOpts, metadata, iterations,
        benchType: "JSPUB_BATCH", cmd, jsonFile, csvFile,
      });
      return 0;
    } catch (err) {
      cmd.stderr(`\nError: ${err.message}\n`);
      if (err.stack) {
        cmd.stderr(err.stack + "\n");
      }
      return 1;
    }
  },
});

// add js pub specific flags
jspub.addFlag({
  name: "multisubject",
  type: "boolean",
  usage: "Multi-subject mode",
  default: false,
});

jspub.addFlag({
  name: "multisubjectmax",
  type: "number",
  usage: "Maximum number of subjects in multi-subject mode",
  default: 100000,
});

jspub.addFlag({
  name: "batch",
  type: "number",
  usage: "Batch size for async publish",
  default: 500,
});

// delete a jetstream consumer (used to reset between iterations)
async function deleteConsumer({ server, stream, consumer, ws, transportNode }) {
  const runtime = detectRuntime();
  let nc;
  if (ws) {
    const core = await import("@nats-io/nats-core");
    const url = server.startsWith("ws://") ? server : `ws://${server}`;
    nc = await core.wsconnect({ servers: url, maxReconnectAttempts: 0 });
  } else if (transportNode || runtime === "node" || runtime === "bun") {
    const mod = await import("@nats-io/transport-node");
    nc = await mod.connect({ servers: server, maxReconnectAttempts: 0 });
  } else {
    const mod = await import("@nats-io/transport-deno");
    nc = await mod.connect({ servers: server, maxReconnectAttempts: 0 });
  }
  try {
    const { jetstreamManager } = await import("@nats-io/jetstream");
    const jsm = await jetstreamManager(nc);
    try {
      await jsm.consumers.delete(stream, consumer);
    } catch (_) {
      // consumer may not exist; ignore
    }
  } finally {
    await nc.close();
  }
}

// shared writer for json/csv outputs
async function writeOutputFile(path, content) {
  if (typeof Deno !== "undefined") {
    await Deno.writeTextFile(path, content);
  } else {
    const { writeFile } = await import("fs/promises");
    await writeFile(path, content);
  }
}

// shared iterated runner: loops N times, builds IterationGroup, emits report + output files
async function runIterated({ command, jobConfig, jobOpts, metadata, iterations, benchType, cmd, jsonFile, csvFile, header, beforeEach, afterEach }) {
  const { IterationGroup } = await importBenchFunctions();
  const group = new IterationGroup("NATS", benchType);
  group.setMetadata(metadata);

  for (let i = 0; i < iterations; i++) {
    if (iterations > 1) cmd.stdout(`\n>>> iteration ${i + 1}/${iterations}\n`);
    if (beforeEach) await beforeEach(i);
    const benchmark = await runWithWorkers(command, jobConfig, jobOpts);
    benchmark.setMetadata(metadata);
    group.add(benchmark);
    if (iterations > 1) {
      cmd.stdout(`    rate: ${benchmark.sampleGroup.rate().toLocaleString()} msgs/sec\n`);
    }
    if (afterEach) await afterEach(i);
  }

  if (header) cmd.stdout(header);
  cmd.stdout(iterations > 1 ? group.report() : group.iterations[0].report());
  cmd.stdout("\n");

  if (jsonFile) {
    const out = iterations > 1 ? group.toJSON() : group.iterations[0].toJSON();
    await writeOutputFile(jsonFile, JSON.stringify(out, null, 2));
    cmd.stdout(`JSON results written to ${jsonFile}\n`);
  }
  if (csvFile) {
    const out = iterations > 1 ? group.csv() : group.iterations[0].csv();
    await writeOutputFile(csvFile, out);
    cmd.stdout(`CSV results written to ${csvFile}\n`);
  }
  return group;
}

// shared runner for js consumer subcommands
async function runJSConsCommand(cmd, args, flags, mode) {
  if (args.length === 0) {
    cmd.stderr(`Error: subject is required\n`);
    cmd.stderr(`Usage: bench js ${mode} <subject> [flags]\n`);
    return 1;
  }

  const subject = args[0];
  const runtime = detectRuntime();
  const runtimeVersion = getRuntimeVersion();

  const server = flags.value("server");
  const workers = flags.value("workers");
  const msgs = flags.value("msgs");
  const stream = flags.value("stream");
  const ws = flags.value("ws");
  const transportNode = flags.value("transport-node");
  const csvFile = flags.value("csv");
  const jsonFile = flags.value("json");
  const iterations = flags.value("iterations");

  const callback = mode !== "fetch" ? flags.value("callback") : false;
  const consumer = mode !== "ordered" ? flags.value("consumer") : null;
  const defaultBatch = mode === "fetch" ? 100 : 500;
  const batchFlag = flags.getFlag("batch");
  const batch = batchFlag?.changed ? flags.value("batch") : defaultBatch;

  cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
  cmd.stdout(`Command: js ${mode} ${subject}\n`);
  cmd.stdout(`Server: ${server}\n`);
  cmd.stdout(`Stream: ${stream}\n`);
  if (consumer) cmd.stdout(`Consumer: ${consumer}\n`);
  cmd.stdout(`Workers: ${workers}\n`);
  cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
  if (mode !== "ordered") cmd.stdout(`Batch: ${batch}\n`);
  if (mode !== "fetch") cmd.stdout(`Callback: ${callback}\n`);
  if (ws) cmd.stdout(`Transport: WebSocket\n`);
  if (iterations > 1) cmd.stdout(`Iterations: ${iterations}\n`);
  cmd.stdout("\n");

  try {
    const jobConfig = { server, subject, stream, consumer, callback, batch, ws, transportNode };
    const jobOpts = { numClients: workers, numMsgs: msgs };
    const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
    const metadata = {
      runtime, runtimeVersion,
      transport: ws ? "websocket" : "tcp",
      transportLib,
      command: `js ${mode}`, subject, stream,
      options: { server, workers, msgs, batch, consumer, callback, iterations },
    };
    const benchTypeMap = {
      ordered: "JSCONS_ORDERED",
      consume: "JSCONS_CONSUME",
      fetch: "JSCONS_FETCH",
    };

    // for durable consumer modes, delete consumer between iters so each iter
    // starts from beginning of stream (otherwise iter 2+ wait forever on empty queue)
    let beforeEach = null;
    if (mode !== "ordered" && iterations > 1) {
      beforeEach = async (i) => {
        if (i === 0) return; // first iter: workload creates fresh
        await deleteConsumer({ server, stream, consumer, ws, transportNode });
      };
    }

    await runIterated({
      command: `jscons-${mode}`, jobConfig, jobOpts, metadata, iterations,
      benchType: benchTypeMap[mode] || "JSCONS_CONSUME",
      cmd, jsonFile, csvFile, beforeEach,
    });
    return 0;
  } catch (err) {
    cmd.stderr(`\nError: ${err.message}\n`);
    if (err.stack) cmd.stderr(err.stack + "\n");
    return 1;
  }
}

// js ordered subcommand (ephemeral ordered consumer)
const jsordered = js.addCommand({
  use: "ordered <subject>",
  short: "Consume JetStream messages with an ephemeral ordered consumer",
  long: `Benchmark consumption with an ephemeral ordered consumer.

Examples:
  bench js ordered bench --msgs 100000`,
  run: (cmd, args, flags) => runJSConsCommand(cmd, args, flags, "ordered"),
});
jsordered.addFlag({
  name: "callback",
  type: "boolean",
  usage: "Use callback API instead of iterator",
  default: false,
});

// js consume subcommand (durable pull consumer, callback or iter)
const jsconsume = js.addCommand({
  use: "consume <subject>",
  short: "Consume JetStream messages from a durable pull consumer",
  long: `Benchmark consumption from a durable pull consumer.

Examples:
  bench js consume bench --msgs 100000
  bench js consume bench --msgs 100000 --consumer my-cons --callback`,
  run: (cmd, args, flags) => runJSConsCommand(cmd, args, flags, "consume"),
});
jsconsume.addFlag({
  name: "consumer",
  type: "string",
  usage: "Durable consumer name",
  default: "nats-bench",
});
jsconsume.addFlag({
  name: "batch",
  type: "number",
  usage: "Max messages per consume batch",
  default: 500,
});
jsconsume.addFlag({
  name: "callback",
  type: "boolean",
  usage: "Use callback API instead of iterator",
  default: false,
});

// js fetch subcommand (durable pull consumer, batched fetch)
const jsfetch = js.addCommand({
  use: "fetch <subject>",
  short: "Consume JetStream messages from a durable pull consumer using fetch",
  long: `Benchmark consumption with batched fetch on a durable pull consumer.

Examples:
  bench js fetch bench --msgs 100000 --batch 100`,
  run: (cmd, args, flags) => runJSConsCommand(cmd, args, flags, "fetch"),
});
jsfetch.addFlag({
  name: "consumer",
  type: "string",
  usage: "Durable consumer name",
  default: "nats-bench",
});
jsfetch.addFlag({
  name: "batch",
  type: "number",
  usage: "Messages per fetch",
  default: 100,
});

// execute
try {
  const exitCode = await root.execute();
  const exit = typeof Deno !== "undefined" ? Deno.exit : process.exit;
  exit(exitCode);
} catch (err) {
  console.error("Error executing command:");
  console.error(err);
  console.error(err.stack);
  process.exit(1);
}
