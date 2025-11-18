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

// import progress factory directly
async function importProgressFactory() {
  const mod = await import("@aricart/nd-progress");
  return mod.progressFactory;
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
// builds job description and distributes to workers
async function runWithWorkers(command, jobConfig, jobOpts) {
  const runtime = detectRuntime();
  const { Benchmark, BenchSample } = await importBenchFunctions();
  const benchmark = new Benchmark("NATS", command.toUpperCase(), jobOpts.numClients);

  // distribute messages across workers
  const msgsPerClient = distributeMsgs(jobOpts.numMsgs, jobOpts.numClients);

  // import cli-progress for multi-bar
  const cliProgress = await import("cli-progress");

  // create multibar for progress tracking
  const multibar = new cliProgress.default.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
  }, cliProgress.default.Presets.shades_classic);

  // create progress bars for each worker
  const progressBars = [];
  for (let i = 0; i < jobOpts.numClients; i++) {
    const bar = multibar.create(msgsPerClient[i], 0);
    progressBars.push(bar);
  }

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

  // stop multibar
  multibar.stop();

  // aggregate results
  samples.forEach((s) => benchmark.addSample(s));

  return benchmark;
}

// create root command
const root = cli({
  use: "bench <command> [subject]",
  short: "NATS benchmarking tool (JavaScript)",
  long: `Benchmark utility for NATS messaging

Available runtimes:
  node    - Run with Node.js
  deno    - Run with Deno (deno run --allow-net bench.js)
  bun     - Run with Bun

Examples:
  bench pub test --msgs 1000000
  bench sub test --clients 5
  bench pub test --ws --json results.json`,
});

// add global flags
root.addFlag({
  short: "s",
  name: "server",
  type: "string",
  usage: "NATS server URL (default: localhost:4222)",
  default: "localhost:4222",
  persistent: true,
});

root.addFlag({
  name: "ws",
  type: "boolean",
  usage: "Use WebSocket transport (default: false)",
  default: false,
  persistent: true,
});

root.addFlag({
  name: "transport-node",
  type: "boolean",
  usage: "Use Node.js transport (default: auto, useful for Deno benchmarks)",
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

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: pub ${subject}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
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
      };

      const jobOpts = {
        numClients: workers,
        numMsgs: msgs,
      };

      // execute job with workers
      const benchmark = await runWithWorkers("pub", jobConfig, jobOpts);

      // set metadata
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      benchmark.setMetadata({
        runtime,
        runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "pub",
        subject,
        options: {
          server,
          workers,
          msgs,
          size,
          multisubject,
        },
      });

      // output results
      cmd.stdout(benchmark.report());
      cmd.stdout("\n");

      // save json if requested
      if (jsonFile) {
        const json = JSON.stringify(benchmark.toJSON(), null, 2);
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(jsonFile, json);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(jsonFile, json);
        }
        cmd.stdout(`JSON results written to ${jsonFile}\n`);
      }

      // save csv if requested
      if (csvFile) {
        const csv = benchmark.csv();
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(csvFile, csv);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(csvFile, csv);
        }
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

// add pub flags
pub.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers (default: 1)",
  default: 1,
});

pub.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages to publish (default: 100000)",
  default: 100000,
});

pub.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the test messages (default: 128)",
  default: 128,
});

pub.addFlag({
  name: "multisubject",
  type: "boolean",
  usage: "Multi-subject mode, publish to subject.<seq> (default: false)",
  default: false,
});

pub.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom message payload (default: empty)",
  default: "",
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

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: sub ${subject}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
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

      // execute job with workers
      const benchmark = await runWithWorkers("sub", jobConfig, jobOpts);

      // set metadata
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      benchmark.setMetadata({
        runtime,
        runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "sub",
        subject,
        options: {
          server,
          workers,
          msgs,
          size,
          multisubject: false,
        },
      });

      // output results
      cmd.stdout(benchmark.report());
      cmd.stdout("\n");

      // save json if requested
      if (jsonFile) {
        const json = JSON.stringify(benchmark.toJSON(), null, 2);
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(jsonFile, json);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(jsonFile, json);
        }
        cmd.stdout(`JSON results written to ${jsonFile}\n`);
      }

      // save csv if requested
      if (csvFile) {
        const csv = benchmark.csv();
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(csvFile, csv);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(csvFile, csv);
        }
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

// add sub flags
sub.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers (default: 1)",
  default: 1,
});

sub.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages to subscribe to (default: 100000)",
  default: 100000,
});

sub.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the test messages (default: 128)",
  default: 128,
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

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: pubsub ${subject}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Pub Workers: ${pubworkers}\n`);
    cmd.stdout(`Sub Workers: ${subworkers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
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

      const pubJobOpts = {
        numClients: pubworkers,
        numMsgs: msgs,
      };

      const subJobOpts = {
        numClients: subworkers,
        numMsgs: msgs,
      };

      // start subscribers first
      const subPromise = runWithWorkers("sub", subJobConfig, subJobOpts);

      // give subscribers time to connect
      await new Promise(resolve => setTimeout(resolve, 1000));

      // start publishers
      const pubPromise = runWithWorkers("pub", pubJobConfig, pubJobOpts);

      // wait for both to complete
      const [subBenchmark, pubBenchmark] = await Promise.all([subPromise, pubPromise]);

      // set metadata for pub
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      pubBenchmark.setMetadata({
        runtime,
        runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "pub",
        subject,
        options: {
          server,
          workers: pubworkers,
          msgs,
          size,
          multisubject,
        },
      });

      // set metadata for sub
      subBenchmark.setMetadata({
        runtime,
        runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "sub",
        subject,
        options: {
          server,
          workers: subworkers,
          msgs,
          size,
          multisubject: false,
        },
      });

      // output results
      cmd.stdout("\n=== Publisher Results ===\n");
      cmd.stdout(pubBenchmark.report());
      cmd.stdout("\n");

      cmd.stdout("\n=== Subscriber Results ===\n");
      cmd.stdout(subBenchmark.report());
      cmd.stdout("\n");

      // save json if requested
      if (jsonFile) {
        const combined = {
          pub: pubBenchmark.toJSON(),
          sub: subBenchmark.toJSON(),
        };
        const json = JSON.stringify(combined, null, 2);
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(jsonFile, json);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(jsonFile, json);
        }
        cmd.stdout(`JSON results written to ${jsonFile}\n`);
      }

      // save csv if requested (both pub and sub)
      if (csvFile) {
        const pubCsv = pubBenchmark.csv();
        const subCsv = subBenchmark.csv();
        const combined = `# Publisher Results\n${pubCsv}\n\n# Subscriber Results\n${subCsv}`;
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(csvFile, combined);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(csvFile, combined);
        }
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
  usage: "Number of publisher workers (default: 1)",
  default: 1,
});

pubsub.addFlag({
  name: "subworkers",
  type: "number",
  usage: "Number of subscriber workers (default: 1)",
  default: 1,
});

pubsub.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages to publish/subscribe (default: 100000)",
  default: 100000,
});

pubsub.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the test messages (default: 128)",
  default: 128,
});

pubsub.addFlag({
  name: "multisubject",
  type: "boolean",
  usage: "Multi-subject mode, publish to subject.<seq> (default: false)",
  default: false,
});

pubsub.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom message payload (default: empty)",
  default: "",
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
  usage: "KV bucket name (default: benchbucket)",
  default: "benchbucket",
  persistent: true,
});

kv.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers (default: 1)",
  default: 1,
  persistent: true,
});

kv.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of key-value operations (default: 100000)",
  default: 100000,
  persistent: true,
});

kv.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the values for put (default: 128)",
  default: 128,
  persistent: true,
});

kv.addFlag({
  name: "keyprefix",
  type: "string",
  usage: "Key prefix (default: empty)",
  default: "",
  persistent: true,
});

kv.addFlag({
  name: "sleep",
  type: "number",
  usage: "Sleep between operations in ms (default: 0)",
  default: 0,
  persistent: true,
});

kv.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom value payload for put (default: empty)",
  default: "",
  persistent: true,
});

kv.addFlag({
  name: "randomize",
  type: "number",
  usage: "Randomize gets from 0 to n-1 for get (default: 0 for sequential)",
  default: 0,
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

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: kv put ${bucket}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    cmd.stdout(`Size: ${size} bytes\n`);
    if (keyprefix) cmd.stdout(`Key Prefix: ${keyprefix}\n`);
    if (sleep > 0) cmd.stdout(`Sleep: ${sleep}ms\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    cmd.stdout("\n");

    try {
      // build job description
      const jobConfig = {
        server,
        bucket,
        msgSize: size,
        payload: payload !== null ? new TextEncoder().encode(payload) : null,
        keyprefix,
        sleep,
        ws,
        transportNode,
      };

      const jobOpts = {
        numClients: workers,
        numMsgs: msgs,
      };

      // execute job with workers
      const benchmark = await runWithWorkers("kvput", jobConfig, jobOpts);

      // set metadata
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      benchmark.setMetadata({
        runtime,
        runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "kv put",
        bucket,
        options: {
          server,
          workers,
          msgs,
          size,
          keyprefix,
          sleep,
        },
      });

      // output results
      cmd.stdout(benchmark.report());
      cmd.stdout("\n");

      // save json if requested
      if (jsonFile) {
        const json = JSON.stringify(benchmark.toJSON(), null, 2);
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(jsonFile, json);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(jsonFile, json);
        }
        cmd.stdout(`JSON results written to ${jsonFile}\n`);
      }

      // save csv if requested
      if (csvFile) {
        const csv = benchmark.csv();
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(csvFile, csv);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(csvFile, csv);
        }
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

    cmd.stdout(`Runtime: ${runtime} ${runtimeVersion}\n`);
    cmd.stdout(`Command: kv get ${bucket}\n`);
    cmd.stdout(`Server: ${server}\n`);
    cmd.stdout(`Workers: ${workers}\n`);
    cmd.stdout(`Messages: ${msgs.toLocaleString()}\n`);
    if (keyprefix) cmd.stdout(`Key Prefix: ${keyprefix}\n`);
    if (randomize > 0) cmd.stdout(`Randomize: 0-${randomize - 1}\n`);
    if (sleep > 0) cmd.stdout(`Sleep: ${sleep}ms\n`);
    if (ws) cmd.stdout(`Transport: WebSocket\n`);
    cmd.stdout("\n");

    try {
      // build job description
      const jobConfig = {
        server,
        bucket,
        keyprefix,
        randomize,
        sleep,
        ws,
        transportNode,
      };

      const jobOpts = {
        numClients: workers,
        numMsgs: msgs,
      };

      // execute job with workers
      const benchmark = await runWithWorkers("kvget", jobConfig, jobOpts);

      // set metadata
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      benchmark.setMetadata({
        runtime,
        runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "kv get",
        bucket,
        options: {
          server,
          workers,
          msgs,
          keyprefix,
          randomize,
          sleep,
        },
      });

      // output results
      cmd.stdout(benchmark.report());
      cmd.stdout("\n");

      // save json if requested
      if (jsonFile) {
        const json = JSON.stringify(benchmark.toJSON(), null, 2);
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(jsonFile, json);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(jsonFile, json);
        }
        cmd.stdout(`JSON results written to ${jsonFile}\n`);
      }

      // save csv if requested
      if (csvFile) {
        const csv = benchmark.csv();
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(csvFile, csv);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(csvFile, csv);
        }
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
  usage: "Stream name (default: benchstream)",
  default: "benchstream",
  persistent: true,
});

js.addFlag({
  name: "workers",
  type: "number",
  usage: "Number of workers (default: 1)",
  default: 1,
  persistent: true,
});

js.addFlag({
  name: "msgs",
  type: "number",
  usage: "Number of messages (default: 100000)",
  default: 100000,
  persistent: true,
});

js.addFlag({
  name: "size",
  type: "number",
  usage: "Size of the messages (default: 128)",
  default: 128,
  persistent: true,
});

js.addFlag({
  name: "sleep",
  type: "number",
  usage: "Sleep between operations in ms (default: 0)",
  default: 0,
  persistent: true,
});

js.addFlag({
  name: "purge",
  type: "boolean",
  usage: "Purge the stream before running (default: false)",
  default: false,
  persistent: true,
});

js.addFlag({
  name: "payload",
  type: "string",
  usage: "Custom message payload (default: empty)",
  default: "",
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
    cmd.stdout("\n");

    try {
      // build job description
      const jobConfig = {
        server,
        subject,
        stream,
        msgSize: size,
        payload: payload !== null ? new TextEncoder().encode(payload) : null,
        multiSubject: multisubject,
        multiSubjectMax: multisubjectmax,
        batch,
        sleep,
        purge,
        ws,
        transportNode,
      };

      const jobOpts = {
        numClients: workers,
        numMsgs: msgs,
      };

      // execute job with workers
      const benchmark = await runWithWorkers("jspub", jobConfig, jobOpts);

      // set metadata
      const transportLib = transportNode ? "node" : (runtime === "deno" ? "deno" : "node");
      benchmark.setMetadata({
        runtime,
        runtimeVersion,
        transport: ws ? "websocket" : "tcp",
        transportLib,
        command: "js pub",
        subject,
        stream,
        options: {
          server,
          workers,
          msgs,
          size,
          batch,
          multisubject,
          multisubjectmax,
          sleep,
          purge,
        },
      });

      // output results
      cmd.stdout(benchmark.report());
      cmd.stdout("\n");

      // save json if requested
      if (jsonFile) {
        const json = JSON.stringify(benchmark.toJSON(), null, 2);
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(jsonFile, json);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(jsonFile, json);
        }
        cmd.stdout(`JSON results written to ${jsonFile}\n`);
      }

      // save csv if requested
      if (csvFile) {
        const csv = benchmark.csv();
        if (typeof Deno !== "undefined") {
          await Deno.writeTextFile(csvFile, csv);
        } else {
          const { writeFile } = await import("fs/promises");
          await writeFile(csvFile, csv);
        }
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

// add js pub specific flags
jspub.addFlag({
  name: "multisubject",
  type: "boolean",
  usage: "Multi-subject mode (default: false)",
  default: false,
});

jspub.addFlag({
  name: "multisubjectmax",
  type: "number",
  usage: "Maximum number of subjects in multi-subject mode (default: 100000)",
  default: 100000,
});

jspub.addFlag({
  name: "batch",
  type: "number",
  usage: "Batch size for async publish (default: 500)",
  default: 500,
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
