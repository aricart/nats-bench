// unified worker for running bench clients across node, deno, and bun
// works in a separate thread per client

// detect runtime
function detectRuntime() {
  if (typeof Deno !== "undefined") return "deno";
  if (typeof Bun !== "undefined") return "bun";
  if (typeof process !== "undefined" && process.versions?.node) return "node";
  return "unknown";
}

const runtime = detectRuntime();

// setup message handler based on runtime
let sendMessage;
let onMessage;

if (runtime === "deno") {
  // deno uses web worker api
  sendMessage = (msg) => self.postMessage(msg);
  onMessage = (handler) => {
    self.onmessage = async (e) => {
      await handler(e.data);
    };
  };
} else {
  // node and bun use worker_threads
  const { parentPort } = await import("node:worker_threads");
  sendMessage = (msg) => parentPort.postMessage(msg);
  onMessage = (handler) => {
    parentPort.on("message", handler);
  };
}

// handle benchmark commands
onMessage(async (data) => {
  const { command, config, workerId } = data;

  try {
    // execute job: connect and run benchmark
    let nc;
    if (config.ws) {
      // use websocket transport
      const core = await import("@nats-io/nats-core");
      const { wsconnect } = core;
      const url = config.server.startsWith("ws://") ? config.server : `ws://${config.server}`;
      nc = await wsconnect({ servers: url, maxReconnectAttempts: 0 });
    } else {
      // use runtime-specific transport
      if (config.transportNode || runtime === "node" || runtime === "bun") {
        const mod = await import("@nats-io/transport-node");
        const { connect } = mod;
        nc = await connect({ servers: config.server, maxReconnectAttempts: 0 });
      } else {
        const mod = await import("@nats-io/transport-deno");
        const { connect } = mod;
        nc = await connect({ servers: config.server, maxReconnectAttempts: 0 });
      }
    }

    // import bench functions
    const {
      benchPub, benchSub, PubBenchOptions, SubBenchOptions,
      benchKVPut, benchKVGet, KVPutOptions, KVGetOptions,
      benchJSPubAsync, JSPubAsyncOptions,
      benchJSConsOrdered, benchJSConsConsume, benchJSConsFetch,
      JSConsOrderedOptions, JSConsConsumeOptions, JSConsFetchOptions,
    } = await import("./mod.js");

    // create progress reporter that sends updates to main thread
    const progressReporter = {
      update: (value) => {
        sendMessage({
          type: 'progress',
          workerId,
          value,
        });
        return Promise.resolve();
      },
      stop: () => {},
      log: (msg) => Promise.resolve(),
      message: (msg) => Promise.resolve(),
    };

    let benchmark;

    if (command === "pub") {
      const opts = new PubBenchOptions();
      opts.subject = config.subject;
      opts.numClients = 1; // each worker runs as single client
      opts.numMsgs = config.msgsPerClient;
      opts.msgSize = config.msgSize;
      opts.multiSubject = config.multiSubject || false;
      opts.progress = false; // disable built-in progress
      opts.warmup = config.warmup || false;

      if (config.payload) {
        opts.payload = config.payload;
      }

      // pass progress reporter to benchPub
      benchmark = await benchPub(nc, opts, progressReporter);
    } else if (command === "sub") {
      const opts = new SubBenchOptions();
      opts.subject = config.subject;
      opts.numClients = 1; // each worker runs as single client
      opts.numMsgs = config.msgsPerClient;
      opts.msgSize = config.msgSize;

      // pass progress reporter to benchSub
      benchmark = await benchSub(nc, opts, progressReporter);
    } else if (command === "kvput") {
      // import Kvm for KV operations
      const { Kvm } = await import("@nats-io/kv");
      const kvm = new Kvm(nc);

      const opts = new KVPutOptions();
      opts.bucket = config.bucket;
      opts.numClients = 1; // each worker runs as single client
      opts.numMsgs = config.msgsPerClient;
      opts.msgSize = config.msgSize;
      opts.keyPrefix = config.keyprefix || "";
      opts.sleep = config.sleep || 0;

      if (config.payload) {
        opts.payload = config.payload;
      }

      // pass progress reporter to benchKVPut
      benchmark = await benchKVPut(nc, kvm, opts, progressReporter);
    } else if (command === "kvget") {
      // import Kvm for KV operations
      const { Kvm } = await import("@nats-io/kv");
      const kvm = new Kvm(nc);

      const opts = new KVGetOptions();
      opts.bucket = config.bucket;
      opts.numClients = 1; // each worker runs as single client
      opts.numMsgs = config.msgsPerClient;
      opts.keyPrefix = config.keyprefix || "";
      opts.randomize = config.randomize || 0;
      opts.sleep = config.sleep || 0;

      // pass progress reporter to benchKVGet
      benchmark = await benchKVGet(nc, kvm, opts, progressReporter);
    } else if (command === "jspub") {
      // import jetstream
      const { jetstream } = await import("@nats-io/jetstream");
      const js = jetstream(nc);

      const opts = new JSPubAsyncOptions();
      opts.subject = config.subject;
      opts.stream = config.stream;
      opts.numClients = 1; // each worker runs as single client
      opts.numMsgs = config.msgsPerClient;
      opts.msgSize = config.msgSize;
      opts.multiSubject = config.multiSubject || false;
      opts.asyncBatch = config.batch || 500;
      opts.sleep = config.sleep || 0;

      if (config.payload) {
        opts.payload = config.payload;
      }

      // pass progress reporter to benchJSPubAsync
      benchmark = await benchJSPubAsync(nc, js, opts, progressReporter);
    } else if (command === "jscons-ordered") {
      const { jetstream } = await import("@nats-io/jetstream");
      const js = jetstream(nc);

      const opts = new JSConsOrderedOptions();
      opts.subject = config.subject;
      opts.stream = config.stream;
      opts.numClients = 1;
      opts.numMsgs = config.msgsPerClient;
      opts.useCallback = config.callback || false;

      benchmark = await benchJSConsOrdered(nc, js, opts);
    } else if (command === "jscons-consume") {
      const { jetstream, jetstreamManager } = await import("@nats-io/jetstream");
      const js = jetstream(nc);
      const jsm = await jetstreamManager(nc);

      const opts = new JSConsConsumeOptions();
      opts.subject = config.subject;
      opts.stream = config.stream;
      opts.consumer = config.consumer || "nats-bench";
      opts.numClients = 1;
      opts.numMsgs = config.msgsPerClient;
      opts.batchSize = config.batch || 500;
      opts.useCallback = config.callback || false;

      benchmark = await benchJSConsConsume(nc, js, jsm, opts);
    } else if (command === "jscons-fetch") {
      const { jetstream, jetstreamManager } = await import("@nats-io/jetstream");
      const js = jetstream(nc);
      const jsm = await jetstreamManager(nc);

      const opts = new JSConsFetchOptions();
      opts.subject = config.subject;
      opts.stream = config.stream;
      opts.consumer = config.consumer || "nats-bench";
      opts.numClients = 1;
      opts.numMsgs = config.msgsPerClient;
      opts.batchSize = config.batch || 100;

      benchmark = await benchJSConsFetch(nc, js, jsm, opts);
    }

    await nc.close();

    // return results
    const sample = benchmark.sampleGroup.samples[0];
    sendMessage({
      type: 'result',
      success: true,
      sample: {
        jobMsgCnt: sample.jobMsgCnt,
        msgCnt: sample.msgCnt,
        msgBytes: sample.msgBytes,
        ioBytes: sample.ioBytes,
        start: sample.start,
        end: sample.end,
      },
    });
  } catch (err) {
    sendMessage({
      type: 'result',
      success: false,
      error: err.message,
    });
  }
});
