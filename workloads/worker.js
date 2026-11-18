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

    // run the job, shared with the browser runner
    const { runJob } = await import("./job.js");

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

    const benchmark = await runJob(nc, command, config, progressReporter);

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
