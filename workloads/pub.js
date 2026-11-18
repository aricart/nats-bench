// core nats publish benchmark
// based on nats-cli bench command

import { Benchmark, BenchSample, BenchType, now } from "./stats.js";
import { nuid } from "@nats-io/nats-core";

export class PubBenchOptions {
  constructor() {
    this.subject = "test";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.msgSize = 128;
    this.payload = null; // optional fixed payload
    this.multiSubject = false; // publish to subject.<seq>
    this.headers = null; // optional headers map
    this.warmup = false; // run warmup before benchmark
  }
}

export async function benchPub(nc, opts = new PubBenchOptions(), externalProgress = null) {
  const benchmark = new Benchmark("NATS", BenchType.PUB, opts.numClients);

  // prepare payload
  const payload = opts.payload || new Uint8Array(opts.msgSize);

  // prepare headers if provided
  let headers;
  if (opts.headers) {
    headers = {};
    for (const [k, v] of Object.entries(opts.headers)) {
      headers[k] = v;
    }
  }

  // for single client, run directly without worker overhead
  if (opts.numClients === 1) {
    // progress is drawn by the caller (bench.js), if at all
    const progress = externalProgress;
    const sample = await runPublisher(
      nc,
      opts.subject,
      opts.numMsgs,
      payload,
      headers,
      opts.multiSubject,
      Promise.resolve(), // no latch needed for single client
      progress,
      opts.warmup,
    );
    if (progress) {
      progress.stop();
    }
    benchmark.addSample(sample);
    return benchmark;
  }

  // for multiple clients, caller must use worker-based runner
  // this function only handles single client direct execution
  throw new Error(
    "Multiple clients require worker-based execution. Use benchPubWorkers() instead.",
  );
}

async function runPublisher(
  nc,
  subject,
  msgCount,
  payload,
  headers,
  multiSubject,
  startLatch,
  progress,
  warmup = false,
) {
  const sample = new BenchSample();
  sample.jobMsgCnt = msgCount;

  // wait for start signal
  await startLatch;

  // warmup: publish to unique subject to avoid interfering with any subscribers
  if (warmup) {
    const warmupSubject = nuid.next();
    for (let i = 0; i < 1000; i++) {
      nc.publish(warmupSubject, payload);
    }
    await nc.flush();
  }

  sample.start = now();

  for (let i = 0; i < msgCount; i++) {
    const subj = multiSubject ? `${subject}.${i}` : subject;

    if (headers) {
      nc.publish(subj, payload, { headers });
    } else {
      nc.publish(subj, payload);
    }

    if (progress && i % 1000 === 0) {
      progress.update(i + 1);
    }
  }

  // set final counts
  sample.msgCnt = msgCount;
  sample.msgBytes = msgCount * payload.length;

  // keep at 99% during flush to keep timer ticking (progress bar stops timer at 100%)
  const flushCount = Math.floor(msgCount * 0.99);

  if (progress) {
    progress.update(flushCount);
  }

  // start interval to keep timer ticking during flush
  const interval = progress ? setInterval(() => {
    progress.update(flushCount);
  }, 100) : null;

  // ensure all messages are flushed
  await nc.flush();

  // stop the refresh interval
  if (interval) {
    clearInterval(interval);
  }

  sample.end = now();

  // final update to show 100%
  if (progress) {
    progress.update(msgCount);
  }

  return sample;
}

// distribute total messages across clients as evenly as possible
function distributeMsgs(total, clients) {
  const distribution = [];
  const base = Math.floor(total / clients);
  const remainder = total % clients;

  for (let i = 0; i < clients; i++) {
    distribution.push(base + (i < remainder ? 1 : 0));
  }

  return distribution;
}
