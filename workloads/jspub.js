// jetstream publish benchmark
// based on nats-cli bench js pub command
// uses @nats-io/jetstream for proper jetstream implementation

import { Benchmark, BenchSample, BenchType, now } from "./stats.js";

export class JSPubSyncOptions {
  constructor() {
    this.subject = "bench";
    this.stream = "benchstream";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.msgSize = 128;
    this.payload = null; // optional fixed payload
    this.multiSubject = false; // publish to subject.<seq>
    this.sleep = 0; // sleep between publishes in ms
  }
}

export class JSPubAsyncOptions {
  constructor() {
    this.subject = "bench";
    this.stream = "benchstream";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.msgSize = 128;
    this.payload = null; // optional fixed payload
    this.multiSubject = false; // publish to subject.<seq>
    this.asyncBatch = 5000; // max in-flight async publishes
    this.sleep = 0; // sleep between publishes in ms
  }
}

export class JSPubBatchOptions {
  constructor() {
    this.subject = "bench";
    this.stream = "benchstream";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.msgSize = 128;
    this.payload = null; // optional fixed payload
    this.multiSubject = false; // publish to subject.<seq>
    this.batchSize = 500; // messages per batch
    this.sleep = 0; // sleep between publishes in ms
  }
}

// benchmark jetstream synchronous publish
// js is JetStreamClient from @nats-io/jetstream
export async function benchJSPubSync(nc, js, opts = new JSPubSyncOptions()) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.JSPUB_SYNC,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runJSPubSync(nc, js, opts, 0);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

// benchmark jetstream asynchronous publish
export async function benchJSPubAsync(nc, js, opts = new JSPubAsyncOptions(), externalProgress = null) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.JSPUB_ASYNC,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runJSPubAsync(nc, js, opts, 0, externalProgress);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

// benchmark jetstream batch publish
export async function benchJSPubBatch(nc, js, opts = new JSPubBatchOptions()) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.JSPUB_BATCH,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runJSPubBatch(nc, js, opts, 0);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

async function runJSPubSync(nc, js, opts, offset) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // prepare payload
  const payload = opts.payload || new Uint8Array(opts.msgSize);

  sample.start = now();

  for (let i = 0; i < opts.numMsgs; i++) {
    const subject = opts.multiSubject
      ? `${opts.subject}.${offset + i}`
      : opts.subject;

    // synchronous jetstream publish - waits for ack
    await js.publish(subject, payload);

    sample.msgCnt++;
    sample.msgBytes += payload.length;
  }

  sample.end = now();

  return sample;
}

async function runJSPubAsync(nc, js, opts, offset, progress) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // prepare payload
  const payload = opts.payload || new Uint8Array(opts.msgSize);

  sample.start = now();

  const allPubs = [];
  const batchSize = opts.asyncBatch;

  // fire off publishes and flush buffer periodically
  for (let i = 0; i < opts.numMsgs; i++) {
    const subject = opts.multiSubject
      ? `${opts.subject}.${offset + i}`
      : opts.subject;

    const pub = js.publish(subject, payload);
    allPubs.push(pub);
    sample.msgCnt++;
    sample.msgBytes += payload.length;

    // update progress every 1000 messages
    if (progress && i % 1000 === 0) {
      progress.update(i + 1);
    }

    // flush connection buffer periodically to keep steady flow
    if (allPubs.length % batchSize === 0) {
      await nc.flush();
    }
  }

  // wait for all acks
  if (allPubs.length) {
    await Promise.all(allPubs);
  }

  sample.end = now();

  // final progress update
  if (progress) {
    progress.update(opts.numMsgs);
  }

  return sample;
}

async function runJSPubBatch(nc, js, opts, offset) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // prepare payload
  const payload = opts.payload || new Uint8Array(opts.msgSize);

  sample.start = now();

  let sent = 0;

  while (sent < opts.numMsgs) {
    const batchCount = Math.min(opts.batchSize, opts.numMsgs - sent);

    // start a batch with the first message
    const firstSubject = opts.multiSubject
      ? `${opts.subject}.${offset + sent}`
      : opts.subject;

    const batch = await js.startBatch(firstSubject, payload);

    sample.msgCnt++;
    sample.msgBytes += payload.length;
    sent++;

    // add remaining messages to the batch (except the last one which goes in commit)
    for (let i = 1; i < batchCount - 1; i++) {
      const subject = opts.multiSubject
        ? `${opts.subject}.${offset + sent}`
        : opts.subject;

      // add message to batch (doesn't wait for ack)
      batch.add(subject, payload);

      sample.msgCnt++;
      sample.msgBytes += payload.length;
      sent++;
    }

    // commit the batch with the last message (waits for server ack)
    if (batchCount > 1) {
      const lastSubject = opts.multiSubject
        ? `${opts.subject}.${offset + sent}`
        : opts.subject;

      await batch.commit(lastSubject, payload);

      sample.msgCnt++;
      sample.msgBytes += payload.length;
      sent++;
    } else {
      // if batch size is 1, just commit without additional message
      await batch.commit(firstSubject, payload);
    }
  }

  sample.end = now();

  return sample;
}
