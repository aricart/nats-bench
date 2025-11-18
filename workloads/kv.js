// kv (key-value) benchmark
// based on nats-cli bench kv command
// uses @nats-io/kv module

import { Benchmark, BenchSample, BenchType } from "./stats.js";

export class KVPutOptions {
  constructor() {
    this.bucket = "benchbucket";
    this.numClients = 1;
    this.numMsgs = 10000;
    this.msgSize = 128;
    this.payload = null; // optional fixed payload
    this.sleep = 0; // sleep between operations in ms
    this.keyPrefix = ""; // optional key prefix
  }
}

export class KVGetOptions {
  constructor() {
    this.bucket = "benchbucket";
    this.numClients = 1;
    this.numMsgs = 10000;
    this.sleep = 0; // sleep between operations in ms
    this.randomize = 0; // if > 0, randomize gets from 0 to randomize-1
    this.keyPrefix = ""; // optional key prefix
  }
}

// benchmark putting values to kv
// nc is NatsConnection, kvm is passed from caller (imported from @nats-io/kv)
export async function benchKVPut(nc, kvm, opts = new KVPutOptions(), externalProgress = null) {
  const benchmark = new Benchmark("NATS", BenchType.KV_PUT, opts.numClients);

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runKvPuts(nc, kvm, opts, 0, externalProgress);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

// benchmark getting values from kv
export async function benchKVGet(nc, kvm, opts = new KVGetOptions(), externalProgress = null) {
  const benchmark = new Benchmark("NATS", BenchType.KV_GET, opts.numClients);

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runKvGets(nc, kvm, opts, 0, externalProgress);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

async function runKvPuts(nc, kvm, opts, offset, progress) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // prepare payload
  const payload = opts.payload || new Uint8Array(opts.msgSize);

  // create or open bucket using Kvm
  const kv = await kvm.create(opts.bucket);

  sample.start = Date.now();

  for (let i = 0; i < opts.numMsgs; i++) {
    const key = `${opts.keyPrefix}${offset + i}`;

    await kv.put(key, payload);

    sample.msgCnt++;
    sample.msgBytes += payload.length;

    // update progress every 100 operations
    if (progress && i % 100 === 0) {
      progress.update(i + 1);
    }

    // optional sleep between operations
    if (opts.sleep > 0) {
      await new Promise((resolve) => setTimeout(resolve, opts.sleep));
    }
  }

  sample.end = Date.now();

  // final progress update
  if (progress) {
    progress.update(opts.numMsgs);
  }

  return sample;
}

async function runKvGets(nc, kvm, opts, offset, progress) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // open bucket using Kvm
  const kv = await kvm.open(opts.bucket);

  sample.start = Date.now();

  for (let i = 0; i < opts.numMsgs; i++) {
    let key;

    if (opts.randomize === 0) {
      // sequential reads
      key = `${opts.keyPrefix}${offset + i}`;
    } else {
      // random reads
      const randomIdx = Math.floor(Math.random() * opts.randomize);
      key = `${opts.keyPrefix}${randomIdx}`;
    }

    const entry = await kv.get(key);

    if (entry) {
      sample.msgCnt++;
      sample.msgBytes += entry.value.length;
    }

    // update progress every 100 operations
    if (progress && i % 100 === 0) {
      progress.update(i + 1);
    }

    // optional sleep between operations
    if (opts.sleep > 0) {
      await new Promise((resolve) => setTimeout(resolve, opts.sleep));
    }
  }

  sample.end = Date.now();

  // final progress update
  if (progress) {
    progress.update(opts.numMsgs);
  }

  return sample;
}
