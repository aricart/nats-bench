// core nats subscribe benchmark
// based on nats-cli bench command

import { Benchmark, BenchSample, BenchType } from "./stats.js";

export class SubBenchOptions {
  constructor() {
    this.subject = "test";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.msgSize = 128; // expected message size (for reporting, actual bytes are measured)
    this.useIterators = false; // use callbacks by default (faster)
  }
}

export async function benchSub(nc, opts = new SubBenchOptions(), externalProgress = null) {
  const benchmark = new Benchmark("NATS", BenchType.SUB, opts.numClients);

  // for single client, run directly without worker overhead
  if (opts.numClients === 1) {
    const sample = opts.useIterators
      ? await runSubscriberIterator(nc, opts.subject, opts.numMsgs, externalProgress)
      : await runSubscriberCallback(nc, opts.subject, opts.numMsgs, externalProgress);
    benchmark.addSample(sample);
    return benchmark;
  }

  // for multiple clients, caller must use worker-based runner
  // this function only handles single client direct execution
  throw new Error(
    "Multiple clients require worker-based execution. Use benchSubWorkers() instead.",
  );
}

// callback-based subscriber (faster)
async function runSubscriberCallback(nc, subject, msgCount, progress) {
  const sample = new BenchSample();
  sample.jobMsgCnt = msgCount;

  let received = 0;
  const done = Promise.withResolvers();

  const sub = nc.subscribe(subject, {
    callback: (err, msg) => {
      if (err) {
        done.reject(err);
        return;
      }

      if (received === 0) {
        sample.start = Date.now();
      }

      received++;
      sample.msgCnt++;
      sample.msgBytes += msg.data.length;

      // update progress every 1000 messages
      if (progress && received % 1000 === 0) {
        progress.update(received);
      }

      if (received >= msgCount) {
        sample.end = Date.now();
        sub.unsubscribe();
        if (progress) {
          progress.update(msgCount);
        }
        done.resolve();
      }
    },
  });

  await done.promise;

  return sample;
}

// iterator-based subscriber (for comparison)
async function runSubscriberIterator(nc, subject, msgCount, progress) {
  const sample = new BenchSample();
  sample.jobMsgCnt = msgCount;

  let received = 0;

  const sub = nc.subscribe(subject);

  sample.start = Date.now();

  for await (const msg of sub) {
    received++;
    sample.msgCnt++;
    sample.msgBytes += msg.data.length;

    // update progress every 1000 messages
    if (progress && received % 1000 === 0) {
      progress.update(received);
    }

    if (received >= msgCount) {
      sample.end = Date.now();
      sub.unsubscribe();
      if (progress) {
        progress.update(msgCount);
      }
      break;
    }
  }

  return sample;
}
