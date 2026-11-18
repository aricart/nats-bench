// jetstream consumer benchmarks
// based on nats-cli bench js ordered/consume/fetch commands
// uses @nats-io/jetstream for proper jetstream implementation

import { Benchmark, BenchSample, BenchType, now } from "./stats.js";

export class JSConsOrderedOptions {
  constructor() {
    this.subject = "bench";
    this.stream = "benchstream";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.useCallback = false; // use callback instead of iterator
  }
}

export class JSConsConsumeOptions {
  constructor() {
    this.subject = "bench";
    this.stream = "benchstream";
    this.consumer = "nats-bench";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.batchSize = 500; // max messages per consume batch
    this.useCallback = false; // use callback instead of iterator
  }
}

export class JSConsFetchOptions {
  constructor() {
    this.subject = "bench";
    this.stream = "benchstream";
    this.consumer = "nats-bench";
    this.numClients = 1;
    this.numMsgs = 100000;
    this.batchSize = 100; // messages per fetch
    this.useCallback = false; // use callback instead of iterator
  }
}

// benchmark jetstream ordered consumer
// js is JetStreamClient from @nats-io/jetstream
export async function benchJSConsOrdered(
  nc,
  js,
  opts = new JSConsOrderedOptions(),
) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.JSCONS_ORDERED,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runJSConsOrdered(nc, js, opts, 0);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

// benchmark jetstream consume (callback-based)
export async function benchJSConsConsume(
  nc,
  js,
  jsm,
  opts = new JSConsConsumeOptions(),
) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.JSCONS_CONSUME,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runJSConsConsume(nc, js, jsm, opts, 0);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

// benchmark jetstream fetch (batch-based)
export async function benchJSConsFetch(
  nc,
  js,
  jsm,
  opts = new JSConsFetchOptions(),
) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.JSCONS_FETCH,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runJSConsFetch(nc, js, jsm, opts, 0);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

async function runJSConsOrdered(nc, js, opts, offset) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // get ordered consumer (no name required, library creates ephemeral)
  const consumer = await js.consumers.get(opts.stream);

  sample.start = now();

  if (opts.useCallback) {
    // callback-based consumption (faster)
    return new Promise((resolve) => {
      let count = 0;

      consumer.consume({
        callback: (msg) => {
          sample.msgCnt++;
          sample.msgBytes += msg.data.length;
          count++;
          msg.ack();

          if (count >= opts.numMsgs) {
            sample.end = now();
            resolve(sample);
          }
        },
      });
    });
  } else {
    // iterator-based consumption
    const iter = await consumer.consume();
    let count = 0;

    for await (const msg of iter) {
      sample.msgCnt++;
      sample.msgBytes += msg.data.length;
      count++;

      // ack the message
      msg.ack();

      if (count >= opts.numMsgs) {
        break;
      }
    }

    sample.end = now();

    // stop the consumer
    iter.stop();

    return sample;
  }
}

async function runJSConsConsume(nc, js, jsm, opts, offset) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // create or get durable pull consumer (idempotent under worker race)
  let consumer;
  try {
    consumer = await js.consumers.get(opts.stream, opts.consumer);
  } catch (err) {
    try {
      await jsm.consumers.add(opts.stream, {
        name: opts.consumer,
        ack_policy: "explicit",
      });
    } catch (e) {
      // race: another worker created it concurrently
    }
    consumer = await js.consumers.get(opts.stream, opts.consumer);
  }

  sample.start = now();

  if (opts.useCallback) {
    // callback-based consumption (faster)
    return new Promise((resolve) => {
      let count = 0;

      consumer.consume({
        max_messages: opts.batchSize,
        callback: (msg) => {
          sample.msgCnt++;
          sample.msgBytes += msg.data.length;
          count++;
          msg.ack();

          if (count >= opts.numMsgs) {
            sample.end = now();
            resolve(sample);
          }
        },
      });
    });
  } else {
    // iterator-based consumption
    const iter = await consumer.consume({
      max_messages: opts.batchSize,
    });

    let count = 0;

    for await (const msg of iter) {
      sample.msgCnt++;
      sample.msgBytes += msg.data.length;
      count++;

      // ack the message
      msg.ack();

      if (count >= opts.numMsgs) {
        break;
      }
    }

    sample.end = now();

    // stop the consumer
    iter.stop();

    return sample;
  }
}

async function runJSConsFetch(nc, js, jsm, opts, offset) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // create or get durable pull consumer (idempotent under worker race)
  let consumer;
  try {
    consumer = await js.consumers.get(opts.stream, opts.consumer);
  } catch (err) {
    try {
      await jsm.consumers.add(opts.stream, {
        name: opts.consumer,
        ack_policy: "explicit",
      });
    } catch (e) {
      // race: another worker created it concurrently
    }
    consumer = await js.consumers.get(opts.stream, opts.consumer);
  }

  sample.start = now();

  let count = 0;

  // fetch messages in batches
  while (count < opts.numMsgs) {
    const batchCount = Math.min(opts.batchSize, opts.numMsgs - count);

    // fetch a batch of messages
    const iter = await consumer.fetch({
      max_messages: batchCount,
    });

    for await (const msg of iter) {
      sample.msgCnt++;
      sample.msgBytes += msg.data.length;
      count++;

      // ack the message
      msg.ack();

      if (count >= opts.numMsgs) {
        break;
      }
    }
  }

  sample.end = now();

  return sample;
}
