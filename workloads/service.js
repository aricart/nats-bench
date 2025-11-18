// service (request/reply) benchmark
// based on nats-cli bench service command
// uses @nats-io/services for proper service implementation

import { Benchmark, BenchSample, BenchType } from "./stats.js";

export class ServiceRequestOptions {
  constructor() {
    this.subject = "bench.service";
    this.numClients = 1;
    this.numMsgs = 10000;
    this.msgSize = 128;
    this.payload = null; // optional fixed payload
    this.headers = null; // optional headers map
    this.sleep = 0; // sleep between requests in ms
    this.async = false; // false: wait for each reply, true: don't wait
    this.asyncBatch = 100; // max in-flight requests for async mode
  }
}

export class ServiceServeOptions {
  constructor() {
    this.subject = "bench.service";
    this.numClients = 1;
    this.numMsgs = 10000;
    this.msgSize = 128;
    this.payload = null; // optional fixed reply payload
    this.headers = null; // optional headers map
    this.sleep = 0; // sleep before replying in ms
  }
}

// benchmark sending requests and waiting for replies
export async function benchServiceRequest(
  nc,
  opts = new ServiceRequestOptions(),
) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.SERVICE_REQUEST,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runRequester(nc, opts);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

// benchmark serving requests (replying to them)
// svcm is the Services Manager from @nats-io/services
export async function benchServiceServe(nc, svcm, opts = new ServiceServeOptions()) {
  const benchmark = new Benchmark(
    "NATS",
    BenchType.SERVICE_SERVE,
    opts.numClients,
  );

  // for single client, run directly
  if (opts.numClients === 1) {
    const sample = await runServer(nc, svcm, opts);
    benchmark.addSample(sample);
    return benchmark;
  }

  throw new Error(
    "Multiple clients require worker-based execution",
  );
}

async function runRequester(nc, opts) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // prepare payload
  const payload = opts.payload || new Uint8Array(opts.msgSize);

  // prepare headers if provided
  const headers = opts.headers ? { ...opts.headers } : undefined;

  sample.start = Date.now();

  if (opts.async) {
    // async mode: send requests in batches without waiting for each reply
    const batchSize = opts.asyncBatch;
    let sent = 0;

    while (sent < opts.numMsgs) {
      const batch = [];
      const batchCount = Math.min(batchSize, opts.numMsgs - sent);

      // send batch of requests
      for (let i = 0; i < batchCount; i++) {
        const req = nc.request(opts.subject, payload, {
          headers,
          timeout: 5000,
        }).then((msg) => {
          sample.msgCnt++;
          sample.msgBytes += payload.length;
          sample.ioBytes += msg.data.length;
        });
        batch.push(req);

        // optional sleep between requests
        if (opts.sleep > 0) {
          await new Promise((resolve) => setTimeout(resolve, opts.sleep));
        }
      }

      // wait for this batch to complete before sending next batch
      await Promise.all(batch);
      sent += batchCount;
    }
  } else {
    // sync mode: wait for each reply before sending next request
    for (let i = 0; i < opts.numMsgs; i++) {
      const msg = await nc.request(opts.subject, payload, {
        headers,
        timeout: 5000,
      });

      sample.msgCnt++;
      sample.msgBytes += payload.length;
      sample.ioBytes += msg.data.length;

      // optional sleep between requests
      if (opts.sleep > 0) {
        await new Promise((resolve) => setTimeout(resolve, opts.sleep));
      }
    }
  }

  sample.end = Date.now();

  return sample;
}

async function runServer(nc, svcm, opts) {
  const sample = new BenchSample();
  sample.jobMsgCnt = opts.numMsgs;

  // prepare reply payload
  const replyPayload = opts.payload || new Uint8Array(opts.msgSize);

  // prepare headers if provided
  const headers = opts.headers ? { ...opts.headers } : undefined;

  let received = 0;
  const done = Promise.withResolvers();

  // create service using @nats-io/services
  const service = await svcm.add({
    name: "bench",
    version: "1.0.0",
    description: "benchmark service",
  });

  // add endpoint with handler callback
  // endpoint name is simple, subject can have dots
  service.addEndpoint("serve", {
    subject: opts.subject,
    handler: async (err, msg) => {
      if (err) {
        done.reject(err);
        return;
      }

      if (received === 0) {
        sample.start = Date.now();
      }

      // optional sleep before replying
      if (opts.sleep > 0) {
        await new Promise((resolve) => setTimeout(resolve, opts.sleep));
      }

      // send reply
      msg.respond(replyPayload, { headers });

      received++;
      sample.msgCnt++;
      sample.msgBytes += msg.data.length;
      sample.ioBytes += replyPayload.length; // count reply bytes sent

      if (received >= opts.numMsgs) {
        sample.end = Date.now();
        // stop the service when done
        await service.stop();
        done.resolve();
      }
    },
  });

  await done.promise;

  return sample;
}
