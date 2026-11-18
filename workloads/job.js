// runs one bench job on an open connection. the cli's worker threads and the
// browser runner both call this, so a workload does the same thing in every
// runtime.

import {
  benchJSConsConsume,
  benchJSConsFetch,
  benchJSConsOrdered,
  benchJSPubAsync,
  benchKVGet,
  benchKVPut,
  benchPub,
  benchSub,
  JSConsConsumeOptions,
  JSConsFetchOptions,
  JSConsOrderedOptions,
  JSPubAsyncOptions,
  KVGetOptions,
  KVPutOptions,
  PubBenchOptions,
  SubBenchOptions,
} from "./mod.js";

// config is the worker job config built by bench.js, with msgsPerClient set
export async function runJob(nc, command, config, progressReporter) {
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
    return await benchPub(nc, opts, progressReporter);
  }

  if (command === "sub") {
    const opts = new SubBenchOptions();
    opts.subject = config.subject;
    opts.numClients = 1; // each worker runs as single client
    opts.numMsgs = config.msgsPerClient;
    opts.msgSize = config.msgSize;
    return await benchSub(nc, opts, progressReporter);
  }

  if (command === "kvput") {
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
    return await benchKVPut(nc, kvm, opts, progressReporter);
  }

  if (command === "kvget") {
    const { Kvm } = await import("@nats-io/kv");
    const kvm = new Kvm(nc);
    const opts = new KVGetOptions();
    opts.bucket = config.bucket;
    opts.numClients = 1; // each worker runs as single client
    opts.numMsgs = config.msgsPerClient;
    opts.keyPrefix = config.keyprefix || "";
    opts.randomize = config.randomize || 0;
    opts.sleep = config.sleep || 0;
    return await benchKVGet(nc, kvm, opts, progressReporter);
  }

  if (command === "jspub") {
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
    return await benchJSPubAsync(nc, js, opts, progressReporter);
  }

  if (command === "jscons-ordered") {
    const { jetstream } = await import("@nats-io/jetstream");
    const js = jetstream(nc);
    const opts = new JSConsOrderedOptions();
    opts.subject = config.subject;
    opts.stream = config.stream;
    opts.numClients = 1;
    opts.numMsgs = config.msgsPerClient;
    opts.useCallback = config.callback || false;
    return await benchJSConsOrdered(nc, js, opts);
  }

  if (command === "jscons-consume") {
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
    return await benchJSConsConsume(nc, js, jsm, opts);
  }

  if (command === "jscons-fetch") {
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
    return await benchJSConsFetch(nc, js, jsm, opts);
  }

  throw new Error(`unknown command ${command}`);
}
