// browser runner: fetches a job from the page's server (bin/browser.js), runs
// it with the same workload code as the cli workers (workloads/job.js) over a
// websocket connection, and posts back the result in the same json shape as
// `bench.js --json`. one client, the way the cli runs a single worker.

import { wsconnect } from "@nats-io/nats-core";
import { runJob } from "../workloads/job.js";
import { Benchmark, IterationGroup } from "../workloads/mod.js";

async function browserVersion() {
  const ua = navigator.userAgentData;
  if (ua?.getHighEntropyValues) {
    const { fullVersionList } = await ua.getHighEntropyValues(["fullVersionList"]);
    const b = fullVersionList.find((e) => /chrom/i.test(e.brand) && !/not/i.test(e.brand)) ??
      fullVersionList[0];
    return `${b.brand} ${b.version}`;
  }
  return navigator.userAgent;
}

const noProgress = {
  update: () => Promise.resolve(),
  stop: () => {},
  log: () => Promise.resolve(),
  message: () => Promise.resolve(),
};

// mirrors runWithWorkers() in bench.js for one worker
async function runOnce(command, config) {
  const nc = await wsconnect({ servers: config.server, maxReconnectAttempts: 0 });
  try {
    const job = await runJob(nc, command, config, noProgress);
    const bm = new Benchmark("NATS", command.toUpperCase(), 1);
    bm.addSample(job.sampleGroup.samples[0]);
    return bm;
  } finally {
    await nc.close();
  }
}

async function main() {
  const spec = await (await fetch("/job")).json();
  const runtimeVersion = await browserVersion();
  const meta = (command, extra = {}) => ({
    runtime: "browser",
    runtimeVersion,
    userAgent: navigator.userAgent,
    transport: "websocket",
    transportLib: "browser",
    command,
    subject: spec.config.subject,
    options: { ...spec.options, ...extra },
  });

  const { command, iterations } = spec;
  const config = { ...spec.config, msgsPerClient: spec.options.msgs };
  let result;

  if (command === "pubsub") {
    // like bench.js: start the subscriber, give it a second, then publish
    const pubGroup = new IterationGroup("NATS", "PUB");
    const subGroup = new IterationGroup("NATS", "SUB");
    pubGroup.setMetadata(meta("pub"));
    subGroup.setMetadata(meta("sub"));
    for (let i = 0; i < iterations; i++) {
      const sub = runOnce("sub", { ...config, payload: null });
      await new Promise((r) => setTimeout(r, 1000));
      const pub = runOnce("pub", config);
      const [subBm, pubBm] = await Promise.all([sub, pub]);
      pubBm.setMetadata(meta("pub"));
      subBm.setMetadata(meta("sub"));
      pubGroup.add(pubBm);
      subGroup.add(subBm);
    }
    result = { pub: pubGroup.toJSON(), sub: subGroup.toJSON() };
  } else {
    const group = new IterationGroup("NATS", command.toUpperCase());
    group.setMetadata(meta(command));
    for (let i = 0; i < iterations; i++) {
      const bm = await runOnce(command, config);
      bm.setMetadata(meta(command));
      group.add(bm);
    }
    result = group.toJSON();
  }

  await fetch("/result", { method: "POST", body: JSON.stringify(result) });
}

main().catch(async (err) => {
  await fetch("/result", {
    method: "POST",
    body: JSON.stringify({ error: `${err?.message ?? err}`, stack: err?.stack }),
  });
});
