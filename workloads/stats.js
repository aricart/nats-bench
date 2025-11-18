// statistics tracking for benchmarks
// based on nats-cli internal/bench/stats.go

export const BenchType = {
  PUB: "PUB",
  SUB: "SUB",
  JSPUB_SYNC: "JSPUB_SYNC",
  JSPUB_ASYNC: "JSPUB_ASYNC",
  JSPUB_BATCH: "JSPUB_BATCH",
  JSCONS_ORDERED: "JSCONS_ORDERED",
  JSCONS_FETCH: "JSCONS_FETCH",
  JSCONS_CONSUME: "JSCONS_CONSUME",
  SERVICE_REQUEST: "SERVICE_REQUEST",
  SERVICE_SERVE: "SERVICE_SERVE",
  KV_PUT: "KV_PUT",
  KV_GET: "KV_GET",
};

export class BenchSample {
  constructor() {
    this.jobMsgCnt = 0;
    this.msgCnt = 0;
    this.msgBytes = 0;
    this.ioBytes = 0;
    this.start = null;
    this.end = null;
  }

  duration() {
    if (!this.start || !this.end) return 0;
    return this.end - this.start;
  }

  rate() {
    const dur = this.duration();
    if (dur === 0) return 0;
    return Math.floor((this.msgCnt * 1000) / dur);
  }

  throughput() {
    const dur = this.duration();
    if (dur === 0) return 0;
    return (this.msgBytes * 1000) / dur;
  }

  avgLatency() {
    if (this.msgCnt === 0) return 0;
    return this.duration() / this.msgCnt;
  }
}

export class SampleGroup extends BenchSample {
  constructor() {
    super();
    this.samples = [];
  }

  addSample(sample) {
    this.samples.push(sample);
    this.msgCnt += sample.msgCnt;
    this.msgBytes += sample.msgBytes;
    this.ioBytes += sample.ioBytes;

    if (!this.start || sample.start < this.start) {
      this.start = sample.start;
    }
    if (!this.end || sample.end > this.end) {
      this.end = sample.end;
    }
  }

  statistics() {
    if (this.samples.length === 0) {
      return { min: 0, avg: 0, max: 0, stddev: 0 };
    }

    const rates = this.samples.map((s) => s.rate());
    const min = Math.min(...rates);
    const max = Math.max(...rates);
    const avg = rates.reduce((a, b) => a + b, 0) / rates.length;

    const variance = rates.reduce((sum, r) => sum + Math.pow(r - avg, 2), 0) /
      rates.length;
    const stddev = Math.sqrt(variance);

    return { min, avg, max, stddev };
  }
}

export class Benchmark {
  constructor(name, benchType, clientCount) {
    this.name = name;
    this.runId = crypto.randomUUID();
    this.benchType = benchType;
    this.sampleGroup = new SampleGroup();
    this.clientCount = clientCount;

    // runtime metadata
    this.metadata = {
      runtime: null,           // "node", "deno", "bun"
      runtimeVersion: null,    // version string
      transport: null,         // "tcp", "websocket"
      transportLib: null,      // "native", "npm"
      command: null,           // "pub", "sub", etc
      subject: null,           // subject name
      timestamp: new Date().toISOString(),
      options: {}              // command options
    };
  }

  setMetadata(meta) {
    this.metadata = { ...this.metadata, ...meta };
  }

  addSample(sample) {
    this.sampleGroup.addSample(sample);
  }

  report() {
    const stats = this.sampleGroup.statistics();
    const duration = this.sampleGroup.duration() / 1000; // convert to seconds
    const msgPerSec = this.sampleGroup.rate();
    const throughput = this.sampleGroup.throughput();

    let report = `\n`;
    report += `${this.name} ${this.benchType} Benchmark\n`;
    report += `=`.repeat(50) + `\n`;
    report += `Messages:    ${this.sampleGroup.msgCnt.toLocaleString()}\n`;
    report += `Bytes:       ${humanizeBytes(this.sampleGroup.msgBytes)}\n`;
    report += `Duration:    ${duration.toFixed(2)}s\n`;
    report += `Throughput:  ${msgPerSec.toLocaleString()} msgs/sec ~ ${
      humanizeBytes(throughput)
    }/sec\n`;
    report += `\n`;

    if (this.sampleGroup.samples.length > 1) {
      report += `Per-client statistics:\n`;
      report += `  min ${stats.min.toLocaleString()} | `;
      report += `avg ${Math.floor(stats.avg).toLocaleString()} | `;
      report += `max ${stats.max.toLocaleString()} | `;
      report += `stddev ${Math.floor(stats.stddev).toLocaleString()} msgs\n`;
    }

    return report;
  }

  csv() {
    const rows = [];
    rows.push([
      "RunID",
      "ClientID",
      "MsgCount",
      "MsgBytes",
      "MsgsPerSec",
      "BytesPerSec",
      "DurationSecs",
      "AvgLatencyMs",
    ].join(","));

    this.sampleGroup.samples.forEach((sample, i) => {
      rows.push([
        this.runId,
        `client-${i}`,
        sample.msgCnt,
        sample.msgBytes,
        sample.rate(),
        sample.throughput().toFixed(2),
        (sample.duration() / 1000).toFixed(3),
        sample.avgLatency().toFixed(3),
      ].join(","));
    });

    return rows.join("\n");
  }

  toJSON() {
    const stats = this.sampleGroup.statistics();
    return {
      runId: this.runId,
      name: this.name,
      benchType: this.benchType,
      clientCount: this.clientCount,
      metadata: this.metadata,
      results: {
        totalMessages: this.sampleGroup.msgCnt,
        totalBytes: this.sampleGroup.msgBytes,
        durationMs: this.sampleGroup.duration(),
        durationSecs: this.sampleGroup.duration() / 1000,
        throughputMsgsPerSec: this.sampleGroup.rate(),
        throughputBytesPerSec: this.sampleGroup.throughput(),
        statistics: stats,
        samples: this.sampleGroup.samples.map((s, i) => ({
          clientId: i,
          msgCnt: s.msgCnt,
          msgBytes: s.msgBytes,
          durationMs: s.duration(),
          msgsPerSec: s.rate(),
          bytesPerSec: s.throughput(),
          avgLatencyMs: s.avgLatency()
        }))
      }
    };
  }
}

function humanizeBytes(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(2)} ${units[unitIndex]}`;
}
