# NATS Bench - JavaScript

A NATS benchmarking tool written in JavaScript, implementing functionality similar to the `nats bench` CLI command.

## Architecture

- **bench.js** - Unified CLI entry point (works with Node.js, Deno, and Bun)
- **workloads/** - Benchmark workload implementations and worker infrastructure
  - Core NATS pub/sub benchmarks
  - JetStream pub/consumer benchmarks
  - KV (key-value) benchmarks
  - Worker thread implementation for parallel execution
  - Statistics tracking and reporting

## Features

- **Core NATS**: pub, sub, pubsub (simultaneous pub/sub)
- **JetStream**: pub with async batching; ordered, consume and fetch consumers
- **KV**: put and get operations with sequential or random access
- **Multi-worker support**: true parallelism using worker threads (Node/Bun) or Web Workers (Deno)
- **Cross-runtime**: works with Node.js, Deno, and Bun
- **Transport options**: TCP (default) or WebSocket
- **Progress tracking**: multi-bar progress display for concurrent workers
- **Statistics**: throughput, latency, min/max/avg/stddev
- **Export**: CSV and JSON output formats
- **Multi-subject**: publish to `subject.<seq>` pattern

## Setup

### Install Dependencies

Node.js and Bun need the npm dependencies; Deno resolves its own on first run.

```bash
npm install
```

### NATS Server

Start a NATS server with JetStream enabled:

```bash
# Simple mode (JetStream enabled)
nats-server -js

# Or use the provided config file
nats-server -c nats-server.conf

# For WebSocket support
nats-server -c nats-server-ws.conf
```

## Usage

### Core NATS Benchmarks

```bash
# Publish benchmark
node bench.js pub test --msgs 100000

# Multi-worker publish (10 workers)
node bench.js pub test --workers 10 --msgs 1000000

# Subscribe benchmark
node bench.js sub test --workers 5 --msgs 100000

# Simultaneous pub/sub
node bench.js pubsub test --pubworkers 5 --subworkers 3 --msgs 100000

# Multi-subject publishing
node bench.js pub test --multisubject --msgs 100000
```

### JetStream Benchmarks

```bash
# JetStream publish with async batching
node bench.js js pub bench --stream benchstream --msgs 100000 --batch 500

# With multiple workers
node bench.js js pub bench --workers 10 --msgs 1000000 --batch 1000

# Multi-subject JetStream publish
node bench.js js pub bench --multisubject --msgs 100000

# Consume what was published (ordered, durable consume, durable fetch)
node bench.js js ordered bench --stream benchstream --msgs 100000
node bench.js js consume bench --stream benchstream --msgs 100000 --batch 500
node bench.js js fetch bench --stream benchstream --msgs 100000 --batch 100
```

### KV Benchmarks

```bash
# KV put operations
node bench.js kv put --bucket mybucket --msgs 10000 --size 256

# KV get operations (sequential)
node bench.js kv get --bucket mybucket --msgs 10000

# KV get with random access
node bench.js kv get --bucket mybucket --msgs 10000 --randomize 10000

# KV with key prefix
node bench.js kv put --bucket mybucket --keyprefix "user:" --msgs 1000
```

### Using Deno

```bash
# Core NATS publish
deno run -A bench.js pub test --msgs 100000

# Multi-worker publish (requires permissions)
deno run -A bench.js pub test --workers 10 --msgs 1000000

# KV operations
deno run -A bench.js kv put --bucket mybucket --msgs 10000
```

### Using Bun

```bash
# Core NATS publish
bun bench.js pub test --msgs 100000

# Multi-worker publish
bun bench.js pub test --workers 10 --msgs 1000000
```

### Transport Options

Which transport a connection uses:

| Runtime | TCP (default) | `--transport-node` | `--ws` |
|---------|---------------|--------------------|--------|
| Node.js | `@nats-io/transport-node` | same | `wsconnect` (nats-core) |
| Bun | `@nats-io/transport-node` | same | `wsconnect` (nats-core) |
| Deno | `@nats-io/transport-deno` | `@nats-io/transport-node` | `wsconnect` (nats-core) |

The server's `/connz` reports `nats.deno` for the Deno transport and `nats.js`
for the Node.js one. History runs (`bin/history.js`) use the defaults.

```bash
# Use WebSocket transport
node bench.js pub test --ws --server localhost:9222

# Force Node.js transport in Deno
deno run -A bench.js pub test --transport-node
```

### Export Results

```bash
# Export to CSV
node bench.js pub test --csv results.csv

# Export to JSON
node bench.js pub test --json results.json

# Both formats
node bench.js pub test --csv results.csv --json results.json
```

## Commands

### Core NATS

- `pub <subject>` - Publish messages to Core NATS
- `sub <subject>` - Subscribe to Core NATS messages
- `pubsub <subject>` - Publish and subscribe simultaneously

### JetStream

- `js pub <subject>` - Publish messages to JetStream
- `js ordered <subject>` - Consume with an ephemeral ordered consumer
- `js consume <subject>` - Consume from a durable pull consumer
- `js fetch <subject>` - Consume from a durable pull consumer using fetch

### KV (Key-Value)

- `kv put` - Put values to KV bucket
- `kv get` - Get values from KV bucket

## Global Flags

```
-s, --server <url>        NATS server URL (default: localhost:4222)
    --ws                  Use WebSocket transport (default: false)
    --transport-node      Use Node.js transport (default: auto)
    --csv <file>          Export results to CSV file
    --json <file>         Export results to JSON file
-h, --help                Show help
```

## Command-Specific Flags

### pub / sub / pubsub

```
--workers <n>           Number of workers (default: 1)
--msgs <n>              Number of messages (default: 100000)
--size <n>              Message size in bytes (default: 128)
--iterations <n>        Repeat the bench; reports min/median/mean/max/stddev (default: 1)
--payload <string>      Custom payload (overrides --size; pub and pubsub)
--multisubject          Publish to subject.<seq> (pub and pubsub)
--warmup                Publish 1000 messages to another subject first (pub only)
```

### pubsub

```
--pubworkers <n>        Number of publisher workers (default: 1)
--subworkers <n>        Number of subscriber workers (default: 1)
```

### js pub

```
--stream <name>         Stream name (default: benchstream)
--workers <n>           Number of workers (default: 1)
--msgs <n>              Number of messages (default: 100000)
--size <n>              Message size in bytes (default: 128)
--batch <n>             Batch size for async publish (default: 500)
--multisubject          Multi-subject mode (default: false)
--multisubjectmax <n>   Max subjects in multi-subject mode (default: 100000)
--iterations <n>        Repeat the bench (default: 1)
--purge                 Purge stream before running (default: false)
--sleep <ms>            Sleep between operations in ms (default: 0)
```

### kv put / get

```
--bucket <name>         Bucket name (default: benchbucket)
--workers <n>           Number of workers (default: 1)
--msgs <n>              Number of operations (default: 100000)
--size <n>              Value size for put (default: 128)
--keyprefix <prefix>    Key prefix (default: empty)
--sleep <ms>            Sleep between operations in ms (default: 0)
--iterations <n>        Repeat the bench (default: 1)
```

### js ordered / consume / fetch

Besides the `js` flags (`--stream`, `--workers`, `--msgs`, `--iterations`, ...):

```
--consumer <name>       Durable consumer name (consume, fetch; default: nats-bench)
--batch <n>             Max messages per batch (consume: 500, fetch: 100)
--callback              Use the callback API instead of the iterator (ordered, consume)
```

### kv put

```
--payload <string>      Custom value payload (default: empty)
```

### kv get

```
--randomize <n>         Random gets from 0 to n-1 (default: 0 for sequential)
```

## Implementation Notes

- Based on `nats bench` CLI functionality
- Unified CLI works across Node.js, Deno, and Bun
- Uses `@nats-io/nats-core` for protocol implementation
- Transport layer: `@nats-io/transport-node` or `@nats-io/transport-deno`
- JetStream: `@nats-io/jetstream` for stream operations
- KV: `@nats-io/kv` for key-value operations
- Multi-worker execution uses worker threads (Node/Bun) or Web Workers (Deno)
- Each worker creates its own NATS connection for true parallelism
- Progress tracking with `cli-progress` MultiBar (one bar per worker)
- Statistics calculated per-worker and aggregated

## Statistics

The benchmark reports:
- Total messages and bytes processed
- Duration (ms)
- Throughput (msgs/sec and MB/sec)
- Per-worker statistics (min/avg/max/stddev)

CSV/JSON export includes:
- Runtime information (node, deno, or bun)
- Transport details (tcp or websocket)
- Per-worker metrics (messages, bytes, throughput, duration)
- Aggregate statistics

## Client Versions and History

The bench runs the latest nats.js release by default (the `@nats-io` versions
in `package.json` and `deno.json`). To compare releases, every released 3.x
version can be installed side by side and run as a matrix.

```bash
# install released versions under .versions/ (gitignored)
node bin/versions.js list
node bin/versions.js install all

# run every client version x runtime (node, deno, bun, browser) x workload
node bin/history.js
node bin/history.js --clients 3.4.0,3.5.0 --runtimes node,browser --iterations 5

# tables, and a chart page per run (history/<run-id>/report.html)
node bin/report.js
```

The workloads are small, over TCP and WebSocket, enough to spot a regression
between releases. Every workload with a payload runs at 0, 8, 32, 64 and 128
bytes (`--sizes 0,8,32,64,128`). By default they are core NATS only
(`--workloads core`):

- `pub`: publish 50k messages
- `pubsub`: a js publisher and a js subscriber at the same time

The Go client is the 100% baseline: every core workload also runs with
`nats bench` (the nats CLI, nats.go) with the same message count, size and
transport, and the report shows every value as a percentage
of Go for the same workload, size and transport. It is skipped if `nats`
isn't on the PATH (or `$NATS_CLI`). `nats bench` rejects `--size 0`, so for
0-byte messages it publishes an empty `--payload` file instead.

`--workloads js` runs JetStream publish and KV put/get instead, and
`--workloads all` runs both; JetStream is only enabled on the server when a
JetStream workload runs.

Each run starts
its own nats-server and writes one record per combination to
`history/<run-id>/`, with the host, the server version, the client version
(checked against the server's `/connz`), the runtime and its version, the
transport and the workload, next to the bench's own JSON. Records from
different runs can be compared on any of those.

How a version is selected:

- Node and Bun: `.versions/<v>` is a full install with `@nats-io/*` pinned to
  `<v>`; the runner copies `bench.js` and `workloads/` into it, so plain module
  resolution picks that version, worker threads included.
- Deno: `deno run --import-map .versions/<v>/import_map.json bench.js`.
- Browser: `browser/entry.js` is bundled with `deno bundle` against the
  version's import map and run in headless Chrome over WebSocket.
  `bin/browser.js` serves the page and collects the result.

Every workload runs through `workloads/job.js`, in the CLI workers and in the
browser, so all runtimes run the same code with the same parameters.

## Project Structure

```
nats-bench/
├── bench.js                # Main CLI entry point
├── bin/
│   ├── versions.js        # Installs released client versions in .versions/
│   ├── history.js         # Runs the history matrix
│   ├── browser.js         # Runs a job in headless chrome
│   └── report.js          # Tables and charts for a history run
├── browser/entry.js        # Browser runner (bundled per client version)
├── history/                # History runs, one json per combination
├── workloads/              # Benchmark workloads
│   ├── job.js             # One job on a connection (cli workers and browser)
│   ├── worker.js          # Worker thread implementation
│   ├── pub.js             # Core NATS publish
│   ├── sub.js             # Core NATS subscribe
│   ├── jspub.js           # JetStream publish
│   ├── jscons.js          # JetStream consumers
│   ├── kv.js              # KV operations
│   ├── service.js         # Service benchmarks
│   ├── stats.js           # Statistics utilities
│   └── mod.js             # Module exports
├── package.json            # Dependencies
├── deno.json              # Deno configuration
├── README.md              # This file
└── CLAUDE.md              # Development notes
```

## Examples

### Compare Runtimes

```bash
# Node.js
node bench.js pub test --msgs 1000000 --workers 10

# Deno
deno run -A bench.js pub test --msgs 1000000 --workers 10

# Bun
bun bench.js pub test --msgs 1000000 --workers 10
```

### End-to-End Workflow

```bash
# Start NATS server
nats-server -js

# Run subscriber in background
node bench.js sub test --msgs 1000000 --workers 5 &

# Run publisher
node bench.js pub test --msgs 1000000 --workers 5

# KV workflow
node bench.js kv put --bucket test --msgs 100000 --workers 3
node bench.js kv get --bucket test --msgs 100000 --workers 3 --randomize 100000
```
