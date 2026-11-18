#!/usr/bin/env node
// installs released nats.js client versions side by side under .versions/<v>,
// so a bench run can pick one. the root package.json and deno.json are the
// latest release; these are for comparing against history.
//
//   node bin/versions.js list              released 3.x versions on npm
//   node bin/versions.js install <v>|all   install one or all of them
//   node bin/versions.js installed         versions under .versions
//
// each .versions/<v> is a complete install of the bench's dependencies with
// every @nats-io package pinned to <v>. sync() copies the current bench.js and
// workloads/ into it before a run, so `node .versions/<v>/bench.js` (or bun)
// resolves that version everywhere, worker threads included. deno uses
// .versions/<v>/import_map.json instead.

import { execFileSync } from "node:child_process";
import {
  copyFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const root = join(dirname(fileURLToPath(import.meta.url)), "..");
export const versionsDir = join(root, ".versions");

// the jsr packages the bench imports under deno
const jsrPackages = ["nats-core", "jetstream", "kv", "transport-deno"];

function compare(a, b) {
  const x = a.split(".").map(Number);
  const y = b.split(".").map(Number);
  for (let i = 0; i < 3; i++) {
    if (x[i] !== y[i]) return x[i] - y[i];
  }
  return 0;
}

// released (no prerelease qualifier) 3.x versions of nats-core on npm
export function released() {
  const out = execFileSync(
    "npm",
    ["view", "@nats-io/nats-core", "versions", "--json"],
    { encoding: "utf8" },
  );
  return JSON.parse(out)
    .filter((v) => !v.includes("-") && v.startsWith("3."))
    .sort(compare);
}

export function installed() {
  if (!existsSync(versionsDir)) return [];
  return readdirSync(versionsDir)
    .filter((d) => existsSync(join(versionsDir, d, "node_modules")))
    .sort(compare);
}

export function latest() {
  const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  return pkg.dependencies["@nats-io/nats-core"];
}

export function install(v) {
  const dir = join(versionsDir, v);
  mkdirSync(dir, { recursive: true });

  // the root dependencies, with the @nats-io ones pinned to v
  const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  for (const name of Object.keys(pkg.dependencies)) {
    if (name.startsWith("@nats-io/")) pkg.dependencies[name] = v;
  }
  delete pkg.overrides;
  pkg.private = true;
  writeFileSync(join(dir, "package.json"), JSON.stringify(pkg, null, 2) + "\n");
  // the @jsr registry for the npm:@jsr/... dependencies
  copyFileSync(join(root, ".npmrc"), join(dir, ".npmrc"));
  execFileSync("npm", ["install", "--silent", "--no-audit", "--no-fund"], {
    cwd: dir,
    stdio: "inherit",
  });

  // deno: the root import map with the @nats-io entries pinned to v
  const config = JSON.parse(readFileSync(join(root, "deno.json"), "utf8"));
  const imports = { ...config.imports };
  for (const p of jsrPackages) {
    imports[`@nats-io/${p}`] = `jsr:@nats-io/${p}@${v}`;
  }
  imports["@nats-io/transport-node"] = `npm:@nats-io/transport-node@${v}`;
  writeFileSync(
    join(dir, "import_map.json"),
    JSON.stringify({ imports }, null, 2) + "\n",
  );

  console.log(`installed ${v} in ${dir}`);
}

// copies the current bench sources into .versions/<v>
export function sync(v) {
  const dir = join(versionsDir, v);
  if (!existsSync(join(dir, "node_modules"))) {
    throw new Error(
      `client ${v} isn't installed, run: node bin/versions.js install ${v}`,
    );
  }
  copyFileSync(join(root, "bench.js"), join(dir, "bench.js"));
  cpSync(join(root, "workloads"), join(dir, "workloads"), { recursive: true });
  return dir;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const [cmd, arg] = process.argv.slice(2);
  if (cmd === "list") {
    console.log(released().join("\n"));
  } else if (cmd === "installed") {
    console.log(installed().join("\n"));
  } else if (cmd === "install" && arg) {
    const all = arg === "all" ? released() : [arg];
    for (const v of all) install(v);
  } else {
    console.error(
      "usage: node bin/versions.js list | installed | install <version>|all",
    );
    process.exit(1);
  }
}
