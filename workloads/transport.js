// connects over tcp with the runtime's transport: transport-node for node and
// bun (and deno with --transport-node), transport-deno for deno. when the
// bench runs from the npm package under deno, transport-deno (published on
// jsr only) isn't installed, so it falls back to transport-node, which deno
// runs too.

export async function tcpConnect(opts, { runtime, transportNode = false }) {
  if (transportNode || runtime !== "deno") {
    const { connect } = await import("@nats-io/transport-node");
    return connect(opts);
  }
  let mod;
  try {
    mod = await import("@nats-io/transport-deno");
  } catch {
    mod = await import("@nats-io/transport-node");
  }
  return mod.connect(opts);
}
