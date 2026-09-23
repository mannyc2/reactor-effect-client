import { pathToFileURL } from "node:url";

const bundle = process.argv[2];
if (bundle === undefined) throw new Error("browser smoke requires its installed bundle");
// Node lazily initializes its own Undici implementation on first access to
// these Web APIs. Initialize the host, not the SDK, before removing Buffer.
for (const api of [
  globalThis.Headers,
  globalThis.Request,
  globalThis.Response,
  globalThis.FormData,
])
  if (typeof api !== "function") throw new Error("host lacks a required Web API");
if (!Reflect.deleteProperty(globalThis, "Buffer"))
  throw new Error("browser smoke could not remove Node Buffer");
await import(pathToFileURL(bundle).href);
