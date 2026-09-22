import { realpathSync } from "node:fs";
import { isAbsolute, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const configured = process.env.PACK_CONSUMER_ROOT;
if (configured === undefined || configured.length === 0) throw new Error("PACK_CONSUMER_ROOT is required");
const root = realpathSync(configured);
const denyNative = process.env.PACK_DENY_NATIVE === "1";

const inside = (path) => {
  const rel = relative(root, path);
  return rel === "" || (!rel.startsWith(`..${sep}`) && rel !== ".." && !isAbsolute(rel));
};

export async function resolve(specifier, context, nextResolve) {
  if (denyNative && specifier === "koffi") throw new Error("portable import attempted to resolve koffi");
  const resolved = await nextResolve(specifier, context);
  if (!resolved.url.startsWith("file:")) return resolved;

  const path = realpathSync(fileURLToPath(resolved.url));
  if (!inside(path)) throw new Error(`module resolved outside isolated consumer: ${path}`);
  if (denyNative && /[/\\]reactor-effect-client[/\\]dist[/\\]native(?:-|\.js)/.test(path)) {
    throw new Error(`portable import reached native implementation: ${path}`);
  }
  return resolved;
}
