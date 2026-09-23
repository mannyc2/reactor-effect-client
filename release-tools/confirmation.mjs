import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import { CandidateIdentity, confirmationFor, readBytes, reject } from "./model.mjs";

/** Print the exact publication confirmation a retained candidate requires. */
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [candidateDirectory] = process.argv.slice(2);
  if (!candidateDirectory) reject("usage: confirmation.mjs candidate-dir");
  const identity = Schema.decodeUnknownSync(CandidateIdentity, { onExcessProperty: "error" })(
    JSON.parse(readBytes(join(candidateDirectory, "identity.json")).toString()),
  );
  console.log(confirmationFor(identity.qualification));
}
