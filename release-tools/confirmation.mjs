import { Console, Effect, Path, Schema, Stdio } from "effect";
import { CandidateIdentity, confirmationFor, readJson, reject } from "./model.mjs";
import { runCommand } from "./host.mjs";

/** Print the exact publication confirmation a retained candidate requires. */
if (import.meta.main)
  runCommand(
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const [candidateDirectory] = yield* (yield* Stdio.Stdio).args;
      if (candidateDirectory === undefined || candidateDirectory === "")
        return yield* reject("usage: confirmation.mjs candidate-dir");
      const identity = yield* readJson(path.join(candidateDirectory, "identity.json")).pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(CandidateIdentity, { onExcessProperty: "error" }),
        ),
      );
      yield* Console.log(confirmationFor(identity.qualification));
    }),
  );
