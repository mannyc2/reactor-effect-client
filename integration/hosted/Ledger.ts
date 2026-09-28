/**
 * A ledger is a directory of evidence files, one per run: every paid run's
 * reservation counts against the total, whatever it spent, so a run is
 * admitted only if the ledger has room for its worst case. One run at a time
 * holds the ledger's lock. Each release keeps a ledger of its own.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { Evidence, EvidenceJson } from "./Evidence.js";
import { ceilingFor, Refused } from "./Spend.js";

/** Holds the ledger's lock for the scope; refused while another run, or a crashed one, holds it. */
export const lock = (
  directory: string,
): Effect.Effect<void, Refused, FileSystem.FileSystem | Path.Path | Scope.Scope> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const file = (yield* Path.Path).join(directory, ".lock");
    yield* fs
      .makeDirectory(directory, { recursive: true })
      .pipe(Effect.mapError(() => Refused.make({ message: `${directory} cannot be created` })));
    yield* Effect.acquireRelease(
      fs.writeFileString(file, "held\n", { flag: "wx" }).pipe(
        Effect.mapError(() =>
          Refused.make({
            message: `${file} exists: another run holds the ledger, or one crashed; remove it once no run is active`,
          }),
        ),
      ),
      () => Effect.ignore(fs.remove(file)),
    );
  });

/** Every run in the ledger; a file that does not decode refuses, since the spend it records is unknown. */
export const entries = (
  directory: string,
): Effect.Effect<ReadonlyArray<Evidence>, Refused, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    if (!(yield* Effect.orElseSucceed(fs.exists(directory), () => false))) return [];
    const names = yield* fs
      .readDirectory(directory)
      .pipe(Effect.mapError(() => Refused.make({ message: `${directory} cannot be read` })));
    return yield* Effect.forEach(names.filter((name) => name.endsWith(".json")).sort(), (name) => {
      const file = path.join(directory, name);
      return fs.readFileString(file).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(EvidenceJson)),
        Effect.mapError(() =>
          Refused.make({
            message: `${file} is not ${Evidence.fields.format.literal} evidence, so the spend it records is unknown`,
          }),
        ),
      );
    });
  });

/** What a run reserves: a paid run's worst case once admitted, or its whole ceiling if it holds a grant without one. */
export const reserved = (evidence: Evidence): number =>
  evidence.mode !== "paid"
    ? 0
    : (evidence.budget.worstCaseUsd ??
      (evidence.grants.length === 0 ? 0 : ceilingFor(evidence.check)));

/** Evidence could not be saved: the run stops opening, minting and submitting. */
export class SaveFailed extends Schema.TaggedError<SaveFailed>(
  "reactor-effect-integration/hosted/Ledger/SaveFailed",
)("SaveFailed", { message: Schema.String }) {}

const encode = Schema.encodeEffect(EvidenceJson);

/**
 * Claims a new evidence file for one run, so a run never overwrites another's;
 * each later save replaces it atomically. A save whose text holds any of the
 * run's secrets writes nothing and fails. Saves run one at a time: two at once
 * would share the one pending file, and the second rename would find it gone.
 */
export const writer = (file: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const claimed = yield* Ref.make(false);
    const one = yield* Semaphore.make(1);
    const failed = () => SaveFailed.make({ message: `${file} could not be written` });
    return (evidence: Evidence, secrets: ReadonlyArray<Redacted.Redacted<string>>) =>
      Effect.gen(function* () {
        const text = yield* encode(evidence).pipe(
          Effect.mapError(() => SaveFailed.make({ message: "the evidence does not encode" })),
        );
        if (
          secrets.some((secret) => {
            const value = Redacted.value(secret);
            return value.length >= 8 && text.includes(value);
          })
        )
          return yield* SaveFailed.make({
            message: "the evidence would contain a credential; it was not written",
          });
        if (!(yield* Ref.get(claimed))) {
          yield* fs
            .writeFileString(file, `${text}\n`, { flag: "wx", mode: 0o600 })
            .pipe(Effect.mapError(failed));
          return yield* Ref.set(claimed, true);
        }
        const pending = `${file}.pending`;
        yield* fs
          .writeFileString(pending, `${text}\n`, { mode: 0o600 })
          .pipe(Effect.andThen(fs.rename(pending, file)), Effect.mapError(failed));
      }).pipe(one.withPermit);
  });
