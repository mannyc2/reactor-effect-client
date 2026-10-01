/**
 * An upload: a slot from the coordinator, the bytes to its presigned URL,
 * then the notification that names the file to the model. How far it got is
 * the evidence a failure carries and the event observers see.
 */
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { ReactorError } from "../../ReactorError.js";
import type { UploadOptions, UploadProgress, Uploaded, UploadReference } from "../../Session.js";
import * as Deadline from "../deadline.js";
import type { Generation } from "./generation.js";
import type { Core } from "./model.js";
import { isKnown, timedOut } from "./model.js";
import type { Requests } from "./requests.js";

export const make = ({
  core,
  generation,
  requests,
}: {
  readonly core: Core;
  readonly generation: Generation;
  readonly requests: Requests;
}) => {
  const { settings, signaling, state, publish } = core;
  const { currentReady, guard } = generation;

  return Effect.fnUntraced(
    function* (name: string, mimeType: string, bytes: Uint8Array, options: UploadOptions = {}) {
      const known = (yield* SubscriptionRef.get(state)).remote;
      if (isKnown(known)) yield* Effect.annotateCurrentSpan("reactor.session.id", known.id);
      const progress = yield* Ref.make<UploadProgress>({
        allocation: "not-requested",
        transfer: "not-requested",
        notification: "not-submitted",
      });
      const reach = (patch: Partial<UploadProgress>) =>
        Ref.update(progress, (p): UploadProgress => ({ ...p, ...patch }));
      const operation = Effect.gen(function* () {
        const { c } = yield* currentReady;
        if (!isKnown(known))
          return yield* ReactorError.fromCode("InvalidState", "no known session id");
        if (
          name.length === 0 ||
          mimeType.length === 0 ||
          bytes.byteLength < 1 ||
          bytes.byteLength > settings.maxUploadBytes
        )
          return yield* ReactorError.fromCode(
            "InvalidInput",
            `an upload needs a name, a MIME type and 1..${String(settings.maxUploadBytes)} bytes`,
            { outcome: "not-submitted" },
          );
        const copy = new Uint8Array(bytes);
        yield* reach({ allocation: "unknown" });
        const slot = yield* guard(
          c,
          signaling.allocateUpload(known.id, name, mimeType, copy.length),
        );
        const file: UploadReference = {
          uploadId: slot.presigned_id,
          name,
          mimeType,
          size: BigInt(copy.length),
        };
        yield* reach({ allocation: "confirmed", file, transfer: "unknown" });
        yield* guard(c, signaling.putUpload(slot, copy, mimeType));
        yield* reach({ transfer: "confirmed", notification: "unknown" });
        yield* requests
          .notification(c, { case: "fileUploaded", value: file })
          .pipe(
            Effect.tapError((error) =>
              error.context.outcome === "not-submitted"
                ? reach({ notification: "not-submitted" })
                : Effect.void,
            ),
          );
        yield* reach({ notification: "submitted" });
        return { file, transfer: "confirmed", notification: "submitted" } satisfies Uploaded;
      });
      const wait = yield* Deadline.decode("uploadTimeout")(
        options.uploadTimeout ?? settings.uploadTimeout,
      );
      return yield* operation.pipe(
        Effect.timeoutOrElse({ duration: wait, orElse: timedOut("upload") }),
        Effect.catch((error) =>
          Ref.get(progress).pipe(
            Effect.flatMap((reached) =>
              Effect.fail(
                ReactorError.fromCode("Upload", error.message, {
                  ...error.context,
                  operation: "upload",
                  detail: { cause: error, progress: reached },
                }),
              ),
            ),
          ),
        ),
        Effect.ensuring(
          Ref.get(progress).pipe(
            Effect.flatMap((reached) => publish({ _tag: "Upload", progress: reached })),
          ),
        ),
      );
    },
    // The MIME type and size only: never the name or the bytes.
    Effect.withSpan(
      "Session.upload",
      (_name, mimeType, bytes) => ({
        kind: "client",
        attributes: {
          "reactor.upload.mime_type": mimeType,
          "reactor.upload.size": bytes.byteLength,
        },
      }),
      { captureStackTrace: false },
    ),
  );
};
