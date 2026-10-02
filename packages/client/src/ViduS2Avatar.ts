/**
 * Vidu S2-Avatar over a connected `Session`: a character made from one photo
 * of a person, who talks with a caller in a live call. Its `session_state` as
 * the model reports it, what the caller and the character say, and each
 * command with the evidence of what answered it.
 *
 * `make(session)` observes a session it neither allocates nor closes. The
 * character arrives on the `main_video` and `main_audio` tracks, which the
 * provider resumes on the connection each time a call goes live; the caller
 * speaks on `mic`, which a browser publishes through `session.tracks`, or
 * types with `say`. Reactor bills the whole session, time between calls
 * included, so close it once the caller is done.
 *
 * The model answers a refusal with `command_error`, which names the command
 * but no request: commands go out one at a time, and one refused fails with a
 * `Refused` reason carrying the model's documented `code`. Another client of
 * the same session sending a command of the same name at that moment can be
 * mistaken for it.
 */
import type * as Effect from "effect/Effect";
import { dual } from "effect/Function";
import * as Predicate from "effect/Predicate";
import type * as Scope from "effect/Scope";
import * as Provider_ from "./internal/vidu/provider.js";
import type { Options, Provider } from "./internal/vidu/provider.js";
import type { CommandFailure, ReactorError } from "./ReactorError.js";
import type { Session } from "./Session.js";

export {
  CallEnded,
  CommandError,
  Phase,
  State,
  Transcript,
  Voice,
  Voices,
} from "./internal/vidu/messages.js";
export {
  AvatarImage,
  Call,
  CallUpdate,
  ImageType,
  Llm,
  ReferenceImage,
  Vad,
} from "./internal/vidu/commands.js";
export type {
  ObservationOptions,
  Options,
  Provider,
  ProviderEvent,
} from "./internal/vidu/provider.js";

/** The model name a session and its tokens are created for. */
export const modelName = "reactor/vidu-s2-avatar";

/** The session's tracks: the caller's microphone and camera in, the character out. */
export const tracks = {
  mic: "mic",
  webcam: "webcam",
  video: "main_video",
  audio: "main_audio",
} as const;

/**
 * The provider over an already connected session. It asks the model for its
 * snapshot first; a failure releases what it acquired. Each deadline is a
 * finite, non-negative `Duration.Input`; any other value fails `make` with
 * `InvalidInput`.
 */
export const make: {
  (
    options?: Options,
  ): (session: Session) => Effect.Effect<Provider, ReactorError | CommandFailure, Scope.Scope>;
  (
    session: Session,
    options?: Options,
  ): Effect.Effect<Provider, ReactorError | CommandFailure, Scope.Scope>;
} = dual(
  (args) => Predicate.hasProperty(args[0], "command"),
  (session: Session, options?: Options) => Provider_.make(session, options ?? {}),
);
