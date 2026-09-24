import { Effect, Exit, Layer, ManagedRuntime, Redacted, Scope } from "effect";
import { HttpApiClient } from "effect/unstable/httpapi";
import * as Reactor from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Browser from "reactor-effect-browser";
import { Api } from "./Api.ts";
import { WebCrypto } from "./WebCrypto.ts";

/**
 * One runtime for the page, built once: the SDK's Client over the browser's
 * own WebRTC, with the fetch-based HTTP client and Web Crypto it runs on, both
 * also used by the page itself. Building `Browser.layer` checks for WebRTC, so
 * an unsupported browser fails here, before any session is paid for. The UI
 * below is ordinary DOM code that runs effects through the runtime.
 */
const runtime = ManagedRuntime.make(
  Reactor.layer().pipe(
    Layer.provideMerge(Layer.mergeAll(Reactor.FetchHttp.layer, WebCrypto, Browser.layer)),
  ),
);

const element = (id: string): HTMLElement => {
  const found = document.getElementById(id);
  if (found === null) throw new Error(`the page has no #${id}`);
  return found;
};
const video = element("video") as HTMLVideoElement;
const audio = element("audio") as HTMLAudioElement;
const log = (line: string) => {
  element("log").textContent =
    `${new Date().toLocaleTimeString()}  ${line}\n${element("log").textContent ?? ""}`;
};
/** What a failure says about itself: its reason and, for a command, whether it may have applied. */
const describe = (error: unknown) =>
  Reactor.isReactorFailure(error)
    ? `${error._tag}: ${error.reason._tag}${"outcome" in error.context ? ` (${error.context.outcome})` : ""}`
    : String(error);

interface Live {
  readonly scope: Scope.Closeable;
  readonly session: Reactor.Session;
  readonly provider: H3.Provider;
  readonly media: Browser.MediaGeneration;
}

/**
 * The page holds at most one session. Starting takes seconds (a token, an
 * allocation, a WebRTC connection), and a second Start in that time must
 * not open a second paid session nobody can stop; a Stop in that time
 * closes the session as soon as it has connected.
 */
type State =
  | { readonly _tag: "Idle" }
  | { readonly _tag: "Starting"; stopRequested: boolean }
  | { readonly _tag: "Live"; readonly live: Live };
let state: State = { _tag: "Idle" };
const startButton = element("start") as HTMLButtonElement;

/**
 * Asks the server for a token, then allocates and connects a session from
 * the browser. Everything the session owns (its WebRTC peer, its tracks,
 * their playback) lives in a scope the page holds until Stop, so one close
 * releases all of it and terminates the paid session.
 */
const start = Effect.gen(function* () {
  const token = yield* (yield* HttpApiClient.make(Api)).token();
  const scope = yield* Scope.make();
  return yield* Effect.gen(function* () {
    const client = yield* Reactor.Client;
    const session = yield* client.createConnected({
      model: H3.modelName,
      jwt: Redacted.make(token.jwt),
    });
    const provider = yield* H3.make(session);
    // H3 changes no playback policy on its own: ask it to play clips as they are ready.
    yield* provider.setAutoplay(true);
    const media = yield* Browser.media(session);
    yield* Browser.play(yield* media.track("main_video"), video);
    return { scope, session, provider, media };
  }).pipe(
    Scope.provide(scope),
    Effect.onError(() => Scope.close(scope, Exit.void)),
  );
});

/** One prompt, followed from acceptance to its end through the clip's operation facts. */
const send = Effect.fn("send")(function* (provider: H3.Provider, prompt: string) {
  const submission = yield* provider.prepare({ prompt, seconds: 8 });
  const acceptance = yield* submission.submit;
  const clip = acceptance.clip.clip_id.slice(-8);
  log(`clip ${clip} accepted`);
  const operation = yield* provider.operation(submission);
  yield* operation.reached("generated");
  log(`clip ${clip} generated`);
  yield* operation.reached("started");
  log(`clip ${clip} playing`);
  yield* operation.ended;
  log(`clip ${clip} ended`);
}, Effect.scoped);

startButton.addEventListener("click", () => {
  if (state._tag !== "Idle") return;
  const starting = { _tag: "Starting" as const, stopRequested: false };
  state = starting;
  startButton.disabled = true;
  log("starting a session…");
  runtime.runPromise(start).then(
    (live) => {
      state = { _tag: "Live", live };
      log(`session ${live.session.id} connected`);
      if (starting.stopRequested) void stop();
    },
    (error: unknown) => {
      state = { _tag: "Idle" };
      startButton.disabled = false;
      log(`could not start: ${describe(error)}`);
    },
  );
});

// Audio needs its own user gesture: browsers refuse unmuted playback that
// starts seconds after the click that asked for it.
element("sound").addEventListener("click", () => {
  if (state._tag !== "Live") return;
  const { media, scope } = state.live;
  runtime
    .runPromise(
      Effect.gen(function* () {
        yield* Browser.play(yield* media.track("main_audio"), audio);
      }).pipe(Scope.provide(scope)),
    )
    .catch((error: unknown) => log(`no sound: ${describe(error)}`));
});

element("form").addEventListener("submit", (event) => {
  event.preventDefault();
  const input = element("prompt") as HTMLInputElement;
  if (state._tag !== "Live" || input.value.trim() === "") return;
  runtime
    .runPromise(send(state.live.provider, input.value.trim()))
    .catch((error: unknown) => log(`prompt failed: ${describe(error)}`));
  input.value = "";
});

/**
 * Stop closes the session, which terminates it and returns its report, then
 * the scope, which releases playback and tracks and reuses that report.
 */
const stop = (): Promise<void> => {
  if (state._tag === "Starting") {
    state.stopRequested = true;
    log("stopping once the session has connected");
    return Promise.resolve();
  }
  if (state._tag !== "Live") return Promise.resolve();
  const { live } = state;
  state = { _tag: "Idle" };
  return runtime
    .runPromise(live.session.close.pipe(Effect.tap(() => Scope.close(live.scope, Exit.void))))
    .then((report) =>
      log(
        `session closed: ${report.remote.confirmed ? "termination confirmed" : "termination not confirmed"}`,
      ),
    )
    .finally(() => {
      startButton.disabled = false;
    });
};
element("stop").addEventListener("click", () => void stop());

// Leaving the page closes what it can; the token's five-minute cap bounds
// anything a closing tab cannot finish.
window.addEventListener("pagehide", () => {
  void stop().then(
    () => runtime.dispose(),
    () => runtime.dispose(),
  );
});
