/**
 * Watch Reactor in this terminal: one H3 session plays a short playlist, its decoded video is
 * drawn here as 24-bit colour text, and each clip is followed through the facts H3 reports about
 * it. `watch` is the same program offline and live; only the layer beneath it changes.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Config, Console, Effect, Layer, Option, Redacted, Stream } from "effect";
import { Argument, Command, Flag } from "effect/cli";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import {
  CoordinatorClient,
  H3,
  Reactor,
  ReactorError,
  ReactorTest,
  References,
  Session,
} from "reactor-effect-client";
import { NativePeer } from "reactor-effect-native";
import * as Screen from "./Screen.ts";

/** Played when no prompts are given: big shapes and strong colours survive being drawn in text. */
const playlist = [
  "A lighthouse on a sea cliff at dusk, storm waves bursting white against the rocks, its warm beam sweeping through the rain, cinematic wide shot",
  "A neon-lit Tokyo alley at night in the rain, magenta and cyan signs reflected in the wet asphalt, steam rising from a noodle stall, slow dolly forward",
  "A humpback whale breaching in slow motion at golden hour, spray glowing orange against a deep blue sea, aerial shot",
  "Green and violet northern lights rippling over a frozen mountain lake, a lone cabin glowing amber on the shore, slow pan",
];

/** Each clip's length; H3 takes 5 to 15.084 seconds. */
const clipSeconds = 8;

/**
 * The session's cap. Reactor ends the session then, whatever becomes of this process, so the cap
 * bounds the bill; ten clips of 8 seconds fit in it.
 */
const capSeconds = 120;

/**
 * The most a session capped at `capSeconds` can cost, counted in the unit Reactor's pricing states
 * the rate in: every second for a rate per second, each started minute whole for one per minute.
 */
const mostItCosts = (rate: CoordinatorClient.Rate): string => {
  const billed = rate.per === "second" ? capSeconds : Math.ceil(capSeconds / 60) * 60;
  const usd = (billed * rate.creditsPerSecond) / rate.creditsPerDollar;
  const price = rate.per === "second" ? rate.creditsPerSecond : rate.creditsPerSecond * 60;
  return (
    `at most ${usd.toFixed(2)} USD: the session is capped at ${capSeconds} seconds, at ` +
    `${price} credits a ${rate.per} (${rate.creditsPerDollar} credits = 1 USD)`
  );
};

/** What closing the session established about its end. */
const closed = (sessionId: string, report: Session.CloseReport): string =>
  report.remote.confirmed
    ? `session ${sessionId} closed: termination confirmed`
    : `session ${sessionId} closed, termination NOT confirmed: it may bill until its cap`;

/** Sends one clip, and returns its operation: the facts H3 will report about it. */
const send = Effect.fn("send")(function* (
  h3: H3.Provider,
  screen: Screen.Screen,
  index: number,
  request: H3.Request,
) {
  const submission = yield* h3.prepare(request);
  yield* submission.submit;
  yield* screen.clip(index, "accepted");
  return yield* h3.operation(submission);
});

/** Follows one clip through H3's facts to its end. */
const follow = Effect.fn("follow")(
  function* (screen: Screen.Screen, index: number, clip: H3.ClipOperation) {
    yield* clip.reached("generated");
    yield* screen.clip(index, "generated");
    yield* clip.reached("started");
    yield* screen.clip(index, "playing");
    yield* clip.ended;
    yield* screen.clip(index, "ended");
  },
  // A clip whose build failed, or that was dropped, ends early; the show goes on.
  (effect, screen, index) =>
    effect.pipe(
      Effect.catchReason("ReactorError", "ClipEnded", () => screen.clip(index, "failed")),
    ),
);

/**
 * Fails, with the reason, once the session cannot come back: Reactor ended it (at its cap, or on
 * a moderation verdict) or it stopped reconnecting. H3 then reports nothing more about any clip,
 * so whatever waits on one waits on this too.
 */
const lost = (session: Session.Session): Effect.Effect<never, ReactorError.ReactorError> =>
  session.changes.pipe(
    Stream.filter((snapshot) => snapshot.status === "disconnected" && !snapshot.reconnecting),
    Stream.runHead,
    Effect.flatMap((snapshot) =>
      Effect.fail(
        Option.getOrUndefined(snapshot)?.lastError ??
          ReactorError.ReactorError.fromCode("Disconnected", "the session's connection is gone"),
      ),
    ),
  );

/** The show: one H3 session plays the prompts in order, and its video is drawn here. */
const watch = Effect.fn("watch")(function* (
  prompts: ReadonlyArray<string>,
  references: ReadonlyArray<H3.ValidatedReference>,
) {
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  // Before anything is allocated: the most this run can cost, at the rate Reactor states.
  const rate = yield* CoordinatorClient.modelRate(yield* coordinator.pricing, H3.modelName);
  yield* Console.log(mostItCosts(rate));

  const session = yield* reactor.create({
    model: H3.modelName,
    // Minted in this process from the API key; each token caps the session it creates.
    tokens: coordinator.tokens({
      modelName: H3.modelName,
      maxSessionDuration: `${capSeconds} seconds`,
    }),
  });
  // However the show ends (done, failed or Ctrl-C), close the session and say whether Reactor
  // confirmed its end. The screen, taken after this, is given back before.
  yield* Effect.addFinalizer(() =>
    session.close.pipe(Effect.flatMap((report) => Console.log(closed(session.id, report)))),
  );
  yield* Console.log(`session ${session.id} connected`);

  const screen = yield* Screen.make({ session: session.id, prompts });
  const h3 = yield* H3.make(session);
  // H3 plays nothing on its own: ask it to play each clip as soon as it is ready.
  yield* h3.setAutoplay(true);
  // The frames themselves, decoded in this process.
  const media = yield* session.decoded;
  yield* screen.show(media.video("main_video"));

  const play = Effect.gen(function* () {
    // Sent one after another, so that H3's queue keeps the playlist's order.
    const clips = yield* Effect.forEach(prompts, (prompt, index) =>
      send(h3, screen, index, { prompt, seconds: clipSeconds, references }),
    );
    yield* Effect.forEach(clips, (clip, index) => follow(screen, index, clip), {
      concurrency: "unbounded",
      discard: true,
    });
  });
  yield* play.pipe(Effect.raceFirst(lost(session)));
}, Effect.scoped);

/**
 * Reactor simulated in memory, at the timing paid runs measured on hosted H3: no key, no native
 * code, nothing billed. Its frames are a solid colour per clip, in H3's shape. Its pricing states
 * H3's rate as Reactor's did on 2026-09-30, so it prints the bound a live run prints.
 */
const Simulated = Reactor.layer().pipe(
  Layer.provideMerge(CoordinatorClient.layer({ apiKey: Redacted.make("simulated") })),
  Layer.provide(
    ReactorTest.layer({
      timing: ReactorTest.Timing.hosted,
      apiKey: "simulated",
      width: 320,
      height: 180,
      creditsPerSecond: 350,
    }),
  ),
);

/** Hosted Reactor: the API key stays in this process, and the native peer decodes the media here. */
const Hosted = (apiKey: Redacted.Redacted<string>, apiUrl: string) =>
  Reactor.layer().pipe(
    Layer.provideMerge(CoordinatorClient.layer({ apiUrl, apiKey })),
    Layer.provide(Layer.mergeAll(NativePeer.layer(), FetchHttpClient.layer)),
  );

/** Hosted Reactor when REACTOR_API_KEY is set, the simulation otherwise: `watch` runs on either. */
const Backend = Layer.unwrap(
  Effect.gen(function* () {
    const apiKey = yield* Config.option(Config.Redacted("REACTOR_API_KEY"));
    if (Option.isNone(apiKey)) {
      yield* Console.log(
        "offline: Reactor simulated in memory, priced as hosted H3; nothing is billed " +
          "(set REACTOR_API_KEY to watch hosted Reactor)",
      );
      return Simulated;
    }
    const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(
      Config.withDefault(CoordinatorClient.defaultApiUrl),
    );
    yield* Console.log(`live: hosted Reactor at ${apiUrl}; a session bills from ready to its end`);
    return Hosted(apiKey.value, apiUrl);
  }),
);

const terminal = Command.make(
  "terminal",
  {
    prompts: Argument.String("prompt").pipe(
      Argument.withDescription(
        "What a clip shows, up to ten played in order; a short playlist plays when none is given",
      ),
      // Ten clips of 8 seconds fit in the session's cap.
      Argument.variadic({ max: 10 }),
    ),
    reference: Flag.File("reference", { mustExist: true }).pipe(
      Flag.withDescription("An image every clip starts from (PNG, JPEG or WebP)"),
      Flag.optional,
    ),
  },
  Effect.fn(function* ({ prompts, reference }) {
    // An image H3 would refuse, or one larger than a session uploads, is refused here, before a
    // session is allocated and billed.
    const references = yield* Option.match(reference, {
      onNone: () => Effect.succeed([]),
      onSome: (path) => Effect.map(References.image(References.file(path)), (image) => [image]),
    });
    yield* watch(prompts.length > 0 ? prompts : playlist, references);
  }),
).pipe(
  Command.provide(Backend),
  Command.withDescription(
    "Watch Reactor H3 in this terminal: hosted Reactor with REACTOR_API_KEY set, simulated otherwise",
  ),
);

terminal.pipe(
  Command.run({ version: "0.10.0" }),
  // The program's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
