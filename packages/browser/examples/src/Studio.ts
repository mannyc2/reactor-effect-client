/**
 * The Studio: one H3 session at a time, H3's queue as the provider reports it,
 * and each clip sent to it followed to its end. Its handlers are ordinary DOM
 * code that run effects through the page's `ManagedRuntime`; the same code
 * runs live and offline, because the runtime's `Stage` carries what differs.
 */
import {
  DateTime,
  Duration,
  Effect,
  Exit,
  FiberHandle,
  Result,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import type { ManagedRuntime } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as H3 from "reactor-effect-client/H3";
import * as Reactor from "reactor-effect-client/Reactor";
import { isReactorFailure, ReactorError, summarize } from "reactor-effect-client/ReactorError";
import type { ReactorFailure } from "reactor-effect-client/ReactorError";
import * as Session from "reactor-effect-client/Session";
import * as Page from "./Page.ts";
import { Stage } from "./Stage.ts";
import type { Services } from "./Stage.ts";

const clock = DateTime.formatLocal({
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  fractionalSecondDigits: 3,
  hourCycle: "h23",
});

/** Puts an entry, stamped with the local time, at the top of the event log. */
const log = Effect.fnUntraced(function* (kind: Page.Kind, text: string, failure?: ReactorFailure) {
  const time = clock(yield* DateTime.now);
  Page.log({ time, kind, text, ...(failure === undefined ? {} : { failure: summarize(failure) }) });
});

/** The same, from the page's event handlers. */
const note = (kind: Page.Kind, text: string): void => Effect.runSync(log(kind, text));

/** A promise's rejection, logged with its tag, reason and outcome when it is the client's. */
const rejected = (text: string, error: unknown): void =>
  isReactorFailure(error)
    ? Effect.runSync(log("failure", text, error))
    : note("failure", `${text}: ${String(error)}`);

/** One open session, and what the page holds of it until Stop. */
interface Live {
  readonly scope: Scope.Closeable;
  readonly session: Session.Session;
  readonly provider: H3.Provider;
  /** The clips this page sent, in the order it sent them. */
  readonly cards: SubscriptionRef.SubscriptionRef<ReadonlyArray<Page.Card>>;
  /** The sound, while it is on. */
  readonly sound: FiberHandle.FiberHandle;
}

/**
 * Runs `present` on each ready connection generation of the session in turn.
 * A reconnect hands the page new media: the last generation's goes as the
 * next one's starts.
 */
const following = Effect.fnUntraced(function* (
  session: Session.Session,
  present: (session: Session.Session) => Effect.Effect<void, ReactorError, Scope.Scope>,
  what: string,
) {
  yield* session.changes.pipe(
    Stream.filter((snapshot) => snapshot.status === "ready"),
    Stream.changesWith((a, b) => a.generation === b.generation),
    Stream.switchMap((snapshot) =>
      Stream.fromEffect(
        Effect.scoped(present(session)).pipe(
          // The session's own close ends its media too, which is not news.
          Effect.catch((error) =>
            Effect.flatMap(session.snapshot, (now) =>
              now.status === "closing" || now.status === "closed"
                ? Effect.void
                : log(
                    "session",
                    `${what} of generation ${String(snapshot.generation)} ended`,
                    error,
                  ),
            ),
          ),
        ),
      ),
    ),
    Stream.runDrain,
  );
});

const statusLine = (snapshot: Session.Snapshot): string => {
  const generation = `generation ${String(snapshot.generation)}`;
  switch (snapshot.status) {
    case "ready":
      return `session ready · ${generation}`;
    case "disconnected":
      return snapshot.reconnecting
        ? `connection dropped · reconnecting ${generation}`
        : `connection down for good · ${generation}`;
    default:
      return `session ${snapshot.status}`;
  }
};

/** The session panel follows every snapshot; the log, every change of status. */
const watch = Effect.fnUntraced(function* (session: Session.Session) {
  yield* session.changes.pipe(
    Stream.tap((snapshot) => Effect.sync(() => Page.session(snapshot))),
    Stream.changesWith(
      (a, b) =>
        a.status === b.status && a.generation === b.generation && a.reconnecting === b.reconnecting,
    ),
    Stream.runForEach((snapshot) =>
      log(
        "session",
        statusLine(snapshot),
        snapshot.status === "disconnected" ? snapshot.lastError : undefined,
      ),
    ),
  );
});

/** What H3 said, for the log; its state and queue reads show in the queue panel instead. */
const said = (message: H3.DecodedMessage): string | undefined => {
  switch (message.type) {
    case "queue_update":
    case "state_update":
      return undefined;
    case "clip_queued":
    case "clip_generated":
    case "clip_started":
    case "clip_finished":
    case "clip_stopped":
    case "clip_popped":
      return `${message.type} ${Page.short(message.data.clip.clip_id)}`;
    case "clip_failed":
      // H3's reason is provider text: it stays out of the page.
      return `clip_failed ${Page.short(message.data.clip.clip_id)}`;
    case "clip_moved":
      return `clip_moved ${Page.short(message.data.clip.clip_id)} to ${message.data.queue} #${String(message.data.position)}`;
    case "command_error":
      return `command_error: H3 refused ${message.data.command}`;
    case "autoplay_accepted":
    case "flush_accepted":
      return `${message.type}: ${message.data.enabled ? "on" : "off"}`;
    case "unknown":
      return `${message.name} (not an H3 message this SDK knows)`;
    default:
      return message.type;
  }
};

/** The provider's events, as the log shows them. */
const record = (event: H3.ProviderEvent): Effect.Effect<void> => {
  switch (event._tag) {
    case "Message": {
      const text = event.disposition === "applied" ? said(event.message) : undefined;
      return text === undefined ? Effect.void : log("h3", text);
    }
    case "Acceptance":
      return log(
        "h3",
        `accepted ${Page.short(event.acceptance.clip.clip_id)} on ${event.acceptance.evidence.kind} evidence`,
      );
    case "Diagnostic":
      return log("failure", "H3 diagnostic", event.error);
    case "Acknowledged":
      return Effect.void;
    case "Session": {
      const source = event.source;
      switch (source._tag) {
        case "Track":
          return log("session", `track ${source.name} received`);
        case "Decoded":
          return log("session", `first decoded ${source.kind} on ${source.name}`);
        case "Moderation":
          return log("failure", `content moderation: ${source.action}`);
        case "Upload":
          return source.progress.notification === "submitted"
            ? log("session", `reference uploaded (${source.progress.file?.mimeType ?? "file"})`)
            : Effect.void;
        case "CommandError":
          return log("failure", "a command failed", source.error);
        case "Diagnostic":
          return log("failure", "session diagnostic", source.error);
        case "Control":
          return log("session", `control message ${source.message._tag}`);
        case "Status":
          return Effect.void;
      }
    }
  }
};

/**
 * Allocates and connects a session, then attaches the page to it: its
 * picture, its H3 provider, the queue and the log. All of it lives in a scope
 * the page holds until Stop, so one close releases everything.
 */
const open = Effect.gen(function* () {
  const reactor = yield* Reactor.Reactor;
  const stage = yield* Stage;
  const scope = yield* Scope.make();
  return yield* Effect.gen(function* () {
    yield* log("session", "creating an H3 session");
    const [took, session] = yield* Effect.timed(reactor.create({ model: H3.modelName }));
    yield* log(
      "session",
      `session ${session.id} connected ${(Duration.toMillis(took) / 1000).toFixed(2)} s after create`,
    );
    yield* watch(session).pipe(Effect.forkScoped);
    yield* following(session, stage.picture, "picture").pipe(Effect.forkScoped);

    const provider = yield* H3.make(session);
    // Observing here, before any command, means the log misses none of their replies.
    const observation = yield* provider.observe({ capacity: 256 });
    yield* observation.events.pipe(
      Stream.runForEach(record),
      Effect.catch((error) => log("failure", "the event log stopped", error)),
      Effect.forkScoped,
    );
    const { deployment, commands } = provider.contract;
    yield* log(
      "h3",
      `${deployment.title ?? H3.modelName} ${deployment.version ?? ""} offers ${[...commands].join(", ")}`,
    );
    // H3 starts with autoplay off and flushes to black at each clip's end. The
    // Studio plays clips as they become ready and holds each one's last frame
    // until the next starts; both are toggles in the session panel.
    if (commands.has("set_autoplay")) yield* provider.setAutoplay(true);
    if (commands.has("set_flush_on_clip_end")) yield* provider.setFlushOnClipEnd(false);

    const cards = yield* SubscriptionRef.make<ReadonlyArray<Page.Card>>([]);
    yield* provider.changes.pipe(
      // A change the provider made to its own bookkeeping leaves the snapshot as it was.
      Stream.changesWith((a, b) => a.revision === b.revision && a._tag === b._tag),
      Stream.zipLatest(SubscriptionRef.changes(cards)),
      Stream.runForEach(([snapshot, sent]) =>
        Effect.sync(() => {
          Page.provider({ snapshot, contract: provider.contract });
          Page.queue({ snapshot, cards: sent, contract: provider.contract });
        }),
      ),
      Effect.forkScoped,
    );
    const sound = yield* FiberHandle.make();
    const live: Live = { scope, session, provider, cards, sound };
    return live;
  }).pipe(
    Scope.provide(scope),
    Effect.onError(() => Scope.close(scope, Exit.void)),
  );
});

/** Draws a change on the clip's card. */
const patch = (live: Live, key: string, change: Partial<Page.Card>) =>
  SubscriptionRef.update(live.cards, (cards) =>
    cards.map((card) => (card.key === key ? { ...card, ...change } : card)),
  );

/**
 * One clip: prepared, submitted, then followed through its operation's facts.
 * A definite failure decides it. An enqueue whose outcome is unknown is never
 * sent again, and a clip that names its submission can still prove it, so the
 * card keeps following its operation.
 */
const follow = Effect.fn("follow")(
  function* (live: Live, key: string, request: H3.Request) {
    const submission = yield* live.provider.prepare(request);
    const submitted = yield* Effect.result(submission.submit);
    if (Result.isFailure(submitted)) {
      yield* patch(live, key, { failure: summarize(submitted.failure) });
      yield* log("failure", `enqueue of "${request.prompt.slice(0, 40)}"`, submitted.failure);
      if (submitted.failure.context.outcome !== "unknown") return;
    }
    const operation = yield* live.provider.operation(submission);
    const accepted = yield* operation.accepted;
    yield* patch(live, key, {
      clipId: accepted.clip.clip_id,
      seconds: accepted.clip.seconds,
      evidence: accepted.evidence.kind,
    });
    // A clip popped or failed before a phase ends without it: `ended` says how.
    if (Exit.isSuccess(yield* Effect.exit(operation.reached("generated"))))
      yield* patch(live, key, { generated: true });
    if (Exit.isSuccess(yield* Effect.exit(operation.reached("started"))))
      yield* patch(live, key, { started: true });
    const ended = yield* operation.ended;
    yield* patch(live, key, { ended: ended.message });
  },
  (effect, live, key) =>
    effect.pipe(
      Effect.catch((error) =>
        patch(live, key, { failure: summarize(error) }).pipe(
          Effect.andThen(log("failure", "a clip's operation failed", error)),
        ),
      ),
    ),
  Effect.scoped,
);

/** Puts a card for the clip on the page, then follows it. */
const send = Effect.fn("send")(function* (live: Live, request: H3.Request) {
  const key = yield* SubscriptionRef.modify(live.cards, (cards) => {
    const card: Page.Card = {
      key: `card-${String(cards.length + 1)}`,
      prompt: request.prompt,
      images: request.references?.length ?? 0,
    };
    return [card.key, [...cards, card]] as const;
  });
  yield* follow(live, key, request);
});

/** A control on a clip, sent to H3; a refusal is logged with its outcome. */
const act = Effect.fn("act")(
  function* (live: Live, action: Page.Action) {
    switch (action._tag) {
      case "play":
        yield* log("command", `play ${Page.short(action.clipId)}`);
        yield* live.provider.play(action.clipId);
        return;
      case "pop":
        yield* log("command", `pop ${Page.short(action.clipId)}`);
        yield* live.provider.pop(action.clipId);
        return;
      case "move":
        yield* log("command", `move ${Page.short(action.clipId)} to #${String(action.position)}`);
        yield* live.provider.move(action.clipId, action.position);
        return;
      case "stop":
        yield* log("command", "stop the playing clip");
        yield* live.provider.stop;
        return;
    }
  },
  (effect, _live, action) =>
    effect.pipe(Effect.catch((error) => log("failure", `${action._tag} failed`, error))),
);

/** A playback setting, sent to H3; the toggle then shows what H3's state reports. */
const setting = Effect.fn("setting")(
  function* (live: Live, which: "autoplay" | "hold", on: boolean) {
    yield* log("command", `${which} ${on ? "on" : "off"}`);
    if (which === "autoplay") yield* live.provider.setAutoplay(on);
    else yield* live.provider.setFlushOnClipEnd(!on);
  },
  // A refused setting leaves the toggle showing H3's state, not the click.
  (effect, live, which) =>
    effect.pipe(
      Effect.catch((error) =>
        log("failure", `${which} failed`, error).pipe(
          Effect.andThen(live.provider.snapshot),
          Effect.flatMap((snapshot) =>
            Effect.sync(() => Page.provider({ snapshot, contract: live.provider.contract })),
          ),
        ),
      ),
    ),
);

/** Reads a dropped or picked file and validates it once, so every clip reuses it as it is. */
const validate = Effect.fn("validate")(function* (bytes: Effect.Effect<ArrayBuffer, ReactorError>) {
  return yield* H3.validateReference({ _tag: "Bytes", bytes: new Uint8Array(yield* bytes) });
});

const readFile = (file: Blob) =>
  Effect.tryPromise({
    try: () => file.arrayBuffer(),
    catch: (cause) =>
      ReactorError.fromCode("InvalidInput", "the file could not be read", {
        operation: "H3 reference",
        outcome: "not-submitted",
        detail: cause,
      }),
  });

/**
 * A small picture drawn in the page, for trying references without a file: a
 * paper boat on dusk water.
 */
const sampleImage = Effect.tryPromise({
  try: () => {
    const canvas = new OffscreenCanvas(768, 432);
    const context = canvas.getContext("2d");
    if (context === null) return Promise.reject(new Error("no 2D context"));
    const sky = context.createLinearGradient(0, 0, 0, 300);
    sky.addColorStop(0, "#1b1035");
    sky.addColorStop(0.65, "#d65a8a");
    sky.addColorStop(1, "#ffb36b");
    context.fillStyle = sky;
    context.fillRect(0, 0, 768, 300);
    context.fillStyle = "#11203a";
    context.fillRect(0, 300, 768, 132);
    context.fillStyle = "#f5f1e8";
    context.beginPath();
    context.moveTo(290, 300);
    context.lineTo(478, 300);
    context.lineTo(436, 344);
    context.lineTo(332, 344);
    context.closePath();
    context.moveTo(384, 186);
    context.lineTo(384, 294);
    context.lineTo(318, 294);
    context.closePath();
    context.fill();
    return canvas.convertToBlob({ type: "image/png" });
  },
  catch: (cause) =>
    ReactorError.fromCode("UnsupportedCapability", "the page could not draw a sample image", {
      operation: "H3 reference",
      outcome: "not-submitted",
      detail: cause,
    }),
});

/**
 * Requests that outlive the page. A page that goes away mid-close drops its
 * ordinary requests, so the session's DELETE would never reach Reactor and the
 * session would run on to its cap. The close starts its DELETE before the
 * `pagehide` handler returns, and a keepalive request is delivered after the
 * page is gone. The close sends no body, within keepalive's 64 KiB.
 * `CoordinatorClient` sets its own `RequestInit`, so the flag goes through the
 * `fetch` it calls; requests that do not use `fetch`, such as the simulator's,
 * are unaffected.
 */
const outliving = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.updateService(
    effect,
    FetchHttpClient.Fetch,
    (fetch): typeof globalThis.fetch =>
      (input, init) =>
        fetch(input, { ...init, keepalive: true }),
  );

/** The page's state: at most one session, which takes seconds to start. */
type State =
  | { readonly _tag: "Idle" }
  | { readonly _tag: "Starting"; stopRequested: boolean }
  | { readonly _tag: "Live"; readonly live: Live; sound: boolean }
  | { readonly _tag: "Stopping" };

/**
 * Wires the page to a runtime built from one of the two layers. Nothing here
 * asks which: the runtime's `Stage` says how this page shows a session.
 */
export const mount = <E>(runtime: ManagedRuntime.ManagedRuntime<Services, E>): void => {
  const {
    start: startButton,
    reconnect: reconnectButton,
    sound: soundButton,
    stop: stopButton,
    send: sendButton,
    prompt,
    seconds,
    files,
    references: zone,
    empty,
  } = Page.ui;

  let state: State = { _tag: "Idle" };
  let staged = false;
  let references: ReadonlyArray<Page.Reference> = [];
  let referenceIds = 0;

  /** Every control's availability follows the state. */
  const controls = (): void => {
    const live = state._tag === "Live";
    startButton.disabled = !staged || state._tag !== "Idle";
    reconnectButton.disabled = !live;
    soundButton.disabled = !live;
    soundButton.textContent = state._tag === "Live" && state.sound ? "Sound off" : "Sound on";
    stopButton.disabled = !(live || state._tag === "Starting");
    sendButton.disabled = !live || prompt.value.trim() === "";
    empty.hidden = state._tag !== "Idle";
  };

  // The seconds a request may ask for, from H3's documented bounds.
  seconds.min = String(H3.requestSeconds.min);
  seconds.max = String(H3.requestSeconds.max);
  Page.length(Number(seconds.value));
  Page.tokens(prompt.value);
  Page.references({ list: references, onRemove: () => undefined });
  controls();

  runtime.runPromise(Stage.useSync((stage) => stage)).then(
    (stage) => {
      staged = true;
      Page.mode({
        name: stage.mode,
        about: stage.about,
        tone: stage.mode === "Live" ? "live" : "offline",
      });
      Page.faults({
        list: stage.faults,
        onArm: (fault) => {
          runtime.runPromise(fault.arm).then(
            () => note("fault", `armed: ${fault.label}`),
            (error: unknown) => rejected("arming a fault", error),
          );
        },
      });
      note("page", `${stage.mode}: ready to start a session`);
      controls();
    },
    (error: unknown) => {
      Page.mode({
        name: "Unavailable",
        about: "This browser cannot run the Studio: the log says why.",
        tone: "none",
      });
      rejected("building the page's runtime", error);
    },
  );

  startButton.addEventListener("click", () => {
    if (state._tag !== "Idle") return;
    const starting: State = { _tag: "Starting", stopRequested: false };
    state = starting;
    Page.hideReport();
    controls();
    runtime.runPromise(open).then(
      (live) => {
        state = { _tag: "Live", live, sound: false };
        controls();
        if (starting.stopRequested) void stop(false);
      },
      (error: unknown) => {
        state = { _tag: "Idle" };
        controls();
        rejected("could not start", error);
      },
    );
  });

  /**
   * Stop closes the session, which terminates it and returns its report, then
   * the scope, which releases the picture, the observers and every clip's
   * follower and reuses that report.
   */
  const stop = (leaving: boolean): Promise<void> => {
    if (state._tag === "Starting") {
      state.stopRequested = true;
      note("page", "stopping once the session has connected");
      return Promise.resolve();
    }
    if (state._tag !== "Live") return Promise.resolve();
    const { live } = state;
    state = { _tag: "Stopping" };
    controls();
    note("command", "stop the session");
    return runtime
      .runPromise(
        Effect.gen(function* () {
          const close = live.session.close;
          const report = yield* leaving ? outliving(close) : close;
          yield* Scope.close(live.scope, Exit.void);
          return {
            report,
            last: yield* live.session.snapshot,
            cards: yield* SubscriptionRef.get(live.cards),
          };
        }),
      )
      .then(
        ({ report, last, cards }) => {
          Page.session(last);
          Page.provider({ snapshot: undefined, contract: undefined });
          Page.report({ closed: report, mayStillBill: Session.mayStillBill(report) });
          Page.closedQueue(cards);
          note(
            "session",
            report.remote.confirmed
              ? "session closed: termination confirmed"
              : "session closed: termination not confirmed",
          );
        },
        (error: unknown) => rejected("closing the session", error),
      )
      .finally(() => {
        state = { _tag: "Idle" };
        controls();
      });
  };
  stopButton.addEventListener("click", () => void stop(false));

  reconnectButton.addEventListener("click", () => {
    if (state._tag !== "Live") return;
    note("command", "reconnect: a new connection generation of the same session");
    runtime.runPromise(state.live.session.reconnect).then(
      () => undefined,
      (error: unknown) => rejected("reconnect", error),
    );
  });

  // Sound needs its own click: browsers refuse sound that starts long after
  // the gesture that asked for it.
  soundButton.addEventListener("click", () => {
    if (state._tag !== "Live") return;
    const current = state;
    const { live } = current;
    current.sound = !current.sound;
    controls();
    const toggle = current.sound
      ? Stage.use((stage) =>
          FiberHandle.run(live.sound, following(live.session, stage.sound, "sound")),
        )
      : FiberHandle.clear(live.sound);
    runtime.runPromise(toggle).then(
      () => note("page", current.sound ? "sound on" : "sound off"),
      (error: unknown) => rejected("sound", error),
    );
  });

  Page.ui.composer.addEventListener("submit", (event) => {
    event.preventDefault();
    if (state._tag !== "Live" || prompt.value.trim() === "") return;
    const { live } = state;
    const request: H3.Request = {
      prompt: prompt.value.trim(),
      seconds: Number(seconds.value),
      references: references.map((entry) => entry.reference),
    };
    runtime.runFork(send(live, request).pipe(Effect.forkIn(live.scope)));
  });
  prompt.addEventListener("input", () => {
    Page.tokens(prompt.value);
    controls();
  });
  seconds.addEventListener("input", () => Page.length(Number(seconds.value)));

  Page.ui.queue.addEventListener("click", (event) => {
    const action = Page.actionOf(event.target);
    if (action === undefined || state._tag !== "Live") return;
    const { live } = state;
    runtime.runFork(act(live, action).pipe(Effect.forkIn(live.scope)));
  });
  for (const which of ["autoplay", "hold"] as const) {
    const toggle = Page.ui[which];
    toggle.addEventListener("change", () => {
      if (state._tag !== "Live") return;
      const { live } = state;
      runtime.runFork(setting(live, which, toggle.checked).pipe(Effect.forkIn(live.scope)));
    });
  }

  const remove = (id: number): void => {
    const gone = references.find((entry) => entry.id === id);
    if (gone !== undefined) URL.revokeObjectURL(gone.url);
    references = references.filter((entry) => entry.id !== id);
    Page.references({ list: references, onRemove: remove });
  };
  /** Validates one image once; every clip after it sends the same validated reference. */
  const add = (name: string, blob: Blob): void => {
    if (references.length >= H3.referenceLimits.maxImages) {
      note("page", `H3 takes at most ${String(H3.referenceLimits.maxImages)} reference images`);
      return;
    }
    runtime.runPromise(validate(readFile(blob))).then(
      (reference) => {
        Page.composerFailure(undefined);
        referenceIds += 1;
        references = [
          ...references,
          { id: referenceIds, name, url: URL.createObjectURL(blob), reference },
        ];
        Page.references({ list: references, onRemove: remove });
        note(
          "page",
          `reference ${name} validated: ${reference.mimeType}, ${String(reference.size)} bytes`,
        );
      },
      (error: unknown) => {
        if (isReactorFailure(error)) Page.composerFailure({ name, summary: summarize(error) });
        rejected(`reference ${name}`, error);
      },
    );
  };
  Page.ui.addImages.addEventListener("click", () => files.click());
  files.addEventListener("change", () => {
    for (const file of files.files ?? []) add(file.name, file);
    files.value = "";
  });
  Page.ui.sampleImage.addEventListener("click", () => {
    runtime.runPromise(sampleImage).then(
      (blob) => add("paper-boat.png", blob),
      (error: unknown) => rejected("sample image", error),
    );
  });
  zone.addEventListener("dragover", (event) => {
    event.preventDefault();
    zone.classList.add("dragging");
  });
  zone.addEventListener("dragleave", () => zone.classList.remove("dragging"));
  zone.addEventListener("drop", (event) => {
    event.preventDefault();
    zone.classList.remove("dragging");
    for (const file of event.dataTransfer?.files ?? []) add(file.name, file);
  });

  // Leaving the page ends its session. A page the browser keeps in its
  // back/forward cache comes back to this same runtime, so only a page that is
  // going away for good disposes it.
  window.addEventListener("pagehide", (event) => {
    if (event.persisted) {
      void stop(true);
      return;
    }
    void stop(true).then(
      () => runtime.dispose(),
      () => runtime.dispose(),
    );
  });
};
