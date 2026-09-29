/** The playout's pure policy on its own: inputs in, actions and as-run out, no clock and no I/O. */
import { assert, describe, it } from "@effect/vitest";
import { Config, Effect, Option, Redacted, Schema } from "effect";
import * as Policy from "../src/internal/playout/policy.js";
import type { ClipTag, SourceClip, SourceEvent, SourceState } from "../src/Playout.js";
import { ItemKey } from "../src/Playout.js";
import { CommandFailure, ReactorError } from "../src/ReactorError.js";

const config: Policy.Config = {
  lanes: [
    { name: "urgent", conflict: "queue", cut: true },
    { name: "line", conflict: "queue", cut: false },
  ],
  filler: undefined,
  maxBuildsInFlight: 1,
  maxHistory: 64,
  unknownTimeoutMs: 60_000,
  leadMs: 30_000,
  graceMs: 250,
  maxSetupFailures: 3,
  maxModerations: 2,
};

const key = (value: string) => ItemKey.make(value);
const spec = (name: string, lane = 1, seconds = 5): Policy.Spec => ({
  key: key(name),
  lane,
  request: { prompt: name, seconds },
  seconds,
  fingerprint: name,
  cues: [],
  continuity: false,
  start: { _tag: "Follow" },
});
const clip = (clipId: string, tag?: ClipTag, seconds = 5): SourceClip => ({ clipId, tag, seconds });
const item = (name: string): ClipTag => ({ _tag: "Item", key: key(name) });
const source = (partial: Partial<SourceState> = {}): SourceState => ({
  available: true,
  building: [],
  ready: [],
  playing: undefined,
  continuable: [],
  ...partial,
});

/** A command that failed: never sent, answered with a refusal, or sent with its reply lost. */
const failed = (outcome: "not-submitted" | "replied" | "unknown"): Policy.CommandResult => ({
  _tag: "Failed",
  cause: CommandFailure.from(
    ReactorError.fromCode("InvalidState", "the provider refused it"),
    outcome === "not-submitted"
      ? { operation: "enqueue", outcome }
      : { operation: "enqueue", outcome, requestId: "request", generation: 1n },
  ),
});
/** The provider failed `value`'s build. */
const buildFailed = (value: SourceClip): SourceEvent => ({
  _tag: "Failed",
  clip: value,
  message: "the build failed",
  provider: Redacted.make("the provider's words"),
});

/** Runs inputs in order at one-millisecond steps and collects every action. */
const run = (
  inputs: ReadonlyArray<Policy.Input>,
  start: Policy.State = Policy.initial,
  from = 0,
) => {
  let state = start;
  const actions: Array<Policy.Action> = [];
  inputs.forEach((input, index) => {
    const result = Policy.step(config, state, input, { mono: from + index, wall: from + index });
    state = result.state;
    actions.push(...result.actions);
  });
  return { state, actions };
};
const commands = (actions: ReadonlyArray<Policy.Action>) =>
  actions.flatMap((action) => (action._tag === "Command" ? [action] : []));
const statuses = (actions: ReadonlyArray<Policy.Action>, name: string) =>
  actions.flatMap((action) =>
    action._tag === "Emit" && action.event._tag === "AsRun" && action.event.event.key === name
      ? [action.event.event.status._tag]
      : [],
  );
/** A session opened with autoplay already confirmed, so the next command is the plan's. */
const opened = (id = "s1"): ReadonlyArray<Policy.Input> => [
  { _tag: "Opened", sessionId: id, lifetimeMs: 600_000 },
  { _tag: "Source", sessionId: id, event: { _tag: "State", state: source() } },
  { _tag: "Result", id: 1, result: { _tag: "Done" } },
];

/** Steps one input at `at` milliseconds, on both clocks. */
const at = (state: Policy.State, input: Policy.Input, time: number) =>
  Policy.step(config, state, input, { mono: time, wall: time });
/** The command in flight on `sessionId`'s lane, or on the first lane that has one. */
const inFlight = (state: Policy.State, sessionId?: string) =>
  state.sessions.find(
    (value) => value.busy !== undefined && (sessionId === undefined || value.id === sessionId),
  )?.busy;
/** The answer to the command in flight on `sessionId`'s lane, or on the first busy one. */
const answer = (
  state: Policy.State,
  result: Policy.CommandResult,
  sessionId?: string,
): Policy.Input => ({ _tag: "Result", id: inFlight(state, sessionId)?.id ?? -1, result });

/**
 * A policy driven input by input, each at the time given: the state it reached and every action,
 * with shorthands for the inputs most cases send.
 */
const drive = (options: { readonly config?: Policy.Config; readonly from?: Policy.State } = {}) => {
  const settings = options.config ?? config;
  let state = options.from ?? Policy.initial;
  const actions: Array<Policy.Action> = [];
  let clock = 0;
  const send = (input: Policy.Input, time = clock + 1) => {
    clock = Math.max(clock, time);
    const result = Policy.step(settings, state, input, { mono: clock, wall: clock });
    state = result.state;
    actions.push(...result.actions);
    return result;
  };
  return {
    state: () => state,
    actions,
    now: () => clock,
    send,
    /** Opens `id` with `lifetimeMs` and confirms whatever autoplay it asks for first. */
    open: (id = "s1", lifetimeMs = 600_000, time?: number) => {
      send({ _tag: "Opened", sessionId: id, lifetimeMs }, time);
      send({ _tag: "Source", sessionId: id, event: { _tag: "State", state: source() } });
      if (inFlight(state, id)?.command._tag === "Autoplay")
        send(answer(state, { _tag: "Done" }, id));
    },
    submit: (value: Policy.Spec, time?: number) =>
      send(
        {
          _tag: "Edit",
          id: 1000 + actions.length,
          edits: [{ _tag: "Submit", spec: value }],
          batch: false,
        },
        time,
      ),
    edit: (edits: ReadonlyArray<Policy.EditInput>, batch = false, time?: number) =>
      send({ _tag: "Edit", id: 1000 + actions.length, edits, batch }, time),
    /** Answers the command in flight on `id`'s lane, or on the first busy one. */
    reply: (result: Policy.CommandResult, time?: number, id?: string) =>
      send(answer(state, result, id), time),
    observe: (partial: Partial<SourceState>, id = "s1", time?: number) =>
      send(
        { _tag: "Source", sessionId: id, event: { _tag: "State", state: source(partial) } },
        time,
      ),
    event: (event: SourceEvent, id = "s1", time?: number) =>
      send({ _tag: "Source", sessionId: id, event }, time),
    tick: (time?: number) => send({ _tag: "Tick" }, time),
    /** The command in flight on `id`'s lane, or on the first busy one, if any. */
    busy: (id?: string) => inFlight(state, id)?.command,
  };
};

describe("PlayoutPolicy", () => {
  it("opens a session, turns autoplay on, then builds the first item in lane order", () => {
    const { actions } = run([
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("b") }], batch: false },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("a", 0) }], batch: false },
      ...opened(),
    ]);
    assert.isTrue(actions.some((action) => action._tag === "Open"));
    const [autoplay, build] = commands(actions);
    assert.deepStrictEqual(autoplay?.command, { _tag: "Autoplay", enabled: true });
    assert.deepStrictEqual(
      build?.command._tag === "Enqueue" ? build.command.tag : undefined,
      item("a"),
    );
  });

  it("orders a session's Ready clips by rank and never moves a clip it does not own", () => {
    const { actions } = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("b") }], batch: false },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("a", 0) }], batch: false },
      { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "ca" } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: {
          _tag: "State",
          state: source({ ready: [clip("foreign"), clip("cb", item("b")), clip("ca", item("a"))] }),
        },
      },
    ]);
    const move = commands(actions).find((action) => action.command._tag === "Move");
    assert.deepStrictEqual(move?.command, { _tag: "Move", clipId: "ca", position: 1 });
  });

  it("cuts only a strictly lower lane's clip, and only with the cutter at the front", () => {
    const cut = (playing: SourceClip) =>
      commands(
        run([
          ...opened(),
          {
            _tag: "Edit",
            id: 1,
            edits: [{ _tag: "Submit", spec: spec("urgent", 0) }],
            batch: false,
          },
          {
            _tag: "Edit",
            id: 2,
            edits: [{ _tag: "Submit", spec: spec("other", 0) }],
            batch: false,
          },
          { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "cu" } },
          { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: playing } },
          {
            _tag: "Source",
            sessionId: "s1",
            event: {
              _tag: "State",
              state: source({ playing, ready: [clip("cu", item("urgent"))] }),
            },
          },
          { _tag: "Result", id: 3, result: { _tag: "Done" } },
        ]).actions,
      ).find((action) => action.command._tag === "Stop");
    assert.deepStrictEqual(cut(clip("filler", { _tag: "Filler", index: 0 }, 15))?.command, {
      _tag: "Stop",
      clipId: "filler",
    });
    assert.isUndefined(cut(clip("peer", item("other"), 15)));
  });

  // The 0.7.0 scheduler-cut paid run: H3 answered the stop before it reported the clip ended,
  // the scheduler cut the clip again, and that second stop cut the cutter 5 ms after it started.
  it("cuts a playing clip once, though its end is reported after the cut's result", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const playing = clip("long", item("other"), 15);
    policy.submit(spec("other"));
    policy.reply({ _tag: "Done", clipId: "long" });
    policy.event({ _tag: "Started", clip: playing });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "cu" });
    const stale: Partial<SourceState> = { playing, ready: [clip("cu", item("urgent"))] };
    policy.observe(stale);
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "long" });
    policy.reply({ _tag: "Done" });
    policy.observe(stale);
    policy.tick();
    const stops = commands(policy.actions).filter((action) => action.command._tag === "Stop");
    assert.strictEqual(stops.length, 1);
  });

  it("ends a cut whose play died, so autoplay airs the cutter at the next boundary", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const playing = clip("long", item("other"), 15);
    policy.submit(spec("other"));
    policy.reply({ _tag: "Done", clipId: "long" });
    policy.event({ _tag: "Started", clip: playing });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "cu" });
    policy.observe({ playing, ready: [clip("cu", item("urgent"))] });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "long" });
    policy.reply({ _tag: "Done" });
    policy.event({ _tag: "Ended", clip: playing, termination: "stopped" });
    policy.observe({ ready: [clip("cu", item("urgent"))] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Play", clipId: "cu" });
    policy.reply({ _tag: "Died" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
  });

  it("withdraws what has no clip at once and removes a clip before it drops it", () => {
    const early = run([
      { _tag: "Edit", id: 1, edits: [{ _tag: "Withdraw", key: key("a") }], batch: false },
    ]);
    assert.isTrue(
      early.actions.some((action) => action._tag === "Withdrawn" && action.outcome === "not-found"),
    );
    const later = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("a") }], batch: false },
      { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "ca" } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ ready: [clip("ca", item("a"))] }) },
      },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Withdraw", key: key("a") }], batch: false },
      { _tag: "Result", id: 3, result: { _tag: "Done" } },
    ]);
    const remove = commands(later.actions).find((action) => action.command._tag === "Remove");
    assert.deepStrictEqual(remove?.command, { _tag: "Remove", clipId: "ca" });
    assert.deepStrictEqual(statuses(later.actions, "a"), [
      "Accepted",
      "Building",
      "Ready",
      "Dropped",
    ]);
    assert.isTrue(
      later.actions.some((action) => action._tag === "Withdrawn" && action.outcome === "withdrawn"),
    );
  });

  // 0.7.0 took an insert after the playing clip as the next boundary; the rehearsed `edits`
  // check found the playout refusing it.
  it("inserts after the playing item at the next boundary, and refuses one before it", () => {
    const edit = (id: number, name: string, side: "before" | "after"): Policy.Input => ({
      _tag: "Edit",
      id,
      edits: [{ _tag: "Insert", spec: spec(name), anchor: key("p1"), side }],
      batch: false,
    });
    const { state, actions } = run([
      ...opened(),
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("p1") }], batch: false },
      { _tag: "Edit", id: 3, edits: [{ _tag: "Submit", spec: spec("p2") }], batch: false },
      { _tag: "Result", id: 2, result: { _tag: "Done", clipId: "c1" } },
      { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: clip("c1", item("p1")) } },
      { _tag: "Result", id: 3, result: { _tag: "Done", clipId: "c2" } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: {
          _tag: "State",
          state: source({ playing: clip("c1", item("p1")), ready: [clip("c2", item("p2"))] }),
        },
      },
      edit(10, "before", "before"),
      edit(11, "next", "after"),
    ]);
    const refused = actions.flatMap((action) => (action._tag === "Refused" ? [action.id] : []));
    assert.deepStrictEqual(refused, [10]);
    assert.strictEqual(state.items.get(key("next"))?.mode, "follow");
    const build = commands(actions).at(-1)?.command;
    assert.deepStrictEqual(build?._tag === "Enqueue" ? build.tag : undefined, item("next"));
    // Once the anchor has ended, the insert still airs ahead of the item that followed it.
    const after = run(
      [
        answer(state, { _tag: "Done", clipId: "cn" }),
        {
          _tag: "Source",
          sessionId: "s1",
          event: { _tag: "Ended", clip: clip("c1", item("p1")), termination: "finished" },
        },
        {
          _tag: "Source",
          sessionId: "s1",
          event: {
            _tag: "State",
            state: source({ ready: [clip("c2", item("p2")), clip("cn", item("next"))] }),
          },
        },
      ],
      state,
      100,
    );
    const move = commands(after.actions).find((action) => action.command._tag === "Move");
    assert.deepStrictEqual(move?.command, { _tag: "Move", clipId: "cn", position: 0 });
  });

  it("counts no air left for a playing clip the provider named without its length", () => {
    const inputs: ReadonlyArray<Policy.Input> = [
      ...opened(),
      {
        _tag: "Source",
        sessionId: "s1",
        event: {
          _tag: "State",
          state: source({
            playing: { clipId: "adopted", tag: undefined, seconds: undefined },
            ready: [clip("next")],
          }),
        },
      },
    ];
    const { state } = run(inputs);
    const view = Policy.view(config, state, { mono: 10, wall: 10 });
    assert.strictEqual(view.runwaySeconds, 5);
    // It was seen playing at the last input, so it started no later than that.
    assert.deepStrictEqual(view.playing, {
      key: "other",
      startedAt: inputs.length - 1,
      seconds: undefined,
    });
  });

  it("reports as runway the air secured: not a held clip, and still once a replacement opens", () => {
    const policy = drive({ config: { ...config, leadMs: 30_000 } });
    policy.tick(0);
    policy.open("s1", 90_000);
    for (const name of ["a", "b"]) {
      policy.submit(spec(name));
      policy.reply({ _tag: "Done", clipId: `c-${name}` });
    }
    policy.submit({ ...spec("held"), start: { _tag: "Manual" } });
    policy.observe({
      ready: [clip("c-a", item("a")), clip("c-b", item("b")), clip("c-held", item("held"))],
    });
    const runway = () =>
      Policy.view(config, policy.state(), { mono: policy.now(), wall: policy.now() }).runwaySeconds;
    // The held clip airs only once released.
    assert.strictEqual(runway(), 10);
    assert.isTrue(policy.tick(60_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 90_000);
    // The replacement takes new work, but what the session on air holds still airs first.
    assert.strictEqual(runway(), 10);
  });

  it("keeps what plays from a start or an end over a report older than it", () => {
    const filler = (index: number) => clip(`f${String(index)}`, { _tag: "Filler", index });
    const on = (event: SourceEvent): Policy.Input => ({ _tag: "Source", sessionId: "s1", event });
    // H3 answers a start or an end with the facts it held before it, and says they are not current.
    const stale = (playing: SourceClip | undefined) =>
      on({ _tag: "State", state: source({ available: false, playing }) });
    const { actions, state } = run([
      ...opened(),
      on({ _tag: "Started", clip: filler(0) }),
      stale(undefined),
      on({ _tag: "Ended", clip: filler(0), termination: "finished" }),
      stale(filler(0)),
      on({ _tag: "Started", clip: filler(1) }),
      stale(undefined),
      on({ _tag: "State", state: source({ playing: filler(1) }) }),
    ]);
    assert.deepStrictEqual(
      actions.flatMap((action) =>
        action._tag === "Emit" && action.event._tag === "Filler"
          ? [`${action.event.phase} ${String(action.event.index)}`]
          : [],
      ),
      ["Started 0", "Ended 0", "Started 1"],
    );
    // Filler 1 started at the eighth input, and a current report of it changes nothing.
    assert.deepStrictEqual(Policy.view(config, state, { mono: 20, wall: 20 }).playing, {
      key: "filler",
      startedAt: 7,
      seconds: 5,
    });
  });

  it("refuses a changed spec under a used key, and a batch with one refused edit changes nothing", () => {
    const { actions, state } = run([
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("a") }], batch: false },
      {
        _tag: "Edit",
        id: 2,
        edits: [{ _tag: "Submit", spec: { ...spec("a"), fingerprint: "changed" } }],
        batch: false,
      },
      {
        _tag: "Edit",
        id: 3,
        edits: [
          { _tag: "Submit", spec: spec("b") },
          { _tag: "Insert", spec: spec("c"), anchor: key("missing"), side: "after" },
        ],
        batch: true,
      },
    ]);
    assert.isTrue(
      actions.some(
        (action) =>
          action._tag === "Refused" && action.id === 2 && action.refusal._tag === "KeyMismatch",
      ),
    );
    assert.isTrue(
      actions.some(
        (action) =>
          action._tag === "Refused" && action.id === 3 && action.refusal._tag === "InvalidItem",
      ),
    );
    assert.isFalse(state.items.has(key("b")));
  });

  it("switches to a replacement only once the retiring session is idle and its grace has passed", () => {
    const setup = run([
      ...opened(),
      { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: clip("x") } },
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ playing: clip("x") }) },
      },
      { _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 },
      { _tag: "Source", sessionId: "s2", event: { _tag: "State", state: source() } },
    ]);
    assert.isFalse(
      setup.actions.some((action) => action._tag === "OnAir" && action.sessionId === "s2"),
    );
    const ended = Policy.step(
      config,
      setup.state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "Ended", clip: clip("x"), termination: "finished" },
      },
      { mono: 100, wall: 100 },
    );
    const idle = Policy.step(
      config,
      ended.state,
      { _tag: "Source", sessionId: "s1", event: { _tag: "State", state: source() } },
      { mono: 101, wall: 101 },
    );
    assert.isFalse(idle.actions.some((action) => action._tag === "OnAir"));
    const after = Policy.step(config, idle.state, { _tag: "Tick" }, { mono: 400, wall: 400 });
    assert.isTrue(
      after.actions.some((action) => action._tag === "OnAir" && action.sessionId === "s2"),
    );
    assert.isTrue(
      after.actions.some((action) => action._tag === "Close" && action.sessionId === "s1"),
    );
  });

  it("rebuilds a lost session's unaired items and settles its unknown ones for good", () => {
    const { actions } = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("a") }], batch: false },
      {
        _tag: "Result",
        id: 2,
        result: failed("unknown"),
      },
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("b") }], batch: false },
      { _tag: "Result", id: 3, result: { _tag: "Done", clipId: "cb" } },
      { _tag: "Lost", sessionId: "s1", reason: "gone" },
    ]);
    assert.deepStrictEqual(statuses(actions, "a").at(-1), "Unknown");
    assert.deepStrictEqual(statuses(actions, "b").at(-1), "Accepted");
    assert.isTrue(
      actions.some(
        (action) =>
          action._tag === "Emit" &&
          action.event._tag === "Session" &&
          action.event.event._tag === "Replaced",
      ),
    );
  });

  // A session going down refuses what it is sent before its source can say so. Only a change in
  // what the session reports can mend that, so a timer would resend it forever.
  it("sends an enqueue refused unsent again once, when its session's availability changes", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    const enqueues = () =>
      commands(policy.actions).filter((action) => action.command._tag === "Enqueue").length;
    policy.reply(failed("not-submitted"));
    policy.tick(5_000);
    policy.tick(60_000);
    assert.strictEqual(enqueues(), 1);
    policy.event({ _tag: "Reconnecting" });
    policy.event({ _tag: "Reconnected", afterMillis: 900 });
    policy.observe({});
    assert.deepStrictEqual(policy.busy(), {
      _tag: "Enqueue",
      request: spec("a").request,
      tag: item("a"),
      continueFrom: undefined,
    });
    assert.strictEqual(enqueues(), 2);
  });

  it("fails an item refused unsent again while its session reports itself available", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.reply(failed("not-submitted"));
    policy.event({ _tag: "Reconnecting" });
    policy.observe({});
    policy.reply(failed("not-submitted"));
    assert.deepStrictEqual(statuses(policy.actions, "a"), ["Accepted", "Failed"]);
    const settled = policy.actions.findLast(
      (action) => action._tag === "Emit" && action.event._tag === "AsRun",
    );
    const status =
      settled?._tag === "Emit" && settled.event._tag === "AsRun"
        ? settled.event.event.status
        : undefined;
    assert.strictEqual(status?._tag === "Failed" ? status.reason._tag : status?._tag, "Command");
  });

  for (const refusal of ["InvalidState", "Disconnected"] as const)
    it(`drops an item withdrawn while its enqueue was in flight once that fails unsent (${refusal})`, () => {
      const policy = drive();
      policy.tick(0);
      policy.open();
      policy.submit(spec("a"));
      policy.edit([{ _tag: "Withdraw", key: key("a") }]);
      policy.reply({
        _tag: "Failed",
        cause: CommandFailure.from(ReactorError.fromCode(refusal, "the provider refused it"), {
          operation: "enqueue",
          outcome: "not-submitted",
        }),
      });
      assert.deepStrictEqual(statuses(policy.actions, "a"), ["Accepted", "Dropped"]);
      assert.deepStrictEqual(
        policy.actions.flatMap((action) => (action._tag === "Withdrawn" ? [action.outcome] : [])),
        ["withdrawn"],
      );
    });

  it("removes a retiring session's filler only while it takes commands, and once per refusal", () => {
    const policy = drive();
    const filler = (index: number) => clip(`f${String(index)}`, { _tag: "Filler", index });
    const retiring = { playing: filler(0), ready: [filler(1)] };
    const removes = () =>
      commands(policy.actions).filter(
        (action) => action.command._tag === "Remove" && action.sessionId === "s1",
      ).length;
    policy.tick(0);
    policy.open("s1");
    policy.observe(retiring, "s1");
    policy.open("s2");
    policy.submit(spec("x"));
    assert.deepStrictEqual(enqueued(policy.actions, "s2"), ["x"]);
    policy.reply({ _tag: "Done", clipId: "cx" });
    policy.event({ _tag: "Reconnecting" }, "s1");
    policy.observe({ ready: [clip("cx", item("x"))] }, "s2");
    assert.strictEqual(removes(), 0);
    policy.observe(retiring, "s1");
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "f1" });
    policy.reply({
      _tag: "Failed",
      cause: CommandFailure.from(ReactorError.fromCode("InvalidState", "the clip is armed"), {
        operation: "pop",
        outcome: "replied",
        requestId: "request",
        generation: 1n,
      }),
    });
    policy.observe(retiring, "s1");
    policy.tick();
    assert.strictEqual(removes(), 1);
  });

  it("a drain withdraws a held Manual item and finishes although it was never released", () => {
    const { actions } = run([
      ...opened(),
      {
        _tag: "Edit",
        id: 1,
        edits: [{ _tag: "Submit", spec: { ...spec("held"), start: { _tag: "Manual" } } }],
        batch: false,
      },
      { _tag: "Drain", id: 2, finish: "accepted" },
      { _tag: "Tick" },
    ]);
    assert.strictEqual(statuses(actions, "held").at(-1), "Dropped");
    assert.isTrue(actions.some((action) => action._tag === "Drained" && action.id === 2));
  });

  it("fires a cue due at a clip's last moment when the clip finishes there", () => {
    const cued: Policy.Spec = { ...spec("a"), cues: [{ name: "out", from: "end", offsetMs: 0 }] };
    let state = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: cued }], batch: false },
    ]).state;
    state = at(state, answer(state, { _tag: "Done", clipId: "ca" }), 10).state;
    const playing = clip("ca", item("a"));
    state = at(
      state,
      { _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: playing } },
      100,
    ).state;
    const ended = at(
      state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "Ended", clip: playing, termination: "finished" },
      },
      5_100,
    );
    assert.deepStrictEqual(
      ended.actions.flatMap((action) =>
        action._tag === "Emit" && action.event._tag === "Cue" ? [action.event.event.name] : [],
      ),
      ["out"],
    );
  });

  it("learns build time only from builds whose dispatch outcome was always known", () => {
    let state = run([
      ...opened(),
      { _tag: "Edit", id: 1, edits: [{ _tag: "Submit", spec: spec("known") }], batch: false },
    ]).state;
    state = at(state, answer(state, { _tag: "Done", clipId: "ck" }), 10).state;
    const known = clip("ck", item("known"));
    state = at(
      state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ ready: [known] }) },
      },
      2_000,
    ).state;
    assert.strictEqual(state.samples.build.length, 1);
    state = at(
      state,
      { _tag: "Edit", id: 2, edits: [{ _tag: "Submit", spec: spec("unsure") }], batch: false },
      2_001,
    ).state;
    state = at(state, answer(state, failed("unknown")), 2_010).state;
    // Its clip turns up Ready much later: that wait includes the uncertainty, so it is no sample.
    const unsure = clip("cu", item("unsure"));
    state = at(
      state,
      {
        _tag: "Source",
        sessionId: "s1",
        event: { _tag: "State", state: source({ ready: [known, unsure] }) },
      },
      40_000,
    ).state;
    assert.strictEqual(state.items.get(key("unsure"))?.phase, "Ready");
    assert.strictEqual(state.samples.build.length, 1);
  });

  /**
   * `p1` plays while `p2` builds, each build taking `buildMs`, and `xc`, inserted before `p2`
   * to continue from the clip before it, waits for the build slot. With no continued build
   * measured, its build is projected at 2.5 times the longest independent one.
   */
  const continuing = (buildMs: number) => {
    let state = run(opened()).state;
    const actions: Array<Policy.Action> = [];
    const send = (input: Policy.Input, time: number) => {
      const result = at(state, input, time);
      state = result.state;
      actions.push(...result.actions);
    };
    const observed = (time: number, partial: Partial<SourceState>) =>
      send(
        {
          _tag: "Source",
          sessionId: "s1",
          event: { _tag: "State", state: source({ continuable: ["c1", "c2"], ...partial }) },
        },
        time,
      );
    const c1 = clip("c1", item("p1"));
    const c2 = clip("c2", item("p2"));
    for (const name of ["p1", "p2"])
      send(
        { _tag: "Edit", id: 0, edits: [{ _tag: "Submit", spec: spec(name) }], batch: false },
        10,
      );
    send(answer(state, { _tag: "Done", clipId: "c1" }), 20);
    const played = 10 + buildMs + 40;
    observed(10 + buildMs, { ready: [c1] });
    send({ _tag: "Source", sessionId: "s1", event: { _tag: "Started", clip: c1 } }, played);
    observed(played, { playing: c1 });
    send(answer(state, { _tag: "Done", clipId: "c2" }), played + 10);
    send(
      {
        _tag: "Edit",
        id: 0,
        edits: [
          {
            _tag: "Insert",
            spec: { ...spec("xc"), continuity: true },
            anchor: key("p2"),
            side: "before",
          },
        ],
        batch: false,
      },
      played + 50,
    );
    const dispatched = 10 + 2 * buildMs;
    observed(dispatched, { playing: c1, ready: [c2] });
    const build = commands(actions).at(-1)?.command;
    return { state: () => state, actions, send, observed, build, dispatched, c1 };
  };

  // The 0.7.0 scheduler-edits paid run: a continued insert waited behind a build in flight,
  // took 5.45 s to build against about 2.2 s, missed the playing clip's end, and aired after
  // the next clip, which it did not continue from.
  it("continues a build projected to miss its predecessor's end from the clip airing by then", () => {
    // Builds of 2 s: xc is projected Ready 6 s after it is sent, after p1 ends.
    const late = continuing(2_000);
    assert.deepStrictEqual(late.build, {
      _tag: "Enqueue",
      request: spec("xc").request,
      tag: item("xc"),
      continueFrom: "c2",
    });
    // Builds of half a second: xc is projected Ready before p1 ends, so it continues from p1.
    const early = continuing(500);
    assert.strictEqual(early.build?._tag === "Enqueue" && early.build.continueFrom, "c1");
  });

  it("holds a clip continued from the clip after its place behind that clip, and measures it apart", () => {
    const { state, actions, send, observed, dispatched, c1 } = continuing(2_000);
    send(answer(state(), { _tag: "Done", clipId: "cx" }), dispatched + 10);
    // xc is Ready 2 s later, before p1 ends, but it continues from p2 and so airs after it.
    const readyAt = dispatched + 2_000;
    const seen = actions.length;
    observed(readyAt, { playing: c1, ready: [clip("c2", item("p2")), clip("cx", item("xc"))] });
    send({ _tag: "Tick" }, readyAt + 1);
    const moves = commands(actions.slice(seen)).filter((action) => action.command._tag === "Move");
    assert.deepStrictEqual(moves, []);
    assert.strictEqual(state().items.get(key("xc"))?.phase, "Ready");
    assert.deepStrictEqual(state().samples.build.length, 2);
    assert.deepStrictEqual(state().samples.continued, [0.4]);
  });
});

const enqueued = (actions: ReadonlyArray<Policy.Action>, sessionId?: string) =>
  commands(actions).flatMap((action) =>
    action.command._tag === "Enqueue" && (sessionId === undefined || action.sessionId === sessionId)
      ? [action.command.tag._tag === "Item" ? String(action.command.tag.key) : "filler"]
      : [],
  );
const unknown = failed("unknown");
/** A filler floor of 5 s and a target of 10 s, in clips of 5 s. */
const filled: Policy.Config = {
  ...config,
  filler: {
    floor: 5,
    target: 10,
    clip: ({ index }) => ({ prompt: `filler ${String(index)}`, seconds: 5 }),
    lengths: { min: 5, max: 15 },
    invalid: () => undefined,
    protect: "air",
  },
};

// What 0.7.0's scheduler guaranteed and the first Playout lost, found by an independent critique.
describe("PlayoutPolicy, uncertainty and loss", () => {
  // 0.7.0 SchedulerRenewal "uncertain filler on the retiring source does not hold replacement
  // runway", and SchedulerUnknownRecovery's per-source filler identities.
  for (const lifetimeMs of [600_000, Infinity])
    it(`a lost filler reply holds only its own session, replaced by the deadline (lifetime ${String(lifetimeMs)})`, () => {
      const policy = drive({ config: filled });
      policy.tick(0);
      policy.open("s1", lifetimeMs);
      assert.deepStrictEqual(enqueued(policy.actions), ["filler"]);
      policy.reply(unknown, 100);
      policy.submit(spec("a"), 200);
      const deadline = policy.tick(100 + filled.unknownTimeoutMs);
      assert.isTrue(deadline.actions.some((action) => action._tag === "Open"));
      policy.open("s2", lifetimeMs);
      assert.deepStrictEqual(enqueued(policy.actions, "s2"), ["a"]);
    });

  for (const lifetimeMs of [600_000, Infinity])
    it(`a lost item reply opens a replacement at the deadline, not at the cap (lifetime ${String(lifetimeMs)})`, () => {
      const policy = drive();
      policy.tick(0);
      policy.open("s1", lifetimeMs);
      policy.submit(spec("lost"), 10);
      policy.reply(unknown, 20);
      policy.submit(spec("next"), 30);
      // The lost reply holds no build slot: what follows builds behind it on the same session.
      assert.deepStrictEqual(enqueued(policy.actions), ["lost", "next"]);
      const deadline = policy.tick(20 + config.unknownTimeoutMs);
      assert.isTrue(deadline.actions.some((action) => action._tag === "Open"));
    });

  // JSON has no Infinity, so a persisted event would read null: a grant leaves an uncapped cap out too.
  it("reports a session no cap ends without a lifetime", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1", Infinity);
    const opened = policy.actions.flatMap((action) =>
      action._tag === "Emit" &&
      action.event._tag === "Session" &&
      action.event.event._tag === "Opened"
        ? [action.event.event]
        : [],
    );
    assert.deepStrictEqual(opened, [{ _tag: "Opened", sessionId: "s1" }]);
  });

  // 0.7.0 SchedulerFates: a session lost is closed, so it cannot bill beside its replacement.
  it("closes a session it lost", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    const lost = policy.send({ _tag: "Lost", sessionId: "s1", reason: "the connection failed" });
    assert.isTrue(
      lost.actions.some((action) => action._tag === "Close" && action.sessionId === "s1"),
    );
  });
});
describe("PlayoutPolicy, lanes", () => {
  // The filler's index follows from its enqueue's result, and a replacement's lane is free while
  // the session on air still carries one.
  it("sends one filler enqueue at a time, whichever session's lane is free", () => {
    const policy = drive({ config: filled });
    const fillers = () =>
      commands(policy.actions).flatMap((action) =>
        action.command._tag === "Enqueue" && action.command.tag._tag === "Filler"
          ? [`${action.sessionId} ${String(action.command.tag.index)}`]
          : [],
      );
    policy.tick(0);
    policy.open("s1", 60_000);
    // A clip of its own keeps s1 on air while its replacement opens.
    const playing = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: playing });
    policy.observe({ playing });
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 60_000);
    assert.deepStrictEqual(fillers(), ["s1 0"]);
    policy.reply({ _tag: "Done", clipId: "f0" }, undefined, "s1");
    assert.deepStrictEqual(fillers(), ["s1 0", "s2 1"]);
  });

  // With one refused move remembered for the whole plan, a second session's refusal made the
  // first's move look new, and the two sessions' moves were sent again in turn.
  it("sends a refused move again only once its own session's queues have changed", () => {
    const policy = drive();
    const filler = (clipId: string, index: number) => clip(clipId, { _tag: "Filler", index });
    const moves = () =>
      commands(policy.actions).flatMap((action) =>
        action.command._tag === "Move" ? [`${action.sessionId} ${action.command.clipId}`] : [],
      );
    policy.tick(0);
    policy.open("s1", 60_000);
    const playing = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: playing });
    policy.observe({ playing, ready: [filler("b", 1), filler("a", 0)] });
    policy.reply(failed("replied"), undefined, "s1");
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 60_000);
    policy.observe({ ready: [filler("d", 3), filler("c", 2)] }, "s2");
    policy.reply(failed("replied"), undefined, "s2");
    policy.tick();
    policy.tick();
    assert.deepStrictEqual(moves(), ["s1 a", "s2 c"]);
    // s1's queues change, so its move is asked again, and s2's is not.
    policy.observe({ playing, ready: [filler("b", 1), filler("a", 0), filler("e", 4)] });
    assert.deepStrictEqual(moves(), ["s1 a", "s2 c", "s1 a"]);
  });
});

/** Three builds measured at 0.4 s per requested second: a continued one is projected at 1 s. */
const measured: Policy.State = {
  ...Policy.initial,
  samples: { build: [0.4, 0.4, 0.4], continued: [], length: [] },
};
/** Filler with a floor and twice that as its target, asking for the length the plan wants. */
const protecting = (
  protect: "air" | "order",
  lengths = { min: 5, max: 15 },
  floor = 5,
): Policy.Config => ({
  ...config,
  filler: {
    floor,
    target: floor * 2,
    clip: ({ index, seconds }) => ({ prompt: `filler ${String(index)}`, seconds }),
    lengths,
    invalid: () => undefined,
    protect,
  },
});

describe("PlayoutPolicy, air before queue order", () => {
  /** s1 on air playing a clip of its own that may be continued, `rest` seconds of it left. */
  const airing = (settings: Policy.Config, rest: number, from = measured) => {
    const policy = drive({ config: settings, from });
    policy.tick(0);
    policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 600_000 });
    const playing = clip("x", undefined, rest);
    policy.observe({ playing, continuable: ["x"] });
    if (policy.busy()?._tag === "Autoplay") policy.reply({ _tag: "Done" });
    return { policy, playing };
  };
  const sent = (command: Policy.Command | undefined) =>
    command?._tag !== "Enqueue"
      ? undefined
      : command.tag._tag === "Item"
        ? String(command.tag.key)
        : `filler of ${command.request.seconds?.toFixed(2) ?? "?"} s`;
  const long: Policy.Spec = { ...spec("long", 1, 15), continuity: true };

  // The review's measured case, in the plan: a 15 s continued build projected at 16 s with its
  // margin, against 12 s secured.
  it("sends a filler clip ahead of a build that would outlast the air secured, then the build", () => {
    const order = airing(protecting("order"), 12).policy;
    order.submit(long);
    assert.strictEqual(sent(order.busy()), "long");
    const { policy, playing } = airing(protecting("air"), 12);
    policy.submit(long);
    // Its own build runs the air down 0.4 s a second, so 6.67 s of it covers the 4 s missing.
    assert.strictEqual(sent(policy.busy()), "filler of 6.67 s");
    const cover = clip("f0", { _tag: "Filler", index: 0 }, 5);
    policy.observe({ playing, building: [cover], continuable: ["x"] });
    policy.reply({ _tag: "Done", clipId: "f0" });
    assert.isUndefined(policy.busy());
    // Built shorter than asked, it leaves the runway short: the item waits for no second one.
    policy.observe({ playing, ready: [cover], continuable: ["x", "f0"] }, "s1", 2_700);
    assert.strictEqual(sent(policy.busy()), "long");
  });

  // At 0.8 s a second, covering the 3 s missing takes more than a 15 s clip, whose build alone
  // outlasts the air; one airing while the item builds, 8 s, leaves the air dark least.
  it("sends a filler clip that airs no longer than the item takes to build", () => {
    const slow = { ...measured, samples: { build: [0.8, 0.8, 0.8], continued: [], length: [] } };
    const { policy } = airing(protecting("air"), 6, slow);
    policy.submit(spec("ten", 1, 10));
    assert.strictEqual(sent(policy.busy()), "filler of 8.00 s");
  });

  it("builds each item at once until three builds were measured", () => {
    const { policy } = airing(protecting("air"), 12, Policy.initial);
    policy.submit(long);
    assert.strictEqual(sent(policy.busy()), "long");
  });

  // Filler only where it covers a build, in clips of at least 10 s: one builds in 4 s, the item
  // in 2 s, so sending it first would leave the air dark longer.
  it("sends no filler clip that would build no sooner than the item", () => {
    const { policy } = airing(protecting("air", { min: 10, max: 15 }, 0), 1);
    policy.submit(spec("short"));
    assert.strictEqual(sent(policy.busy()), "short");
  });

  // A filler clip first would put its projected start at 8.4 s, past the 7 s it must start by,
  // and the plan would drop it as late.
  it("builds an item with a time to meet at once", () => {
    const { policy } = airing(protecting("air", { min: 5, max: 15 }, 0), 3);
    policy.submit({ ...spec("firm", 1, 15), window: { startByMs: 7_000, firm: true } });
    assert.strictEqual(sent(policy.busy()), "firm");
  });

  // The item it continues may not start for a minute, so it cannot be built yet; once it can, the
  // air its build needs is already there.
  it("keeps the runway at the build of the item next in line while that item waits", () => {
    const refill = (protect: "air" | "order") => {
      const { policy } = airing(protecting(protect), 12);
      policy.submit({ ...spec("first"), window: { notBeforeMs: 60_000, firm: false } });
      policy.submit(long);
      return sent(policy.busy());
    };
    assert.isUndefined(refill("order"));
    assert.strictEqual(refill("air"), "filler of 5.00 s");
  });
});

const withdrawn = (actions: ReadonlyArray<Policy.Action>) =>
  actions.flatMap((action) => (action._tag === "Withdrawn" ? [action.outcome] : []));

describe("PlayoutPolicy, edits", () => {
  /** Builds `names` one after another on s1, each Ready once built; returns their clip ids. */
  const built = (policy: ReturnType<typeof drive>, names: ReadonlyArray<string>) => {
    const current = policy.state().sessions[0]?.source;
    const ready: Array<SourceClip> = [...(current?.ready ?? [])];
    for (const name of names) {
      const command = policy.busy();
      assert.deepStrictEqual(
        command?._tag === "Enqueue" && command.tag._tag === "Item" ? command.tag.key : command,
        key(name),
      );
      policy.reply({ _tag: "Done", clipId: `c-${name}` });
      const seconds = command?._tag === "Enqueue" ? (command.request.seconds ?? 5) : 5;
      ready.push(clip(`c-${name}`, item(name), seconds));
      policy.observe({ playing: current?.playing, ready: [...ready] });
    }
    return ready;
  };

  // 0.7.0 SchedulerReplace: replacing one part of a group keeps the parts after it.
  it("replacing a group part keeps the parts after it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2"), spec("p3")],
        fingerprint: "g",
      },
    ]);
    const ready = built(policy, ["p1", "p2", "p3"]);
    policy.edit([{ _tag: "Replace", key: key("p2"), spec: spec("p2b") }]);
    built(policy, ["p2b"]);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-p2" });
    policy.reply({ _tag: "Done" });
    policy.observe({
      ready: [...ready.filter((value) => value.clipId !== "c-p2"), clip("c-p2b", item("p2b"))],
    });
    assert.deepStrictEqual(statuses(policy.actions, "p2").at(-1), "Dropped");
    assert.isUndefined(policy.state().items.get(key("p3"))?.withdraw);
    assert.notDeepEqual(policy.busy(), { _tag: "Remove", clipId: "c-p3" });
  });

  // 0.7.0 SchedulerWindows: an At item Ready too early is taken off and built again later.
  it("rebuilds an At item exposed after it was Ready, rather than dropping it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const foreign = clip("x", undefined, 5);
    policy.event({ _tag: "Started", clip: foreign }, "s1", 10);
    policy.observe({ playing: foreign }, "s1", 10);
    policy.submit(spec("cover", 1, 10), 20);
    built(policy, ["cover"]);
    policy.submit(
      { ...spec("timed"), start: { _tag: "At", time: 12_000, late: "nextBoundary" } },
      30,
    );
    built(policy, ["timed"]);
    policy.edit([{ _tag: "Withdraw", key: key("cover") }], false, 40);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-cover" });
    policy.reply({ _tag: "Done" }, 50);
    policy.observe({ playing: foreign, ready: [clip("c-timed", item("timed"))] }, "s1", 60);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-timed" });
    policy.reply({ _tag: "Done" }, 70);
    policy.observe({ playing: foreign }, "s1", 80);
    assert.notInclude(statuses(policy.actions, "timed"), "Dropped");
    assert.strictEqual(policy.state().items.get(key("timed"))?.phase, "Accepted");
  });

  // 0.7.0 SchedulerFates: a withdrawal answers what became of the clip.
  it("answers already-started for a clip that aired before its removal landed", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    built(policy, ["a"]);
    policy.edit([{ _tag: "Withdraw", key: key("a") }]);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-a" });
    policy.event({ _tag: "Started", clip: clip("c-a", item("a")) });
    policy.reply(failed("replied"));
    policy.event({ _tag: "Ended", clip: clip("c-a", item("a")), termination: "finished" });
    assert.deepStrictEqual(withdrawn(policy.actions), ["already-started"]);
  });

  it("answers not-found for a clip whose build failed before its removal landed", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.reply({ _tag: "Done", clipId: "c-a" });
    policy.observe({ building: [clip("c-a", item("a"))] });
    policy.edit([{ _tag: "Withdraw", key: key("a") }]);
    policy.reply(failed("replied"));
    policy.event(buildFailed(clip("c-a", item("a"))));
    assert.deepStrictEqual(withdrawn(policy.actions), ["not-found"]);
  });

  // One command at a time: a cut that stopped the playing clip and played its cutter in one call
  // played a cutter withdrawn while the stop was landing, and answered already-started.
  it("drops a cutter withdrawn while its cut stops the playing clip, and never plays it", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const long = clip("c-long", item("long"), 15);
    policy.submit(spec("long", 1, 15));
    built(policy, ["long"]);
    policy.event({ _tag: "Started", clip: long });
    policy.observe({ playing: long });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "c-urgent" });
    const cutter = clip("c-urgent", item("urgent"));
    policy.observe({ playing: long, ready: [cutter] });
    // Autoplay goes off first, so nothing starts in the stopped clip's place.
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: false });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "c-long" });
    policy.edit([{ _tag: "Withdraw", key: key("urgent") }]);
    policy.event({ _tag: "Ended", clip: long, termination: "stopped" });
    policy.observe({ ready: [cutter] });
    policy.reply({ _tag: "Done" });
    // The plan looks again once the stop has landed: the cutter goes instead of playing.
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-urgent" });
    policy.reply({ _tag: "Done" });
    policy.observe({});
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
    assert.deepStrictEqual(statuses(policy.actions, "urgent").at(-1), "Dropped");
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
    assert.isUndefined(commands(policy.actions).find((action) => action.command._tag === "Play"));
  });

  it("plays the cutter once the stop has landed, then puts autoplay back", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const long = clip("c-long", item("long"), 15);
    policy.submit(spec("long", 1, 15));
    built(policy, ["long"]);
    policy.event({ _tag: "Started", clip: long });
    policy.observe({ playing: long });
    policy.submit(spec("urgent", 0));
    policy.reply({ _tag: "Done", clipId: "c-urgent" });
    const cutter = clip("c-urgent", item("urgent"));
    policy.observe({ playing: long, ready: [cutter] });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Stop", clipId: "c-long" });
    // The stop's reply can come before H3 reports the clip ended: the play waits for that.
    policy.reply({ _tag: "Done" });
    assert.isUndefined(policy.busy());
    policy.event({ _tag: "Ended", clip: long, termination: "stopped" });
    policy.observe({ ready: [cutter] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Play", clipId: "c-urgent" });
    policy.reply({ _tag: "Done" });
    policy.event({ _tag: "Started", clip: cutter });
    policy.observe({ playing: cutter });
    assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
    assert.deepStrictEqual(statuses(policy.actions, "urgent").at(-1), "Started");
  });

  // 0.7.0 answered a group key's withdrawal `withdrawn` if any part was; the first Playout
  // answered with the first part's outcome.
  it("answers withdrawn for a group whose first part aired and whose next part it drops", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p1"), spec("p2")],
        fingerprint: "g",
      },
    ]);
    built(policy, ["p1", "p2"]);
    const first = clip("c-p1", item("p1"));
    policy.event({ _tag: "Started", clip: first });
    policy.observe({ playing: first, ready: [clip("c-p2", item("p2"))] });
    policy.edit([{ _tag: "Withdraw", key: key("g") }]);
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-p2" });
    assert.deepStrictEqual(withdrawn(policy.actions), []);
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(statuses(policy.actions, "p2").at(-1), "Dropped");
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
  });

  // A drain stops admissions, not withdrawals: the critique's withdraw during a drain answered
  // not-found and the item then aired.
  it("withdraws during a drain", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    built(policy, ["a"]);
    policy.send({ _tag: "Drain", id: 500, finish: "accepted" });
    // b's enqueue is in flight: the withdrawal waits for its clip, then removes it.
    policy.edit([{ _tag: "Withdraw", key: key("b") }]);
    assert.isFalse(policy.actions.some((action) => action._tag === "Refused"));
    policy.reply({ _tag: "Done", clipId: "c-b" });
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-b" });
    policy.reply({ _tag: "Done" });
    assert.deepStrictEqual(statuses(policy.actions, "b").at(-1), "Dropped");
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
  });
});
describe("PlayoutPolicy, time", () => {
  // The critique's 60 s cap: n10 was cut mid-clip at the cap and later items aired out of order.
  it("builds on a capped session only what can air before its cap, and replaces it at the lead", () => {
    const policy = drive({ config: { ...config, leadMs: 5_000 } });
    policy.tick(0);
    policy.open("s1", 60_000);
    const foreign = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: foreign }, "s1", 1_000);
    policy.observe({ playing: foreign }, "s1", 1_000);
    policy.submit(spec("a"), 1_010);
    assert.deepStrictEqual(enqueued(policy.actions), ["a"]);
    policy.reply({ _tag: "Done", clipId: "c-a" }, 1_020);
    policy.observe({ playing: foreign, ready: [clip("c-a", item("a"))] }, "s1", 1_030);
    const next = policy.submit(spec("b"), 1_040);
    assert.deepStrictEqual(enqueued(policy.actions), ["a"]);
    // The session holds air nearly to its cap: a replacement opened now would bill idle.
    assert.isFalse(next.actions.some((action) => action._tag === "Open"));
    assert.isTrue(policy.tick(56_000).actions.some((action) => action._tag === "Open"));
  });

  // A refusal that allocated nothing billed nothing, so while a session holds the air it neither
  // pauses renewal nor counts toward the opens that do.
  it("pauses renewal only once enough opens that may bill have failed, and fails with no air", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    policy.tick(0);
    policy.open("s1", 120_000);
    assert.isTrue(policy.tick(60_010).actions.some((action) => action._tag === "Open"));
    // Each retry waits a second longer than the one before.
    const retried = (allocated: boolean) => {
      policy.send({ _tag: "OpenFailed", reason: "refused", fatal: false, allocated });
      return policy.tick(policy.now() + 7_000).actions.some((action) => action._tag === "Open");
    };
    const opened = [true, false, false, true, false, true].map(retried);
    // The third open that may bill pauses renewal while s1 holds the air.
    assert.deepStrictEqual(opened, [true, true, true, true, true, false]);
    const unheld = drive();
    unheld.tick(0);
    for (let attempt = 0; attempt < 3; attempt++)
      unheld.send({ _tag: "OpenFailed", reason: "refused", fatal: false, allocated: false });
    assert.isTrue(unheld.actions.some((action) => action._tag === "Fail"));
  });

  it("times an end cue from the provider's length, not the requested one", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit({ ...spec("a"), cues: [{ name: "out", from: "end", offsetMs: 0 }] });
    policy.reply({ _tag: "Done", clipId: "c-a" });
    const aired = clip("c-a", item("a"), 5.167);
    policy.event({ _tag: "Started", clip: aired }, "s1", 1_000);
    const cues = (actions: ReadonlyArray<Policy.Action>) =>
      actions.flatMap((action) =>
        action._tag === "Emit" && action.event._tag === "Cue" ? [action.event.event.at] : [],
      );
    assert.deepStrictEqual(cues(policy.tick(6_000).actions), []);
    assert.deepStrictEqual(cues(policy.tick(6_167).actions), [6_167]);
  });

  it("does not report starvation while a replacement waits to take the air", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    // Owed air: an item that may not start for a long while.
    policy.submit({ ...spec("later"), window: { notBeforeMs: 3_600_000, firm: false } }, 5);
    const x = clip("x", undefined, 5);
    policy.observe({ ready: [x] }, "s1", 9);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x }, "s1", 10);
    policy.open("s2", 600_000, 20);
    policy.observe({ ready: [clip("y", undefined, 5)] }, "s2", 30);
    policy.event({ _tag: "Ended", clip: x, termination: "finished" }, "s1", 5_010);
    policy.observe({}, "s1", 5_011);
    policy.tick(5_300);
    assert.isFalse(
      policy.actions.some((action) => action._tag === "Emit" && action.event._tag === "Starved"),
    );
  });
});

/**
 * A provider for the property below: it answers the command in flight as the
 * script says, builds and plays clips when told, and loses sessions.
 */
const steps = [
  "submit",
  "urgent",
  "withdraw",
  "replace",
  "insert",
  "batch",
  "group",
  "drain",
  "done",
  "unknown",
  "refused",
  "ready",
  "fail",
  "start",
  "end",
  "lost",
  "open",
  "tick",
] as const;
type Script = ReadonlyArray<(typeof steps)[number]>;

const simulate = (script: Script, from: Policy.State) => {
  const settings: Policy.Config = {
    ...config,
    filler: {
      floor: 5,
      target: 10,
      clip: ({ index, seconds }) => ({ prompt: `filler ${String(index)}`, seconds }),
      lengths: { min: 5, max: 15 },
      invalid: () => undefined,
      protect: "air",
    },
  };
  let state = from;
  let clock = 0;
  let clips = 0;
  let opened = 0;
  let wanted = 0;
  const actions: Array<Policy.Action> = [];
  const inputs: Array<Policy.Input> = [];
  const sessions = new Map<
    string,
    { building: Array<SourceClip>; ready: Array<SourceClip>; playing: SourceClip | undefined }
  >();
  const lanes = new Map<string, number>();
  const edits = new Map<number, { readonly batch: boolean; readonly keys: Map<number, string> }>();
  const drains: Array<number> = [];
  const problems: Array<string> = [];
  /** Keys submitted so far, group keys among them, and what a withdrawal may drop. */
  const known: Array<string> = [];
  /**
   * Each group's parts by place, replacements included: a replacement takes
   * its part's place, as 0.7.0 counted "every part's key, replacements included".
   */
  const groups = new Map<string, Array<{ readonly key: string; readonly place: number }>>();
  const partOf = new Map<string, { readonly group: string; readonly place: number }>();
  const named = new Set<string>();
  /** Each replacement's key, and the key it replaces. */
  const replaced = new Map<string, string>();
  /** Each session's command in flight, by session. */
  const outstanding = new Map<string, number>();

  const send = (input: Policy.Input) => {
    clock += 7;
    inputs.push(input);
    if (input._tag === "Result")
      for (const [sessionId, id] of outstanding) if (id === input.id) outstanding.delete(sessionId);
    if (input._tag === "Lost") outstanding.delete(input.sessionId);
    const result = Policy.step(settings, state, input, { mono: clock, wall: clock });
    state = result.state;
    for (const action of result.actions) {
      actions.push(action);
      if (action._tag === "Command") {
        const unsettled = outstanding.get(action.sessionId);
        if (unsettled !== undefined)
          problems.push(
            `command ${String(action.id)} sent to ${action.sessionId} while ${String(unsettled)} was unsettled there`,
          );
        outstanding.set(action.sessionId, action.id);
        // A cut stops only filler, a clip the plan does not own, or a strictly lower lane's clip.
        const command = action.command;
        if (command._tag === "Stop") {
          const value = sessions.get(action.sessionId);
          const cutter = value?.ready.find((clip) => clip.clipId === state.cutting?.next)?.tag;
          const cut = value?.playing?.clipId === command.clipId ? value.playing.tag : undefined;
          if (
            cutter?._tag === "Item" &&
            cut?._tag === "Item" &&
            !((lanes.get(cut.key) ?? -1) > (lanes.get(cutter.key) ?? -1))
          )
            problems.push(`${cutter.key} cut ${cut.key}, which is not of a lower lane`);
        }
      }
      if (action._tag === "Close") {
        sessions.delete(action.sessionId);
        outstanding.delete(action.sessionId);
      }
      if (action._tag === "Open") wanted++;
    }
  };
  const observe = (sessionId: string) => {
    const value = sessions.get(sessionId);
    if (value === undefined) return;
    send({
      _tag: "Source",
      sessionId,
      event: {
        _tag: "State",
        state: source({
          building: [...value.building],
          ready: [...value.ready],
          playing: value.playing,
          continuable: value.ready.map((clip) => clip.clipId),
        }),
      },
    });
  };
  const onAir = () => sessions.get(state.air ?? "");
  const edit = (index: number, list: ReadonlyArray<Policy.EditInput>, batch = false) => {
    const id = 100 + index;
    const keys = new Map<number, string>();
    list.forEach((value, position) => {
      if (value._tag === "Withdraw") {
        keys.set(position, value.key);
        // A group key withdraws its parts; a part key, the parts from its place on.
        const part = partOf.get(value.key);
        const reached =
          groups.get(value.key) ??
          (part === undefined
            ? [{ key: value.key, place: 0 }]
            : (groups.get(part.group) ?? []).filter((other) => other.place >= part.place));
        for (const other of reached) named.add(other.key);
      }
      if (value._tag === "Submit" || value._tag === "Insert" || value._tag === "Replace") {
        lanes.set(value.spec.key, value.spec.lane);
        known.push(value.spec.key);
      }
      if (value._tag === "Replace") {
        replaced.set(value.spec.key, value.key);
        const part = partOf.get(value.key);
        if (part !== undefined && !partOf.has(value.spec.key)) {
          partOf.set(value.spec.key, part);
          groups.get(part.group)?.push({ key: value.spec.key, place: part.place });
        }
      }
      if (value._tag === "SubmitGroup" && !groups.has(value.key)) {
        groups.set(
          value.key,
          value.parts.map((part, place) => ({ key: part.key, place })),
        );
        known.push(value.key);
        value.parts.forEach((part, place) => {
          lanes.set(part.key, part.lane);
          known.push(part.key);
          if (!partOf.has(part.key)) partOf.set(part.key, { group: value.key, place });
        });
      }
    });
    edits.set(id, { batch, keys });
    send({ _tag: "Edit", id, edits: list, batch });
  };
  const cued = (name: string, lane: number, index: number): Policy.Spec => ({
    ...spec(name, lane, 5 + (index % 3) * 5),
    cues: index % 2 === 0 ? [{ name: "cue", from: "end", offsetMs: 500 }] : [],
    continuity: index % 4 === 3,
  });

  send({ _tag: "Tick" });
  script.forEach((step, index) => {
    // Edits name keys already submitted, group parts among them, as often as fresh ones.
    const name =
      index % 2 === 0 || known.length === 0
        ? `k${String(index % 5)}`
        : known[(index * 7) % known.length]!;
    // A command in flight on some lane: the oldest or the newest, so lanes answer in either order.
    const lanes = state.sessions
      .flatMap((value) =>
        value.busy === undefined ? [] : [{ ...value.busy, sessionId: value.id }],
      )
      .sort((a, b) => a.id - b.id);
    const busy = index % 2 === 0 ? lanes[0] : lanes.at(-1);
    switch (step) {
      case "submit":
        return edit(index, [{ _tag: "Submit", spec: cued(name, 1, index) }]);
      case "urgent":
        return edit(index, [{ _tag: "Submit", spec: cued(`u${String(index)}`, 0, index) }]);
      case "withdraw":
        return edit(index, [{ _tag: "Withdraw", key: key(name) }]);
      case "replace":
        return edit(index, [
          { _tag: "Replace", key: key(name), spec: cued(`r${String(index)}`, 1, index) },
        ]);
      case "insert":
        return edit(index, [
          {
            _tag: "Insert",
            spec: cued(`i${String(index)}`, 1, index),
            anchor: key(name),
            side: index % 2 === 0 ? "after" : "before",
          },
        ]);
      case "batch":
        return edit(
          index,
          [
            { _tag: "Withdraw", key: key(name) },
            { _tag: "Submit", spec: cued(`b${String(index)}`, 1, index) },
          ],
          true,
        );
      case "group":
        return edit(index, [
          {
            _tag: "SubmitGroup",
            key: key(`g${String(index)}`),
            lane: 1,
            parts: [cued(`g${String(index)}a`, 1, index), cued(`g${String(index)}b`, 1, index + 1)],
            fingerprint: `g${String(index)}`,
          },
        ]);
      case "drain":
        drains.push(100 + index);
        return send({
          _tag: "Drain",
          id: 100 + index,
          finish: index % 2 === 0 ? "accepted" : "playing",
        });
      case "done": {
        if (busy === undefined) return send({ _tag: "Tick" });
        const value = sessions.get(busy.sessionId);
        const command = busy.command;
        let clipId: string | undefined;
        if (value !== undefined)
          switch (command._tag) {
            case "Enqueue":
              clipId = `c${String(clips++)}`;
              value.building.push({
                clipId,
                tag: command.tag,
                seconds: command.request.seconds ?? 5,
              });
              break;
            case "Remove":
              value.building = value.building.filter((clip) => clip.clipId !== command.clipId);
              value.ready = value.ready.filter((clip) => clip.clipId !== command.clipId);
              break;
            case "Move": {
              const moved = value.ready.find((clip) => clip.clipId === command.clipId);
              if (moved !== undefined) {
                value.ready = value.ready.filter((clip) => clip !== moved);
                value.ready.splice(command.position, 0, moved);
              }
              break;
            }
            case "Stop": {
              if (value.playing?.clipId !== command.clipId) break;
              const cut = value.playing;
              value.playing = undefined;
              send({
                _tag: "Source",
                sessionId: busy.sessionId,
                event: { _tag: "Ended", clip: cut, termination: "stopped" },
              });
              break;
            }
            case "Play": {
              const next = value.ready.find((clip) => clip.clipId === command.clipId);
              if (next === undefined || value.playing !== undefined) break;
              value.ready = value.ready.filter((clip) => clip !== next);
              value.playing = next;
              send({
                _tag: "Source",
                sessionId: busy.sessionId,
                event: { _tag: "Started", clip: next },
              });
              break;
            }
            case "Autoplay":
              break;
          }
        send({ _tag: "Result", id: busy.id, result: { _tag: "Done", clipId } });
        return observe(busy.sessionId);
      }
      case "unknown":
      case "refused":
        if (busy === undefined) return send({ _tag: "Tick" });
        return send({
          _tag: "Result",
          id: busy.id,
          result: failed(step === "unknown" ? "unknown" : "replied"),
        });
      case "fail": {
        const [sessionId, value] =
          [...sessions].find(([, entry]) => entry.building.length > 0) ?? [];
        if (sessionId === undefined || value === undefined) return send({ _tag: "Tick" });
        send({ _tag: "Source", sessionId, event: buildFailed(value.building.shift()!) });
        return observe(sessionId);
      }
      case "ready": {
        const [sessionId, value] =
          [...sessions].find(([, entry]) => entry.building.length > 0) ?? [];
        if (sessionId === undefined || value === undefined) return send({ _tag: "Tick" });
        value.ready.push(value.building.shift()!);
        return observe(sessionId);
      }
      case "start": {
        const value = onAir();
        if (value === undefined || value.playing !== undefined || value.ready.length === 0)
          return send({ _tag: "Tick" });
        const next = value.ready.shift()!;
        value.playing = next;
        send({ _tag: "Source", sessionId: state.air!, event: { _tag: "Started", clip: next } });
        return observe(state.air!);
      }
      case "end": {
        const value = onAir();
        if (value?.playing === undefined) return send({ _tag: "Tick" });
        const ended = value.playing;
        value.playing = undefined;
        send({
          _tag: "Source",
          sessionId: state.air!,
          event: { _tag: "Ended", clip: ended, termination: "finished" },
        });
        return observe(state.air!);
      }
      case "lost": {
        const sessionId = [...sessions.keys()][0];
        if (sessionId === undefined) return send({ _tag: "Tick" });
        sessions.delete(sessionId);
        return send({ _tag: "Lost", sessionId, reason: "gone" });
      }
      case "open": {
        if (wanted === 0) return send({ _tag: "Tick" });
        wanted--;
        const sessionId = `s${String(++opened)}`;
        sessions.set(sessionId, { building: [], ready: [], playing: undefined });
        send({ _tag: "Opened", sessionId, lifetimeMs: 120_000 });
        return observe(sessionId);
      }
      case "tick":
        clock += 5_000;
        return send({ _tag: "Tick" });
    }
  });
  send({ _tag: "Close" });
  return { actions, inputs, edits, drains, problems, groups, named, replaced };
};

/**
 * The plan's promises for one script, run with no build measured and with
 * three, so that filler protects the air from the start; the property and the
 * pinned counterexamples check them.
 */
const check = (script: Script): void => {
  for (const from of [Policy.initial, measured]) keeps(script, from);
};
const keeps = (script: Script, from: Policy.State): void => {
  const { actions, edits, drains, problems, groups, named, replaced } = simulate(script, from);
  // One provider command at a time on each session, so a refusal is always attributable.
  assert.deepStrictEqual(problems, []);
  const history = new Map<string, Array<Policy.Action & { readonly _tag: "Emit" }>>();
  for (const action of actions)
    if (action._tag === "Emit" && action.event._tag === "AsRun")
      history.set(action.event.event.key, [...(history.get(action.event.event.key) ?? []), action]);
  const tags = (name: string) =>
    (history.get(name) ?? []).flatMap((action) =>
      action.event._tag === "AsRun" ? [action.event.event.status._tag] : [],
    );
  const terminal = ["Ended", "Dropped", "Failed", "Unobserved"];
  for (const name of history.keys()) {
    const statuses = tags(name);
    assert.strictEqual(statuses[0], "Accepted", name);
    const ends = statuses.filter((status) => terminal.includes(status));
    // At most one terminal status, and nothing after it.
    assert.isAtMost(ends.length, 1, `${name}: ${statuses.join(",")}`);
    if (ends.length === 1)
      assert.strictEqual(statuses.at(-1), ends[0], `${name}: ${statuses.join(",")}`);
  }
  // An enqueue whose outcome is unknown is never sent again, unless its session was lost
  // and the item carried to another.
  for (const name of history.keys()) {
    const events = actions.filter(
      (action) =>
        (action._tag === "Emit" &&
          action.event._tag === "AsRun" &&
          action.event.event.key === name) ||
        (action._tag === "Command" &&
          action.command._tag === "Enqueue" &&
          action.command.tag._tag === "Item" &&
          action.command.tag.key === name),
    );
    let uncertain = false;
    for (const action of events) {
      if (action._tag === "Command")
        assert.isFalse(uncertain, `${name} was sent again after an unknown outcome`);
      else if (action._tag === "Emit" && action.event._tag === "AsRun") {
        const status = action.event.event.status;
        if (status._tag === "Unknown") uncertain = true;
        if (status._tag === "Accepted") uncertain = false;
      }
    }
  }
  // Every withdrawal of an accepted edit is answered, once.
  const accepted = new Set(
    actions.flatMap((action) => (action._tag === "Accepted" ? [action.id] : [])),
  );
  for (const [id, value] of edits)
    if (accepted.has(id))
      for (const position of value.keys.keys())
        assert.strictEqual(
          actions.filter(
            (action) =>
              action._tag === "Withdrawn" && action.id === id && action.index === position,
          ).length,
          1,
          `the withdrawal of ${value.keys.get(position) ?? ""} was answered other than once`,
        );
  // A withdrawal answers what became of its item. A group key's answers `withdrawn` if any
  // part was dropped by then, else `already-started` if any started, else `not-found`.
  const seen = new Map<string, ReadonlyArray<string>>();
  for (const action of actions) {
    if (action._tag === "Emit" && action.event._tag === "AsRun")
      seen.set(action.event.event.key, [
        ...(seen.get(action.event.event.key) ?? []),
        action.event.event.status._tag,
      ]);
    if (action._tag !== "Withdrawn") continue;
    const name = edits.get(action.id)?.keys.get(action.index);
    const parts = name === undefined ? undefined : groups.get(name);
    if (parts === undefined) continue;
    const so = parts.flatMap((part) => seen.get(part.key) ?? []);
    const expected = so.includes("Dropped")
      ? "withdrawn"
      : so.includes("Started")
        ? "already-started"
        : "not-found";
    assert.strictEqual(action.outcome, expected, `group ${name ?? ""}: ${so.join(",")}`);
  }
  for (const action of actions)
    if (action._tag === "Withdrawn") {
      const name = edits.get(action.id)?.keys.get(action.index);
      if (name === undefined || groups.has(name)) continue;
      const statuses = tags(name);
      if (action.outcome === "withdrawn")
        assert.include(statuses, "Dropped", `${name} withdrawn: ${statuses.join(",")}`);
      if (action.outcome === "already-started")
        assert.include(statuses, "Started", `${name} already started: ${statuses.join(",")}`);
      if (action.outcome === "not-found")
        assert.notInclude(statuses, "Started", `${name} not found: ${statuses.join(",")}`);
    }
  // An item is dropped as withdrawn only when a withdrawal or a drain named it, an earlier
  // part of its group failed or was dropped (a replaced part keeps those after it), or it is
  // a replacement whose item started first, as 0.7.0 withdrew it.
  if (drains.length === 0) {
    const reasons = (name: string) =>
      (history.get(name) ?? []).flatMap((action) =>
        action.event._tag === "AsRun" && action.event.event.status._tag === "Dropped"
          ? [action.event.event.status.reason]
          : [],
      );
    const allowed = new Set(named);
    for (const parts of groups.values()) {
      const broken = parts.filter(
        (part) =>
          tags(part.key).includes("Failed") ||
          reasons(part.key).some((reason) => reason !== "replaced"),
      );
      const from = Math.min(...broken.map((part) => part.place));
      for (const part of parts) if (part.place > from) allowed.add(part.key);
    }
    for (const next of replaced.keys())
      for (let old = replaced.get(next); old !== undefined; old = replaced.get(old))
        if (tags(old).includes("Started")) allowed.add(next);
    for (const name of history.keys())
      if (reasons(name).includes("withdrawn"))
        assert.isTrue(allowed.has(name), `${name} was dropped though nothing withdrew it`);
  }
  // An item and its replacement never both air.
  for (const [next, old] of replaced)
    assert.isFalse(
      tags(next).includes("Started") && tags(old).includes("Started"),
      `${old} and its replacement ${next} both aired`,
    );
  // Every batch and drain is answered once the playout closes.
  const answered = new Set(
    actions.flatMap((action) =>
      action._tag === "Committed" || action._tag === "Refused" ? [action.id] : [],
    ),
  );
  for (const [id, value] of edits)
    if (value.batch) assert.isTrue(answered.has(id), `batch ${String(id)} never answered`);
  const drained = new Set(
    actions.flatMap((action) => (action._tag === "Drained" ? [action.id] : [])),
  );
  for (const id of drains) assert.isTrue(drained.has(id), `drain ${String(id)} never finished`);
};

/**
 * The seeds the gate runs, 3,000 scripts each, so every run of it is the same.
 * 6763954388057357 found a replacement's withdrawal leaving the replacement on
 * air (pinned above). To explore wider, set `PLAYOUT_PROPERTY_RUNS` to a count
 * and optionally `PLAYOUT_PROPERTY_SEED`; without a seed each run draws one and
 * a failure reports it. A seed that finds something joins this list, and its
 * shrunk script becomes a unit test.
 */
const gateSeeds: ReadonlyArray<string> = ["6763954388057357", "1", "2"];
const explore = Effect.runSync(
  Config.all({
    runs: Config.Int("PLAYOUT_PROPERTY_RUNS").pipe(Config.withDefault(0)),
    seed: Config.String("PLAYOUT_PROPERTY_SEED").pipe(Config.option),
  }),
);
const propertyRuns: ReadonlyArray<{ readonly seed: string | undefined; readonly runs: number }> =
  explore.runs > 0
    ? [
        {
          seed: explore.seed.pipe(
            Option.filter((value) => value.length > 0),
            Option.getOrUndefined,
          ),
          runs: explore.runs,
        },
      ]
    : gateSeeds.map((seed) => ({ seed, runs: 3_000 }));

/**
 * Scripts the property falsified, each shrunk: the plan's promises hold for
 * them on every run, whatever the seeds explore.
 */
const counterexamples: ReadonlyArray<Script> = [
  // A replacement's withdrawal left the replacement waiting and was never answered.
  ["unknown", "group", "batch", "replace", "ready", "withdraw"],
  // A batch's withdrawal still pending at close was never answered.
  ["batch", "open", "done", "batch"],
  ["batch", "withdraw", "fail", "batch"],
  ["urgent", "withdraw", "lost", "batch"],
  // A replacement whose item started first is dropped as withdrawn, as 0.7.0 dropped it.
  ["submit", "open", "done", "done", "ready", "replace", "start"],
];

describe("PlayoutPolicy, any script", () => {
  for (const script of counterexamples)
    it(`keeps every promise of the plan for ${script.join(", ")}`, () => check(script));

  // Any sequence of edits, provider answers, builds, plays and losses keeps the plan's promises,
  // the invariants #64's review pinned among them.
  for (const { seed, runs } of propertyRuns)
    it.effect.prop(
      `keeps every promise of the plan (seed ${seed ?? "drawn"}, ${String(runs)} scripts)`,
      [Schema.Array(Schema.Literals(steps)).check(Schema.isMaxLength(80))],
      ([script]) => Effect.sync(() => check(script)),
      { arbitrary: { runs, ...(seed === undefined ? {} : { seed }) }, timeout: 600_000 },
    );
});

// Claims from the critique's delegated pass, each checked here before any fix.
describe("PlayoutPolicy, edit claims", () => {
  /** Builds `names` on s1 one after another, each Ready once built. */
  const build = (policy: ReturnType<typeof drive>, names: ReadonlyArray<string>) => {
    for (const name of names) {
      const command = policy.busy();
      assert.deepStrictEqual(
        command?._tag === "Enqueue" && command.tag._tag === "Item" ? command.tag.key : command,
        key(name),
      );
      policy.reply({ _tag: "Done", clipId: `c-${name}` });
      const current = policy.state().sessions[0]?.source;
      policy.observe({
        playing: current?.playing,
        ready: [...(current?.ready ?? []), clip(`c-${name}`, item(name))],
        continuable: [...(current?.continuable ?? []), `c-${name}`],
      });
    }
  };

  it("a replace lane keeps its Ready item as cover until the new one is Ready", () => {
    const policy = drive({
      config: {
        ...config,
        lanes: [...config.lanes, { name: "news", conflict: "replace", cut: false }],
      },
    });
    policy.tick(0);
    policy.open();
    policy.submit(spec("x", 2));
    build(policy, ["x"]);
    policy.submit(spec("y", 2));
    assert.notInclude(statuses(policy.actions, "x"), "Dropped");
    assert.notDeepEqual(policy.busy(), { _tag: "Remove", clipId: "c-x" });
  });

  it("a continuing replacement continues from the clip before its place, not the one it replaces", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    build(policy, ["a", "b"]);
    policy.edit([{ _tag: "Replace", key: key("b"), spec: { ...spec("b2"), continuity: true } }]);
    const command = policy.busy();
    assert.deepStrictEqual(command?._tag === "Enqueue" ? command.continueFrom : command, "c-a");
  });

  it("refuses an insert under a used key with another anchor", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    policy.edit([{ _tag: "Insert", spec: spec("i"), anchor: key("a"), side: "after" }]);
    const again = policy.edit([
      { _tag: "Insert", spec: spec("i"), anchor: key("b"), side: "after" },
    ]);
    assert.isTrue(
      again.actions.some(
        (action) => action._tag === "Refused" && action.refusal._tag === "KeyMismatch",
      ),
    );
  });

  it("refuses a group whose part key another item already uses", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("p"));
    build(policy, ["p"]);
    const group = policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("p"), spec("q")],
        fingerprint: "g",
      },
    ]);
    assert.isTrue(group.actions.some((action) => action._tag === "Refused"));
    assert.strictEqual(policy.state().items.get(key("p"))?.phase, "Ready");
  });

  // The property's counterexample, shrunk: ["unknown","group","batch","replace","ready","withdraw"].
  // A replacement takes its part's place, so withdrawing it withdraws it and the parts after it,
  // and answers with its own outcome, as 0.7.0 counted "every part's key, replacements included".
  it("withdrawing a group part's replacement withdraws it and the parts after it, and answers", () => {
    const policy = drive();
    policy.tick(0);
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("ga"), spec("gb")],
        fingerprint: "g",
      },
    ]);
    policy.edit([{ _tag: "Replace", key: key("ga"), spec: spec("r") }]);
    assert.deepStrictEqual(statuses(policy.actions, "ga").at(-1), "Dropped");
    const withdrawal = policy.edit([{ _tag: "Withdraw", key: key("r") }]);
    assert.deepStrictEqual(statuses(withdrawal.actions, "r"), ["Dropped"]);
    assert.deepStrictEqual(statuses(withdrawal.actions, "gb"), ["Dropped"]);
    assert.deepStrictEqual(withdrawn(withdrawal.actions), ["withdrawn"]);
  });

  it("an insert after a group follows its last part by place, not the replacement added last", () => {
    const policy = drive();
    policy.tick(0);
    policy.edit([
      {
        _tag: "SubmitGroup",
        key: key("g"),
        lane: 1,
        parts: [spec("ga"), spec("gb")],
        fingerprint: "g",
      },
    ]);
    policy.edit([{ _tag: "Replace", key: key("ga"), spec: spec("r") }]);
    policy.edit([{ _tag: "Insert", spec: spec("i"), anchor: key("g"), side: "after" }]);
    assert.strictEqual(policy.state().items.get(key("i"))?.group?.index, 1);
  });

  it("a batch's cover airs only when nothing else can", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    policy.submit(spec("b"));
    build(policy, ["a", "b"]);
    policy.edit(
      [
        { _tag: "Withdraw", key: key("a") },
        { _tag: "Submit", spec: spec("c") },
      ],
      true,
    );
    // As in 0.7.0, what a pending batch withdraws ranks after the rest of its lane.
    policy.reply({ _tag: "Done", clipId: "c-c" });
    const moves = commands(policy.actions).filter((action) => action.command._tag === "Move");
    assert.deepStrictEqual(moves.at(-1)?.command, { _tag: "Move", clipId: "c-b", position: 0 });
  });
});
