/** The playout's pure policy on its own: inputs in, actions and as-run out, no clock and no I/O. */
import { assert, describe, it } from "@effect/vitest";
import * as Policy from "../src/internal/playout/policy.js";
import type { SourceClip, SourceEvent, SourceState } from "../src/Playout.js";
import { CommandFailure, ReactorError } from "../src/ReactorError.js";
import {
  buildFailed,
  check,
  clip,
  config,
  counterexamples,
  failed,
  item,
  key,
  measured,
  source,
  spec,
} from "./PlayoutPolicy.js";

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

  // Once Reactor or its moderation has ended a session, the provider refuses what it is sent
  // with why, before its source reports the session lost.
  for (const reason of ["TerminalSession", "Moderated"] as const)
    it(`carries an enqueue an ended session refused unsent to the next session (${reason})`, () => {
      const policy = drive();
      policy.tick(0);
      policy.open();
      policy.submit(spec("a"));
      policy.reply({
        _tag: "Failed",
        cause: CommandFailure.from(ReactorError.fromCode(reason, "the session ended"), {
          operation: "enqueue",
          outcome: "not-submitted",
        }),
      });
      policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
      policy.open("s2");
      assert.deepStrictEqual(statuses(policy.actions, "a"), ["Accepted"]);
      assert.deepStrictEqual(policy.busy("s2"), {
        _tag: "Enqueue",
        request: spec("a").request,
        tag: item("a"),
        continueFrom: undefined,
      });
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
    // Once its queues change it is asked again, and at once after an answer that may have applied.
    policy.observe({ ...retiring, ready: [filler(1), filler(2)] }, "s1");
    policy.reply(failed("unknown"));
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "f1" });
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
  const fillersOf = (policy: ReturnType<typeof drive>) =>
    commands(policy.actions).flatMap((action) =>
      action.command._tag === "Enqueue" && action.command.tag._tag === "Filler"
        ? [
            `${action.sessionId} ${String(action.command.tag.index)} ${action.command.request.prompt}`,
          ]
        : [],
    );
  /** s1 on air with a clip of its own and filler 0 in flight, and its replacement s2 open. */
  const renewing = () => {
    const policy = drive({ config: filled });
    policy.tick(0);
    policy.open("s1", 60_000);
    // A clip of its own keeps s1 on air while its replacement opens.
    const playing = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: playing });
    policy.observe({ playing });
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 60_000);
    return policy;
  };

  // A filler enqueue whose command is lost holds its own lane for about 20 s, and no other: each
  // clip takes its index as its enqueue goes out, so the replacement sends filler of its own.
  it("sends filler on the replacement's lane while the session on air carries some", () => {
    const policy = renewing();
    assert.deepStrictEqual(fillersOf(policy), ["s1 0 filler 0", "s2 1 filler 1"]);
    policy.reply({ _tag: "Done", clipId: "f0" }, undefined, "s1");
    assert.deepStrictEqual(fillersOf(policy), ["s1 0 filler 0", "s2 1 filler 1"]);
  });

  it("asks for a refused filler clip again as it was, on whichever lane is free", () => {
    const policy = renewing();
    policy.reply(failed("replied"), 30_100, "s1");
    policy.reply({ _tag: "Done", clipId: "f1" }, 30_200, "s2");
    policy.tick(31_100);
    assert.deepStrictEqual(fillersOf(policy), ["s1 0 filler 0", "s2 1 filler 1", "s2 0 filler 0"]);
  });

  it("records a filler clip under the index it was asked for, though moderation moved on", () => {
    const policy = drive({ config: filled });
    policy.tick(0);
    policy.open("s1", 60_000);
    policy.event({ _tag: "Moderated", action: "terminate", categories: ["test"] }, "s1");
    policy.reply({ _tag: "Done", clipId: "f0" }, undefined, "s1");
    assert.strictEqual(policy.state().sessions[0]?.fillers.get("f0")?.index, 0);
  });

  // An enqueue in flight as its session goes made no clip that can still air, so its clip is
  // asked for on the next session: unless moderation flagged it, which would end that one too.
  it("asks for a lost session's filler clip again on the next, unless moderation flagged it", () => {
    for (const flagged of [false, true]) {
      const policy = drive({ config: filled });
      policy.tick(0);
      policy.open("s1");
      if (flagged)
        policy.event({ _tag: "Moderated", action: "terminate", categories: ["test"] }, "s1");
      policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
      policy.open("s2", 600_000, 2_000);
      assert.deepStrictEqual(
        fillersOf(policy),
        flagged ? ["s1 0 filler 0", "s2 1 filler 1"] : ["s1 0 filler 0", "s2 0 filler 0"],
      );
    }
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

  // A provider may refuse autoplay every time, and a source method may throw every time: each is
  // asked again a second later, not at every reply. Until its autoplay is as its role wants it,
  // nothing else goes to the session, whose clips would otherwise air, or not, as they must not.
  it("sends a failed autoplay, or a move that died, again a second later", () => {
    for (const result of [{ _tag: "Died" } as const, failed("replied")]) {
      const policy = drive();
      policy.tick(0);
      policy.send({ _tag: "Opened", sessionId: "s1", lifetimeMs: 600_000 });
      policy.submit(spec("a"));
      policy.observe({});
      assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
      assert.strictEqual(policy.reply(result, 10).wake, 1_010);
      assert.isUndefined(policy.busy());
      policy.tick(1_010);
      assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: true });
      policy.reply({ _tag: "Done" });
      assert.deepStrictEqual(enqueued(policy.actions), ["a"]);
    }
    const policy = drive();
    const filler = (clipId: string, index: number) => clip(clipId, { _tag: "Filler", index });
    policy.tick(0);
    policy.open();
    policy.observe({ playing: clip("x", undefined, 50), ready: [filler("b", 1), filler("a", 0)] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Move", clipId: "a", position: 0 });
    assert.strictEqual(policy.reply({ _tag: "Died" }, 10).wake, 1_010);
    assert.isUndefined(policy.busy());
    policy.tick(1_010);
    assert.deepStrictEqual(policy.busy(), { _tag: "Move", clipId: "a", position: 0 });
  });

  // s2 opens as s1's replacement, and its autoplay off dies at 40 s. Within the second, s1 goes
  // idle or is lost and s2 takes the air: autoplay on is not the value that failed, so it goes.
  it("turns autoplay on as a session takes the air, though its autoplay off just failed", () => {
    for (const how of ["idle", "lost"] as const) {
      const policy = drive();
      policy.tick(0);
      policy.open("s1", 60_000);
      const x = clip("x", undefined, 40);
      policy.event({ _tag: "Started", clip: x }, "s1", 100);
      policy.observe({ playing: x }, "s1", 100);
      assert.isTrue(policy.tick(30_001).actions.some((action) => action._tag === "Open"));
      policy.send({ _tag: "Opened", sessionId: "s2", lifetimeMs: 600_000 }, 30_100);
      policy.observe({}, "s2", 30_200);
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: false });
      policy.reply({ _tag: "Died" }, 40_000, "s2");
      if (how === "idle") {
        policy.event({ _tag: "Ended", clip: x, termination: "finished" }, "s1", 40_100);
        policy.observe({}, "s1", 40_100);
        policy.tick(40_350);
      } else policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" }, 40_200);
      assert.isTrue(
        policy.actions.some((action) => action._tag === "OnAir" && action.sessionId === "s2"),
      );
      assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: true });
    }
  });

  // A cut begins with autoplay off, and the cut ends if that fails. One that died, or whose reply
  // was lost, may have applied, so autoplay on goes again; one refused applied nothing.
  it("turns autoplay on again once a cut's autoplay off may have applied", () => {
    const again = { _tag: "Autoplay", enabled: true } as const;
    for (const [result, after] of [
      [{ _tag: "Died" } as const, again],
      [failed("unknown"), again],
      [failed("replied"), undefined],
    ] as const) {
      const policy = drive();
      policy.tick(0);
      policy.open();
      const f = clip("f", { _tag: "Filler", index: 0 }, 30);
      policy.event({ _tag: "Started", clip: f }, "s1", 10);
      policy.observe({ playing: f }, "s1", 10);
      policy.submit(spec("u", 0), 20);
      policy.reply({ _tag: "Done", clipId: "c-u" }, 30);
      policy.observe({ playing: f, ready: [clip("c-u", item("u"))] }, "s1", 40);
      assert.deepStrictEqual(policy.busy(), { _tag: "Autoplay", enabled: false });
      policy.reply(result, 50);
      assert.deepStrictEqual(policy.busy(), after);
    }
  });
});

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

  // A 15 s continued build, projected at 16 s with its margin, against 12 s secured.
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
    const slow = {
      ...measured,
      samples: { build: [0.8, 0.8, 0.8], continued: [], length: [], aired: [] },
    };
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

  // The plan refills as the runway reaches this floor, not only at the clip floor, 11 s later.
  // Here only the plan's own wakes come.
  it("refills as the runway falls to the floor that covers the next item's build", () => {
    const { policy } = airing(protecting("air"), 30);
    policy.submit({ ...spec("first"), window: { notBeforeMs: 60_000, firm: false } });
    let wake = policy.submit(long).wake;
    for (let guard = 0; guard < 10 && policy.busy() === undefined && wake !== undefined; guard++)
      wake = policy.tick(wake).wake;
    // x was seen playing at 2 ms with 30 s left: the runway falls to long's floor, 16 s, at 14 s.
    assert.strictEqual(policy.now(), 14_002);
    assert.strictEqual(sent(policy.busy()), "filler of 5.00 s");
  });

  // A refused filler clip is asked for again as it was: 15 s of it, asked to cover a long build,
  // take 6 s to build, and the 5 s item only 2 s, so the item goes first rather than wait for it.
  it("covers an item with a refused filler clip only if that clip builds sooner", () => {
    const { policy } = airing(protecting("air", { min: 1, max: 15 }, 0), 1);
    policy.submit(long, 10);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    policy.submit(spec("short"), 1_100);
    assert.strictEqual(sent(policy.busy()), "short");
  });

  it("projects a refused filler clip's build at the length it is asked for again", () => {
    const { policy, playing } = airing(protecting("air"), 6);
    // x's start is seen, so the runway falls with the time: below the 5 s floor from 1,004 on.
    policy.event({ _tag: "Started", clip: playing }, "s1", 4);
    policy.submit(long, 10);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    // Below the floor the refill asks for the shortest clip, and the refused 15 s one goes again.
    policy.tick(1_030);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    // It builds until 7,030, and a 5 s item 2 s after it: one that must start by 8,040 cannot.
    const firm = { ...spec("firm"), window: { startByMs: 7_000, firm: true } };
    const refused = policy
      .submit(firm, 1_040)
      .actions.flatMap((action) => (action._tag === "Refused" ? [action.refusal._tag] : []));
    assert.deepStrictEqual(refused, ["WouldMissDeadline"]);
  });

  // Refused, the clip builds nothing. A 5 s item that must start by 7,040 can, as x ends at
  // 6,002, and is not refused as if 15 s of filler built ahead of it until 6,010.
  it("projects no build for a filler clip the provider refused", () => {
    const { policy } = airing(protecting("air"), 6);
    policy.submit(long, 10);
    assert.strictEqual(sent(policy.busy()), "filler of 15.00 s");
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    const firm = { ...spec("firm"), window: { startByMs: 7_000, firm: true } };
    const refused = policy
      .submit(firm, 40)
      .actions.flatMap((action) => (action._tag === "Refused" ? [action.refusal._tag] : []));
    assert.deepStrictEqual(refused, []);
  });

  // A cover goes out for dark air projected, not for the margin alone. A 15 s item builds in 6 s,
  // and nearly 7 s are secured.
  it("sends no filler clip ahead of a build the air secured outlasts, by less than the margin", () => {
    const { policy } = airing(protecting("air"), 7);
    policy.submit(spec("item", 1, 15));
    assert.strictEqual(sent(policy.busy()), "item");
  });

  // An item on a cut lane, or an Asap one, has a time to meet: now. With 5.5 s secured, above the
  // floor, and a 6 s build, only an item without one waits for a cover.
  it("sends a cut lane's item and an Asap one ahead of the cover their builds would need", () => {
    for (const [value, first] of [
      [spec("cutter", 0, 15), "cutter"],
      [{ ...spec("asap", 1, 15), start: { _tag: "Asap" } } as const, "asap"],
      [spec("line", 1, 15), "filler of 5.00 s"],
    ] as const) {
      const { policy } = airing(protecting("air"), 5.5);
      policy.submit(value);
      assert.strictEqual(sent(policy.busy()), first);
    }
  });

  // Released, a Manual item airs as an Asap one does, at the next boundary.
  it("sends a released Manual item ahead of the cover its build would need, as an Asap one", () => {
    const { policy } = airing(protecting("air"), 5.5);
    policy.submit({ ...spec("held", 1, 15), start: { _tag: "Manual" } });
    // Held, it is built ahead only while the runway keeps the floor that covers its build.
    assert.strictEqual(sent(policy.busy()), "filler of 5.00 s");
    policy.send({ _tag: "Release", id: 1, key: key("held") });
    policy.reply({ _tag: "Done", clipId: "f0" });
    assert.strictEqual(sent(policy.busy()), "held");
  });

  // The cover an item had went with its lost session, and on the next no air is secured: it is
  // covered again there.
  it("covers a carried item again on the session it is carried to", () => {
    const { policy, playing } = airing(protecting("air"), 12);
    policy.submit(long);
    assert.strictEqual(sent(policy.busy()), "filler of 6.67 s");
    policy.reply({ _tag: "Done", clipId: "f0" });
    const cover = clip("f0", { _tag: "Filler", index: 0 }, 6.67);
    policy.observe({ playing, ready: [cover], continuable: ["x", "f0"] });
    assert.strictEqual(sent(policy.busy()), "long");
    policy.reply({ _tag: "Done", clipId: "c-long" });
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    policy.open("s2");
    // With no clip to continue there, it builds in 6 s, and a clip that airs as long covers it.
    assert.strictEqual(sent(policy.busy("s2")), "filler of 6.00 s");
  });

  // With 14.5 s left before s2's cap, the refill's shortest clip would air whole there, but the
  // refused 15 s clip it asks for again would not: that waits for a session it fits on.
  it("sends a refused filler clip again only where it airs whole before the cap", () => {
    const { policy } = airing(protecting("air"), 6);
    policy.submit(long, 10);
    policy.edit([{ _tag: "Withdraw", key: key("long") }], false, 20);
    policy.reply(failed("replied"), 30);
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" }, 40);
    policy.open("s2", 16_500, 50);
    policy.tick(1_030);
    assert.strictEqual(sent(policy.busy("s2")), undefined);
  });
});

// ReactorTest numbers clip ids per session, so two sessions' clips can share one.
// A clip is looked up by id only on the session it was reported by or a command went to.
describe("PlayoutPolicy, clip ids", () => {
  const filler = (clipId: string, index: number) => clip(clipId, { _tag: "Filler", index });
  /** s1 on air with a clip of its own and filler 0 in flight, and s2 with filler 1 in flight. */
  const renewing = () => {
    const policy = drive({ config: filled });
    policy.tick(0);
    policy.open("s1", 60_000);
    const x = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: x });
    policy.observe({ playing: x });
    policy.tick(30_010);
    policy.open("s2", 60_000);
    // Each session names its filler clip c1.
    policy.reply({ _tag: "Done", clipId: "c1" }, undefined, "s1");
    policy.reply({ _tag: "Done", clipId: "c1" }, undefined, "s2");
    return { policy, x };
  };

  it("settles on a removal's result only the clip on the session it went to", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    policy.observe({ playing: filler("c0", 0), ready: [filler("c1", 1)] }, "s1");
    policy.open("s2");
    policy.submit(spec("x"));
    policy.reply({ _tag: "Done", clipId: "c1" }, undefined, "s2");
    policy.observe({ ready: [clip("c1", item("x"))] }, "s2");
    assert.deepStrictEqual(policy.busy("s1"), { _tag: "Remove", clipId: "c1" });
    policy.reply({ _tag: "Done" }, undefined, "s1");
    assert.strictEqual(policy.state().items.get(key("x"))?.phase, "Ready");
  });

  it("measures each session's filler build, though their clips share an id", () => {
    const { policy, x } = renewing();
    policy.observe({ playing: x, ready: [filler("c1", 0)] }, "s1");
    policy.observe({ ready: [filler("c1", 1)] }, "s2");
    assert.strictEqual(policy.state().samples.build.length, 2);
  });

  it("forgets a failed filler clip only on its own session", () => {
    const { policy } = renewing();
    policy.event(buildFailed(filler("c1", 0)), "s1");
    policy.observe({ ready: [filler("c1", 1)] }, "s2");
    assert.strictEqual(policy.state().samples.build.length, 1);
  });

  it("adopts an item's clip on its new session, though its clip taken off before had the id", () => {
    const policy = drive();
    const timed = {
      ...spec("timed"),
      start: { _tag: "At", time: 12_000, late: "nextBoundary" },
    } as const;
    policy.tick(0);
    policy.open("s1");
    const x = clip("x", undefined, 5);
    const y = clip("y", undefined, 10);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x, ready: [y] }, "s1", 10);
    policy.submit(timed, 20);
    policy.reply({ _tag: "Done", clipId: "c1" }, 30);
    policy.observe({ playing: x, ready: [y, clip("c1", item("timed"))] }, "s1", 40);
    // y goes, and timed is Ready too early: it is taken off, to be built again.
    policy.observe({ playing: x, ready: [clip("c1", item("timed"))] }, "s1", 50);
    assert.deepStrictEqual(policy.busy("s1"), { _tag: "Remove", clipId: "c1" });
    policy.reply({ _tag: "Done" }, 60);
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "the connection failed" }, 70);
    policy.open("s2", 600_000, 80);
    const z = clip("z", undefined, 20);
    policy.event({ _tag: "Started", clip: z }, "s2", 90);
    policy.observe({ playing: z }, "s2", 90);
    assert.deepStrictEqual(enqueued(policy.actions, "s2"), ["timed"]);
    policy.reply({ _tag: "Done", clipId: "c1" }, 100, "s2");
    policy.observe({ playing: z, ready: [clip("c1", item("timed"))] }, "s2", 110);
    assert.strictEqual(policy.state().items.get(key("timed"))?.phase, "Ready");
  });

  it("cuts a clip on the session on air, though a clip of its id was cut on another", () => {
    const policy = drive();
    policy.tick(0);
    policy.open("s1");
    const first = clip("c1", { _tag: "Filler", index: 0 }, 15);
    policy.event({ _tag: "Started", clip: first }, "s1", 10);
    policy.submit(spec("u", 0), 20);
    policy.reply({ _tag: "Done", clipId: "c2" }, 30);
    policy.observe({ playing: first, ready: [clip("c2", item("u"))] }, "s1", 40);
    assert.deepStrictEqual(policy.busy("s1"), { _tag: "Autoplay", enabled: false });
    // s1 goes before the cut ends: u is built again on s2, which airs filler of its own named c1.
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "the connection failed" }, 50);
    policy.open("s2", 600_000, 60);
    const second = clip("c1", { _tag: "Filler", index: 1 }, 15);
    policy.event({ _tag: "Started", clip: second }, "s2", 70);
    policy.reply({ _tag: "Done", clipId: "c2" }, 80, "s2");
    policy.observe({ playing: second, ready: [clip("c2", item("u"))] }, "s2", 90);
    assert.deepStrictEqual(policy.busy("s2"), { _tag: "Autoplay", enabled: false });
  });

  it("keeps a continued clip behind the clip it follows on its own session", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open("s1", 60_000);
    // s1 airs a clip of its own, with another of its own Ready, named c2.
    const x = clip("x", undefined, 50);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x, ready: [clip("c2")] }, "s1", 10);
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 60_000, 30_020);
    // p1 and p2 build on s2 in 2 s each, as measured, and p2's clip is named c2 too.
    const ready: Array<SourceClip> = [];
    for (const [name, clipId, at] of [
      ["p1", "c1", 30_030],
      ["p2", "c2", 32_030],
    ] as const) {
      policy.submit(spec(name), at);
      policy.reply({ _tag: "Done", clipId }, at + 10, "s2");
      ready.push(clip(clipId, item(name)));
      policy.observe({ ready: [...ready], continuable: ["c1", "c2"] }, "s2", at + 2_000);
    }
    // xc, projected Ready only after p1 would end, continues from p2 and follows it.
    const continued = { ...spec("xc"), continuity: true };
    policy.edit([{ _tag: "Insert", spec: continued, anchor: key("p2"), side: "before" }]);
    assert.deepStrictEqual(policy.busy("s2"), {
      _tag: "Enqueue",
      request: continued.request,
      tag: item("xc"),
      continueFrom: "c2",
    });
    policy.reply({ _tag: "Done", clipId: "cx" }, 34_050, "s2");
    ready.push(clip("cx", item("xc")));
    policy.observe({ ready, continuable: ["c1", "c2", "cx"] }, "s2", 40_040);
    const moves = commands(policy.actions).filter((action) => action.command._tag === "Move");
    assert.deepStrictEqual(moves, []);
  });
});

const withdrawn = (actions: ReadonlyArray<Policy.Action>) =>
  actions.flatMap((action) => (action._tag === "Withdrawn" ? [action.outcome] : []));

describe("PlayoutPolicy, edits", () => {
  /**
   * `a` starts while its replacement `r` builds, and `r`'s clip is being removed: a replacement
   * whose item started first is dropped.
   */
  const replacedAfterStart = () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(spec("a"));
    built(policy, ["a"]);
    policy.edit([{ _tag: "Replace", key: key("a"), spec: spec("r") }]);
    policy.reply({ _tag: "Done", clipId: "c-r" });
    const first = clip("c-a", item("a"));
    policy.event({ _tag: "Started", clip: first });
    const building = clip("c-r", item("r"));
    policy.observe({ playing: first, building: [building] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
    return { policy, removing: { first, clip: building } };
  };
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

  // Found by the property across renewals: a replacement whose item started first is dropped,
  // but its removal, refused while it built, went again only once the session's queues changed,
  // and its build finishing was no change to them. It aired after its item.
  it("asks again for a refused removal once the clip it removes has finished building", () => {
    const { policy, removing } = replacedAfterStart();
    policy.reply(failed("replied"));
    policy.observe({ playing: removing.first, ready: [removing.clip] });
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
  });

  // One whose outcome is unknown was held back the same way, with nothing left to change.
  it("asks again at once for a removal whose outcome is unknown", () => {
    const { policy, removing } = replacedAfterStart();
    policy.observe({ playing: removing.first, ready: [removing.clip] });
    policy.reply(failed("unknown"));
    assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-r" });
  });

  // A refused removal waits for its session's queues to change, but a held clip about to air
  // cannot: its removal is asked again then, once, and drops it.
  it("drops a held item withdrawn while its removal was refused, as it comes to air", () => {
    for (const again of [{ _tag: "Done" }, failed("replied")] as const) {
      const policy = drive();
      policy.tick(0);
      policy.open();
      policy.submit({ ...spec("h"), start: { _tag: "Manual" } }, 10);
      const playing = clip("x", undefined, 10);
      policy.event({ _tag: "Started", clip: playing }, "s1", 20);
      const ready = [clip("y", undefined, 1), clip("c-h", item("h"))];
      policy.observe({ playing, ready }, "s1", 21);
      policy.edit([{ _tag: "Withdraw", key: key("h") }], false, 30);
      policy.reply(failed("replied"), 40);
      // x ends at 10,020 and 1 s of y airs after it: h is 1.5 s from airing at 9,520.
      policy.tick(9_520);
      assert.deepStrictEqual(policy.busy(), { _tag: "Remove", clipId: "c-h" });
      policy.reply(again, 9_530);
      if (again._tag === "Failed") {
        // Refused again, it waits for the queues to change, as any refused removal does.
        assert.isUndefined(policy.busy());
        policy.observe({ playing, ready: [...ready, clip("z")] }, "s1", 9_540);
        policy.reply({ _tag: "Done" }, 9_550);
      }
      assert.deepStrictEqual(statuses(policy.actions, "h").at(-1), "Dropped");
      assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn"]);
    }
  });

  // A group key's withdrawal answers for the parts the group had when it was made. A part's
  // replacement made afterwards is no part of it, and its drop leaves that answer as it was.
  it("answers a group's withdrawal from its parts then, not from a replacement made after it", () => {
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
    built(policy, ["p1"]);
    const first = clip("c-p1", item("p1"));
    policy.event({ _tag: "Started", clip: first });
    policy.observe({ playing: first });
    // p2's enqueue is in flight: the group's withdrawal waits on it.
    policy.edit([{ _tag: "Withdraw", key: key("g") }]);
    policy.edit([{ _tag: "Replace", key: key("p2"), spec: spec("r") }]);
    policy.edit([{ _tag: "Withdraw", key: key("r") }]);
    policy.reply(failed("unknown"));
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    assert.deepStrictEqual(statuses(policy.actions, "r"), ["Accepted", "Dropped"]);
    assert.deepStrictEqual(withdrawn(policy.actions), ["withdrawn", "already-started"]);
  });

  // An edit refused as it would miss a deadline took back its items only: a part's replacement
  // in it took its whole group with it, so a later withdrawal of a part was never answered, and
  // a withdrawal in it of a key the plan did not know was answered beside the refusal.
  it("leaves the plan as it was when it refuses an edit that would miss a deadline", () => {
    const policy = drive({ from: measured });
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
    const before = policy.state();
    const refused = policy.edit([
      { _tag: "Replace", key: key("p2"), spec: spec("p2b") },
      { _tag: "Withdraw", key: key("unknown") },
      { _tag: "Submit", spec: { ...spec("late"), window: { startByMs: 1_000, firm: true } } },
    ]);
    const answers = refused.actions.flatMap((action) =>
      action._tag === "Refused" || action._tag === "Withdrawn" ? [action._tag] : [],
    );
    assert.deepStrictEqual(answers, ["Refused"]);
    assert.deepStrictEqual(policy.state().items, before.items);
    assert.deepStrictEqual(policy.state().groups, before.groups);
    assert.strictEqual(policy.state().nextOrder, before.nextOrder);
    const withdrawal = policy.edit([{ _tag: "Withdraw", key: key("p2") }]);
    assert.deepStrictEqual(withdrawn(withdrawal.actions), ["withdrawn"]);
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

  // Two renewal opens are refused with nothing allocated while s1 holds the air, the second
  // asking to wait past s1's cap. They count toward no limit, then or once s1 is gone.
  it("counts no refusal that allocated nothing refused while a session held the air, then or after", () => {
    const policy = drive({ config: { ...config, leadMs: 30_000 } });
    const refuse = (retryAfterMs?: number) =>
      policy.send({
        _tag: "OpenFailed",
        reason: "429",
        fatal: false,
        allocated: false,
        ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      });
    policy.tick(0);
    policy.open("s1", 60_000);
    policy.event({ _tag: "Started", clip: clip("x", undefined, 70) }, "s1", 10);
    policy.tick(30_001);
    refuse();
    policy.tick(policy.now() + 1_000);
    refuse(40_000);
    policy.tick(policy.now() + 40_000);
    // s1's cap has ended it. With nothing holding the air every refusal counts, and the third
    // fails the playout.
    assert.deepStrictEqual(policy.state().sessions, []);
    const failed = [3_000, 4_000, 5_000].map((wait) => {
      refuse();
      const failing = policy.actions.some((action) => action._tag === "Fail");
      policy.tick(policy.now() + wait);
      return failing;
    });
    assert.deepStrictEqual(failed, [false, false, true]);
  });

  // Three renewal opens that may have billed ran out while s1 held the air, and the one open
  // after s1's cap gave s2, which holds the air before it airs a clip. A refusal of s2's renewal
  // that allocated nothing counts toward no limit, so it pauses nothing either.
  it("asks again after a refusal that allocated nothing once the setups that count ran out", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    policy.tick(0);
    policy.open("s1", 120_000);
    policy.tick(60_010);
    for (let failure = 0; failure < 3; failure++) {
      policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true });
      policy.tick(policy.now() + 5_000);
    }
    assert.isTrue(policy.state().openingPaused);
    const paused = opens();
    policy.tick(120_010);
    policy.open("s2", 30_000);
    // s2's cap is within the lead, so its renewal opens at once.
    assert.strictEqual(opens(), paused + 2);
    policy.send({ _tag: "OpenFailed", reason: "429", fatal: false, allocated: false });
    policy.tick(policy.now() + 5_000);
    assert.strictEqual(opens(), paused + 3);
  });

  // s1 airs a 5 s clip after another while each renewal open fails 2 s after it is asked, having
  // allocated a session or maybe so. A clip on the session that already held the air says nothing
  // of those opens: only a session's first clip ends their run.
  it("pauses renewal after three opens that may bill, though the session on air airs clips", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    policy.tick(0);
    policy.open("s1", 120_000);
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    const failed: Array<number> = [];
    let asked = opens();
    let failing: number | undefined;
    for (let at = 60_000; at < 119_000; at += 100) {
      if (at % 5_000 === 0)
        policy.event({ _tag: "Started", clip: clip(`x${String(at)}`) }, "s1", at);
      policy.tick(at);
      if (opens() > asked) {
        asked = opens();
        failing = at + 2_000;
      }
      if (failing !== undefined && at >= failing) {
        failing = undefined;
        failed.push(at);
        policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true }, at);
      }
    }
    // Each retry waits a second longer; the third failure pauses opening until s1's cap.
    assert.deepStrictEqual(failed, [62_100, 65_100, 69_100]);
    assert.isTrue(policy.state().openingPaused);
  });

  it("ends a run of failed setups at a session's first clip, seen in a read or at its start", () => {
    for (const how of ["read", "start"] as const) {
      const policy = drive({ config: { ...config, leadMs: 60_000 } });
      policy.tick(0);
      policy.open("s1", 120_000);
      policy.tick(60_010);
      policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true });
      policy.tick(policy.now() + 1_000);
      policy.send({ _tag: "OpenFailed", reason: "503", fatal: false, allocated: true });
      policy.tick(policy.now() + 2_000);
      policy.open("s2", 120_000);
      assert.strictEqual(policy.state().setupFailures, 2, how);
      const first = clip("y");
      if (how === "read") policy.observe({ playing: first }, "s2");
      else policy.event({ _tag: "Started", clip: first }, "s2");
      assert.strictEqual(policy.state().setupFailures, 0, how);
    }
  });

  // The one more open after a pause waits out the last refusal's Retry-After, though the session
  // on air is gone sooner.
  it("waits out the last refusal's Retry-After for the open after a pause", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    const fail = (retryAfterMs?: number) =>
      policy.send({
        _tag: "OpenFailed",
        reason: "503",
        fatal: false,
        allocated: true,
        ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      });
    policy.tick(0);
    policy.open("s1", 120_000);
    policy.tick(60_010);
    fail();
    policy.tick(policy.now() + 1_000);
    fail();
    policy.tick(policy.now() + 2_000);
    fail(60_000);
    const refused = policy.now();
    const asked = opens();
    const lost = policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" }, refused + 5_000);
    assert.strictEqual(opens(), asked);
    assert.strictEqual(lost.wake, refused + 60_000);
    policy.tick(refused + 60_000);
    assert.strictEqual(opens(), asked + 1);
  });

  // A session lost before any clip sent to it started is a failed setup, as a failed open is.
  it("waits a second longer for each session lost before any clip sent to it started", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opensIn = (step: Policy.Step) => step.actions.some((action) => action._tag === "Open");
    policy.tick(0);
    policy.open("s1", 120_000);
    policy.event({ _tag: "Started", clip: clip("x", undefined, 70) }, "s1", 10);
    assert.isTrue(opensIn(policy.tick(60_010)));
    const waits: Array<number> = [];
    for (const id of ["r1", "r2"]) {
      // Each replacement is sent an item, then lost.
      policy.open(id, 600_000, policy.now() + 100);
      policy.submit(spec(id));
      const lost = policy.send({ _tag: "Lost", sessionId: id, reason: "gone" });
      const lostAt = policy.now();
      if (!opensIn(lost)) assert.isTrue(opensIn(policy.tick(lost.wake)));
      waits.push(policy.now() - lostAt);
    }
    assert.deepStrictEqual(waits, [1_000, 2_000]);
  });

  // s1's renewal is refused with a Retry-After of 20 s, then s1 is lost before the item sent to it
  // started: a failed setup, whose own wait, 2 s as the second failure in a row, is shorter.
  it("waits out a refusal's Retry-After after a session lost before any clip", () => {
    const policy = drive({ config: { ...config, leadMs: 60_000 } });
    const opens = () => policy.actions.filter((action) => action._tag === "Open").length;
    policy.tick(0);
    policy.open("s1", 30_000);
    policy.submit(spec("a"));
    // s1's cap is within the lead, so its renewal opens at once.
    const asked = opens();
    policy.send({
      _tag: "OpenFailed",
      reason: "429",
      fatal: false,
      allocated: false,
      retryAfterMs: 20_000,
    });
    const refusedAt = policy.now();
    policy.send({ _tag: "Lost", sessionId: "s1", reason: "gone" });
    policy.tick(refusedAt + 19_999);
    assert.strictEqual(opens(), asked);
    policy.tick(refusedAt + 20_000);
    assert.strictEqual(opens(), asked + 1);
  });

  // Six renewal opens are refused with nothing allocated while s1 holds the air, then the
  // replacement is lost before the item sent to it started. Each failure lengthens the wait by a
  // second only up to three, so the next open still goes before s1's cap.
  it("waits at most a second for each of three failures before it opens again", () => {
    const policy = drive();
    const opensIn = (step: Policy.Step) => step.actions.some((action) => action._tag === "Open");
    policy.tick(0);
    policy.open("s1", 60_000);
    const x = clip("x", undefined, 70);
    policy.event({ _tag: "Started", clip: x }, "s1", 100);
    policy.observe({ playing: x }, "s1", 100);
    assert.isTrue(opensIn(policy.tick(30_001)));
    const waits: Array<number> = [];
    for (let refusal = 0; refusal < 6; refusal++) {
      const refused = policy.send(
        { _tag: "OpenFailed", reason: "429", fatal: false, allocated: false },
        policy.now() + 100,
      );
      const refusedAt = policy.now();
      assert.isTrue(opensIn(policy.tick(refused.wake)));
      waits.push(policy.now() - refusedAt);
    }
    assert.deepStrictEqual(waits, [1_000, 2_000, 3_000, 3_000, 3_000, 3_000]);
    policy.open("s2", 600_000, policy.now() + 100);
    policy.submit(spec("i", 1, 20));
    assert.strictEqual(policy.busy("s2")?._tag, "Enqueue");
    policy.reply({ _tag: "Done", clipId: "c-i" }, undefined, "s2");
    let step = policy.send({ _tag: "Lost", sessionId: "s2", reason: "gone" }, policy.now() + 2_000);
    const lostAt = policy.now();
    for (let wakes = 0; !opensIn(step) && step.wake !== undefined && wakes < 10; wakes++)
      step = policy.tick(step.wake);
    assert.strictEqual(policy.now() - lostAt, 3_000);
    assert.isBelow(policy.now(), 60_001);
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

// Each decision that waits on the time comes at its own deadline, which the wake names; the
// property below checks that nothing falls due between wakes.
describe("PlayoutPolicy, wakes", () => {
  it("wakes as the runway falls to the filler's floor, and does not poll below it", () => {
    const policy = drive({ config: protecting("order") });
    policy.tick(0);
    policy.open("s1", 600_000, 1);
    const filler = clip("f0", { _tag: "Filler", index: 0 }, 20);
    policy.observe({ ready: [filler] });
    policy.reply({ _tag: "Done", clipId: "f0" });
    policy.event({ _tag: "Started", clip: filler }, "s1", 1_000);
    // 20 s from 1 s: the runway falls to the floor of 5 s at 16 s.
    assert.strictEqual(policy.observe({ playing: filler }, "s1", 1_001).wake, 16_000);
    const floor = policy.tick(16_000);
    assert.deepStrictEqual(enqueued(floor.actions), ["filler"]);
    // Below the floor nothing else is due until the lead ahead of the session's cap.
    assert.strictEqual(floor.wake, 570_001);
    assert.strictEqual(policy.reply({ _tag: "Done", clipId: "f1" }, 16_010).wake, 570_001);
  });

  it("drops a firm item once its projection reaches its startBy, when it does", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    // a builds from 10 ms to 2,010. b, on the lane that cuts, airs once Ready, not after a: with
    // 2 s of build after a's, it could start by 4,010.
    policy.submit(spec("a"), 10);
    policy.reply({ _tag: "Done", clipId: "c-a" }, 11);
    const firm = { ...spec("b", 0), window: { startByMs: 4_000, firm: true } };
    // Past 2,010 its projection grows with the time, and reaches 4,020 at 2,020.
    assert.strictEqual(policy.submit(firm, 20).wake, 2_020);
    assert.deepStrictEqual(statuses(policy.tick(2_019).actions, "b"), []);
    assert.deepStrictEqual(statuses(policy.tick(2_020).actions, "b"), ["Dropped"]);
  });

  it("drops an At item that has not started by its time, at that time", () => {
    const policy = drive();
    policy.tick(0);
    const at = { ...spec("a"), start: { _tag: "At", time: 5_000, late: "drop" } } as const;
    assert.strictEqual(policy.submit(at, 10).wake, 5_000);
    assert.deepStrictEqual(statuses(policy.tick(5_000).actions, "a"), ["Dropped"]);
  });

  it("takes back a held item Ready behind the clip on air as it comes within the margin", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit({ ...spec("h"), start: { _tag: "Manual" } }, 10);
    const playing = clip("x", undefined, 10);
    policy.event({ _tag: "Started", clip: playing }, "s1", 20);
    // x ends at 10,020 and 1 s of y airs after it: h is 1.5 s from airing at 9,520.
    const ready = [clip("y", undefined, 1), clip("c-h", item("h"))];
    assert.strictEqual(policy.observe({ playing, ready }, "s1", 21).wake, 9_520);
    assert.deepStrictEqual(commands(policy.tick(9_519).actions), []);
    assert.deepStrictEqual(
      commands(policy.tick(9_520).actions).map((action) => action.command),
      [{ _tag: "Remove", clipId: "c-h" }],
    );
  });

  it("builds an At item once the air secured lasts to its time, though nothing plays", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    const x = clip("x", undefined, 5);
    policy.event({ _tag: "Started", clip: x }, "s1", 20);
    policy.observe({ playing: x, ready: [clip("y", undefined, 5)] }, "s1", 21);
    const at = { ...spec("a"), start: { _tag: "At", time: 20_000, late: "nextBoundary" } } as const;
    policy.submit(at, 30);
    // x overran its 5 s without an end reported: y's 5 s last to 20 s from 15 s on.
    assert.strictEqual(policy.tick(5_020).wake, 15_000);
    assert.deepStrictEqual(enqueued(policy.tick(15_000).actions), ["a"]);
  });

  it("drops in the same step a firm item that the build it sends makes late", () => {
    const policy = drive({ from: measured });
    policy.tick(0);
    policy.open();
    // b may not build before 1,010, and has 2 s of build to start by 4,010.
    const firm = { ...spec("b"), window: { notBeforeMs: 1_000, startByMs: 4_000, firm: true } };
    policy.submit(firm, 10);
    // a builds meanwhile, from 20 ms to 2,020: b could then start at 4,020 at the earliest.
    const sent = policy.submit(spec("a"), 20);
    assert.deepStrictEqual(enqueued(sent.actions), ["a"]);
    assert.deepStrictEqual(statuses(sent.actions, "b"), ["Dropped"]);
  });

  // During a renewal the runway measured is the replacement's, which does not fall while the
  // session on air airs: the plan names no wake for it to fall to the floor.
  it("wakes for no floor while the replacement's runway, which does not fall, is above it", () => {
    const policy = drive({ config: protecting("air"), from: measured });
    policy.tick(0);
    policy.open("s1", 60_000);
    const x = clip("x", undefined, 60);
    policy.event({ _tag: "Started", clip: x }, "s1", 10);
    policy.observe({ playing: x, continuable: ["x"] }, "s1", 10);
    assert.isTrue(policy.tick(30_010).actions.some((action) => action._tag === "Open"));
    policy.open("s2", 600_000, 30_020);
    const fillers = [0, 1, 2].map((index) =>
      clip(`f${String(index)}`, { _tag: "Filler", index }, index === 2 ? 6.001 : 5),
    );
    policy.observe({ ready: fillers, continuable: ["f0", "f1", "f2"] }, "s2", 30_030);
    policy.submit({ ...spec("first"), window: { notBeforeMs: 60_000, firm: false } }, 30_040);
    const long: Policy.Spec = { ...spec("long", 1, 15), continuity: true };
    // s2's 16.001 s stay Ready 1 ms above long's floor: the next deadline is s1's cap.
    assert.strictEqual(policy.submit(long, 30_050).wake, 60_001);
  });

  it("cuts for no item settled while its clip is still listed Ready", () => {
    const policy = drive();
    policy.tick(0);
    policy.open();
    policy.submit(
      { ...spec("u", 0), start: { _tag: "At", time: 5_000, late: "nextBoundary" } },
      10,
    );
    const filler = clip("f", { _tag: "Filler", index: 0 }, 20);
    policy.event({ _tag: "Started", clip: filler }, "s1", 20);
    policy.observe({ playing: filler, ready: [clip("c-u", item("u"))] }, "s1", 21);
    policy.edit([{ _tag: "Withdraw", key: key("u") }], false, 30);
    policy.reply({ _tag: "Done" }, 40);
    assert.deepStrictEqual(statuses(policy.actions, "u"), ["Accepted", "Ready", "Dropped"]);
    // Its clip is gone, though no read has shown it yet; its time comes and cuts nothing.
    assert.deepStrictEqual(commands(policy.tick(5_000).actions), []);
  });
});

describe("PlayoutPolicy, any script", () => {
  for (const script of counterexamples)
    it(`keeps every promise of the plan for ${script.join(", ")}`, () => check(script));
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
