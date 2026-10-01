/**
 * The page's DOM: its elements, and how the Studio's state is drawn into
 * them. Plain DOM code that builds text nodes, never markup, so a prompt is
 * shown as typed. `Studio.ts` decides what to show and when.
 */
import * as H3 from "reactor-effect-client/H3";
import { summarize } from "reactor-effect-client/ReactorError";
import type { FailureSummary } from "reactor-effect-client/ReactorError";
import type { CloseReport, Snapshot, Status } from "reactor-effect-client/Session";

/** The page's element with this id, checked to be the kind the page uses it as. */
const element = <E extends HTMLElement>(id: string, kind: new () => E): E => {
  const found = document.getElementById(id);
  if (!(found instanceof kind)) throw new Error(`the page has no ${kind.name} #${id}`);
  return found;
};

/** The elements the Studio reads or listens to; module scripts run once the page is parsed. */
export const ui = {
  screen: element("screen", HTMLElement),
  empty: element("empty", HTMLElement),
  start: element("start", HTMLButtonElement),
  reconnect: element("reconnect", HTMLButtonElement),
  sound: element("sound", HTMLButtonElement),
  stop: element("stop", HTMLButtonElement),
  composer: element("composer", HTMLFormElement),
  prompt: element("prompt", HTMLTextAreaElement),
  seconds: element("seconds", HTMLInputElement),
  send: element("send", HTMLButtonElement),
  files: element("files", HTMLInputElement),
  addImages: element("add-images", HTMLButtonElement),
  sampleImage: element("sample-image", HTMLButtonElement),
  references: element("references", HTMLElement),
  autoplay: element("autoplay", HTMLInputElement),
  hold: element("hold", HTMLInputElement),
  queue: element("queue", HTMLElement),
};

type Tone = "ok" | "warn" | "err" | "info" | "accent";

interface Props {
  readonly class?: string;
  readonly title?: string;
  readonly data?: Readonly<Record<string, string>>;
}

const h = <K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props: Props,
  ...children: ReadonlyArray<Node | string>
): HTMLElementTagNameMap[K] => {
  const node = document.createElement(tag);
  if (props.class !== undefined) node.className = props.class;
  if (props.title !== undefined) node.title = props.title;
  for (const [name, value] of Object.entries(props.data ?? {})) node.dataset[name] = value;
  node.append(...children);
  return node;
};

const chip = (text: string, tone?: Tone, title?: string): HTMLElement =>
  h(
    "span",
    {
      class: tone === undefined ? "chip" : `chip ${tone}`,
      ...(title === undefined ? {} : { title }),
    },
    text,
  );

/** A clip id as the page shows it: its last four digits, enough within one session. */
export const short = (clipId: string): string => `…${clipId.slice(-4)}`;

const outcomes = {
  "not-submitted": { tone: "info", title: "Nothing reached Reactor, so trying again is safe." },
  unknown: {
    tone: "warn",
    title: "It may have reached Reactor, so the SDK never sends it again.",
  },
  replied: { tone: "err", title: "Reactor answered it with a refusal or a failure." },
} as const;

/** A failure's tag, reason and dispatch outcome, the facts a caller routes on. */
const failureChips = (summary: FailureSummary): HTMLElement => {
  const outcome = summary.outcome === undefined ? undefined : outcomes[summary.outcome];
  return h(
    "span",
    { class: "chips" },
    chip(summary._tag, "err"),
    chip(summary.reason, "err"),
    ...(outcome === undefined || summary.outcome === undefined
      ? []
      : [chip(summary.outcome, outcome.tone, outcome.title)]),
    ...(summary.operation === undefined ? [] : [chip(summary.operation)]),
  );
};

const failureBox = (summary: FailureSummary): HTMLElement =>
  h("div", { class: "failure" }, failureChips(summary), h("p", {}, summary.message));

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

export const mode = ({
  name,
  about,
  tone,
}: {
  readonly name: string;
  readonly about: string;
  readonly tone: "live" | "offline" | "none";
}): void => {
  const badge = element("mode", HTMLElement);
  badge.textContent = name;
  badge.className = tone === "none" ? "badge" : `badge ${tone}`;
  element("about", HTMLElement).textContent = about;
};

// ---------------------------------------------------------------------------
// Event log
// ---------------------------------------------------------------------------

export type Kind = "page" | "session" | "h3" | "command" | "fault" | "failure";

export interface Entry {
  readonly time: string;
  readonly kind: Kind;
  readonly text: string;
  readonly failure?: FailureSummary;
}

/** The log keeps this many entries, newest first. */
const logLength = 400;

export const log = (entry: Entry): void => {
  const list = element("log", HTMLOListElement);
  const text = h("span", { class: "text" }, entry.text);
  if (entry.failure !== undefined)
    text.append(`: ${entry.failure.message}`, failureChips(entry.failure));
  list.prepend(
    h(
      "li",
      {},
      h("time", {}, entry.time),
      h("span", { class: `kind is-${entry.kind}` }, entry.kind),
      text,
    ),
  );
  while (list.childElementCount > logLength) list.lastElementChild?.remove();
};

// ---------------------------------------------------------------------------
// Session panel
// ---------------------------------------------------------------------------

const statusTones: Record<Status, Tone | undefined> = {
  idle: undefined,
  connecting: "info",
  waiting: "info",
  ready: "ok",
  disconnected: "warn",
  closing: undefined,
  closed: undefined,
};

/** The session as its last snapshot shows it; undefined while none is open. */
export const session = (snapshot: Snapshot | undefined): void => {
  element("session-id", HTMLElement).textContent =
    snapshot === undefined ? "none" : (snapshot.remote?.sessionId ?? "allocating…");
  element("session-state", HTMLElement).replaceChildren(
    snapshot === undefined ? chip("idle") : chip(snapshot.status, statusTones[snapshot.status]),
    ...(snapshot?.reconnecting === true ? [" ", chip("reconnecting", "warn")] : []),
  );
  const generation = element("session-generation", HTMLElement);
  if (snapshot === undefined) {
    generation.replaceChildren("no connection yet");
    return;
  }
  generation.replaceChildren(
    `generation ${String(snapshot.generation)}`,
    ...(snapshot.lastError === undefined ? [] : [snapshot.lastError.pipe(summarize, failureBox)]),
  );
};

const availabilityTones = { Ready: "ok", Synchronizing: "info", Unavailable: "err" } as const;

/** What H3 last reported, and its two playback settings as toggles. */
export const provider = ({
  snapshot,
  contract,
}: {
  readonly snapshot: H3.ProviderSnapshot | undefined;
  readonly contract: H3.Contract | undefined;
}): void => {
  const { autoplay, hold } = ui;
  const target = element("provider-state", HTMLElement);
  if (snapshot === undefined || contract === undefined) {
    target.replaceChildren("not observed");
    autoplay.disabled = true;
    hold.disabled = true;
    return;
  }
  const facts = snapshot._tag === "Ready" ? snapshot : snapshot.lastFacts;
  const deployment = [contract.deployment.title, contract.deployment.version]
    .filter((part) => part !== null)
    .join(" ");
  target.replaceChildren(
    chip(snapshot._tag, availabilityTones[snapshot._tag]),
    ...(facts === null
      ? []
      : [
          ` ${String(facts.state.clips_played)} played · ${facts.state.seconds_sent.toFixed(1)} s sent`,
        ]),
    ...(deployment === "" ? [] : [h("div", { class: "hint" }, deployment)]),
    ...(snapshot._tag === "Unavailable" ? [snapshot.cause.pipe(summarize, failureBox)] : []),
  );
  const ready = snapshot._tag === "Ready";
  autoplay.disabled = !ready || !contract.commands.has("set_autoplay");
  hold.disabled = !ready || !contract.commands.has("set_flush_on_clip_end");
  if (facts !== null) {
    autoplay.checked = facts.state.autoplay;
    hold.checked = !facts.state.flush_on_clip_end;
  }
};

/** A closed session's cleanup evidence: termination is confirmed only by an independent read. */
export const report = ({
  closed,
  mayStillBill,
}: {
  readonly closed: CloseReport;
  readonly mayStillBill: boolean;
}): void => {
  const box = element("report", HTMLElement);
  const { remote } = closed;
  const evidence = (): string => {
    switch (remote.evidence) {
      case "terminal":
        return `read back as ${remote.state ?? "terminal"}`;
      case "absent":
        return "the coordinator no longer lists it";
      case null:
        return "none";
    }
  };
  box.hidden = false;
  box.replaceChildren(
    h(
      "h3",
      {},
      remote.confirmed ? "Closed · termination confirmed" : "Closed · termination not confirmed",
    ),
    h(
      "dl",
      {},
      h("dt", {}, "Evidence"),
      h("dd", {}, evidence()),
      h("dt", {}, "DELETE"),
      h(
        "dd",
        {},
        remote.deleteStatus === null ? "no response" : `HTTP ${String(remote.deleteStatus)}`,
      ),
      h("dt", {}, "May still bill"),
      h("dd", {}, chip(mayStillBill ? "yes" : "no", mayStillBill ? "err" : "ok")),
      ...(closed.localErrors.length === 0
        ? []
        : [
            h("dt", {}, "Local errors"),
            h("dd", {}, ...closed.localErrors.map((error) => failureBox(error))),
          ]),
    ),
  );
};

export const hideReport = (): void => {
  element("report", HTMLElement).hidden = true;
};

// ---------------------------------------------------------------------------
// The H3 queue
// ---------------------------------------------------------------------------

/** What the Studio knows of one clip it sent: the facts its operation established so far. */
export interface Card {
  readonly key: string;
  readonly prompt: string;
  readonly images: number;
  readonly clipId?: string;
  /** The length H3 builds, on its frame grid. */
  readonly seconds?: number;
  /** How the acceptance was proven: the enqueue's reply, or the clip's own metadata. */
  readonly evidence?: "correlated" | "metadata";
  readonly generated?: boolean;
  readonly started?: boolean;
  /** The lifecycle message that ended the clip. */
  readonly ended?: H3.MessageType;
  readonly failure?: FailureSummary;
}

/** A control on a clip row, as its button's data says it. */
export type Action =
  | { readonly _tag: "play"; readonly clipId: string }
  | { readonly _tag: "pop"; readonly clipId: string }
  | { readonly _tag: "move"; readonly clipId: string; readonly position: number }
  | { readonly _tag: "stop" };

const ends: Partial<Record<H3.MessageType, { readonly text: string; readonly tone?: Tone }>> = {
  clip_finished: { text: "finished", tone: "ok" },
  clip_stopped: { text: "stopped", tone: "warn" },
  clip_failed: { text: "failed · clip_failed", tone: "err" },
  clip_popped: { text: "popped" },
};

const acceptedTitle = {
  correlated: "Proven by the enqueue's own reply.",
  metadata: "Proven by the clip's metadata, which names this submission: its reply never came.",
} as const;

/** Whether the clip was accepted, and on what evidence; or why that is not known. */
const acceptance = (card: Card): HTMLElement => {
  if (card.clipId !== undefined)
    return chip(
      card.evidence === "metadata" ? "accepted · metadata" : "accepted",
      "ok",
      acceptedTitle[card.evidence ?? "correlated"],
    );
  if (card.failure === undefined) return chip("sending", "info");
  // An enqueue whose outcome is unknown may still be proven by a clip that names it.
  return card.failure.outcome === "unknown"
    ? chip("acceptance unknown", "warn", outcomes.unknown.title)
    : chip("not accepted", "err");
};

/**
 * The operation's facts as chips: acceptance, then each phase it reached, then
 * how it ended. Once its session closed, a clip with no end says so.
 */
const factChips = (card: Card | undefined, closed: boolean): ReadonlyArray<HTMLElement> => {
  if (card === undefined)
    return [
      chip(
        "not attributed yet",
        undefined,
        "H3 lists this clip; no evidence has yet tied it to a submission.",
      ),
    ];
  const chips: Array<HTMLElement> = [acceptance(card)];
  if (card.generated === true) chips.push(chip("generated", "ok"));
  if (card.started === true) chips.push(chip("started", "ok"));
  if (card.ended !== undefined) {
    const end = ends[card.ended] ?? { text: card.ended };
    chips.push(chip(end.text, end.tone));
  } else if (closed && card.failure === undefined)
    chips.push(
      chip("no end observed", "warn", "The session closed before H3 reported how this clip ended."),
    );
  if (card.images > 0)
    chips.push(chip(card.images === 1 ? "1 image" : `${String(card.images)} images`, "accent"));
  return chips;
};

interface Row {
  readonly key: string;
  readonly clipId: string | undefined;
  readonly seconds: number | undefined;
  readonly prompt: string;
  readonly card: Card | undefined;
  readonly playing: boolean;
  /** Its session closed: the row is a record, with no controls. */
  readonly closed: boolean;
  readonly controls: ReadonlyArray<{
    readonly label: string;
    readonly title: string;
    readonly action: Action;
    readonly enabled: boolean;
    readonly danger?: boolean;
  }>;
}

/** Each control button's action, so a click needs no parsing of the page's own attributes. */
const actions = new WeakMap<Element, Action>();

/** The action of the control a click landed on, if it landed on one. */
export const actionOf = (target: EventTarget | null): Action | undefined => {
  const control = target instanceof Element ? target.closest("button") : null;
  return control === null ? undefined : actions.get(control);
};

const button = (control: Row["controls"][number]): HTMLButtonElement => {
  const node = h(
    "button",
    { class: control.danger === true ? "icon danger" : "icon", title: control.title },
    control.label,
  );
  node.type = "button";
  node.disabled = !control.enabled;
  node.setAttribute("aria-label", control.title);
  actions.set(node, control.action);
  return node;
};

const rowElement = (row: Row): HTMLLIElement =>
  h(
    "li",
    { class: row.playing ? "clip playing" : "clip", data: { key: row.key } },
    h(
      "div",
      {},
      h(
        "div",
        { class: "meta" },
        h("span", {}, row.clipId === undefined ? "no clip yet" : short(row.clipId)),
        ...(row.seconds === undefined ? [] : [h("span", {}, `${row.seconds.toFixed(2)} s`)]),
      ),
      h("p", { class: "prompt", title: row.prompt }, row.prompt),
      h("div", { class: "chips" }, ...factChips(row.card, row.closed)),
      ...(row.card?.failure === undefined ? [] : [failureBox(row.card.failure)]),
    ),
    h("div", { class: "controls" }, ...row.controls.map(button)),
  );

/**
 * Draws `rows` into `list`, keeping the element of a row whose content did not
 * change, so a snapshot that moves nothing leaves the buttons under the
 * pointer in place.
 */
const reconcile = (list: HTMLElement, rows: ReadonlyArray<Row>): void => {
  const existing = new Map<string, HTMLElement>();
  for (const child of list.children)
    if (child instanceof HTMLElement && child.dataset.key !== undefined)
      existing.set(child.dataset.key, child);
  list.replaceChildren(
    ...rows.map((row) => {
      const signature = JSON.stringify(row);
      const kept = existing.get(row.key);
      if (kept?.dataset.signature === signature) return kept;
      const created = rowElement(row);
      created.dataset.signature = signature;
      return created;
    }),
  );
};

/** Clips that ended, newest first; the section keeps this many. */
const doneLength = 8;

/**
 * H3's queue as its provider last reported it, each clip with the facts the
 * Studio's operation for it established, and the controls H3 offers for it
 * now: a command the state does not list as valid, or the deployment does
 * not offer, is shown disabled.
 */
export const queue = ({
  snapshot,
  cards,
  contract,
}: {
  readonly snapshot: H3.ProviderSnapshot;
  readonly cards: ReadonlyArray<Card>;
  readonly contract: H3.Contract;
}): void => {
  const facts = snapshot._tag === "Ready" ? snapshot : snapshot.lastFacts;
  const byClip = new Map<string, Card>();
  for (const card of cards) if (card.clipId !== undefined) byClip.set(card.clipId, card);
  const observed = new Map(snapshot.clips.map((entry) => [entry.clip.clip_id, entry.clip]));
  const valid = new Set(facts?.state.valid_commands ?? []);
  const can = (command: H3.CommandName): boolean =>
    snapshot._tag === "Ready" && valid.has(command) && contract.commands.has(command);
  const row = (clip: H3.Clip, playing: boolean, controls: Row["controls"]): Row => ({
    key: clip.clip_id,
    clipId: clip.clip_id,
    seconds: clip.seconds,
    prompt: clip.prompt,
    card: byClip.get(clip.clip_id),
    playing,
    closed: false,
    controls,
  });
  /** Reordering and removal, the same for both queues. */
  const edits = (clip: H3.Clip, index: number, length: number): Row["controls"] => [
    {
      label: "↑",
      title: "Move up",
      action: { _tag: "move", clipId: clip.clip_id, position: index - 1 },
      enabled: index > 0 && can("move"),
    },
    {
      label: "↓",
      title: "Move down",
      action: { _tag: "move", clipId: clip.clip_id, position: index + 1 },
      enabled: index < length - 1 && can("move"),
    },
    {
      label: "✕",
      title: "Pop from the queue",
      action: { _tag: "pop", clipId: clip.clip_id },
      enabled: can("pop"),
      danger: true,
    },
  ];

  const playingId = facts?.state.playing_clip_id ?? null;
  const playingClip =
    playingId === null
      ? undefined
      : (facts?.queue.playout.find((clip) => clip.clip_id === playingId) ??
        observed.get(playingId));
  reconcile(
    element("playing", HTMLElement),
    playingClip === undefined
      ? []
      : [
          row(playingClip, true, [
            {
              label: "■",
              title: "Stop the playing clip",
              action: { _tag: "stop" },
              enabled: can("stop"),
              danger: true,
            },
          ]),
        ],
  );
  onAir(playingClip);

  // H3 reports a clip armed for its seam as playing while it may still head playout.
  const playout = (facts?.queue.playout ?? []).filter((clip) => clip.clip_id !== playingId);
  reconcile(
    element("playout", HTMLElement),
    playout.map((clip, index) =>
      row(clip, false, [
        {
          label: "▶",
          title: "Play now",
          action: { _tag: "play", clipId: clip.clip_id },
          enabled: can("play"),
        },
        ...edits(clip, index, playout.length),
      ]),
    ),
  );

  const generation = facts?.queue.generation ?? [];
  const listed = new Set([...generation, ...playout].map((clip) => clip.clip_id));
  if (playingId !== null) listed.add(playingId);
  const sending = cards.filter((card) => card.clipId === undefined && card.failure === undefined);
  reconcile(element("generation", HTMLElement), [
    ...generation.map((clip, index) => row(clip, false, edits(clip, index, generation.length))),
    ...sending.map((card): Row => ({
      key: card.key,
      clipId: undefined,
      seconds: undefined,
      prompt: card.prompt,
      card,
      playing: false,
      closed: false,
      controls: [],
    })),
  ]);

  record(
    cards.filter(
      (card) =>
        card.ended !== undefined ||
        (card.failure !== undefined && (card.clipId === undefined || !listed.has(card.clipId))),
    ),
    false,
  );

  element("queue-counts", HTMLElement).textContent =
    facts === null
      ? ""
      : `${String(facts.state.generation_queued)}/${String(facts.state.generation_capacity)} building · ${String(facts.state.playout_queued)}/${String(facts.state.playout_capacity)} ready`;
};

/** Clips that ended, newest first, with what their operations established. */
const record = (cards: ReadonlyArray<Card>, closed: boolean): void =>
  reconcile(
    element("done", HTMLElement),
    cards
      .slice(-doneLength)
      .reverse()
      .map((card) => ({
        key: card.key,
        clipId: card.clipId,
        seconds: card.seconds,
        prompt: card.prompt,
        card,
        playing: false,
        closed,
        controls: [],
      })),
  );

/** Once a session closed, every clip it was sent is part of its record. */
export const closedQueue = (cards: ReadonlyArray<Card>): void => {
  for (const id of ["playing", "playout", "generation"]) element(id, HTMLElement).replaceChildren();
  element("queue-counts", HTMLElement).textContent = "";
  onAir(undefined);
  record(cards, true);
};

let airing: string | undefined;

/**
 * The playing clip over the picture. The bar runs the clip's length from when
 * the page saw it start: an estimate, since H3 reports no position.
 */
const onAir = (clip: H3.Clip | undefined): void => {
  const banner = element("on-air", HTMLElement);
  banner.hidden = clip === undefined;
  if (clip === undefined || clip.clip_id === airing) {
    airing = clip?.clip_id;
    return;
  }
  airing = clip.clip_id;
  element("on-air-text", HTMLElement).textContent = `${short(clip.clip_id)}  ${clip.prompt}`;
  const progress = element("on-air-progress", HTMLElement);
  const fresh = h("span", { class: "progress" });
  fresh.id = progress.id;
  fresh.style.animationDuration = `${String(clip.seconds)}s`;
  progress.replaceWith(fresh);
};

// ---------------------------------------------------------------------------
// The composer
// ---------------------------------------------------------------------------

const profile = H3.h3ReferenceTurboRealtime;

/** The requested length, and what H3 builds from it on its frame grid. */
export const length = (seconds: number): void => {
  const frames = H3.alignFrames(profile, seconds);
  element("length", HTMLElement).replaceChildren(
    h("b", {}, `${seconds.toFixed(1)} s`),
    ` → ${String(frames)} frames, ${(frames / profile.fps).toFixed(2)} s at ${String(profile.fps)} fps`,
  );
};

/** The prompt's estimated share of H3's text budget; the provider's tokenizer decides. */
export const tokens = (prompt: string): void => {
  const estimate = H3.estimateTokens(profile, prompt);
  const target = element("tokens", HTMLElement);
  target.textContent = `≈ ${String(estimate)} of ${String(profile.prompt.maxTokens)} tokens`;
  target.style.color = estimate > profile.prompt.maxTokens ? "var(--err)" : "";
};

export interface Reference {
  readonly id: number;
  readonly name: string;
  readonly url: string;
  readonly reference: H3.ValidatedReference;
}

/** The validated references every clip sends until they are removed. */
export const references = ({
  list,
  onRemove,
}: {
  readonly list: ReadonlyArray<Reference>;
  readonly onRemove: (id: number) => void;
}): void => {
  const zone = ui.references;
  const hint = element("references-hint", HTMLElement);
  hint.hidden = list.length > 0;
  zone.replaceChildren(
    hint,
    ...list.map((entry) => {
      const image = h("img", {});
      image.src = entry.url;
      image.alt = entry.name;
      const remove = h("button", { title: "Remove" }, "✕");
      remove.type = "button";
      remove.addEventListener("click", () => onRemove(entry.id));
      const { width, height, mimeType } = entry.reference;
      const size = width === null || height === null ? "" : `${String(width)}×${String(height)} `;
      return h(
        "div",
        { class: "reference", title: entry.name },
        image,
        h("span", {}, `${size}${mimeType.replace("image/", "")}`),
        remove,
      );
    }),
  );
};

export const composerFailure = (
  failure: { readonly name: string; readonly summary: FailureSummary } | undefined,
): void => {
  const target = element("composer-failure", HTMLElement);
  if (failure === undefined) {
    target.replaceChildren();
    return;
  }
  const box = failureBox(failure.summary);
  box.prepend(h("strong", {}, `${failure.name} `));
  target.replaceChildren(box);
};

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

export const faults = <F extends { readonly label: string; readonly shows: string }>({
  list,
  onArm,
}: {
  readonly list: ReadonlyArray<F>;
  readonly onArm: (fault: F) => void;
}): void => {
  const panel = element("faults-panel", HTMLElement);
  panel.hidden = list.length === 0;
  element("faults", HTMLElement).replaceChildren(
    ...list.map((fault) => {
      const arm = h("button", {}, fault.label);
      arm.type = "button";
      arm.addEventListener("click", () => onArm(fault));
      return h("div", { class: "fault" }, arm, h("p", {}, fault.shows));
    }),
  );
};
