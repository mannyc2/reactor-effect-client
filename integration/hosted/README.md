# Hosted qualification

CI cannot check one thing: that the library works against hosted Reactor. These checks do, for money, so they run only when a maintainer authorizes the spend. They gated 0.3.0: the published 0.3.0-rc.0 failed them on three library causes, and 0.3.0 released the fixes after they passed from a checkout ([the record](./evidence/0.3.0-rc.0/summary.md)).

`main.ts` is the command line. Each check is one Effect program over the public API (`Checks.ts`); a rehearsal runs the same program against `ReactorTest` instead of hosted Reactor, for free.

## The spending limit

|                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The limit           | The operator gives it for every run: `--total-budget-usd` for all paid runs in the ledger, and `--budget-usd` for the one check. Each check declares in `Spend.ts` how many sessions it opens and each one's cap. The script refuses a check's budget above every started minute of its sessions at $0.75 a minute ($0.75 for one 50 s session, $1.50 for the two-session `renewal`), or a total above $5.00. Going past that is a reviewed code change (`Spend.ts`).                                                                                                                                                                                                                                |
| What a check costs  | One 50 s session, except `renewal`, which uses two; longer runs declare their own. Reactor bills a session from `ready` until it is terminated. The billing page says it bills "per session-minute"; the pricing endpoint states H3's rate per second (125 credits a second at 10,000 credits a dollar, $0.75 a minute, read September 24 and 28, 2026); and 0.3.0's measured charges were lower than even that ([its summary](./evidence/0.3.0-rc.0/summary.md)). So the gates bill in the unit the pricing states, each started unit whole: a 50 s session reserves $0.625 per second, or a whole minute, $0.75, if the rate is stated per minute. Each token caps its session at its check's cap. |
| How the total holds | Every paid run writes a new evidence file into a ledger directory. Before a token exists, that file records the worst case the run reserves. A run is admitted only if everything already reserved, plus its own worst case, fits the limit. Reservations are never refunded from estimates, and an interrupted run still counts. One run at a time holds the ledger's lock.                                                                                                                                                                                                                                                                                                                         |

A capability added after 0.3.0 gets one check of its own, run once against the published bytes of the release that carries it, in that release's own ledger (`evidence/<version>/`). Until its check has run, it is qualified only by the tests and the rehearsal. Nothing repeats automatically; a re-run follows only a failure whose cause was found and fixed.

## The checks

| Check      | Was (≤ 0.7.0)                                 | What it adds                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ---------- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vertical` | `vertical`                                    | Always first: pricing, a token, allocation, connection, a 5 s clip from submission to playback, its media and termination. It passes on correlated acceptance, the clip generated and started, live video after the start, audio when the session offers it, the clip's metadata echoed, and a candidate pair that carried the media.                                                                                                                                                                                                |
| `takeover` | `takeover`                                    | A child process owns the session, plays a 15 s clip with a 5 s one queued, and is killed mid-clip. This process attaches within 5 s, names the playing clip, reads the queued clip's metadata, sends no enqueue, receives fresh frames, and ends the session through the dead owner's record.                                                                                                                                                                                                                                        |
| `turn`     | `turn`                                        | The vertical from a network that blocks outbound UDP: the media must be carried by a relay pair. It runs only while no earlier paid run in the ledger selected one.                                                                                                                                                                                                                                                                                                                                                                  |
| `audio`    | `audio`                                       | The vertical with a gray reference image and a 3 s tone as its reference audio (H3 takes audio only beside an image or a continuation). The clip must report `has_reference_audio` and one audio reference.                                                                                                                                                                                                                                                                                                                          |
| `resume`   | `resume`                                      | The takeover, but `H3Source.resume` adopts the session from the owner's record: it sends only reads, since the owner already gave the session the playout's defaults, and closing the resumed source terminates the session.                                                                                                                                                                                                                                                                                                         |
| `queue`    | `scheduler`, and `scheduler-cut`'s raw probes | H3's own queue, raw: position zero behind a running build, a pop of the build in flight, whether a queue read sent right behind an enqueue lists it, and a `move` or `pop` before each of five boundaries at a set distance from the playing clip's end, with which clip starts next and the pause at each seam.                                                                                                                                                                                                                     |
| `renewal`  | `scheduler-renewal`                           | `Playout` over two capped sessions: it opens the replacement 40 s before the first grant ends and switches at a boundary. A airs on the first session and B on the second, one planned switch, the playout's picture carries both, the drain completes and both sessions end.                                                                                                                                                                                                                                                        |
| `edits`    | `scheduler-edits`                             | `Playout`'s edits on one session: a group of three beats, an insert continuing from the second, one before the third, and a batch that withdraws a queued clip and inserts one after the third, sent to take effect a second before the boundary. The clips must start in the planned order, and every seam is measured.                                                                                                                                                                                                             |
| `cut`      | `scheduler-cut`                               | A cut lane on one session: a 15 s clip plays and a 5 s clip in a lane with `cut: true` must stop it with one `stop` once Ready, start next, and not be stopped itself. With `--moderation-prompt-file`, it then records what content moderation does with that prompt ([below](#moderation)).                                                                                                                                                                                                                                        |
| `tokens`   | —                                             | A session outliving the token that created it. The owner creates it on a 20 s token, streams and is killed; once that token expired, this process adopts the session with 12 s tokens bound to it, names the playing clip, sees fresh frames, refreshes its token for a clip with a reference image and reference audio (which must report `has_reference_audio`), finds an expired token refused (401) and an unbound one refused (403), and ends the session with the API key as the bearer. It records the free token probes too. |
| `tour`     | —                                             | One 90 s session through the raw API, a phase each ([below](#tour)): the SDK's own tokens, which the session refreshes to one bound to itself; a canvas, a seed and references sent as bytes then reused as uploads; H3's queue by hand, stop and play, a build past the text budget, recordings, a reconnect and a reset; the API key ending the session, and what an ended or unknown session answers.                                                                                                                             |

Every check also requires each session it allocated to be confirmed ended. Playout sessions hold the last frame at boundaries, as a show does; by default H3 flushes to black there, as its schema documents.

The evidence stays narrow: one sample of each edit shows it can land, not a stable margin; one prompt serves every clip, so a continued join cannot be told from similar prompts; `Started` is a provider fact, not proof of encoded output; and a reply does not prove an edit was free of charge. Billing and presented output need dashboard or output evidence.

### Tour

`tour` walks the public API's provider calls through one session, so that each is seen on hosted Reactor once without a session of its own. Its session is capped at 90 s, reserves $1.125 at H3's per-second rate, and its work ends within 80 s of the allocation; the plan ends about 40 s in.

- **Each phase on its own.** A phase judges its own criteria under its own time limit. One that fails is a failed criterion named after it, with its failure's tag and message, and the phases after it still run where they can.
- **Tokens.** The session is created on `Coordinator.tokens`, not a fixed grant, with tokens that live 30 s. Every token is proven and accepted before it can allocate: the creating one by its cap, each one bound to the session by Reactor's echo of the bind. Past the refresh point (a quarter of the token's life before it expires) a clip uploads a reference, and the session must mint a token bound to itself before the creating one expires. After that expiry it reconnects on the refreshed token.
- **Judged, as H3's schema or the SDK documents them.** The canvas asked for and its size in `canvas_accepted` and the state; clip 1's correlated acceptance, lifecycle, live video, audio, `has_reference_audio` and metadata echo; clip 2 accepted on clip 1's uploads with nothing uploaded again, reporting one image and one audio reference; the queue read after a `move` to the front; a stop acknowledged, the playing clip stopped and nothing started with autoplay off until a `play` of the clip it names; a prompt past the text budget failing its clip with `clip_failed`, which the clip's operation reports as `ClipEnded`; popped clips never starting, and a build popped in flight never generated; the reconnect making the next generation, H3 answering on it with the ready clip kept, and fresh frames on it; a reset stopping the playing clip and leaving both queues empty; the API key ending the session and the session's own close confirming it; an attach to the ended session refused with `TerminalSession` or 404; and the key reading and ending an unknown session answered 404 and absent.
- **Recorded, not judged.** The seed H3 echoes and its default before and after; a position-zero clip's place in the queue; each queue command's round trip; what `reset` restores; whether the deployment records (Reactor leaves recording to each deployment: `RecorderDisabled`, or the clip's markers and its download's size, never its content); and what the SDK reads from the echo of a three-session token and an uncapped one, which mint nothing that is used.
- **Rehearsed.** ReactorTest answers as a disabled recorder does; a rehearsal test turns its recorder on so the download runs too, and another refuses clip 1's image, after which the later phases still run and the session still ends.

## What we gather, and why

Everything comes from sessions the checks pay for anyway.

- **Environment:** commit, package versions, runtime and OS, and the operator's description of the network. It ties the evidence to the exact bytes.
- **Cost:** the published rate, each grant, time from allocation to confirmed end and an estimate that bills no less than Reactor does. The dashboard's charge beside the estimate settles how a started minute rounds.
- **Coordinator facts:** cluster, zone, server version and transport, so Reactor can find its side of a run.
- **H3's contract:** the deployment's title and version, counts of each message type, unknown ones, duplicates, stale deliveries and diagnostics. Drift is fixed here and reported upstream.
- **Timing:** milestones, the library's own spans (only `reactor.*` attributes and `error.type`), acceptance, generation and start of each clip, and submission to Ready of each build.
- **Media:** frames, sizes, frame rate and intervals, lit and distinct frames, losses; audio blocks and levels; at each seam the longest pause, dark frames and the largest picture change against the ending clip's own motion.
- **Network:** one statistics sample a second and the local candidate type of the pair that carried the media.
- **Adoption:** each read the adopting process makes of the session it takes over (`takeover`, `resume`, `tokens`), with the time since the owner died: status, state and key names. The first paid `tokens` run read `INACTIVE` 9 s after the kill, and the SDK then counted that session as ended; it was still running.
- **Termination:** the library's close report, then the coordinator's answer every half second until the session is `CLOSED` or gone.

The evidence never holds a credential, SDP, a candidate address, a frame, audio or provider text. A save whose text would contain the API key or a session token writes nothing and stops the run. The two frames on either side of a playout seam are written at half size to a `reactor-seams-*` temporary directory for a person to look at; they are never part of the ledger.

## Making sure we get all of it

1. **One schema.** `Evidence.ts` defines the evidence and the sections each check must fill. A run that lacks one fails as incomplete, even when every criterion it evaluated passed.
2. **Saved as it happens.** The run claims a new file, recording its worst case, before any token exists, and saves after every milestone with an atomic replace. A crash still leaves the session ids, the reservation and everything observed so far. Ctrl-C interrupts the check, so its finalizers still close the sessions.
3. **Bounded by the session.** All work finishes 10 s before the session's cap: within 40 s of allocation for a 50 s session.
4. **Stops at uncertainty.** An unknown outcome, an allocation it cannot confirm, or an unconfirmed termination fails the run, never repeated by itself; the summary then says which session to confirm in the dashboard and by when its cap ends it.
5. **Free preflight.** `preflight` checks the live rate against the budget, the ledger, a minted token's granted limits (minting allocates nothing) and that the native library loads, then prints the token probes: what a 15 s and a 7 h token live, what an uncapped and a three-session token echo, the `/tokens` reply's key names and types, and what a bind of an unknown session and the API key reading or ending one answer. Each is one request that allocates nothing; only statuses, codes, numbers and key names are kept.
6. **Rehearsed end to end.** `rehearse <check>` runs the same program against `ReactorTest` at the timing paid runs measured, under a test clock, so it takes a moment and repeats exactly. The takeover's owner runs in this process there: its kill cuts its network before interrupting it, so it cannot close or terminate what it held. CI rehearses every check and these failure paths as `ReactorTest` faults: a lost enqueue reply, a termination confirmed only after the library's read, black and frozen video, missing audio and an over-granting token; and `cut`'s moderation three ways (a verdict, a session ended with no verdict, a prompt moderation lets through). A rehearsal checks every save but writes only its claim and its last: its clock moves while it runs, so a file written mid-check would move the simulated session's time.

A rehearsal cannot prove what only hosted Reactor decides: how it notices a vanished owner and accepts a new connection, its build and seam timing, billing, relay behaviour, or what a viewer sees.

## Runbook

Qualify the published bytes from a machine whose network carries WebRTC media (outbound UDP, or TURN over TCP or TLS), with Bun 1.4.2, in a scratch project with the release candidate:

```sh
mkdir reactor-qualification && cd reactor-qualification && npm init -y > /dev/null
npm install reactor-effect-client@<version> reactor-effect-native@<version> \
  effect@4.0.0-rc.117 @effect/platform-node@4.0.0-rc.117
cp -R <this repository>/integration/hosted ./hosted

# Free.
bun hosted/main.ts rehearse vertical
export REACTOR_API_KEY=...
bun hosted/main.ts preflight --total-budget-usd 1.50 --ledger ledger

# Paid, one at a time. Read each result before the next.
bun hosted/main.ts run vertical --budget-usd 0.75 --total-budget-usd 1.50 \
  --ledger evidence/<version> --network "home fiber, no VPN" --i-authorize-paid-sessions
bun hosted/main.ts run renewal --budget-usd 1.50 --total-budget-usd 3.75 \
  --ledger evidence/<version> --network "home fiber, no VPN" --i-authorize-paid-sessions

bun hosted/main.ts summarize evidence/<version> > evidence/<version>/summary.md
```

A run exits 0 when it passes, 1 when it fails, and 2 when it refused before claiming its evidence file, and so before spending anything. To re-run a fix before it is released, run `bun --no-env-file integration/hosted/main.ts` from a built checkout instead; the evidence records the commit.

After a failure, read its reasons and fix the cause before running it again. To run a fix before it is released, run from a checkout of it: `bun run build`, then, if the native sources are unchanged, copy the published candidate's addon from its platform package into `packages/native/npm/<platform>/` and run `node packages/native/scripts/stage.mjs` on it, which refuses it unless its `sourceSha256` matches the checkout. The evidence records the commit. After an unknown outcome or an unconfirmed termination, confirm in the Reactor dashboard that the session ended. After all the runs, compare the dashboard's charges with each run's estimate and note any difference in `summary.md`.

### 0.8.0 from a checkout

Before 0.8.0 is published, runs from a built checkout go in a ledger of their own (`evidence/0.8.0-dev/`; `evidence/0.8.0/` stays for the published bytes). Stage the linux-x64 addon as above. At H3's per-second rate a capped session reserves $0.625. The maintainer approved $2.00 for `tokens`, `cut` and `edits`; `tokens` ran twice, which left no room for `edits`. $1.25 more, a $3.25 total, then ran `vertical` and `edits` on the rewrite's final head, and the ledger now holds $3.125 of it.

```sh
bun run build
export REACTOR_API_KEY=...   # never echoed, never in the evidence
L=integration/hosted/evidence/0.8.0-dev
N="<where, without addresses>"

# Free: the rate, the ledger, a token, the addon, and the token probes.
bun --no-env-file integration/hosted/main.ts preflight --total-budget-usd 2.00 --ledger $L

# Paid, one at a time. Read each result before the next.
bun --no-env-file integration/hosted/main.ts run tokens --budget-usd 0.75 --total-budget-usd 2.00 \
  --ledger $L --network "$N" --i-authorize-paid-sessions
bun --no-env-file integration/hosted/main.ts run cut --budget-usd 0.75 --total-budget-usd 2.00 \
  --ledger $L --network "$N" --i-authorize-paid-sessions --moderation-prompt-file <file outside the repository>

# The final head, under the later $3.25 total.
bun --no-env-file integration/hosted/main.ts run vertical --budget-usd 0.625 --total-budget-usd 3.25 \
  --ledger $L --network "$N" --i-authorize-paid-sessions
bun --no-env-file integration/hosted/main.ts run edits --budget-usd 0.625 --total-budget-usd 3.25 \
  --ledger $L --network "$N" --i-authorize-paid-sessions

bun --no-env-file integration/hosted/main.ts summarize $L > $L/summary.md
```

### Moderation

Reactor screens prompts and reference images and terminates a session given flagged content (resources › Content moderation); a session it ends "can" carry a reason (reactor-runtime 3.2.0 notes), so none is promised. `cut --moderation-prompt-file <file>` ends the cut with an item carrying that file's prompt, queued behind a 15 s guard clip so it could air only after the watch; if it comes back Ready unflagged it is withdrawn. The playout may open no second session and fails after one moderation. The evidence records whether the enqueue replied, any verdict (its action, categories, input kind and command, and whether its request id is the enqueue's), the session's statuses and control messages, the playout's events, and the coordinator's read afterwards. The prompt is a run secret: it is never logged or saved, and a save that would hold it fails. Keep the file outside the repository. Submitting content meant to be flagged is the account owner's decision under Reactor's Acceptable Use Policy.

## Sharing the results

Commit the paid evidence files and `summary.md` under `evidence/<version>/` in the pull request that records the qualification. Session ids stay in: they let Reactor find its side of each run, and without a token they are useless. The release notes quote the summary; contract drift and surprising termination or cap behaviour go to Reactor with the session ids and times involved.

The ledgers up to 0.7.0 are records in the v1 schema, written by the harness those releases shipped, under the checks' old names. The CLI reads only the v2 schema, so each release from 0.8.0 starts a ledger of its own.
