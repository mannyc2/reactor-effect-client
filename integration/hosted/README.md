# Hosted qualification

CI cannot check one thing: that the library works against hosted Reactor. These checks do, for money, so they run only when a maintainer authorizes the spend. They gated 0.3.0: the published 0.3.0-rc.0 failed them on three library causes, and 0.3.0 released the fixes after they passed from a checkout ([the record](./evidence/0.3.0-rc.0/summary.md)).

`main.ts` is the command line. Each check is one Effect program over the public API (`Checks.ts`); a rehearsal runs the same program against `ReactorTest` instead of hosted Reactor, for free.

## The spending limit

|                     |                                                                                                                                                                                                                                                                                                                                                                              |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The limit           | The operator gives it for every run: `--total-budget-usd` for all paid runs in the ledger, and `--budget-usd` for the one check. The script refuses more than $0.75 for a one-session check, $1.50 for the two-session `renewal`, or $3.75 in total. Going past that is a reviewed code change (`Spend.ts`).                                                                 |
| What a check costs  | One session, except `renewal`, which uses two. Reactor bills a session by the minute, from `ready` until it is terminated. On September 24, 2026, the pricing endpoint stated H3's rate as $0.75 a minute. How a started minute rounds is undocumented, so the gates count it whole. Each token caps its session at 50 s.                                                    |
| How the total holds | Every paid run writes a new evidence file into a ledger directory. Before a token exists, that file records the worst case the run reserves. A run is admitted only if everything already reserved, plus its own worst case, fits the limit. Reservations are never refunded from estimates, and an interrupted run still counts. One run at a time holds the ledger's lock. |

A capability added after 0.3.0 gets one check of its own, run once against the published bytes of the release that carries it, in that release's own ledger (`evidence/<version>/`). Until its check has run, it is qualified only by the tests and the rehearsal. Nothing repeats automatically; a re-run follows only a failure whose cause was found and fixed.

## The checks

| Check      | Was (≤ 0.7.0)                                 | What it adds                                                                                                                                                                                                                                                                                                                          |
| ---------- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vertical` | `vertical`                                    | Always first: pricing, a token, allocation, connection, a 5 s clip from submission to playback, its media and termination. It passes on correlated acceptance, the clip generated and started, live video after the start, audio when the session offers it, the clip's metadata echoed, and a candidate pair that carried the media. |
| `takeover` | `takeover`                                    | A child process owns the session, plays a 15 s clip with a 5 s one queued, and is killed mid-clip. This process attaches within 5 s, names the playing clip, reads the queued clip's metadata, sends no enqueue, receives fresh frames, and ends the session through the dead owner's record.                                         |
| `turn`     | `turn`                                        | The vertical from a network that blocks outbound UDP: the media must be carried by a relay pair. It runs only while no earlier paid run in the ledger selected one.                                                                                                                                                                   |
| `audio`    | `audio`                                       | The vertical with a gray reference image and a 3 s tone as its reference audio (H3 takes audio only beside an image or a continuation). The clip must report `has_reference_audio` and one audio reference.                                                                                                                           |
| `resume`   | `resume`                                      | The takeover, but `H3Source.resume` adopts the session from the owner's record: it may send only reads and the playout's session defaults, and closing the resumed source terminates the session.                                                                                                                                     |
| `queue`    | `scheduler`, and `scheduler-cut`'s raw probes | H3's own queue, raw: position zero behind a running build, a pop of the build in flight, whether a queue read sent right behind an enqueue lists it, and a `move` or `pop` before each of five boundaries at a set distance from the playing clip's end, with which clip starts next and the pause at each seam.                      |
| `renewal`  | `scheduler-renewal`                           | `Playout` over two capped sessions: it opens the replacement 40 s before the first grant ends and switches at a boundary. A airs on the first session and B on the second, one planned switch, the playout's picture carries both, the drain completes and both sessions end.                                                         |
| `edits`    | `scheduler-edits`                             | `Playout`'s edits on one session: a group of three beats, an insert continuing from the second, one before the third, and a batch that withdraws a queued clip and inserts one after the third, sent to take effect a second before the boundary. The clips must start in the planned order, and every seam is measured.              |
| `cut`      | `scheduler-cut`                               | A cut lane on one session: a 15 s clip plays and a 5 s clip in a lane with `cut: true` must stop it with one `stop` once Ready, start next, and not be stopped itself.                                                                                                                                                                |

Every check also requires each session it allocated to be confirmed ended. Playout sessions hold the last frame at boundaries, as a show does; by default H3 flushes to black there, as its schema documents.

The evidence stays narrow: one sample of each edit shows it can land, not a stable margin; one prompt serves every clip, so a continued join cannot be told from similar prompts; `Started` is a provider fact, not proof of encoded output; and a reply does not prove an edit was free of charge. Billing and presented output need dashboard or output evidence.

## What we gather, and why

Everything comes from sessions the checks pay for anyway.

- **Environment:** commit, package versions, runtime and OS, and the operator's description of the network. It ties the evidence to the exact bytes.
- **Cost:** the published rate, each grant, time from allocation to confirmed end and an estimate that bills no less than Reactor does. The dashboard's charge beside the estimate settles how a started minute rounds.
- **Coordinator facts:** cluster, zone, server version and transport, so Reactor can find its side of a run.
- **H3's contract:** the deployment's title and version, counts of each message type, unknown ones, duplicates, stale deliveries and diagnostics. Drift is fixed here and reported upstream.
- **Timing:** milestones, the library's own spans (only `reactor.*` attributes and `error.type`), acceptance, generation and start of each clip, and submission to Ready of each build.
- **Media:** frames, sizes, frame rate and intervals, lit and distinct frames, losses; audio blocks and levels; at each seam the longest pause, dark frames and the largest picture change against the ending clip's own motion.
- **Network:** one statistics sample a second and the local candidate type of the pair that carried the media.
- **Termination:** the library's close report, then the coordinator's answer every half second until the session is terminal or gone.

The evidence never holds a credential, SDP, a candidate address, a frame, audio or provider text. A save whose text would contain the API key or a session token writes nothing and stops the run. The two frames on either side of a playout seam are written at half size to a `reactor-seams-*` temporary directory for a person to look at; they are never part of the ledger.

## Making sure we get all of it

1. **One schema.** `Evidence.ts` defines the evidence and the sections each check must fill. A run that lacks one fails as incomplete, even when every criterion it evaluated passed.
2. **Saved as it happens.** The run claims a new file, recording its worst case, before any token exists, and saves after every milestone with an atomic replace. A crash still leaves the session ids, the reservation and everything observed so far. Ctrl-C interrupts the check, so its finalizers still close the sessions.
3. **Bounded by the session.** All work finishes within 40 s of allocation, inside the 50 s cap.
4. **Stops at uncertainty.** An unknown outcome, an allocation it cannot confirm, or an unconfirmed termination fails the run, never repeated by itself; the summary then says which session to confirm in the dashboard and by when its cap ends it.
5. **Free preflight.** `preflight` checks the live rate against the budget, the ledger, a minted token's granted limits (minting allocates nothing) and that the native library loads.
6. **Rehearsed end to end.** `rehearse <check>` runs the same program against `ReactorTest` at the timing paid runs measured, under a test clock, so it takes a moment and repeats exactly. The takeover's owner runs in this process there: its kill cuts its network before interrupting it, so it cannot close or terminate what it held. CI rehearses every check and these failure paths as `ReactorTest` faults: a lost enqueue reply, a termination confirmed only after the library's read, black and frozen video, missing audio and an over-granting token.

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

## Sharing the results

Commit the paid evidence files and `summary.md` under `evidence/<version>/` in the pull request that records the qualification. Session ids stay in: they let Reactor find its side of each run, and without a token they are useless. The release notes quote the summary; contract drift and surprising termination or cap behaviour go to Reactor with the session ids and times involved.

The ledgers up to 0.7.0 are records in the v1 schema, written by the harness those releases shipped, under the checks' old names. The CLI reads only the v2 schema, so each release from 0.8.0 starts a ledger of its own.
