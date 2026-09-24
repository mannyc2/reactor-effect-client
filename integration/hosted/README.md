# Hosted qualification

CI cannot check one thing: that the library works against hosted Reactor. These checks do, for money, so they run only when a maintainer authorizes the spend. They gated 0.3.0: the published 0.3.0-rc.0 failed them on three library causes, and 0.3.0 released the fixes after they passed from a checkout ([the record](./evidence/0.3.0-rc.0/summary.md)).

This directory holds:

- the plan: what the checks cost, what they gather and why, how we make sure they gather it, and where the results go;
- the script that runs the checks (`qualify.ts`);
- a local twin of hosted Reactor (`twin/`) that rehearses them for free.

## The spending limit

|                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The limit           | The operator gives it for every run: `--total-budget-usd` for all paid runs in the ledger, and `--budget-usd` for the one check. The script refuses more than $0.75 a check or $3.75 in total (`maxCheckUsd` and `maxTotalUsd` in `gates.ts`): the whole plan below, then one re-run of the vertical and the takeover once 0.3.0-rc.0's failures were fixed. Going past that is a reviewed code change.                                                                                                                                                                                     |
| What a check costs  | One session. Reactor bills a session by the minute, from `ready` until it is terminated; connecting and waiting are free. On September 24, 2026, the pricing endpoint stated H3's rate as 125 credits a second at 10,000 credits a dollar: $0.75 a minute. The billing documentation does not say how a started minute rounds, so the gates count it whole. Every token caps its session at 50 s server-side, short of a minute, so a cap enforced a few seconds late still bills one. A check's worst case is therefore one billed minute, and the preflight prints it from the live rate. |
| How the total holds | Every paid run writes a new evidence file into a ledger directory. Before a token exists, that file records the worst case the run reserves. A run is admitted only if everything already reserved, plus its own worst case, fits the limit. Reservations are never refunded from estimates, and an interrupted run still counts. One run at a time holds the ledger's lock.                                                                                                                                                                                                                |

What each limit buys, at today's rate:

| Limit | Adds       | When                                                                                                                                                                                             |
| ----- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| $0.75 | `vertical` | Always first. It covers pricing, allocation, connection, a clip from submission to playback, media, and termination.                                                                             |
| $1.50 | `takeover` | Always. The owner process is killed mid-clip, and a new process takes the session over. With the vertical, it is what 0.3.0 waits for, so this is the recommended limit.                         |
| $2.25 | `turn`     | Only if neither earlier run selected a relay pair. It runs from a network that blocks outbound UDP, so only TURN over TCP or TLS can carry media.                                                |
| $3.75 | a re-run   | 0.3.0-rc.0's three runs reserved $2.25 and failed on three library causes. With those fixed, the vertical and the takeover run once more, in a ledger seeded with the earlier evidence.          |
| more  | a re-run   | Only after a failure whose cause was found and fixed, and only by raising `maxTotalUsd` in review. Nothing repeats automatically. The preflight and the rehearsals are free and reserve nothing. |

Two extra checks are deliberately not planned:

- A soak check (a whole capped session, to measure pacing drift) would repeat what the vertical measures over six seconds.
- A cap-backstop check (never terminate, and watch the server end the session at its cap) would spend a full worst case. Its main assurance is already covered: the preflight shows the token grants at most 50 s, and the takeover shows a session outliving its owner and still ending cleanly.

### Checks added after 0.3.0

A capability added after 0.3.0 gets one check of its own, run once, against the published bytes of the release that carries it or from the checkout that adds it. Each release keeps its own ledger, `evidence/<version>/`, under the same limits: at most $0.75 a check. Until its check has run, the capability is qualified only by the tests and the twin.

| Check    | Release | What it adds to the vertical                                                                                                                                                                                                                                                                                                                                                                                           |
| -------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `audio`  | 0.3.1   | The clip carries a gray reference image and a 3 s tone as its reference audio, because H3 takes audio only beside an image or a continuation. It records whether the deployment declares `reference_audios`, and passes only if the accepted clip reports `has_reference_audio` and one audio reference, as H3 documents.                                                                                              |
| `resume` | 0.3.2   | The takeover, but the new process resumes the session with `Orchestration.resumeH3` from the owner record `openH3` handed the owner, which adopts it. It passes only if the resume sends nothing but state and queue reads, identifies the playing clip, keeps the queued clip's metadata, receives fresh frames, and closing the resumed source terminates the session and confirms it, with no help from the record. |

## What we gather, and why

Every item below comes from sessions the checks pay for anyway, so gathering more costs nothing extra. Each item informs a decision.

| Data                                                                                                                                                                                                                                                                                                                                      | Informs                                                                                                                                                                                                    | Source                                               |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Commit, package versions, the loaded native library's identity (sha256, ABI, libwebrtc prebuilt), runtime, OS, and the operator's description of the network                                                                                                                                                                              | Ties the evidence to the exact published bytes and the environment                                                                                                                                         | Package manifests, `native-identity.json`, `git`     |
| Published rate, worst case, the token's granted limits, time from allocation to confirmed end, and an estimate: that time rounded up to whole minutes, which bills no less than Reactor does                                                                                                                                              | Checks the cost model. The dashboard's charge for each session, next to its estimate, says how a started minute rounds. Gives the-show a per-session cost                                                  | `pricing`, `mintToken`, timings                      |
| Coordinator facts: cluster, zone, server version, transport                                                                                                                                                                                                                                                                               | Lets Reactor correlate our runs with their logs                                                                                                                                                            | `Coordinator.inspect`                                |
| Connect phases (described, prepared, registered, offered, answered, ready) and command round trips                                                                                                                                                                                                                                        | Defaults for the connect, ready and setup timeouts; the-show's startup budget                                                                                                                              | The library's own spans                              |
| Deployment title and version against the documented H3 0.5.5; counts of each message type; unknown message types; duplicate and stale deliveries; diagnostics; the last `state_update` (canvas, capacities, clip-length bounds)                                                                                                           | Whether the H3 codec matches the live deployment. Drift is fixed here and reported upstream                                                                                                                | `provider.events`, `provider.contract`               |
| Time from submission to acceptance, and whether acceptance was correlated or matched by metadata; when the clip was generated, started and ended; the first clip frame; which later messages echoed the submission's metadata                                                                                                             | Checks the correlation design and the reconcile-window default. Gives the-show its time to first generated frame                                                                                           | `submit`, `provider.operation`, and the event tally  |
| Video: frames, size, format, frame rate, spread of frame intervals, frames lost and in how many gaps, how many frames were lit and distinct, and whether frames carry metadata, ids and timestamps. The live-video criterion reads only the frames that arrived after the clip started                                                    | Media buffer defaults, and what the README claims about frames                                                                                                                                             | `recorder` over `Native.media`, summarized per frame |
| Audio: blocks, sample rate, channels, block length, peak RMS, loss                                                                                                                                                                                                                                                                        | What the README claims about audio                                                                                                                                                                         | Same                                                 |
| Pressure counters: delivered, dropped, reader overflows                                                                                                                                                                                                                                                                                   | Whether the default bounds hold at the live frame size and rate                                                                                                                                            | `media.snapshot`                                     |
| The ICE pair carrying the media, by its local candidate type (the native host names no remote candidate), and once a second: RTT, received kbps, fps, jitter and loss                                                                                                                                                                     | Whether TURN is needed; the network expectations in the README                                                                                                                                             | `session.stats`                                      |
| Takeover: time from kill to attach; whether the attached state names the owner's playing clip (H3 keeps no history, so a playing clip is known only by `playing_clip_id`) and the clip queued behind it keeps its metadata; that no enqueue follows the attach; whether fresh frames arrive; termination through the durable owner record | The recovery design the-show relies on                                                                                                                                                                     | The takeover check                                   |
| Termination: the library's own report (confirmed, the DELETE status, the confirmation state), then the coordinator's answer every half second until the session is terminal or gone, with the HTTP status of any refused read                                                                                                             | Whether `terminate` needs a bounded confirmation poll. If the session only turns terminal after the library's single confirmation read, the library reports sessions as unconfirmed even though they ended | `session.close` or `terminate`, then `inspect`       |

The evidence never holds:

- a credential;
- SDP, candidate addresses or IP addresses;
- a frame or an audio sample (only per-frame digests and summaries);
- provider error text.

### What the runs settle that no document does

Reactor's documentation leaves these open. Each has a step that answers it, and the free ones come first:

1. **Whether a token's claims carry its granted limits.** The library refuses a grant it cannot read from the token, and Reactor documents the limits only in the `/tokens` response. The preflight mints a token, which allocates nothing, so a mismatch shows up before any money is spent.
2. **How a started minute bills.** The pricing endpoint states a rate per second; the documentation bills per session-minute. The dashboard's charge per session, against each run's estimate, settles it.
3. **What reading an ended session returns to its own token.** `terminate` confirms by reading the session back. If that read answers something other than a terminal state or 404, every termination reports as unconfirmed, and the termination trail shows the answer and its status.
4. **Whether an attached connection receives media without `resume_track`.** The protocol documents `resume_track` for input tracks. 0.3.0-rc.0's takeover answered it: it does not, and 0.3.0-rc.0 resumed tracks only for a session it created. The library now resumes them on every connection, and the takeover's fresh frames check that it does.
5. **Whether H3 matches its documented contract.** The contract tally counts every message type, unknown message, duplicate and diagnostic.

## Making sure we get all of it

1. **One schema.** `evidence.ts` defines the evidence, and each check declares the fields it must fill. A run that lacks any of them fails as _incomplete evidence_, even when every criterion it did evaluate passed. A paid session that comes back without its data is a failed qualification, never a pass with gaps.
2. **Saved as it happens.** The run saves after every milestone, with an atomic replace, starting before the first token. A crash still leaves the session id, the reserved worst case, and everything observed so far. Ctrl-C interrupts the check, so its finalizers still close the session.
3. **Bounded by the session.** All work finishes within 40 s of allocation, inside the 50 s cap, and every wait has a deadline derived from that.
4. **Free preflight.** Before any money is spent, the preflight checks:
   - the live rate against the budget;
   - the ledger;
   - a minted token's granted limits (minting allocates nothing);
   - that the native library loads.
5. **Rehearsed end to end.** `rehearse <check>` runs the same code against `twin/`, a local stand-in for:
   - the coordinator's routes, including pricing (listed by bare model name, as the live endpoint does), tokens, termination and the session cap;
   - the H3 model's messages, in the documented order (a command's reply before the broadcasts it causes, and autoplay off until a client turns it on), except where hosted H3 was seen to differ: an enqueue's queue broadcast comes before its reply;
   - media, which each connection receives only once it resumes its tracks, as on hosted Reactor;
   - stats as the native host reports them: each candidate pair with its local candidate only, and the relay pair ICE nominated first left nominated beside the direct pair that carries the media.

   CI rehearses every check and every failure path, and asserts that each evidence file decodes, that passing runs are complete, and that no file holds a credential. The failure paths are:
   - a lost enqueue reply (outcome unknown, so the check stops);
   - a DELETE the coordinator completes only after the library's one confirmation read (unconfirmed termination, with cleanup instructions and a trail of when it ended);
   - black or frozen video;
   - missing audio;
   - an over-granting token (refused).

   The paid run is then not the script's first run. Building the qualification found three library bugs before any money was spent: pricing that could not find a model by its slug, a pricing reader that rejected Reactor's documented shape, and an H3 command refused while the broadcasts of the one before it were in flight. All three are fixed in 0.3.0-rc.0.

6. **Redaction by construction.** The writer refuses to save any text that contains the API key or the session token. Spans keep only the library's `reactor.*` attributes and `error.type`. The library never puts a credential, an input, a reply or provider text in those.

## Runbook

Qualify the published bytes. Run from a machine whose network carries WebRTC media: outbound UDP, or TURN over TCP or TLS. Sandboxed CI and agent containers usually allow neither. Use a scratch project with the release candidate, and run with Bun 1.4.2:

```sh
mkdir reactor-qualification && cd reactor-qualification && npm init -y > /dev/null
npm install reactor-effect-client@0.3.0 reactor-effect-native@0.3.0 \
  effect@4.0.0-rc.115 @effect/platform-node@4.0.0-rc.115
cp -R <this repository>/integration/hosted ./hosted

# Free: the same checks against the local twin, over the installed packages.
bun hosted/qualify.ts rehearse vertical
bun hosted/qualify.ts rehearse takeover

# Free: live pricing, a token's limits and the native library.
export REACTOR_API_KEY=...
bun hosted/qualify.ts preflight --total-budget-usd=1.50 --ledger=ledger

# Paid, one at a time. Read each result before the next.
bun hosted/qualify.ts vertical --budget-usd=0.75 --total-budget-usd=1.50 \
  --ledger=ledger --network="home fiber, no VPN" --i-authorize-paid-sessions
bun hosted/qualify.ts takeover --budget-usd=0.75 --total-budget-usd=1.50 \
  --ledger=ledger --network="home fiber, no VPN" --i-authorize-paid-sessions
# Only if neither run selected a relay pair, from a network that blocks outbound UDP:
bun hosted/qualify.ts turn --budget-usd=0.75 --total-budget-usd=2.25 \
  --ledger=ledger --network="office, outbound UDP blocked" --i-authorize-paid-sessions
# A check added after 0.3.0, in its release's own ledger, e.g. 0.3.1's:
bun hosted/qualify.ts rehearse audio
bun hosted/qualify.ts audio --budget-usd=0.75 --total-budget-usd=0.75 \
  --ledger=evidence/0.3.1 --network="home fiber, no VPN" --i-authorize-paid-sessions
bun hosted/qualify.ts rehearse resume
bun hosted/qualify.ts resume --budget-usd=0.75 --total-budget-usd=0.75 \
  --ledger=evidence/0.3.2 --network="home fiber, no VPN" --i-authorize-paid-sessions

bun hosted/qualify.ts summarize ledger > summary.md
```

A run exits 0 when it passes, 1 when it fails, and 2 when it refused before spending anything.

- **After a failure,** read the reasons in its evidence and fix the cause before running it again. A re-run is a new paid run in the same ledger.
- **To re-run a fix before it is released,** run from a checkout of it instead of a scratch project:
  1. `bun run build`;
  2. stage the published candidate's native library, if the native sources are unchanged: copy its `lib/<platform>` into `packages/native/lib/` when its `sourceSha256` matches `node packages/native/scripts/stage.mjs --source-hash`;
  3. commit, and seed a new ledger with every earlier paid evidence file;
  4. run `bun integration/hosted/qualify.ts` as above. The evidence records the commit.
- **After an unknown outcome or an unconfirmed termination,** confirm in the Reactor dashboard that the session ended. The evidence says by when its cap ends it.
- **After all the runs,** compare the dashboard's charges with each run's estimate and note any difference in `summary.md`. That comparison is how a started minute's billing gets settled.

## Sharing the results

- **In this repository.** Commit the paid evidence files and `summary.md` under `integration/hosted/evidence/<version>/`, in the pull request that records the qualification.
  - By construction they hold no credential or address.
  - Session ids stay in. They let Reactor find its side of each run. Without a token they are useless, and each token expires less than two minutes after it is minted.
- **In the release.**
  - The 0.3.0 changelog entry quotes the summary: date, versions, checks, verdicts and key timings, as 0.2.0's entry does for its local qualification.
  - The README's support section replaces modeled assumptions with measured values, each linked to its evidence.
- **In the library.** The evidence settles:
  - the confirmation poll for `terminate`, from the termination trail;
  - whether attaching must resume tracks, from the takeover's fresh frames;
  - the connect, ready and setup timeouts and the reconcile window, against the measured phases;
  - the media bounds, against the measured frame size and rate;
  - any codec change, from unknown messages, diagnostics or deployment drift.
- **Upstream.** Contract drift, surprising termination or cap behaviour, and the questions above that the runs answer differently from the documentation go to Reactor, with the summary and the session ids and times involved.
- **To the-show.** The startup timeline (allocation to first clip frame), frame pacing, takeover time and cost per session feed its loading states, its recovery flow and its session-length planning.
