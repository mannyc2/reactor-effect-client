# Hosted qualification

CI cannot check one thing: that the library works against hosted Reactor. These checks do, for money, so they run only when a maintainer authorizes the spend. They gated 0.3.0: the published 0.3.0-rc.0 failed them on three library causes, and 0.3.0 released the fixes after they passed from a checkout ([the record](./evidence/0.3.0-rc.0/summary.md)).

This directory holds:

- the plan: what the checks cost, what they gather and why, how we make sure they gather it, and where the results go;
- the script that runs the checks (`qualify.ts`);
- a local twin of hosted Reactor (`twin/`) that rehearses them for free.

## The spending limit

|                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The limit           | The operator gives it for every run: `--total-budget-usd` for all paid runs in the ledger, and `--budget-usd` for the one check. The script refuses more than $0.75 for a one-session check, $1.50 for the two-session `scheduler` or `scheduler-renewal` check, or $3.75 in total. Going past that is a reviewed code change.                                                                                                                                                                                         |
| What a check costs  | One session, except `scheduler` and `scheduler-renewal`, which use two. Reactor bills a session by the minute, from `ready` until it is terminated. On September 24, 2026, the pricing endpoint stated H3's rate as 125 credits a second at 10,000 credits a dollar: $0.75 a minute. The billing documentation does not say how a started minute rounds, so the gates count it whole. Each token caps one session at 50 s server-side. Either two-session run reserves two billed minutes before minting either token. |
| How the total holds | Every paid run writes a new evidence file into a ledger directory. Before a token exists, that file records the worst case the run reserves. A run is admitted only if everything already reserved, plus its own worst case, fits the limit. Reservations are never refunded from estimates, and an interrupted run still counts. One run at a time holds the ledger's lock.                                                                                                                                           |

What each limit buys, at the historical rate above:

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

A capability added after 0.3.0 gets one check of its own, run once, against the published bytes of the release that carries it or from the checkout that adds it. Each release keeps its own ledger, `evidence/<version>/`, under the same limits: at most $0.75 for a one-session check or $1.50 for either two-session check. Until its check has run, the capability is qualified only by the tests and the twin.

| Check       | Release | What it adds to the vertical                                                                                                                                                                                                                                                                                                                                                                                           |
| ----------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `audio`     | 0.3.1   | The clip carries a gray reference image and a 3 s tone as its reference audio, because H3 takes audio only beside an image or a continuation. It records whether the deployment declares `reference_audios`, and passes only if the accepted clip reports `has_reference_audio` and one audio reference, as H3 documents.                                                                                              |
| `resume`    | 0.3.2   | The takeover, but the new process resumes the session with `Orchestration.resumeH3` from the owner record `openH3` handed the owner, which adopts it. It passes only if the resume sends nothing but state and queue reads, identifies the playing clip, keeps the queued clip's metadata, receives fresh frames, and closing the resumed source terminates the session and confirms it, with no help from the record. |
| `scheduler` | 0.5.0   | Two capped H3 sessions. It records submission-to-Ready samples by requested duration, Ready clip seconds, a Ready move reply and its latency, position zero while a build is active, events after popping a queued build, metadata on observed clip messages, and decoded frame arrival timing when the old clip stops and the replacement starts. The run reserves both worst cases before either token.              |

The `scheduler` evidence is deliberately narrow. A single sample per requested duration reports p50 and p95 with its sample count; it does not establish a stable latency distribution. A move reply and elapsed time do not prove that a move is free of charge. A bounded lack of `clip_started` after pop does not prove that no picture was rendered or that generation compute was refunded. The evidence retains every decoded frame and audio-block arrival in `arrivalsMs`, relative to the run start, without media bytes. Scheduler evidence includes these arrays for both sessions under `scheduler.media`; older ledgers may omit them. The handoff gap uses decoded frame arrival timestamps from two sessions; it does not prove the encoded stream or a viewer's presentation gap. These questions require a hosted paid run and, for billing and presented output, separate dashboard or output evidence.

## Public scheduler renewal (offline implementation)

`scheduler` remains provider calibration. `scheduler-renewal` composes the public renewing owner, `openH3`, and `makeScheduler`; the SDK controls preparation, autoplay, switching, and retirement. The harness defaults to `makeContinuous`; offline tests also cover legacy `make` through `--constructor=legacy`. Paid runs always use continuous. Neither an offline rehearsal nor an earlier legacy run qualifies that API against hosted Reactor.

The check reserves both billed minutes at the freshly fetched rate, rounded up to the ledger's four decimals, before minting either independent grant. Each grant must allow exactly one 50-second session. The harness asks for a 110-second token; the SDK itself refuses only a token that expires less than 30 seconds after the session cap. The existing ceilings remain $1.50 per two-session run and $3.75 across the ledger. It uses the actual granted lifetime, a 40-second lead, 250 ms handoff grace, two successful openings, and a separate two-attempt grant limit that refuses before a third allocation.

It submits keyed five-second `qualification-A`, observes its Started/Ended, then waits for Prepared, a preferred replacement source and A's recorded Ended before submitting `qualification-B`. Accepted drain begins after B is admitted on that source. Passing evidence requires A then B to finish, exactly one planned Switched with the SDK's eligibility facts, logical video from both identified sources, a completed drain with no later allocation, and both original canonical owned-lease cleanup reports. Continuous uses `retainedSuccessfulCleanups: 1` and `maxUnresolvedCleanups: 2`. Its real `CleanupSummary` must reconcile two total retirements, one retained complete owned termination, one omitted complete owned termination, no incomplete rows and no exhaustion. Both source-close reports remain in the two allocation records, so omission counts cannot substitute for lease proof. Unknown submissions remain incomplete even when lease cleanup is confirmed. Filler requests and actual as-run events are recorded; this two-line scenario does not qualify every filler policy. The replacement is prepared only once the first session is ten seconds old (its 50-second lifetime less the 40-second lead), so the switch follows A's end by seconds (about 4.7 s in rehearsal): the 250 ms final-clip grace is recorded but never binding, and a passing run does not qualify the bounded final-clip grace on hosted Reactor.

The optional `schedulerRenewal` subtree has version 1 and one Effect monotonic origin. Its elapsed measurements must not be subtracted from historical wall-clock fields or `AsRunEvent.at`. Physical source media taps tag unchanged frame objects in a WeakMap before renewal forwards them. Logical readers use those tags, reset sequence accounting at source/generation boundaries, and reuse the existing media summarizers. Untagged frames fail qualification. The decoded boundary is the last retiring-source logical video frame to the first replacement-source logical frame; it is an observation, with no universal latency, encoded-output, presentation, or audio-completeness claim. The retiring source keeps sending idle or hold frames after A ends, so the boundary measures the handoff between sources, not continuity from A's last clip frame to B's first.

Setup has 20 seconds before allocation. Scenario work shares 40 seconds from the first allocation, including the second source's setup. Cleanup observation has 20 seconds and starts with a failed/incomplete checkpoint. Checkpoints are best effort: the first evidence write that fails stops the scenario, and after it nothing more is minted, opened or submitted, including a replacement the SDK would open on its own, but no close is skipped; the run fails with it once cleanup has finished; once a check has claimed its evidence file it exits 1, never 2, whatever fails after that, its final save included. The SDK may own an uninterruptible finalizer beyond that observation budget; the CLI has no process supervisor. Offline tests act as the external emergency owner: the stalled-close rehearsal kills its child as soon as the checkpoint shows the first source close was requested, before the 20-second observation budget runs out, so the committed matrix does not exercise that budget's expiry. For a paid run the review checklist must name the maintainer/CI intervention owner and deadline separately; interruption does not refund the reservation, and unconfirmed remote termination stays unconfirmed.

Run the real CLI against the local twin without credentials or paid authorization:

```sh
bun --no-env-file integration/hosted/qualify.ts rehearse scheduler-renewal
bun --no-env-file integration/hosted/qualify.ts rehearse scheduler-renewal --constructor=legacy
bun --no-env-file test integration/test/hosted-renewal.test.ts
bun --no-env-file test integration/test/hosted-rehearsal.test.ts -t 'public renewal'
```

The new evidence schema keeps historical v1 ledgers valid and accepts incomplete checkpoints. Final validation independently reconciles named criteria, source identities, counts, reservations, drain, switch and canonical cleanup; supplying a list of passing criteria cannot manufacture a pass, and a summary re-judges a stored pass from its evidence. Ordering is checked only as far as the harness flow guarantees it: each allocation before the items on its source, Prepared before B, A's recorded end and the retiring close before the switch, and the switch before cleanup once the drain completed. Events the SDK reports on separate streams stay unordered; B's start may precede the switch's report, since the SDK resumes autoplay before it closes the retiring source. An open whose allocation stays unknown is recorded on its slot and stops the run at once as an unknown outcome, before the SDK could try another allocation; the operator gets its grant's expiry and a dashboard check. A failed run retains partial evidence and requires investigation. No paid `scheduler-renewal` run has been performed. The committed [0.3.1 audio record](./evidence/0.3.1/summary.md) is historical completed audio qualification; resume and the public scheduler-renewal path remain unqualified on hosted Reactor.

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
npm install reactor-effect-client@0.5.0 reactor-effect-native@0.5.0 \
  effect@4.0.0-rc.117 @effect/platform-node@4.0.0-rc.117
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

# Issue 31: rehearse offline, then run only with a staged native library,
# credentials, an explicit $1.50 authorization and room in the ledger.
bun hosted/qualify.ts rehearse scheduler
bun hosted/qualify.ts scheduler --budget-usd=1.50 --total-budget-usd=1.50 \
  --ledger=evidence/0.5.0 --network="home fiber, no VPN" --i-authorize-paid-sessions

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
