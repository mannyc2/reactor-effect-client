# Hosted qualification

CI cannot check one thing: that the library works against hosted Reactor. These checks do, for money, so they run only when a maintainer authorizes the spend. They gated 0.3.0: the published 0.3.0-rc.0 failed them on three library causes, and 0.3.0 released the fixes after they passed from a checkout ([the record](./evidence/0.3.0-rc.0/summary.md)).

This directory holds:

- the plan: what the checks cost, what they gather and why, how we make sure they gather it, and where the results go;
- the script that runs the checks (`qualify.ts`);
- a local twin of hosted Reactor (`twin/`) that rehearses them for free.

## The spending limit

|                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The limit           | The operator gives it for every run: `--total-budget-usd` for all paid runs in the ledger, and `--budget-usd` for the one check. The script refuses more than $0.75 for a one-session check, $1.50 for the two-session `scheduler-renewal` check, or $3.75 in total. Going past that is a reviewed code change.                                                                                                                                                                                      |
| What a check costs  | One session, except `scheduler-renewal`, which uses two. Reactor bills a session by the minute, from `ready` until it is terminated. On September 24, 2026, the pricing endpoint stated H3's rate as 125 credits a second at 10,000 credits a dollar: $0.75 a minute. The billing documentation does not say how a started minute rounds, so the gates count it whole. Each token caps one session at 50 s server-side. The two-session run reserves two billed minutes before minting either token. |
| How the total holds | Every paid run writes a new evidence file into a ledger directory. Before a token exists, that file records the worst case the run reserves. A run is admitted only if everything already reserved, plus its own worst case, fits the limit. Reservations are never refunded from estimates, and an interrupted run still counts. One run at a time holds the ledger's lock.                                                                                                                         |

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

A capability added after 0.3.0 gets one check of its own, run once, against the published bytes of the release that carries it or from the checkout that adds it. Each release keeps its own ledger, `evidence/<version>/`, under the same limits: at most $0.75 for a one-session check or $1.50 for the two-session check. Until its check has run, the capability is qualified only by the tests and the twin.

| Check             | Release | What it adds to the vertical                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ----------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `audio`           | 0.3.1   | The clip carries a gray reference image and a 3 s tone as its reference audio, because H3 takes audio only beside an image or a continuation. It records whether the deployment declares `reference_audios`, and passes only if the accepted clip reports `has_reference_audio` and one audio reference, as H3 documents.                                                                                                                             |
| `resume`          | 0.3.2   | The takeover, but the new process resumes the session with `Orchestration.resumeH3` from the owner record `openH3` handed the owner, which adopts it. It passes only if the resume sends nothing but state and queue reads, identifies the playing clip, keeps the queued clip's metadata, receives fresh frames, and closing the resumed source terminates the session and confirms it, with no help from the record.                                |
| `scheduler`       | 0.5.0   | One capped H3 session with autoplay on. Before five clip boundaries it moves a waiting clip to the front, or pops the next one, at a set distance from the boundary, and records which clip starts and what the seam looks like in the messages and decoded frames. It also records position zero while a build is active, a pop of the build in flight, and metadata on every watched clip message. See [the scheduler check](#the-scheduler-check). |
| `scheduler-edits` | 0.7.0   | One capped session through the public owner and `makeScheduler`: inserts with and without `continuity: "previous"` inside a group, and an edit batch timed to take effect a second before a boundary. It records the order clips air in, when the batch took effect, and each seam's pause and picture change. See [the edit checks](#the-070-edit-checks).                                                                                           |
| `scheduler-cut`   | 0.7.0   | One capped session: raw H3 probes of position zero behind a running build, how long a popped build holds the build slot, and whether a queue read sent right behind an enqueue lists the new clip; then a cut lane stopping a playing 15 s clip, with the cut's seam measured. See [the edit checks](#the-070-edit-checks).                                                                                                                           |

### The scheduler check

Queue edits in the scheduler, such as putting a clip in at the next boundary or dropping a queued one, rest on what H3 does with `move` and `pop` while a clip plays. Its documentation says what each command does to the queue. It does not say how close to a boundary an edit still decides the next clip, or what a viewer sees at the boundary. This check answers both in one capped session. Build time and delivered length are recorded along the way. They calibrate defaults: the scheduler learns them from its own evidence and does not depend on their values.

The session starts with autoplay off. It submits a clip and pops it while it builds. It then submits another, and while that one builds, a third at position zero, which H3 documents as next behind the running build. Every clip asks for 5 seconds, so a build stays short and the Ready queue grows while clips play. Autoplay goes on when the first clip is Ready, and each boundary then gets one edit:

| Boundary | Edit before it                                                                            | It shows                                                 | Judged                                                                   |
| -------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------ |
| 1        | none                                                                                      | a seam with no edit                                      | recorded                                                                 |
| 2        | the second waiting clip moves to position zero, at least 2 s before the playing clip ends | whether a clip put in at the next boundary plays there   | it must start next                                                       |
| 3        | the same move, about 250 ms before the end                                                | whether a move that late still decides the next clip     | recorded                                                                 |
| 4        | the first waiting clip is popped, about 1 s before the end                                | whether the clip behind a dropped one plays in its place | the popped clip must never start, and the clip behind it must start next |
| 5        | the same pop, about 250 ms before the end                                                 | whether a pop that late still counts                     | a clip whose pop was accepted must never start                           |

At every boundary the evidence keeps the ending clip's `clip_finished` time, the next clip's `clip_started` time, and the longest stretch around the boundary with no new picture, whether a held frame, black frames or no frames, which a viewer sees as a pause. An edit's distance is measured from its reply to the ending clip's `clip_finished`, so an edit that missed its boundary still records by how much. A pop the provider refuses is recorded as refused. Its reason is provider text, so it is not kept.

Everything fits in the 40 seconds after allocation. An edit whose clips are not Ready in time is not staged, and the run fails as incomplete. Builds slower than playback would be a finding in themselves, since one build slot could then not stay ahead.

The evidence stays narrow:

- One edit at each distance shows whether an edit can land that late. It does not measure a stable cutoff.
- A reply and its elapsed time do not prove that an edit is free of charge.
- No `clip_started` after a pop does not prove that no picture was rendered or that compute was refunded.
- Frames are digested and never kept. Their arrival times are the decoded frames', not the encoded stream's or a viewer's.
- Whether a clip moved between two others matches their pose depends on its prompt, which this check's one prompt cannot show.
- The handoff between two sessions is measured by `scheduler-renewal`.

Billing and presented output need separate dashboard or output evidence.

### The 0.7.0 edit checks

0.7.0's scheduler inserts clips, applies edit batches, cuts from a lane and continues a clip from the one before it. `scheduler` showed that `move` and `pop` decide the next clip close to a boundary; these two checks put the scheduler's own edits on hosted H3, at $0.75 each. Both run the public renewing owner over one capped `openH3` source that it never renews, with filler off. The source holds the last frame at boundaries (`holdLastFrame: true`), as a show does; by default H3 flushes to black there, as its schema documents.

`scheduler-edits` turns autoplay on and submits a group of three 5 s beats, `p1` to `p3`, and a fourth clip `w1` behind it. Once `p1` plays, it inserts `xc` after `p2` with `continuity: "previous"`, so `xc` continues from `p2`, and `xn` before `p3`, built on its own. A continued build took 5.45 s on hosted H3, so `xc` needs `p2`'s whole length to build in; the 0.7.0 run put it before `p2`, where it missed its place. Once `p3` plays, it sends one `edit` batch that withdraws `w1` and inserts `y` after `p3`. The batch goes one measured build plus a second before `p3` ends, so it should take effect about a second before that boundary. It passes only if:

- the clips start in the order `p1`, `p2`, `xc`, `xn`, `p3`, `y`;
- the batch takes effect before `p3` ends;
- `w1` never starts, and is dropped as withdrawn;
- every one of the five seams has its pause and picture change measured.

`scheduler-cut` starts with autoplay off and uses the H3 provider directly:

- a lone build, for its time;
- a clip, a clip at position zero and a third, then a queue read: position zero must be listed behind the running build and ahead of the third;
- a build popped while it runs, and the time the clip queued behind it takes to be Ready, against the lone build;
- three enqueues, each followed by a queue read sent as soon as the enqueue is committed, long before its reply: every read must list the new clip. The scheduler still treats an enqueue whose reply is lost as unknown. If H3 applies commands in the order they arrive, a later queue read can settle it.

It then pops every raw clip, turns autoplay on, and submits a 15 s clip to the scheduler's `line` lane. 2.5 s after it starts, a 5 s clip goes to an `urgent` lane with `cut: true`. That clip must stop the long one once it is Ready, and start next.

At every seam the evidence keeps:

- the ending clip's end and the next clip's start;
- the longest stretch with no new picture;
- the largest change between two consecutive decoded frames. That change is set against the median change between consecutive frames in the two seconds before it, the ending clip's own motion. A ratio near 1 means the picture carries across the seam; a hard cut scores far higher.

Comparing the continued seam `p1` to `xc` with the independent ones is what the check says about continuity. The two frames on either side of each seam's largest change are written at half size as PNGs to `reactor-seams-<run id>` in the system's temporary directory, for a person to look at. They are never written into the ledger, which holds no frame. Delete them after review.

The evidence stays narrow:

- one prompt serves every clip, so independent clips already resemble each other, and one run cannot separate continuity from similar prompts;
- one sample of each edit shows it can land, not a stable margin;
- the queue-read probes show order on this deployment, three times, not a documented guarantee.

Both ran once against published 0.7.0, in [its ledger](./evidence/0.7.0/summary.md), for $1.50, before their sessions held the last frame. Both failed:

- **`scheduler-edits`** failed its order. The batch took effect 1.04 s before its boundary, and its withdrawn clip never started. The continued insert, though, waited behind a build in flight, and its continued build took 5.45 s against about 2.2 s for an independent one. It missed its place and aired after a clip it did not continue from, so no continued join was measured. Every clip ended on the one black frame that H3's default flush documents.
- **`scheduler-cut`** found a defect, since fixed: the scheduler stopped the long clip twice, and H3's `stop`, which names no clip, cut the cut-lane clip 5 ms after it started. Its position-zero criterion failed too: the queue read listed position zero ahead of the running build. Its queue reads, each sent right behind an enqueue, all listed the new clip.

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

The new evidence schema keeps historical v1 ledgers valid and accepts incomplete checkpoints. Final validation independently reconciles named criteria, source identities, counts, reservations, drain, switch and canonical cleanup; supplying a list of passing criteria cannot manufacture a pass, and a summary re-judges a stored pass from its evidence. Ordering is checked only as far as the harness flow guarantees it: the first allocation before A's items, the replacement's allocation before Prepared and Prepared before B's items (so B follows its source's allocation), A's recorded end and the retiring close before the switch, and the switch before cleanup once the drain completed. Events the SDK reports on separate streams stay unordered; B's start may precede the switch's report, since the SDK resumes autoplay before it closes the retiring source. An open whose allocation stays unknown is recorded on its slot and stops the run at once as an unknown outcome, before the SDK could try another allocation; the operator gets its grant's expiry and a dashboard check. A failed run retains partial evidence and requires investigation. The committed [0.3.1 audio record](./evidence/0.3.1/summary.md) is historical completed audio qualification. The `scheduler` and `scheduler-renewal` checks ran once each against published 0.6.0, in [its ledger](./evidence/0.6.0/summary.md), for $2.25. `scheduler-renewal` passed: a planned switch after all 124 frames of the final clip, an accepted drain and both terminations confirmed. `scheduler` failed one criterion, position zero while building, because the check popped the build in flight first and that popped build kept the build slot; its moves and pops before four boundaries, down to 160 ms before `clip_finished`, all decided the next clip, and no boundary paused beyond frame spacing. Resume remains unqualified on hosted Reactor.

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
- a frame or an audio sample (only per-frame digests and summaries; the 0.7.0 edit checks write a few seam frames beside the run, never into it);
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
   - the H3 model's messages, in the documented order (a command's reply before the broadcasts it causes, and autoplay off until a client turns it on), except where hosted H3 was seen to differ: an enqueue's queue broadcast comes before its reply, and a stop lands about 20 ms after its acknowledgement, with another stop sent meanwhile handled once the next clip has started;
   - media, which each connection receives only once it resumes its tracks, as on hosted Reactor;
   - stats as the native host reports them: each candidate pair with its local candidate only, and the relay pair ICE nominated first left nominated beside the direct pair that carries the media.

   CI rehearses every check before 0.7.0 and every failure path, and asserts that each evidence file decodes, that passing runs are complete, and that no file holds a credential. `scheduler-edits` and `scheduler-cut` are rehearsed by hand before their paid runs. The failure paths are:
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
npm install reactor-effect-client@0.7.0 reactor-effect-native@0.7.0 \
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
# credentials, an explicit authorization and room in the ledger of the
# release they qualify: $0.75 for scheduler, then $1.50 for scheduler-renewal.
bun hosted/qualify.ts rehearse scheduler
bun hosted/qualify.ts scheduler --budget-usd=0.75 --total-budget-usd=2.25 \
  --ledger=evidence/<version> --network="home fiber, no VPN" --i-authorize-paid-sessions
bun hosted/qualify.ts rehearse scheduler-renewal
bun hosted/qualify.ts scheduler-renewal --budget-usd=1.50 --total-budget-usd=2.25 \
  --ledger=evidence/<version> --network="home fiber, no VPN" --i-authorize-paid-sessions

# 0.7.0's edit checks, $0.75 each.
bun hosted/qualify.ts rehearse scheduler-edits
bun hosted/qualify.ts rehearse scheduler-cut
bun hosted/qualify.ts scheduler-edits --budget-usd=0.75 --total-budget-usd=1.50 \
  --ledger=evidence/0.7.0 --network="home fiber, no VPN" --i-authorize-paid-sessions
bun hosted/qualify.ts scheduler-cut --budget-usd=0.75 --total-budget-usd=1.50 \
  --ledger=evidence/0.7.0 --network="home fiber, no VPN" --i-authorize-paid-sessions

bun hosted/qualify.ts summarize ledger > summary.md
```

A run exits 0 when it passes, 1 when it fails, and 2 when it refused before claiming its evidence file, and therefore before spending anything. A run whose final evidence cannot be saved prints the session identities it holds.

- **After a failure,** read the reasons in its evidence and fix the cause before running it again. A re-run is a new paid run in the same ledger.
- **To re-run a fix before it is released,** run from a checkout of it instead of a scratch project:
  1. `bun run build`;
  2. stage the published candidate's native addon, if the native sources are unchanged: copy the addon from its platform package into `packages/native/npm/<platform>/` and run `node packages/native/scripts/stage.mjs` on it, which refuses it unless its `sourceSha256` matches the checkout;
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
