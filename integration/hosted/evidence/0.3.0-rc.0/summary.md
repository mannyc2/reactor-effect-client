# Hosted qualification of 0.3.0-rc.0

Run on September 24, 2026, in two rounds:

- **The published 0.3.0-rc.0.** The plan's two checks ran under a $1.50 limit: the vertical, then the takeover. At the maintainer's request, a third run, a second vertical, then settled how Reactor bills and how H3 answers an enqueue. All three runs failed, on three causes in the library.
- **The fixes.** Commit `0255871` fixes the three causes. The vertical and the takeover then ran once more, from a checkout of that commit, under the $3.75 total it allows, and both passed.

Every session ended with its termination confirmed. The ledger now reserves $3.75, the most `gates.ts` allows, so nothing more runs without a reviewed change. 0.3.0-rc.0 stays on `next`: the passing runs qualify sources that no release candidate has published yet.

The runbook's own summary, from `bun hosted/qualify.ts summarize ledger`:

| Run      | Check    | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | -------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 61f5b7fb | vertical | paid | fail    | 2026-09-24T02:40:23.016Z | $0.750     | $0.750    |
| 8c148d36 | takeover | paid | fail    | 2026-09-24T02:42:32.555Z | $0.750     | $0.750    |
| 9ded60b5 | vertical | paid | fail    | 2026-09-24T03:18:19.562Z | $0.750     | $0.750    |
| 6438e4e9 | vertical | paid | pass    | 2026-09-24T05:10:26.250Z | $0.750     | $0.750    |
| d7b0b5db | takeover | paid | pass    | 2026-09-24T05:10:56.961Z | $0.750     | $0.750    |

### vertical: fail (paid, run 61f5b7fb, 2026-09-24T02:40:23.016Z)

- **Environment:** reactor-effect-client 0.3.0-rc.0, reactor-effect-native 0.3.0-rc.0, effect 4.0.0-rc.115, @effect/platform-node 4.0.0-rc.115, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** Linux workstation on Wi-Fi, no VPN or exit node, outbound UDP open
- **Server:** cluster e87af224-6e25-4031-b689-da90664578c9, zone us-west, version 1.20260922.28209, transport webrtc/1.0
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.27 s · minted 0.48 s · allocated 0.97 s · connected 3.91 s · accepted 4.17 s · clip started 6.23 s · observed 12.23 s · closed 12.74 s · trail 12.96 s
- **Connect phases:** described +0.44 s, prepared +0.09 s, registered +0.24 s, offered +0.36 s, answered +0.64 s, ready +0.76 s
- **Clip:** accepted 0.09 s after submit (metadata); generated +2.06 s; started +2.06 s; first clip frame +0.10 s after start; ended +7.19 s; metadata echoed by clip_queued 1, clip_generated 1, queue_update 1, clip_started 1, clip_finished 1
- **Video:** 124 frames, 1344x768 BGRA, 24.1 fps, interval p50 41 / p95 47 / max 114 ms, 0 lost in 0 gaps, 123 lit, 124 distinct, metadata on 0
- **Audio:** 736 blocks, 48000 Hz, 1 channel(s), 480 samples a block, peak RMS 0.0211, 0 lost
- **Pressure:** delivered 124 video / 763 audio, dropped 0 / 0, reader overflows 0
- **Network path:** no pair, RTT median 79 ms, received median 0 kbps over 9 samples
- **Contract:** h3-reference-to-video-turbo-realtime v0.0.0 (documented 0.5.5); messages state_update 5, autoplay_accepted 1, queue_update 3, clip_queued 1, clip_generated 1, clip_started 1, clip_finished 1; unknown none; 0 duplicate, 0 stale; diagnostics none
- **Termination:** confirmed; coordinator terminal +0.73 s after the request; trail CLOSED@+0.73 s
- **Criteria:** ✗ correlated acceptance · ✓ lifecycle progression · ✓ live video · ✓ audio when offered · ✓ metadata preserved · ✗ ICE pair selected · ✓ confirmed termination
  - correlated acceptance: acceptance was only by metadata
  - ICE pair selected: no stats sample named the selected pair
  - incomplete evidence: network.pair

### takeover: fail (paid, run 8c148d36, 2026-09-24T02:42:32.555Z)

- **Environment:** reactor-effect-client 0.3.0-rc.0, reactor-effect-native 0.3.0-rc.0, effect 4.0.0-rc.115, @effect/platform-node 4.0.0-rc.115, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** Linux workstation on Wi-Fi, no VPN or exit node, outbound UDP open
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.21 s · minted 0.30 s · owner killed 12.37 s · attached 15.14 s · observed 21.27 s · terminated 21.83 s · trail 22.03 s
- **Connect phases:** described +0.10 s, prepared +0.20 s, registered +0.26 s, offered +0.56 s, answered +0.64 s, ready +0.98 s
- **Video:** 0 frames, , ? fps, interval p50 ? / p95 ? / max ? ms, 0 lost in 0 gaps, 0 lit, 0 distinct, metadata on 0
- **Takeover:** attached 2.77 s after the kill; clip identified; metadata preserved; 0 enqueue(s) after attach; first fresh frame none
- **Termination:** confirmed; coordinator terminal +0.76 s after the request; trail CLOSED@+0.76 s
- **Criteria:** ✓ attach within 5 s · ✓ clip identified · ✓ metadata preserved · ✓ no enqueue on attach · ✗ fresh frames · ✓ confirmed termination
  - fresh frames: no frame arrived after attaching; the attach resumed no track, so hosted Reactor may need it to
  - incomplete evidence: takeover.firstFreshFrameMs

### vertical: fail (paid, run 9ded60b5, 2026-09-24T03:18:19.562Z)

- **Environment:** reactor-effect-client 0.3.0-rc.0, reactor-effect-native 0.3.0-rc.0, effect 4.0.0-rc.115, @effect/platform-node 4.0.0-rc.115, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** Linux workstation on Wi-Fi, no VPN or exit node, outbound UDP open
- **Server:** cluster 5a004973-4a49-4f0e-96ac-6cd7637f61eb, zone us-east, version 1.20260922.28209, transport webrtc/1.0
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.25 s · minted 0.46 s · allocated 0.93 s · connected 4.00 s · accepted 4.25 s · clip started 6.43 s · observed 12.43 s · closed 12.99 s · trail 13.20 s
- **Connect phases:** described +0.42 s, prepared +0.09 s, registered +0.23 s, offered +0.65 s, answered +0.38 s, ready +0.99 s
- **Clip:** accepted 0.11 s after submit (metadata); generated +2.18 s; started +2.18 s; first clip frame +0.08 s after start; ended +7.31 s; metadata echoed by clip_queued 1, clip_generated 1, queue_update 1, clip_started 1, clip_finished 1
- **Video:** 124 frames, 1344x768 BGRA, 23.7 fps, interval p50 42 / p95 64 / max 80 ms, 0 lost in 0 gaps, 123 lit, 124 distinct, metadata on 0
- **Audio:** 743 blocks, 48000 Hz, 1 channel(s), 480 samples a block, peak RMS 0.0228, 0 lost
- **Pressure:** delivered 124 video / 759 audio, dropped 0 / 0, reader overflows 0
- **Network path:** no pair, RTT median 21 ms, received median 7179.8 kbps over 9 samples
- **Contract:** h3-reference-to-video-turbo-realtime v0.0.0 (documented 0.5.5); messages state_update 5, autoplay_accepted 1, queue_update 3, clip_queued 1, clip_generated 1, clip_started 1, clip_finished 1; unknown none; 0 duplicate, 0 stale; diagnostics none
- **Termination:** confirmed; coordinator terminal +0.77 s after the request; trail CLOSED@+0.77 s
- **Criteria:** ✗ correlated acceptance · ✓ lifecycle progression · ✓ live video · ✓ audio when offered · ✓ metadata preserved · ✗ ICE pair selected · ✓ confirmed termination
  - correlated acceptance: acceptance was only by metadata
  - ICE pair selected: no stats sample named the selected pair
  - incomplete evidence: network.pair

### vertical: pass (paid, run 6438e4e9, 2026-09-24T05:10:26.250Z)

- **Environment:** reactor-effect-client 0.3.0-rc.0, reactor-effect-native 0.3.0-rc.0, effect 4.0.0-rc.115, @effect/platform-node 4.0.0-rc.115, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic, commit 0255871b2621
- **Network:** Linux workstation on Wi-Fi, no VPN or exit node, outbound UDP open
- **Server:** cluster 5a004973-4a49-4f0e-96ac-6cd7637f61eb, zone us-east, version 1.20260922.28209, transport webrtc/1.0
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.24 s · minted 0.44 s · allocated 0.91 s · connected 3.64 s · accepted 3.78 s · clip started 5.93 s · observed 11.94 s · closed 12.36 s · trail 12.59 s
- **Connect phases:** described +0.29 s, prepared +0.21 s, registered +0.24 s, offered +0.64 s, answered +0.50 s, ready +0.73 s
- **Clip:** accepted 0.03 s after submit (correlated); generated +2.15 s; started +2.15 s; first clip frame +0.10 s after start; ended +7.28 s; metadata echoed by clip_queued 1, clip_generated 1, queue_update 1, clip_started 1, clip_finished 1
- **Video:** 124 frames, 1344x768 BGRA, 24.2 fps, interval p50 41 / p95 45 / max 71 ms, 0 lost in 0 gaps, 123 lit, 124 distinct, metadata on 0
- **Audio:** 708 blocks, 48000 Hz, 1 channel(s), 480 samples a block, peak RMS 0.0212, 0 lost
- **Pressure:** delivered 124 video / 719 audio, dropped 0 / 0, reader overflows 0
- **Network path:** local prflx, RTT median 20 ms, received median 8303.5 kbps over 9 samples
- **Contract:** h3-reference-to-video-turbo-realtime v0.0.0 (documented 0.5.5); messages state_update 5, autoplay_accepted 1, queue_update 3, clip_queued 1, clip_generated 1, clip_started 1, clip_finished 1; unknown none; 0 duplicate, 0 stale; diagnostics none
- **Termination:** confirmed; coordinator terminal +0.65 s after the request; trail CLOSED@+0.65 s
- **Criteria:** ✓ correlated acceptance · ✓ lifecycle progression · ✓ live video · ✓ audio when offered · ✓ metadata preserved · ✓ ICE pair selected · ✓ confirmed termination

### takeover: pass (paid, run d7b0b5db, 2026-09-24T05:10:56.961Z)

- **Environment:** reactor-effect-client 0.3.0-rc.0, reactor-effect-native 0.3.0-rc.0, effect 4.0.0-rc.115, @effect/platform-node 4.0.0-rc.115, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic, commit 0255871b2621
- **Network:** Linux workstation on Wi-Fi, no VPN or exit node, outbound UDP open
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.24 s · minted 0.45 s · owner killed 11.68 s · attached 14.37 s · observed 16.52 s · terminated 16.94 s · trail 17.15 s
- **Connect phases:** described +0.22 s, prepared +0.08 s, registered +0.23 s, offered +0.65 s, answered +0.51 s, ready +0.97 s
- **Video:** 48 frames, 1344x768 BGRA, 24.3 fps, interval p50 42 / p95 45 / max 45 ms, 0 lost in 0 gaps, 48 lit, 48 distinct, metadata on 0
- **Takeover:** attached 2.69 s after the kill; clip identified; metadata preserved; 0 enqueue(s) after attach; first fresh frame +0.21 s after attach
- **Termination:** confirmed; coordinator terminal +0.63 s after the request; trail CLOSED@+0.63 s
- **Criteria:** ✓ attach within 5 s · ✓ clip identified · ✓ metadata preserved · ✓ no enqueue on attach · ✓ fresh frames · ✓ confirmed termination

## Why the first three runs failed

The hosted sessions did what the plan asked of them: allocation, connection, a clip from submission to playback, 24 fps video with no loss, audio, and confirmed termination. The three failures are in how the library observes or drives a session. None of them is a stop rule: no outcome was unknown and no termination unconfirmed.

1. **The verticals could not name the ICE pair.** The native library's stats (`packages/native/rust/src/protocol/stats.rs`) give each candidate pair with its local candidate only. They give no remote candidate and no selected pair, so `remoteCandidateType` is never set for a native peer, on any network. Those stats come from reactor-webrtc, Reactor's own crate that the native library wraps, and it reports no more than that. The sampler in `packages/client/src/stats.ts` also takes the first pair that is nominated and succeeded, which need not be the pair carrying the media.
   - In the first vertical, that pair's local candidate was `relay`. It received at 37 kbps in its first second and at 0 kbps after that, while video arrived at 24 fps.
   - In the third run, the first such pair changed after one second from `relay` to `prflx` and carried 6 to 10 Mbps. So from this network the media went direct, over a peer-reflexive candidate, not through TURN.

   The twin's peer reports both candidate types, which is why the rehearsal passed.

2. **The verticals accepted their clip by metadata, though a correlated reply followed.** The third run recorded every reply the session received:
   - H3 answered the enqueue with a `clip_queued` reply matched to its request, as Reactor documents ([reactor-class](https://docs.reactor.inc/sdk-reference/reactor-class), [FastH3 schema](https://docs.reactor.inc/model-api-reference/fast-h3/schema)).
   - It sent no bodyless acknowledgement to any command.
   - It broadcast the `queue_update` that lists the new clip before that reply.

   The H3 client accepts a submission on the first message that lists its clip with its metadata (`packages/client/src/h3/_internal/client.ts`). So acceptance was proved `by queue_update unsolicited`, and its evidence reads `metadata`. The #17 fix and the twin both assume that H3 replies before it broadcasts what a command changed. For this enqueue, hosted H3 did the opposite.

3. **The attached process received no media.** For 6 s after attaching, no frame arrived, although the owner's 15 s clip was still playing: the attached state named it in `playing_clip_id`. The library resumes receive-only tracks only for a session it created (`autoResumeTracks ?? intent._tag === "Create"` in `packages/client/src/session.ts`). The verticals' created sessions resumed their tracks and received 124 frames each. So hosted Reactor holds an attached connection's media until `resume_track`. Reactor documents exactly this. "Each connection subscribes to the output tracks it wants on its own" ([sessions](https://docs.reactor.inc/concepts/sessions#tracks-across-connections)), and its SDK's `autoResumeTracks` defaults to `true` for every connection, adopted ones included ([types](https://docs.reactor.inc/sdk-reference/types#connectoptions)). The library's default for an attach departs from that.

## What the fixes change

Commit `0255871` changes only the TypeScript client, the twin and the qualification script. The native package is untouched.

- **The ICE pair.** `session.stats` reports the pair that carries the connection. That is the pair a transport names in `selectedCandidatePairId`, as browsers report it, or else the nominated, succeeded pair that received the most since the previous sample. libwebrtc leaves a pair nominated after ICE moves off it. The native host numbers pairs by their position in its report, so across samples a pair is known by its priority and its local candidate. The vertical records the pair from the last sample in which it was receiving, by its local candidate type. That type says whether this side went through TURN. The remote candidate stays unnamed, because reactor-webrtc reports none.
- **Acceptance.** While an enqueue awaits its reply, the H3 client holds evidence by metadata, such as the `queue_update` that hosted H3 broadcasts first. The matched `clip_queued` reply then proves the acceptance as `correlated`. Held evidence decides it only if the reply cannot: after an acknowledgement or a lost reply, or when a new transport generation, a provider failure or the provider's close ends the wait. A definite refusal discards it.
- **Attach.** Every connection resumes its receive-only tracks once ready, whether it created the session or attached to it, on connect and on every reconnect.
- **The twin.** It now behaves as hosted Reactor did:
  - it broadcasts an enqueue's queue before its reply;
  - it holds each connection's tracks until that connection resumes them;
  - it reports pairs as the native host does: local candidates only, with the relay pair ICE nominated first still nominated beside the direct pair that carries the media.

  Rehearsed against the published 0.3.0-rc.0, it fails in all three ways hosted Reactor did: acceptance by metadata, no receiving pair named, and no frame after attaching. With the fixes, both rehearsals pass.

## The re-run

Both checks passed. They ran from a checkout of `0255871`, not from published bytes. Their evidence therefore names that commit, clean, beside the package versions the checkout declares, 0.3.0-rc.0.

- **Vertical (`6438e4e9`).**
  - The clip was accepted 0.03 s after submission, by the matched `clip_queued` reply: `correlated`.
  - It was generated and started 2.15 s after acceptance. Its first frame came 0.10 s after the start, and it ended 7.28 s after acceptance.
  - 124 frames of 1344x768 video arrived at 24.2 fps with no loss, and audio arrived too.
  - The relay pair carried the first second after connecting, at 43 kbps. From then on, a direct pair over a peer-reflexive local candidate carried the media, at a median 8.3 Mbps with a 20 ms round trip to the us-east cluster. The stats named that pair throughout the clip.
- **Takeover (`d7b0b5db`).**
  - The owner was streaming 10.7 s after allocation when it was killed.
  - The attacher connected 2.69 s after the kill and resumed its tracks. Its first frame came 0.21 s later, and 48 frames arrived at 24.3 fps with no loss.
  - The attached state named the owner's playing clip, the clip queued behind it kept its metadata, and nothing was enqueued.
  - The durable owner record ended the session.
- **Termination.** Both DELETEs answered 200, and the library's confirmation read returned `CLOSED`. The trail saw it 0.65 s and 0.63 s after the request.

## What the runs settle

The plan's five open questions:

1. **Whether a token's claims carry its granted limits.** Yes. The preflights and all five runs read a grant of one session of at most 50 s, which is what they asked for.
2. **How a started minute bills.** It does not bill as a whole minute, and it does not bill at the published rate either. The third run took the balance from $6.12 to $6.08, and the balance still read $6.08 18 minutes after the session ended. That is a charge of $0.03 to $0.05 once the rounding to cents is allowed for. A whole minute would have taken $0.75, and 125 credits a second from `ready` to termination would have taken $0.11. What Reactor does bill is a question for Reactor ([the dashboard](#the-dashboard)). The billing documentation says billing is "per session-minute" and that the meter starts at `ready`. The live pricing endpoint states H3's rate as 125 credits a `second`. Usage endpoints are "coming soon" ([billing](https://docs.reactor.inc/resources/billing)). After the re-run the balance read $5.92, $0.16 less, and the dashboard listed only the re-run's two sessions since the last reading. So those two cost $0.15 to $0.17 together. Whole minutes would have taken $1.50, and the published rate from allocation $0.33. For the plan, a check's worst case of one whole minute ($0.75) is 15 to 25 times what the third check cost, and about 9 times what each re-run check cost on average.
3. **What reading an ended session returns to its own token.** A terminal state. All five DELETEs answered 200, and each confirmation returned `CLOSED`. The trail saw `CLOSED` 0.63 s to 0.77 s after the request. On this evidence `terminate` needs no confirmation poll.
4. **Whether an attached connection receives media without `resume_track`.** No; see failure 3. Since `0255871` an attach resumes its tracks, and in the re-run its first frame came 0.21 s after attaching.
5. **Whether H3 matches its documented contract.** Its messages do. In every run each message type was known, with no unknown, duplicate or stale delivery and no diagnostic. The order differs, as failure 2 describes. The deployment does not report the documented version: it calls itself `h3-reference-to-video-turbo-realtime` at version `v0.0.0`, where the library's codec follows the documented 0.5.5. Both go upstream. Reactor's model catalog now lists FastH3 (`fast-h3`), whose schema is this command set plus picture-based enqueues. It no longer lists `h3-reference-to-video-turbo-realtime`, which the pricing endpoint still prices.

Measured for the-show and the library's defaults:

- `create` took 0.46 s. Allocation to ready took 2.5 s, 2.8 s and 2.6 s in the three verticals. The attaches connected in 2.73 s and 2.65 s.
- Submission to acceptance took 0.09 s and 0.11 s by metadata in the first round, and 0.03 s by the matched reply in the re-run. Acceptance to the clip starting took 2.06 s, 2.18 s and 2.15 s, and the first clip frame came 0.08 s to 0.10 s after the start. A 5 s clip ended 7.19 s to 7.31 s after acceptance.
- In the takeovers, the owner was streaming 11.6 s and 10.7 s after allocation, and attaching took 2.77 s and 2.69 s after the kill.
- Over the direct path, 1344x768 video with its audio arrived at a median 7.2 Mbps and 8.3 Mbps, with round trips of 21 ms and 20 ms to the us-east cluster. The first run's 79 ms and 0 kbps belong to the pair that failure 1 describes, which did not carry the media.

## The dashboard

The Sessions page lists all five sessions as `CLOSED`, with no other session among them:

- The first vertical ran 11 s, on cluster `e87af224-…`, which its evidence also names.
- The takeover ran 20 s, on cluster `5a004973-…`. The takeover never inspects its session, so its evidence names no cluster.
- The third run ran 12 s, on `5a004973-…`.
- The re-run's vertical ran 11 s and its takeover 15 s, both on `5a004973-…`. The dashboard names the takeover's cluster, which its evidence does not.

Those durations follow each session from its creation to its termination. The time from allocation to the DELETE was 11.26 s, 20.51 s and 11.51 s. The server's own start and end fall a little before and after those, because allocation is when `create` returned and the DELETE takes up to half a second. The durations are not the time from `ready` to the end, which was 8.74 s and 8.75 s in the two verticals. So the dashboard counts a session from its creation, earlier than the `ready` that the billing documentation names.

The page shows no charges. Instead the balance was read three times:

- $6.12 before the third run;
- $6.08 at 03:23 UTC, 4.5 minutes after the run ended;
- $6.08 again at 03:36 UTC, when the page counted exactly one more session than it had before the run;
- $5.92 after the re-run, when the page listed its two sessions and none other since the third run.

That session ran 11.51 s from allocation to the DELETE, 8.75 s of it after `ready`. Charged as a whole minute, it would have cost $0.75. Charged by the second at 125 credits a second, it would have cost $0.11 to $0.14. The balance fell by $0.03 to $0.05, or 300 to 500 credits, which is 2.4 s to 4.0 s at the published rate.

The re-run's two sessions ran 11.04 s and 15.52 s from allocation to the DELETE. Charged as whole minutes, they would have cost $1.50. By the second at 125 credits a second, they would have cost $0.14 and $0.19. The balance fell by $0.15 to $0.17 for the two, or 1,500 to 1,700 credits, which is 12.0 s to 13.6 s at the published rate against 26.6 s from allocation to the DELETE. The third run was charged for 21% to 35% of that time and the re-run for 45% to 51%, so the readings fit no single rate over a session's time. What Reactor bills stays a question for Reactor.

| Run      | Session                                | Cluster                                | Allocated    | DELETE sent  | Dashboard duration | Estimate                    | Charge              |
| -------- | -------------------------------------- | -------------------------------------- | ------------ | ------------ | ------------------ | --------------------------- | ------------------- |
| 61f5b7fb | `00852b71-b4bb-448e-b1dd-2323d6ac9b63` | `e87af224-6e25-4031-b689-da90664578c9` | 02:40:23.99Z | 02:40:35.25Z | 11 s               | $0.75; by the second, $0.14 | not measured        |
| 8c148d36 | `4635b882-f9a7-4af6-9238-d119eb45d793` | `5a004973-4a49-4f0e-96ac-6cd7637f61eb` | 02:42:33.32Z | 02:42:53.83Z | 20 s               | $0.75; by the second, $0.25 | not measured        |
| 9ded60b5 | `c9754939-43b8-45dd-86b0-bc00507ea2f0` | `5a004973-4a49-4f0e-96ac-6cd7637f61eb` | 03:18:20.49Z | 03:18:32.00Z | 12 s               | $0.75; by the second, $0.14 | $0.03–0.05          |
| 6438e4e9 | `41793d68-c355-4464-b3af-dce7a72d1806` | `5a004973-4a49-4f0e-96ac-6cd7637f61eb` | 05:10:27.16Z | 05:10:38.19Z | 11 s               | $0.75; by the second, $0.14 | $0.15–0.17 for both |
| d7b0b5db | `c572461d-260e-407d-807d-2a241bbddfc7` | `5a004973-4a49-4f0e-96ac-6cd7637f61eb` | 05:10:57.97Z | 05:11:13.49Z | 15 s               | $0.75; by the second, $0.19 | $0.15–0.17 for both |

## The third run

The plan runs nothing twice without a fix. The maintainer asked for one more session anyway, to settle billing by the balance. It reran the vertical on the same published bytes, for at most $0.75. The ledger admitted it under a $2.25 total, the cap `gates.ts` had then. It qualifies nothing new.

Before it ran, the script gained one piece of evidence, rehearsed and tested for free first. The vertical now tallies every reply on the data channel, by kind or message type and by how the correlator attributed it (`tallyReply` in `collect.ts`), and names the reply that proved acceptance. Both go into milestone details, so the evidence schema is unchanged. That tally is what settled failure 2.

## How it ran

### The first round

- **Machine and network.** An Ubuntu 24.04 workstation on Wi-Fi, reaching the internet directly: Tailscale was up with no exit node, and there was no VPN. Before the runs, STUN binding requests to two public servers were answered, so outbound UDP was open.
- **Bytes.** A scratch project outside the repository, as in the runbook, installed the four packages from npm, and the checks ran with Bun 1.4.2. The integrity of `reactor-effect-client` and `reactor-effect-native` matches the release candidate that CI built at `430644c` (run 35946980595). The loaded `libreactor_effect_native.so` has sha256 `506f426b…` and matches its `native-identity.json`.
  - `reactor-effect-native@0.3.0-rc.0` appeared in the registry 11 minutes after npm accepted its publish, and its tarball about 3 minutes after that. The release run (35947070537) ended before either happened, reporting the registry state as unconfirmed.
- **One pin beyond the runbook.** `@effect/platform-node@4.0.0-rc.115` depends on `@effect/platform-node-shared@^4.0.0-rc.115`, which npm now resolves to rc.117. That version requires `effect@^4.0.0-rc.117`. The scratch project therefore overrides it to rc.115, as the repository's root `package.json` does.
- **Scripts.** They are a copy of `integration/hosted` at `430644c`. The scratch project is not a git checkout, so the evidence has no `commit` field. Two changes followed the first two runs:
  - `report.ts` now joins the summary table's rows with single newlines, so the table renders. It changes no evidence.
  - Before the third run, `qualify.ts` and `collect.ts` gained the reply tally.
- **Steps.** The runs went in the runbook's order:
  1. `rehearse vertical`, then `rehearse takeover`. Both passed against the twin, over the installed packages.
  2. `preflight --total-budget-usd=1.50`. The rate is $0.75 a minute, so the worst case is $0.75 a check, and the limit buys two sessions.
  3. `vertical`, then `takeover`. The takeover ran after reading the vertical's evidence, because none of the vertical's failures bears on the takeover's criteria.
  4. At the maintainer's request, `rehearse vertical` with the tally, then `preflight --total-budget-usd=2.25`, then a third `vertical`.

### The re-run

- **Sources.** A clean checkout of `0255871`, built with `bun run build` and run from the checkout with Bun 1.4.2, so the evidence records the commit. The scripts are the checkout's own `integration/hosted`.
- **Native library.** The published 0.3.0-rc.0 library, staged in `packages/native/lib/linux-x64/` from its npm tarball, whose integrity matches the registry's. Its sha256 is `506f426b…`, the library the first round loaded. Its `sourceSha256` (`6b4ad6bf…`) matches the native sources at `0255871`, which have not changed since 0.3.0-rc.0.
- **Ledger.** A new ledger, seeded with the first round's three evidence files, so its total counts all five sessions.
- **Machine and network.** The same workstation and network. STUN binding requests to the same two servers were answered again.
- **Steps.**
  1. `rehearse vertical` and `rehearse takeover`, against the twin that now behaves as hosted Reactor did. Both passed at `0255871`. Against the published 0.3.0-rc.0, the same rehearsals fail in the first round's three ways.
  2. `preflight --total-budget-usd=3.75`: the same rate, a grant of one session of at most 50 s, and room for two sessions.
  3. `vertical`, then `takeover`, which started 30 s after it.
