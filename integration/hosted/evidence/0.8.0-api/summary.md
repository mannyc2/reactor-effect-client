# 0.8.0 API verification from a checkout (September 28, 2026)

Paid runs from checkouts of `claude/effect-native` that verify the public API against hosted
Reactor in as few sessions as possible: `tour` (one 90 s session), `adoption` (one 75 s session)
and `show` (three 75 s sessions). The maintainer approved $5 for the three; the per-check worst
cases total $4.875, and the runs' estimates at the published per-second rate total $2.388.

What they found:

- **`adoption` passed on 799ed3d, every criterion.** An owner running under Node on the isolated
  native host, which no paid run had used, created the session on a 20 s token and was killed
  12.7 s in. A raw attach was ready 3.06 s after the kill, named the owner's playing clip, read the
  queued clip's metadata, sent no enqueue and closed without ending the session. `H3Source.resume`
  started 0.57 s after the creating token expired, was ready 2.71 s later, named the owner's
  playing clip, refreshed its 12 s bound token and enqueued a clip with a reference image and
  reference audio on it (`has_reference_audio` true). An expired token got 401 and an unbound one
  403, and closing the resumed source ended the session (`CLOSED`).
- **`tour` passed on c548077, all 30 criteria, in one 90 s session.** It walked the raw API's
  provider calls once each:
  - The session was created on `Coordinator.tokens` with 30 s tokens and minted a token bound to
    itself 22.9 s in, before the creating token expired at 29.8 s. A reconnect 2 s after that
    expiry succeeded, which no paid run had done: generation 1 to 2 in 2.17 s, with the ready clip
    kept and 71 fresh frames.
  - A 1:1 canvas was accepted at 768x768, and a seed of 4242 was echoed. Clip 2 reused clip 1's
    uploads, uploading nothing, and reported both references.
  - A move was reflected in the queue read, and the popped clips were never built or started.
    `stop` cut the playing clip (`clip_stopped` 62 ms after sending), nothing started with
    autoplay off, and `play` started the clip it named 52 ms after sending.
  - A 16,171-character prompt was accepted, then ended by `clip_failed` 2.39 s later, as H3's
    schema documents; the SDK reported `ClipEnded` for its own clip.
  - `reset` stopped the playing clip and emptied both queues; recorded, not judged, it restored
    the canvas to 16:9 and the seed to 1000.
  - The API key ended the session (DELETE 200, `CLOSED`). Attaching afterwards failed with
    `TerminalSession`, and the key reading an unknown session got 404, ending it confirmed absent.
- **`show` passed on 87d9f67, all 25 criteria, over three 75 s sessions.** One playout ran as a
  show runs, with filler holding the air:
  - Session 1's connection was cut with 8.07 s of air secured. It read ready 1.64 s later and its
    picture resumed 1.82 s after the drop, with no replacement opened. The playout reported the
    reconnect once, measured at 1686 ms, its two reports 1.69 s apart. No paid run had recovered a
    dropped connection before.
  - The edits aired in their planned order. The batch took effect 2.24 s after it was sent and
    0.91 s before its boundary, and its withdrawn clip never started. Each seam paused 53 to 169 ms
    with no dark frame.
  - The planned switch came 57.28 s into the run, once the grace had passed; its gap on air, recorded and not
    judged, was 420 ms. X at `Asap` aired ahead of L1, whose cues fired 0 and 1 ms from due. T, due
    `At` an instant, aired 559 ms after it, at the end of a 6 s filler tile that ran 6.58 s.
  - Content moderation ended session 2 with a `terminate` verdict 0.40 s after the flagged item was
    submitted, with no categories and naming no request of ours. The flagged item failed as
    moderated and never aired, the guard on air failed as lost, and c1, Ready, was carried to
    session 3, which opened 3.17 s after the verdict and aired it.
  - On session 3 the stepped cut, which no paid run had used, stopped the long clip with one `stop`
    and started the cutter next after a 168 ms pause, with no dark frame. The `replace` lane's new
    item took its waiting item's place, the `skip` lane refused a second item, and the drain
    completed.
  - On one session each clip followed the last within 75 ms; the loss left 5.34 s between the
    guard's failure and c1. The playout's state named each of the 20 clips as its start was
    reported, with that start. The show's reader never fell behind, and the picture lost no frame:
    2,372 frames at 22.2 fps.
- **Hosted H3 records.** Recorded, not judged: a clip request came back `ClipReady` in 2.6 s, but
  downloading it timed out after 15.1 s, and a whole-recording request timed out too. Whether the
  download or the check's window is at fault is open.
- **A session read `ACTIVE`, not `INACTIVE`, for the first 2.1 s after its last connection
  closed.** Recorded, not judged: the 0.8.0-dev `tokens` run first read `INACTIVE` 8.4 s after its
  owner's kill, so hosted takes some seconds to mark a session with no connection.

## The dashboard

The Usage page, read after the `show` run, lists the round's five sessions, all `CLOSED` on
cluster `5a004973-…`:

| Run      | Check      | Session      | Allocated    | End requested | Dashboard duration | Estimate |
| -------- | ---------- | ------------ | ------------ | ------------- | ------------------ | -------- |
| af351108 | `adoption` | `2b56408f-…` | 15:51:25.21Z | 15:51:55.72Z  | 30 s               | $0.388   |
| a2382505 | `tour`     | `ef93e126-…` | 16:12:11.10Z | 16:12:47.23Z  | 36 s               | $0.463   |
| 3a197a9a | `show`     | `224ecfa3-…` | 23:37:46.17Z | 23:38:42.99Z  | 57 s               | $0.725   |
| 3a197a9a | `show`     | `afe348f1-…` | 23:38:34.07Z | 23:39:06.98Z  | 33 s               | $0.425   |
| 3a197a9a | `show`     | `ef93d110-…` | 23:39:07.50Z | 23:39:38.14Z  | 30 s               | $0.388   |

Each dashboard duration matches the time from allocation to the end request to within a second.
Each estimate counts its session to the close report, rounded up to the second, which puts it one
second above the dashboard's every time. At the published rate the dashboard's 186 s come to
$2.325, against estimates of $2.388.

The balance then read $10.41. The maintainer had added $10 of credits during the day, and the
0.8.0-dev round's summary read $5.38 at 12:54 UTC, so if the credits came after that reading,
$4.97 left the account since. The only sessions since 12:54 UTC are these five. At the published
rate they come to $2.325, and $3.775 with the 0.8.0-dev round's 116 s, which had not been charged
at 12:54. A whole minute per session would be $7.50 for both rounds. The maintainer read billing
as not updated yet, so the balance may not hold every session's charge, and what Reactor bills
stays open.

| Run      | Check    | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | -------- | ---- | ------- | ------------------------ | ---------- | --------- |
| af351108 | adoption | paid | pass    | 2026-09-28T15:51:24.513Z | $0.938     | $0.388    |
| a2382505 | tour     | paid | pass    | 2026-09-28T16:12:10.204Z | $1.125     | $0.463    |
| 3a197a9a | show     | paid | pass    | 2026-09-28T23:37:45.721Z | $2.813     | $1.538    |

### adoption: pass (paid, run af351108, 2026-09-28T15:51:24.513Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit 799ed3d9
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.938, estimated $0.388 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.22 s · owner streaming 11.65 s · owner killed 12.72 s · attached 15.77 s · attached session closed 17.95 s · resumed 22.76 s · enqueued on a refreshed token 30.58 s · refusals read 31.21 s · closed 31.64 s · adoption observed 31.65 s · settled 31.86 s
- **Adopter's session reads:** +0.44 s 200 ACTIVE (session_id, cluster, zone, state, server_info, model, selected_transport, capabilities, origin_country, region); +7.63 s 200 ACTIVE (session_id, cluster, zone, state, server_info, model, selected_transport, capabilities, origin_country, region); +18.93 s 200 CLOSED (session_id, cluster, zone, state, server_info, model, origin_country, region)
- **Owner:** node v24.14.1, the isolated native peer; streaming at 11.65 s, killed at 12.72 s; its creating token expired at 19.47 s
- **Raw attach:** ready 3.06 s after the kill; playing the owner's playing clip; the queued clip's metadata read; commands get_state 1, get_queue 1; closed without ending the session
- **With nothing connected:** +0.21 s 200 ACTIVE; +0.80 s 200 ACTIVE; +1.39 s 200 ACTIVE; +2.10 s 200 ACTIVE
- **Resume:** started 0.57 s after the creating token expired, ready 2.71 s later, owned; playing the owner's playing clip; refreshed at 29.11 s; clip accepted in 1.71 s, has_reference_audio true; commands get_state 1, get_queue 1, enqueue 1
- **Tokens:** create at 0.22 s living 19.5 s; read at 11.86 s living 134.8 s; attach at 12.94 s living 134.7 s; resume at 20.26 s living 11.4 s; resume at 29.11 s living 11.6 s; unbound at 31.00 s living 14.7 s
- **Refusals:** expired token 401, unbound token 403
- **Termination:** 2b56408f-e115-44eb-9318-ec8af31acf7f confirmed (trail CLOSED)
- **Criteria:** ✓ the attach came within 5 s of the kill · ✓ the attach names the owner's playing clip · ✓ the attach reads the queued clip's metadata · ✓ fresh frames on attach · ✓ no enqueue on attach · ✓ the attached close attempts no termination · ✓ the session still reads live after the attached close · ✓ the resume came after the creating token expired · ✓ the resume adopts the session · ✓ the resume names one of the owner's clips playing · ✓ fresh frames on resume · ✓ a refreshed bound token carried the next call · ✓ reference audio reported · ✓ an expired token is refused · ✓ an unbound token is refused · ✓ closing the resumed source ends the session · ✓ confirmed termination

### tour: pass (paid, run a2382505, 2026-09-28T16:12:10.204Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit c5480777
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $1.125, estimated $0.463 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · free mints 0.43 s · allocated 0.90 s · connected 3.83 s · H3 ready 4.49 s · canvas set 4.57 s · clip 1 accepted 6.08 s · clip 2 accepted 6.17 s · queue edited 8.61 s · over budget accepted 8.93 s · by hand accepted 9.00 s · clip 1 aired 13.66 s · stopped and played 14.37 s · reset 14.53 s · build failed as documented 14.53 s · long clip accepted 14.61 s · recordings asked for 29.78 s · token refreshed 29.79 s · reconnected 33.95 s · closed 37.46 s · ended with the API key 37.89 s · after the end 38.35 s · settled 38.57 s
- **Video:** 204 frames at 7.1 fps, 201 lit, 204 distinct, 0 lost; audio 2990 blocks, peak RMS 0.1951
- **Pair:** prflx
- **Tokens:** create at 0.53 s living 29.3 s; bind (its own id) at 22.91 s living 30.1 s; the creating token expired at 29.78 s, refresh due at 22.45 s
- **Calls:** past the refresh point, an upload at 22.70 s, succeeded; after the expiry, reconnect at 31.78 s, succeeded
- **Canvas:** asked 1:1 (listed in valid_commands); accepted 1:1 at 768x768; state 1:1
- **Clip 1:** accepted correlated in 1.45 s, generated at 8.53 s, started at 8.54 s, ended by clip_finished; 2 upload(s); reports 1 image and 1 audio reference(s), has_reference_audio true; seed 4242 sent, 4242 echoed, default 1000 before and 1000 after; 122 frames while it played
- **Clip 2:** accepted correlated in 0.06 s, generated at 11.21 s, started at 13.68 s, ended by clip_stopped; 0 upload(s); reports 1 image and 1 audio reference(s), has_reference_audio true
- **Queue:** generation position zero, clip 1, clip 2, moved; moved to generation 0, then moved, position zero, clip 1, clip 2; pops position zero, moved (heading the generation queue); clip 2 generated 2.60 s after the moved clip's pop; refresh refreshed; round trips get_queue 44 ms, move 22 ms, pop position zero 25 ms, pop moved 39 ms, get_state 34 ms, refresh 76 ms
- **Stop and play:** stop (Acknowledged) cut clip 2, clip_stopped 62 ms after it was sent; started before the play: none; play (Acknowledged) of by hand started 52 ms after it was sent
- **Build past the text budget:** 16171 characters, ended by clip_failed 2.39 s after its acceptance; waiting for it to generate failed with ClipEnded (clip_failed, its own clip); reason 60 characters; never started
- **Recording, clip:** ClipReady (snap, markers 5, ready in 2591 ms); download TimeoutError in 15.13 s
- **Recording, recording:** TimeoutError
- **Reconnect:** generation 1 to 2 in 2.17 s, from 2.00 s after the creating token expired; the long clip still ready; 71 frames, the first 92 ms after the start; get_state answered
- **Reset:** was_playing true, 0 cleared, clip_stopped 43 ms after it was sent; before 1:1, seed 1005, autoplay off, 0 queued, by hand playing; after 16:9, seed 1000, autoplay off, 0 queued, nothing playing
- **Ended:** the API key: DELETE 200, confirmed CLOSED; the session's own close: DELETE 200, confirmed CLOSED
- **After the end:** attaching TerminalSession; the key reading an unknown session Http 404, ending it DELETE 404, confirmed absent
- **Free mints:** three sessions: granted, max_sessions 3, cap 1; no session cap: granted, max_sessions 1, cap absent
- **Termination:** ef93e126-d2c4-4c68-ae2d-418309c9c4b1 confirmed (trail CLOSED)
- **Criteria:** ✓ the deployment offers every command the tour sends · ✓ canvas_accepted names the canvas asked for at its size · ✓ the state reports the new canvas · ✓ clip 2 is accepted on clip 1's uploads · ✓ clip 2 reports its reused references · ✓ the queue read reflects the move · ✓ clip 1: correlated acceptance · ✓ clip 1: lifecycle progression · ✓ clip 1: live video · ✓ clip 1: audio when offered · ✓ clip 1: reference audio reported · ✓ clip 1: metadata preserved · ✓ stop cuts the playing clip · ✓ nothing starts after a stop with autoplay off · ✓ play starts the clip it names · ✓ reset stops the playing clip · ✓ reset leaves both queues empty and nothing playing · ✓ a prompt past H3's text budget fails its clip · ✓ the operation reports the failure as ClipEnded · ✓ the session refreshes to a token bound to itself before its token expires · ✓ a call after the creating token expired succeeds · ✓ the reconnect makes the next generation · ✓ H3 answers on the new connection with the session's state kept · ✓ fresh frames on the new connection · ✓ popped clips are never built or started · ✓ the API key ends the session · ✓ the session's own close confirms it ended · ✓ attaching to the ended session is refused · ✓ the API key finds no unknown session · ✓ confirmed termination

### show: pass (paid, run 3a197a9a, 2026-09-28T23:37:45.721Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit 87d9f679
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $2.813, estimated $1.538 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · opened 0.01 s · minted 0.10 s · allocated 0.46 s · connection dropped 7.53 s · recovered 9.54 s · edits submitted 9.54 s · inserted 15.59 s · batch submitted 38.36 s · edits observed 45.07 s · minted 47.99 s · allocated 48.35 s · L1 and X submitted 50.62 s · switched 57.28 s · T submitted 57.45 s · closed 57.70 s · start modes observed 74.34 s · guard on air, c1 Ready 80.86 s · flagged item queued behind the guard 80.86 s · minted 81.49 s · closed 81.58 s · moderation observed 81.72 s · allocated 81.78 s · replaced 84.43 s · cutter submitted 94.23 s · lane conflicts submitted 94.23 s · cut observed 99.84 s · lanes observed 106.98 s · drained 112.11 s · show observed 112.11 s · closed 112.42 s · settled 112.81 s
- **Order:** started p1, p2, xc, xn, p3, y, X, L1, T, guard, c1, long, cutter, new, solo
- **Seam p1 to p2:** pause 169 ms (0 frames, 0 dark); 0 dark frames; join change 46.17 against 3.81 (×12.13)
- **Seam p2 to xc (continued):** pause 53 ms (0 frames, 0 dark); 0 dark frames; join change 10.15 against 4.43 (×2.29)
- **Seam xc to xn:** pause 80 ms (0 frames, 0 dark); 0 dark frames; join change 63.82 against 4.08 (×15.64)
- **Seam xn to p3:** pause 72 ms (0 frames, 0 dark); 0 dark frames; join change 50.45 against 2.58 (×19.52)
- **Seam p3 to y:** pause 122 ms (0 frames, 0 dark); 0 dark frames; join change 38.86 against 2.88 (×13.48)
- **Seam long to cutter:** pause 168 ms (0 frames, 0 dark); 0 dark frames; join change 44.7 against 3.94 (×11.33)
- **Batch:** took effect 2.24 s after it was sent, 0.91 s before its boundary
- **Switch:** at 57.28 s, grace-elapsed
- **Stops:** 1
- **Filler:** 6 clips asked for (5 s at 0 s secured, 5 s at 5.17 s secured, 5 s at 5 s secured, 5 s at 8 s secured, 6 s at 10.33 s secured, 5 s at 5 s secured); starved at 112.11 s
- **On air:** 5 filler clips started and 5 ended; 19 gaps between clips, the longest 5338 ms from guard to c1 at 81.27 s, 420 ms from filler 3 to X at 57.02 s, 75 ms from long to cutter at 96.52 s
- **State on air:** named 20 clips as they started, 0 read after a later start
- **Sessions:** opened 224ecfa3-d408-4bed-9b2e-4f9c3d507ee7 > reconnecting 224ecfa3-d408-4bed-9b2e-4f9c3d507ee7 > reconnected 224ecfa3-d408-4bed-9b2e-4f9c3d507ee7 after 1686 ms > opened afe348f1-102f-4a07-83f5-bc1512fe3599 > switched 224ecfa3-d408-4bed-9b2e-4f9c3d507ee7 to afe348f1-102f-4a07-83f5-bc1512fe3599 > moderated afe348f1-102f-4a07-83f5-bc1512fe3599 blaming flagged > reconnecting afe348f1-102f-4a07-83f5-bc1512fe3599 > replaced afe348f1-102f-4a07-83f5-bc1512fe3599, 1 carried > opened ef93d110-7c10-4789-b840-2a1d00ea67d9
- **Reconnects:** 224ecfa3-d408-4bed-9b2e-4f9c3d507ee7 at 7.53 s, back 1.69 s later, measured 1686 ms; afe348f1-102f-4a07-83f5-bc1512fe3599 at 81.27 s, never back
- **Readers:** never fell behind
- **Failed:** flagged Moderated at 81.26 s, guard Lost with afe348f1-102f-4a07-83f5-bc1512fe3599 at 81.27 s
- **Recovery:** 1 connection dropped at 7.53 s with 8.07 s secured; disconnected > connecting > waiting > ready; ready 1.64 s later, first frame 1.82 s after the drop
- **At:** due at 73.78 s, started 559 ms after it
- **Cues:** L1 in 0 ms from due, L1 out 1 ms from due
- **Loss:** afe348f1-102f-4a07-83f5-bc1512fe3599 ended by moderation from 80.86 s; replaced 0.40 s later; ef93d110-7c10-4789-b840-2a1d00ea67d9 opened at 84.43 s
- **Moderation:** flagged; enqueue ok in 27 ms; item Accepted > Building > Failed
- **Verdict:** terminate at 0.40 s after the submission, categories none, input unnamed, command unnamed, names no request of ours
- **Afterwards:** session moderation terminate > status disconnected > diagnostic ChannelClosed > status closing > status closed; playout moderated afe348f1-102f-4a07-83f5-bc1512fe3599 blaming flagged > reconnecting afe348f1-102f-4a07-83f5-bc1512fe3599 > replaced afe348f1-102f-4a07-83f5-bc1512fe3599, 1 carried; read 200 CLOSED (keys session_id, cluster, zone, state, server_info, model, origin_country, region)
- **Termination:** 224ecfa3-d408-4bed-9b2e-4f9c3d507ee7 confirmed (trail CLOSED); afe348f1-102f-4a07-83f5-bc1512fe3599 confirmed (trail CLOSED); ef93d110-7c10-4789-b840-2a1d00ea67d9 confirmed (trail CLOSED)
- **Criteria:** ✓ recovery on the same session · ✓ the playout reports the reconnect · ✓ inserts and the batch air in their planned places · ✓ a batch takes effect before its boundary · ✓ a batch's withdrawn clip never starts · ✓ every seam measured · ✓ an Asap item airs ahead of one waiting · ✓ an At item airs, never before its time · ✓ cues fire at their offsets · ✓ the ended session is replaced · ✓ a cut lane's clip stops a lower lane's playing clip · ✓ a replacing lane's new item takes its waiting item's place · ✓ a skipping lane refuses an item while one waits · ✓ the drain completes on the last session · ✓ a clip on air with the lost session fails as lost · ✓ a clip not yet aired is carried to the next session · ✓ moderation fails the item it blames · ✓ one planned switch · ✓ each item airs on the session on air · ✓ video from every session · ✓ filler keeps the air covered · ✓ no gap on air between clips, filler included · ✓ the state names the clip on air as it started · ✓ the show's own reader keeps up · ✓ confirmed termination
