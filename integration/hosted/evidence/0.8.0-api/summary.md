# 0.8.0 API verification from a checkout (September 28, 2026)

Paid runs from checkouts of `claude/effect-native` that verify the public API against hosted
Reactor in as few sessions as possible: `tour` (one 90 s session), `adoption` (one 75 s session)
and `show` (three 75 s sessions). The maintainer approved $5 for the three; the per-check worst
cases total $4.875.

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
- **Hosted H3 records.** Recorded, not judged: a clip request came back `ClipReady` in 2.6 s, but
  downloading it timed out after 15.1 s, and a whole-recording request timed out too. Whether the
  download or the check's window is at fault is open.
- **A session read `ACTIVE`, not `INACTIVE`, for the first 2.1 s after its last connection
  closed.** Recorded, not judged: the 0.8.0-dev `tokens` run first read `INACTIVE` 8.4 s after its
  owner's kill, so hosted takes some seconds to mark a session with no connection.

| Run      | Check    | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | -------- | ---- | ------- | ------------------------ | ---------- | --------- |
| af351108 | adoption | paid | pass    | 2026-09-28T15:51:24.513Z | $0.938     | $0.388    |

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
  | Run | Check | Mode | Verdict | Started | Worst case | Estimated |
  | --- | ----- | ---- | ------- | ------- | ---------- | --------- |
  | af351108 | adoption | paid | pass | 2026-09-28T15:51:24.513Z | $0.938 | $0.388 |
  | a2382505 | tour | paid | pass | 2026-09-28T16:12:10.204Z | $1.125 | $0.463 |

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
