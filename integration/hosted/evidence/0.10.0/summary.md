# Published 0.10.0 on hosted H3 (October 3, 2026)

Three one-session checks ran against the npm packages `reactor-effect-client@0.10.0`
and `reactor-effect-native@0.10.0`, with Effect and both Node platform packages at 4.0.0,
under Bun 1.4.2 on the Linux workstation. The harness was copied from merged main
`d464419`, including connection phases, liveness and allocation-time session events.
The native addon is the published 0.10.0 linux-x64-gnu package; no checkout library
or locally rebuilt addon was used.

The approved ledger limit was $8.40. It reserved $6.65 at the live rate of 350 credits
per second and 10,000 credits per dollar. The three estimates total **$3.010**;
dashboard durations and charges have not yet been read, so these remain estimates.
All three checks passed, and every session's termination was independently confirmed.

What they found:

- `vertical` passed all seven criteria: correlated acceptance, clip lifecycle, live video,
  audio, metadata, an ICE pair and confirmed termination. It received 124 frames at 24.1 fps,
  none lost, and estimated $0.455.
- `tour` passed all 30 criteria, including reference image/audio reuse, queue edits,
  stop/play/reset, refused over-budget text, token refresh after expiry and reconnection.
  Its planned reconnect moved generation 1 to 2 in **2.014 s**, kept the Ready clip,
  and delivered 70 fresh frames at 23.9 fps. The connection span recorded all phases:
  described at +226 ms, prepared at +434 ms, registered at +434 ms, offered at +644 ms,
  answered at +1,020 ms and ready at +2,014 ms. It estimated $1.295.
- `edits` aired `p1`, `p2`, `xc`, `xn`, `p3`, `y` in the planned order. Its batch took
  effect 2.25 s after submission, 0.92 s before the boundary; the withdrawn clip never
  started. Seams paused 71–143 ms with no dark frame. It estimated $1.260.
- The one-second liveness timer's maximum lateness was **0.4 ms** in `vertical`,
  **5.7 ms** in `tour` and **1.0 ms** in `edits`. These runs did not reproduce the earlier
  82 s reconnect hang or native `Protocol` failure.
- `tour` ends its session with the API key before its own close. Reactor closing the
  channel showed as one `ChannelClosed` diagnostic at 37.141 s. The session can't know an
  outside DELETE ended it, so it began its own reconnect (generation 3) until the check's
  `session.close` ended that attempt 187 ms later, with an error. That is the expected
  order, not a fault. Both the API key and the session's own close confirmed `CLOSED`;
  there were no local cleanup errors. No Diagnostic was recorded in the other two checks. The `edits` check
  does not expose its playout's sessions for the raw event observer, so it has liveness
  evidence but no raw session-event records.
- `tour` again records, without judging it, what the 0.8.0-api tour found. A clip request
  came back `ClipReady` in 2.6 s, but downloading the clip timed out after 16.6 s, and a
  whole-recording request timed out too. Whether the download or the check's window is at
  fault is still open.

The scratch project rehearsed all three checks before spending. Free preflight initially
refused its isolated Node owner because `npm init` selected CommonJS. Setting `type` to
`module` let the owner run and the corrected preflight pass; the runbook now includes
that setting. The refusal allocated no session and consumed no ledger reservation.

## Generated run summary

| Run      | Check    | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | -------- | ---- | ------- | ------------------------ | ---------- | --------- |
| ed6ab9fe | vertical | paid | pass    | 2026-10-03T12:26:15.479Z | $1.750     | $0.455    |
| 258c60b5 | tour     | paid | pass    | 2026-10-03T12:27:22.052Z | $3.150     | $1.295    |
| 6e958291 | edits    | paid | pass    | 2026-10-03T12:28:24.798Z | $1.750     | $1.260    |

### vertical: pass (paid, run ed6ab9fe, 2026-10-03T12:26:15.479Z)

- **Environment:** reactor-effect-client 0.10.0, reactor-effect-native 0.10.0, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux workstation, no VPN, outbound UDP open
- **Cost:** worst case $1.750, estimated $0.455 at 350 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.22 s · allocated 0.61 s · connected 3.41 s · accepted 4.27 s · clip started 6.39 s · closed 12.94 s · settled 13.16 s
- **Liveness:** the runner's 1 s timer fired at most 0.00 s late; 25 session events kept
- **Clip:** accepted correlated in 0.04 s, generated at 6.38 s, started at 6.39 s
- **Video:** 124 frames at 24.1 fps, 123 lit, 124 distinct, 0 lost; audio 629 blocks, peak RMS 0.022
- **Pair:** prflx
- **Termination:** 03dc4ba5-0711-4e7f-b509-66086963a5e5 confirmed (trail CLOSED)
- **Criteria:** ✓ correlated acceptance · ✓ lifecycle progression · ✓ live video · ✓ audio when offered · ✓ metadata preserved · ✓ ICE pair selected · ✓ confirmed termination

### tour: pass (paid, run 258c60b5, 2026-10-03T12:27:22.052Z)

- **Environment:** reactor-effect-client 0.10.0, reactor-effect-native 0.10.0, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux workstation, no VPN, outbound UDP open
- **Cost:** worst case $3.150, estimated $1.295 at 350 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · free mints 0.30 s · allocated 0.91 s · connected 3.73 s · H3 ready 4.16 s · canvas set 4.19 s · clip 1 accepted 5.63 s · clip 2 accepted 5.70 s · queue edited 7.09 s · over budget accepted 7.32 s · by hand accepted 7.38 s · clip 1 aired 12.16 s · stopped and played 12.92 s · reset 13.20 s · build failed as documented 13.22 s · long clip accepted 13.29 s · recordings asked for 29.94 s · token refreshed 29.94 s · reconnected 33.95 s · closed 37.33 s · ended with the API key 37.63 s · after the end 38.09 s · settled 38.30 s
- **Liveness:** the runner's 1 s timer fired at most 0.01 s late; 116 session events kept
- **Video:** 203 frames at 6.7 fps, 200 lit, 203 distinct, 0 lost; audio 2772 blocks, peak RMS 0.1911
- **Pair:** prflx
- **Tokens:** create at 0.52 s living 29.6 s; bind (its own id) at 23.00 s living 30.2 s; the creating token expired at 29.94 s, refresh due at 22.53 s
- **Calls:** past the refresh point, an upload at 22.78 s, succeeded; after the expiry, reconnect at 31.94 s, succeeded
- **Canvas:** asked 1:1 (listed in valid_commands); accepted 1:1 at 768x768; state 1:1
- **Clip 1:** accepted correlated in 1.39 s, generated at 7.03 s, started at 7.03 s, ended by clip_finished; 2 upload(s); reports 1 image and 1 audio reference(s), has_reference_audio true; seed 4242 sent, 4242 echoed, default 1000 before and 1000 after; 121 frames while it played
- **Clip 2:** accepted correlated in 0.05 s, generated at 9.71 s, started at 12.19 s, ended by clip_stopped; 0 upload(s); reports 1 image and 1 audio reference(s), has_reference_audio true
- **Queue:** generation position zero, clip 1, clip 2, moved; moved to generation 0, then moved, position zero, clip 1, clip 2; pops position zero, moved (heading the generation queue); clip 2 generated 2.61 s after the moved clip's pop; refresh refreshed; round trips get_queue 28 ms, move 29 ms, pop position zero 32 ms, pop moved 32 ms, get_state 33 ms, refresh 58 ms
- **Stop and play:** stop (Acknowledged) cut clip 2, clip_stopped 63 ms after it was sent; started before the play: none; play (Acknowledged) of by hand started 70 ms after it was sent
- **Build past the text budget:** 16171 characters, ended by clip_failed 2.49 s after its acceptance; waiting for it to generate failed with ClipEnded (clip_failed, its own clip); reason 60 characters; never started
- **Recording, clip:** ClipReady (snap, markers 5, ready in 2583 ms); download TimeoutError in 16.61 s
- **Recording, recording:** TimeoutError
- **Reconnect:** generation 1 to 2 in 2.01 s, from 2.00 s after the creating token expired; the long clip still ready; 70 frames, the first 76 ms after the start; get_state answered
- **Reset:** was_playing true, 0 cleared, clip_stopped 68 ms after it was sent; before 1:1, seed 1005, autoplay off, 0 queued, by hand playing; after 16:9, seed 1000, autoplay off, 0 queued, nothing playing
- **Ended:** the API key: DELETE 200, confirmed CLOSED; the session's own close: DELETE 200, confirmed CLOSED
- **After the end:** attaching TerminalSession; the key reading an unknown session Http 404, ending it DELETE 404, confirmed absent
- **Free mints:** three sessions: granted, max_sessions 3, cap 1; no session cap: granted, max_sessions 1, cap absent
- **Termination:** 8abbdfbe-0f15-4846-8ed7-c7e87a9110ce confirmed (trail CLOSED)
- **Criteria:** ✓ the deployment offers every command the tour sends · ✓ canvas_accepted names the canvas asked for at its size · ✓ the state reports the new canvas · ✓ clip 2 is accepted on clip 1's uploads · ✓ clip 2 reports its reused references · ✓ the queue read reflects the move · ✓ clip 1: correlated acceptance · ✓ clip 1: lifecycle progression · ✓ clip 1: live video · ✓ clip 1: audio when offered · ✓ clip 1: reference audio reported · ✓ clip 1: metadata preserved · ✓ stop cuts the playing clip · ✓ nothing starts after a stop with autoplay off · ✓ play starts the clip it names · ✓ reset stops the playing clip · ✓ reset leaves both queues empty and nothing playing · ✓ a prompt past H3's text budget fails its clip · ✓ the operation reports the failure as ClipEnded · ✓ the session refreshes to a token bound to itself before its token expires · ✓ a call after the creating token expired succeeds · ✓ the reconnect makes the next generation · ✓ H3 answers on the new connection with the session's state kept · ✓ fresh frames on the new connection · ✓ popped clips are never built or started · ✓ the API key ends the session · ✓ the session's own close confirms it ended · ✓ attaching to the ended session is refused · ✓ the API key finds no unknown session · ✓ confirmed termination

### edits: pass (paid, run 6e958291, 2026-10-03T12:28:24.798Z)

- **Environment:** reactor-effect-client 0.10.0, reactor-effect-native 0.10.0, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux workstation, no VPN, outbound UDP open
- **Cost:** worst case $1.750, estimated $1.260 at 350 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · opened 0.00 s · line submitted 0.02 s · minted 0.22 s · allocated 0.58 s · inserted 6.39 s · batch submitted 29.13 s · edits observed 35.80 s · closed 36.52 s · settled 36.73 s
- **Liveness:** the runner's 1 s timer fired at most 0.00 s late; 0 session events kept
- **Order:** started p1, p2, xc, xn, p3, y
- **Seam p1 to p2:** pause 143 ms (0 frames, 0 dark); 0 dark frames; join change 49.41 against 3.41 (×14.49)
- **Seam p2 to xc (continued):** pause 83 ms (0 frames, 0 dark); 0 dark frames; join change 25.02 against 4.31 (×5.81)
- **Seam xc to xn:** pause 74 ms (0 frames, 0 dark); 0 dark frames; join change 51.72 against 2.78 (×18.61)
- **Seam xn to p3:** pause 71 ms (0 frames, 0 dark); 0 dark frames; join change 51.07 against 3.86 (×13.21)
- **Seam p3 to y:** pause 87 ms (0 frames, 0 dark); 0 dark frames; join change 48.2 against 2.87 (×16.78)
- **Batch:** took effect 2.25 s after it was sent, 0.92 s before its boundary
- **Termination:** 96bcbff2-47af-4889-89c4-e57e3e5a2423 confirmed (trail CLOSED)
- **Criteria:** ✓ inserts and the batch air in their planned places · ✓ a batch takes effect before its boundary · ✓ a batch's withdrawn clip never starts · ✓ every seam measured · ✓ confirmed termination
