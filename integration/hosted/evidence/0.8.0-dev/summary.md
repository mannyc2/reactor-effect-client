# 0.8.0 paid checks from a checkout (September 28, 2026)

Five one-session runs from checkouts of `claude/effect-native`. The maintainer approved $2 for the
first three, then $1.25 more for `vertical` and `edits` on the rewrite's final head: $3.25 in
total. The ledger reserved $3.125; the runs' estimates at the published per-second rate total
$1.513. The account balance before the first run read $5.38.

What they found:

- **A session left without a connection reads `INACTIVE`, and it is still live.** In the first
  `tokens` run the adopter arrived 8.4 s after the owner was killed and read `INACTIVE`. The client
  counted that as ended, as every release before it had, and refused to adopt. The API-key `DELETE`
  then returned 200 and the session read `CLOSED`. Fixed in e9eba10. The second run read
  `INACTIVE` at 8.35 s, adopted, and the descriptor still listed `capabilities` and
  `selected_transport`.
- **Tokens work as documented.** Adopting after the creating token expired, with a token bound to
  the session, attached in 2.9 s. A refreshed bound token carried an upload and a clip with a
  reference image and reference audio (`has_reference_audio` true). An expired token was refused
  with 401, a token without the bind with 403, and the API key ended the session (DELETE 200, then
  `CLOSED`). Probes: a 15 s token lives 15 s; 7 h is clamped to 6 h; an uncapped token's echo states
  no cap; `max_sessions` is echoed; a bind naming an unknown session is refused with 403; the API
  key reading or ending an unknown session gets 404.
- **The second `tokens` run failed one criterion of the check's own making.** Its adoption came
  after the owner's first clip ended, so the clip queued behind it was playing; the check now
  accepts either (59136a2).
- **The fenced cut works on hosted H3.** One `stop`, the cutter started next, and the seam paused
  150 ms with no dark frame.
- **Moderation sends a verdict, but it names nothing.** A flagged text prompt's enqueue was
  answered in 22 ms, so the command was admitted first. The verdict, `terminate`, came 1.01 s after
  the submission, with no categories, no input kind, no command and no request id. The session then
  went closing, then `CLOSED`. `Playout` blamed the latest enqueue, which was the flagged item,
  settled it `Failed` as moderated and, with `maxModerations: 1`, failed rather than open another
  session. The prompt text appears nowhere in this ledger.
- **The rewritten stack works end to end.** `vertical` ran on 69f34e9, after the session,
  coordinator, H3 and native host changes that followed the first three runs. It minted a token,
  allocated, connected 2.7 s later, had its clip accepted and correlated in 30 ms, saw it generated
  and started, received all 124 of its frames at 24.2 fps with none lost, heard audio, saw its
  metadata echoed and confirmed the termination. It failed one criterion of the check's own making:
  "live video" required the latest eight frames all lit, but H3 ends a clip on one black frame
  unless the session holds its last frame (`flush_on_clip_end`, on by default), and a 6 s window
  over a 5 s clip always reads that frame. Rehearsals read for 1.5 s and never reached it. The rule
  now allows that one frame, and rehearsals read as long as paid runs (3539ed4); without the rule
  change the `vertical`, `audio` and `turn` rehearsals fail as this run did. It was not run again.
- **`Playout`'s edits work on hosted H3.** `edits` ran on 3539ed4, whose library is 69f34e9's. The
  group's three beats, the insert continuing from the second, the insert before the third and the
  batch's insert after it aired in the planned order (p1, p2, xc, xn, p3, y). The batch took effect
  2.12 s after it was sent and 1.05 s before its boundary, and the clip it withdrew never started.
  The seams paused 58–169 ms with no dark frame. The continued join changed 5.8× the clip's own
  motion, against 13–19× at the independent joins, and its seam frames show the same glasses and
  plate shifted a little, where the independent joins cut to a new picture.

## The commits

The branch's commit messages were reworded after these runs, which gave every commit a new hash
and changed no tree. The commits the runs name are still reachable from the tag
`archive/effect-native-pre-reword`, and each is on the branch as:

| The evidence names | On the branch | Runs                  |
| ------------------ | ------------- | --------------------- |
| 8458d081           | b799f9b       | 83d17eb7 (`tokens`)   |
| f5daf554           | cc6df75       | 7bc779d4 (`tokens`)   |
| 59136a27           | 52d0e96       | c1796473 (`cut`)      |
| 69f34e9f           | 5cf54a2       | d56fd1ee (`vertical`) |
| 3539ed45           | 4bfb5b3       | 3ae36b29 (`edits`)    |

## The dashboard

The Usage page, read after the last run, lists these sessions, all `CLOSED` on cluster `5a004973-…`
(which the page calls the region, and `vertical`'s evidence names too):

| Run      | Session      | Allocated    | DELETE sent  | Dashboard duration | Estimate |
| -------- | ------------ | ------------ | ------------ | ------------------ | -------- |
| 7bc779d4 | `f9bca4a1-…` | 05:12:11.77Z | 05:12:42.69Z | 31 s               | $0.400   |
| c1796473 | `d9530be0-…` | 05:14:32.10Z | 05:14:51.12Z | 19 s               | $0.250   |
| d56fd1ee | `ba3df1cc-…` | 12:03:08.08Z | 12:03:19.43Z | 11 s               | $0.150   |
| 3ae36b29 | `1f84da65-…` | 12:11:20.61Z | 12:11:55.45Z | 35 s               | $0.438   |

The first `tokens` session (`9737e480-…`, allocated 04:51:40.41Z, 20.8 s to the DELETE) was not
among the rows read then; read again after the 0.8.0-api `show` run, the page lists it at 20 s. Each listed duration is the time from allocation to the DELETE, to the second.

The page shows no charges. The balance read $5.38 at 04:01 UTC, before the first run, and $5.38 at
12:54 UTC, after all five. At the published rate the four listed sessions' 96 s come to $1.20, and
all five to about $1.46; a whole minute each would be $3.75. So by that reading these sessions had
not been charged, where 0.3.0's moved the balance within minutes, by a fraction of the published
rate ([its summary](../0.3.0-rc.0/summary.md#the-dashboard)). What Reactor bills stays a question
for Reactor.

| Run      | Check    | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | -------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 83d17eb7 | tokens   | paid | fail    | 2026-09-28T04:51:39.053Z | $0.625     | $0.275    |
| 7bc779d4 | tokens   | paid | fail    | 2026-09-28T05:12:10.312Z | $0.625     | $0.400    |
| c1796473 | cut      | paid | pass    | 2026-09-28T05:14:31.589Z | $0.625     | $0.250    |
| d56fd1ee | vertical | paid | fail    | 2026-09-28T12:03:07.569Z | $0.625     | $0.150    |
| 3ae36b29 | edits    | paid | pass    | 2026-09-28T12:11:19.747Z | $0.625     | $0.438    |

### tokens: fail (paid, run 83d17eb7, 2026-09-28T04:51:39.053Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit 8458d081
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.625, estimated $0.275 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · probed 1.13 s · minted 1.22 s · owner killed 12.51 s · closed 22.47 s · settled 22.48 s
- **Probe, a 15 s token:** 200; lives 15 s of 15 asked; echo echoed true, maxSessions 1, maxSessionSeconds 50, capStated true, bound 0
- **Probe, a 7 h token:** 200; lives 21600 s of 25200 asked; echo echoed true, maxSessions 1, maxSessionSeconds 1, capStated true, bound 0
- **Probe, an uncapped token:** 200; lives 15 s of 15 asked; echo echoed true, maxSessions 1, maxSessionSeconds null, capStated false, bound 0
- **Probe, a token for three sessions:** 200; lives 14 s of 15 asked; echo echoed true, maxSessions 3, maxSessionSeconds 50, capStated true, bound 0
- **Probe, a token bound to an unknown session:** 403
- **Probe, the API key reading an unknown session:** 404
- **Probe, the API key ending an unknown session:** 404
- **Tokens:** create at 1.22 s living 19.8 s; bind at 21.55 s living 11.5 s
- **Refusals:** expired token –, unbound token –; API key termination –
- **Termination:** 9737e480-528a-4bf8-93b9-ec09f49c97a7 confirmed
- **Criteria:** ✓ confirmed termination
  - TerminalSession: INACTIVE

### tokens: fail (paid, run 7bc779d4, 2026-09-28T05:12:10.312Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit f5daf554
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.625, estimated $0.400 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · probed 1.10 s · minted 1.32 s · owner killed 13.25 s · adopted 24.08 s · enqueued on a refreshed token 31.75 s · closed 32.80 s · tokens observed 32.80 s · settled 33.33 s
- **Adopter's session reads:** +8.35 s 200 INACTIVE (session_id, cluster, zone, state, server_info, model, selected_transport, capabilities, origin_country, region); +19.86 s 200 CLOSED (session_id, cluster, zone, state, server_info, model, origin_country, region)
- **Probe, a 15 s token:** 200; lives 15 s of 15 asked; echo echoed true, maxSessions 1, maxSessionSeconds 50, capStated true, bound 0
- **Probe, a 7 h token:** 200; lives 21600 s of 25200 asked; echo echoed true, maxSessions 1, maxSessionSeconds 1, capStated true, bound 0
- **Probe, an uncapped token:** 200; lives 14 s of 15 asked; echo echoed true, maxSessions 1, maxSessionSeconds null, capStated false, bound 0
- **Probe, a token for three sessions:** 200; lives 14 s of 15 asked; echo echoed true, maxSessions 3, maxSessionSeconds 50, capStated true, bound 0
- **Probe, a token bound to an unknown session:** 403
- **Probe, the API key reading an unknown session:** 404
- **Probe, the API key ending an unknown session:** 404
- **Tokens:** create at 1.32 s living 19.6 s; bind at 21.40 s living 11.5 s; bind at 30.28 s living 11.6 s; unbound at 32.17 s living 14.7 s
- **Adoption:** started 0.50 s after the creating token expired, 7.92 s after the owner died, attached 2.90 s later; playing clip not identified
- **Refreshed call:** refreshed at 30.28 s; clip accepted in 1.69 s, has_reference_audio true
- **Refusals:** expired token 401, unbound token 403; API key termination confirmed (DELETE 200)
- **Termination:** f9bca4a1-42fe-4ed5-99f5-ce3e2adbf62e confirmed (trail CLOSED)
- **Criteria:** ✓ adopted after the creating token expired · ✗ clip identified · ✓ fresh frames · ✓ a refreshed bound token carried the next call · ✓ reference audio reported · ✓ an expired token is refused · ✓ an unbound token is refused · ✓ the API key ends the session · ✓ confirmed termination
  - clip identified: the adopted state named 77531f5a-634c-4b70-a95b-cf33f5dd9a5c playing, not the owner's

### cut: pass (paid, run c1796473, 2026-09-28T05:14:31.589Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit 59136a27
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.625, estimated $0.250 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · opened 0.00 s · minted 0.09 s · allocated 0.51 s · cutter submitted 12.99 s · cut observed 18.52 s · flagged item queued behind the guard 18.52 s · closed 19.97 s · moderation observed 22.99 s · settled 23.08 s
- **Order:** started long, cutter
- **Seam long to cutter:** pause 150 ms (0 frames, 0 dark); 0 dark frames; join change 38.69 against 5.92 (×6.53)
- **Stops:** 1
- **Moderation:** flagged; enqueue ok in 22.2 ms; item Accepted > Building > Failed
- **Verdict:** terminate at 1.01 s after the submission, categories none, input unnamed, command unnamed, names no request of ours
- **Afterwards:** session moderation terminate > status closing > status closed; playout moderated d9530be0-47e3-4671-bcb0-1a1ca397490d blaming flagged > failed Moderated; read 200 CLOSED (keys session_id, cluster, zone, state, server_info, model, origin_country, region)
- **Termination:** d9530be0-47e3-4671-bcb0-1a1ca397490d confirmed (trail CLOSED)
- **Criteria:** ✓ a cut lane's clip stops a lower lane's playing clip · ✓ confirmed termination

### vertical: fail (paid, run d56fd1ee, 2026-09-28T12:03:07.569Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit 69f34e9f
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.625, estimated $0.150 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.08 s · allocated 0.51 s · connected 3.16 s · accepted 3.65 s · clip started 5.86 s · closed 12.30 s · settled 12.52 s
- **Clip:** accepted correlated in 0.03 s, generated at 5.85 s, started at 5.86 s
- **Video:** 124 frames at 24.2 fps, 123 lit, 124 distinct, 0 lost; audio 638 blocks, peak RMS 0.0214
- **Pair:** prflx
- **Termination:** ba3df1cc-4cf5-4f3c-b0dd-00e1c9c5c26a confirmed (trail CLOSED)
- **Criteria:** ✓ correlated acceptance · ✓ lifecycle progression · ✗ live video · ✓ audio when offered · ✓ metadata preserved · ✓ ICE pair selected · ✓ confirmed termination
  - live video: fewer than eight recent lit frames arrived

### edits: pass (paid, run 3ae36b29, 2026-09-28T12:11:19.747Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit 3539ed45
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $0.625, estimated $0.438 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · opened 0.00 s · line submitted 0.01 s · minted 0.22 s · allocated 0.86 s · inserted 5.66 s · batch submitted 28.42 s · edits observed 35.10 s · closed 35.70 s · settled 35.91 s
- **Order:** started p1, p2, xc, xn, p3, y
- **Seam p1 to p2:** pause 169 ms (0 frames, 0 dark); 0 dark frames; join change 49.4 against 3.41 (×14.47)
- **Seam p2 to xc (continued):** pause 86 ms (0 frames, 0 dark); 0 dark frames; join change 25.06 against 4.31 (×5.81)
- **Seam xc to xn:** pause 61 ms (0 frames, 0 dark); 0 dark frames; join change 51.71 against 2.79 (×18.52)
- **Seam xn to p3:** pause 58 ms (0 frames, 0 dark); 0 dark frames; join change 50.98 against 3.86 (×13.21)
- **Seam p3 to y:** pause 82 ms (0 frames, 0 dark); 0 dark frames; join change 48.3 against 2.88 (×16.79)
- **Batch:** took effect 2.12 s after it was sent, 1.05 s before its boundary
- **Termination:** 1f84da65-7428-4a85-a70a-e922e90273c0 confirmed (trail CLOSED)
- **Criteria:** ✓ inserts and the batch air in their planned places · ✓ a batch takes effect before its boundary · ✓ a batch's withdrawn clip never starts · ✓ every seam measured · ✓ confirmed termination
