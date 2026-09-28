# 0.8.0 paid checks from a checkout (September 28, 2026)

Three one-session runs from checkouts of `claude/effect-native`, approved by the maintainer at $2 in
total. The ledger reserved $1.875; the runs' estimates at the published per-second rate total
$0.925. The account balance before the first run read $5.38.

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

| Run      | Check  | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | ------ | ---- | ------- | ------------------------ | ---------- | --------- |
| 83d17eb7 | tokens | paid | fail    | 2026-09-28T04:51:39.053Z | $0.625     | $0.275    |
| 7bc779d4 | tokens | paid | fail    | 2026-09-28T05:12:10.312Z | $0.625     | $0.400    |
| c1796473 | cut    | paid | pass    | 2026-09-28T05:14:31.589Z | $0.625     | $0.250    |

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
