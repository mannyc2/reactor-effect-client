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
