| Run      | Check | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | ----- | ---- | ------- | ------------------------ | ---------- | --------- |
| 063e7e59 | show  | paid | pass    | 2026-09-29T14:03:15.297Z | $2.813     | $1.413    |

### show: pass (paid, run 063e7e59, 2026-09-29T14:03:15.297Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit c9f3f960
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $2.813, estimated $1.413 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · opened 0.00 s · minted 0.09 s · allocated 0.35 s · connection dropped 7.02 s · recovered 9.02 s · edits submitted 9.03 s · inserted 15.09 s · batch submitted 37.82 s · edits observed 44.49 s · minted 45.27 s · allocated 45.61 s · L1 and X submitted 47.84 s · switched 51.58 s · closed 52.06 s · T submitted 52.26 s · start modes observed 68.64 s · guard on air, c1 Ready 74.93 s · ended with the API key 75.37 s · minted 75.57 s · closed 75.60 s · allocated 75.93 s · replaced 78.34 s · cutter submitted 88.14 s · lane conflicts submitted 88.15 s · cut observed 93.74 s · lanes observed 100.87 s · drained 106.00 s · show observed 106.01 s · closed 106.51 s · settled 107.06 s
- **Order:** started p1, p2, xc, xn, p3, y, X, L1, T, guard, c1, long, cutter, new, solo
- **Seam p1 to p2:** pause 168 ms (0 frames, 0 dark); 0 dark frames; join change 46.2 against 3.85 (×11.99)
- **Seam p2 to xc (continued):** pause 46 ms (0 frames, 0 dark); 0 dark frames; join change 10.12 against 4.44 (×2.28)
- **Seam xc to xn:** pause 77 ms (0 frames, 0 dark); 0 dark frames; join change 63.76 against 4.08 (×15.63)
- **Seam xn to p3:** pause 48 ms (0 frames, 0 dark); 0 dark frames; join change 50.41 against 2.58 (×19.57)
- **Seam p3 to y:** pause 56 ms (0 frames, 0 dark); 0 dark frames; join change 38.84 against 2.9 (×13.39)
- **Seam long to cutter:** pause 165 ms (0 frames, 0 dark); 0 dark frames; join change 44.68 against 3.94 (×11.34)
- **Batch:** took effect 2.13 s after it was sent, 1.05 s before its boundary
- **Switch:** at 51.58 s, grace-elapsed
- **Stops:** 1
- **Filler:** 6 clips asked for (5 s at 0 s secured, 5 s at 5.17 s secured, 5 s at 5 s secured, 5 s at 5 s secured, 6 s at 9.83 s secured, 5 s at 5 s secured); starved at 106.00 s
- **On air:** 4 filler clips started and 4 ended; 17 gaps between clips, the longest 5196 ms from guard to c1 at 75.32 s, 432 ms from filler 2 to X at 51.33 s, 60 ms from long to cutter at 90.43 s
- **State on air:** named 19 clips as they started, 0 read after a later start
- **Sessions:** opened 1e779bb9-cf7d-4548-838e-bcc83b49a2c1 > reconnecting 1e779bb9-cf7d-4548-838e-bcc83b49a2c1 > reconnected 1e779bb9-cf7d-4548-838e-bcc83b49a2c1 after 1897 ms > opened 752519ba-8393-412c-9eaa-ea9df4129e28 > switched 1e779bb9-cf7d-4548-838e-bcc83b49a2c1 to 752519ba-8393-412c-9eaa-ea9df4129e28 > reconnecting 752519ba-8393-412c-9eaa-ea9df4129e28 > replaced 752519ba-8393-412c-9eaa-ea9df4129e28, 1 carried > opened 282c9eb0-360d-4256-bad7-6a9ba17765de
- **Reconnects:** 1e779bb9-cf7d-4548-838e-bcc83b49a2c1 at 7.02 s, back 1.90 s later, measured 1897 ms; 752519ba-8393-412c-9eaa-ea9df4129e28 at 75.13 s, never back
- **Readers:** never fell behind
- **Failed:** guard Lost with 752519ba-8393-412c-9eaa-ea9df4129e28 at 75.32 s
- **Recovery:** 1 connection dropped at 7.02 s with 8.08 s secured; disconnected > connecting > waiting > ready; ready 1.84 s later, first frame 1.95 s after the drop
- **At:** due at 68.09 s, started 545 ms after it
- **Cues:** L1 in 1 ms from due, L1 out 0 ms from due
- **Loss:** 752519ba-8393-412c-9eaa-ea9df4129e28 ended by the API key from 74.93 s (DELETE 200, confirmed); replaced 0.39 s later; 282c9eb0-360d-4256-bad7-6a9ba17765de opened at 78.34 s
- **Termination:** 1e779bb9-cf7d-4548-838e-bcc83b49a2c1 confirmed (trail CLOSED); 752519ba-8393-412c-9eaa-ea9df4129e28 confirmed (trail CLOSED); 282c9eb0-360d-4256-bad7-6a9ba17765de confirmed (trail CLOSED)
- **Criteria:** ✓ recovery on the same session · ✓ the playout reports the reconnect · ✓ inserts and the batch air in their planned places · ✓ a batch takes effect before its boundary · ✓ a batch's withdrawn clip never starts · ✓ every seam measured · ✓ an Asap item airs ahead of one waiting · ✓ an At item airs, never before its time · ✓ cues fire at their offsets · ✓ the ended session is replaced · ✓ a cut lane's clip stops a lower lane's playing clip · ✓ a replacing lane's new item takes its waiting item's place · ✓ a skipping lane refuses an item while one waits · ✓ the drain completes on the last session · ✓ a clip on air with the lost session fails as lost · ✓ a clip not yet aired is carried to the next session · ✓ one planned switch · ✓ each item airs on the session on air · ✓ video from every session · ✓ filler keeps the air covered · ✓ no gap on air between clips, filler included · ✓ the state names the clip on air as it started · ✓ the show's own reader keeps up · ✓ confirmed termination

## The dashboard

Read on September 29, 2026, at about 14:15 UTC. The Usage page lists the run's three sessions, all `CLOSED` on cluster `5a004973-…`, at 51 s, 29 s and 30 s, each within a second of its time from allocation to the end requested. It shows no charge per session; see the 0.8.0-probe summary for the balance.
