| Run      | Check           | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | --------------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 05cb348c | scheduler-edits | paid | fail    | 2026-09-28T00:16:15.568Z | $0.750     | $0.750    |
| e8d72392 | scheduler-cut   | paid | fail    | 2026-09-28T00:19:45.231Z | $0.750     | $0.750    |
| 54a1f1a4 | scheduler-edits | paid | pass    | 2026-09-28T02:31:17.927Z | $0.750     | $0.750    |

### scheduler-edits: fail (paid, run 05cb348c, 2026-09-28T00:16:15.568Z)

- **Environment:** reactor-effect-client 0.7.0, reactor-effect-native 0.7.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** linux workstation, outbound UDP verified by STUN
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.28 s · minted 0.36 s · allocated 0.77 s · opened 3.36 s · line submitted 3.45 s · inserted 5.63 s · batch submitted 28.36 s · edits observed 34.81 s · closed 35.39 s · trail 35.61 s
- **Connect phases:** described +0.42 s, prepared +0.29 s, registered +0.11 s, offered +0.45 s, answered +0.63 s, ready +0.50 s
- **Order:** planned p1, xc, p2, xn, p3, y; started p1, p2, xc, xn, p3, y
- **Seam 1:** p1 to p2: pause 0.13 s (0 frames, 0 dark); join change 68.8 against 3.4 within the clip (x20.1)
- **Seam 2:** p2 to xc (continued): pause 0.08 s (0 frames, 0 dark); join change 68.8 against 4.3 within the clip (x15.9)
- **Seam 3:** xc to xn: pause 0.12 s (0 frames, 0 dark); join change 72.0 against 4.9 within the clip (x14.7)
- **Seam 4:** xn to p3: pause 0.08 s (0 frames, 0 dark); join change 78.2 against 3.9 within the clip (x20.2)
- **Seam 5:** p3 to y: pause 0.07 s (0 frames, 0 dark); join change 78.8 against 2.9 within the clip (x27.1)
- **Edit batch:** took effect 2.12 s after it was sent, 1.04 s before its boundary; withdrawn clip never started
- **Estimates:** build 0.436 s median and 1.090 s p95 per requested second; actual over requested length 1.0334
- **Termination:** confirmed; coordinator terminal +0.79 s after the request; trail CLOSED@+0.79 s
- **Criteria:** ✗ inserts and the batch air in their planned places · ✓ a batch takes effect before its boundary · ✓ a batch's withdrawn clip never starts · ✓ every seam measured · ✓ confirmed termination
  - inserts and the batch air in their planned places: started in the order p1, p2, xc, xn, p3, y

### scheduler-cut: fail (paid, run e8d72392, 2026-09-28T00:19:45.231Z)

- **Environment:** reactor-effect-client 0.7.0, reactor-effect-native 0.7.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** linux workstation, outbound UDP verified by STUN
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.34 s · minted 0.43 s · allocated 0.86 s · opened 3.53 s · raw probes done 12.22 s · cutter submitted 23.24 s · cut observed 28.87 s · closed 29.47 s · trail 29.68 s
- **Connect phases:** described +0.29 s, prepared +0.21 s, registered +0.23 s, offered +0.57 s, answered +0.64 s, ready +0.54 s
- **Position zero:** generation order position zero, running build, tail
- **Popped build:** the clip behind it took 2.09 s to be Ready; a lone build took 2.19 s
- **Queue read after enqueue:** 3 of 3 listed the new clip; 0 were answered before the enqueue
- **Cut:** the long clip ended stopped after 4.75 s; long to cutter: pause 0.12 s (0 frames, 0 dark); 2 dark frames around it; join change 65.3 against 3.9 within the clip (x16.9)
- **Termination:** confirmed; coordinator terminal +0.81 s after the request; trail CLOSED@+0.81 s
- **Criteria:** ✗ position zero goes next, behind the build that is running · ✓ a queue read sent right behind an enqueue lists the new clip · ✓ a cut lane's clip stops a lower lane's playing clip · ✓ confirmed termination
  - position zero goes next, behind the build that is running: the position-zero clip went ahead of the build that was running

### scheduler-edits: pass (paid, run 54a1f1a4, 2026-09-28T02:31:17.927Z)

- **Environment:** reactor-effect-client 0.7.0, reactor-effect-native 0.7.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic, commit aabe52f59940
- **Network:** linux workstation, outbound UDP verified by STUN
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.24 s · minted 0.33 s · allocated 0.75 s · opened 3.38 s · line submitted 3.42 s · inserted 5.52 s · batch submitted 28.37 s · edits observed 34.72 s · closed 35.30 s · trail 35.51 s
- **Connect phases:** described +0.29 s, prepared +0.21 s, registered +0.24 s, offered +0.62 s, answered +0.51 s, ready +0.50 s
- **Order:** planned p1, p2, xc, xn, p3, y; started p1, p2, xc, xn, p3, y
- **Seam 1:** p1 to p2: pause 0.17 s (0 frames, 0 dark); 0 dark frames around it; join change 49.4 against 3.4 within the clip (x14.5)
- **Seam 2:** p2 to xc (continued): pause 0.06 s (0 frames, 0 dark); 0 dark frames around it; join change 25.1 against 4.3 within the clip (x5.8)
- **Seam 3:** xc to xn: pause 0.07 s (0 frames, 0 dark); 0 dark frames around it; join change 51.7 against 2.8 within the clip (x18.3)
- **Seam 4:** xn to p3: pause 0.06 s (0 frames, 0 dark); 0 dark frames around it; join change 51.0 against 3.9 within the clip (x13.1)
- **Seam 5:** p3 to y: pause 0.06 s (0 frames, 0 dark); 0 dark frames around it; join change 48.3 against 2.9 within the clip (x16.7)
- **Edit batch:** took effect 2.15 s after it was sent, 0.92 s before its boundary; withdrawn clip never started
- **Estimates:** build 0.423 s median and 0.441 s p95 per requested second; actual over requested length 1.0334
- **Termination:** confirmed; coordinator terminal +0.79 s after the request; trail CLOSED@+0.79 s
- **Criteria:** ✓ inserts and the batch air in their planned places · ✓ a batch takes effect before its boundary · ✓ a batch's withdrawn clip never starts · ✓ every seam measured · ✓ confirmed termination

## Reading

The first two runs, one of each check, ran against published 0.7.0 through the public owner, $1.50 in all. Their sessions kept H3's default `flush_on_clip_end: true`; the harness now opens them with `holdLastFrame: true`, as a show does. Each observation is one sample.

- **A black frame at every seam, as H3 documents.** In `scheduler-edits`, 5 of 692 frames were dark: the last of each clip's 124 frames, for the five clips that finished. H3's schema says `set_flush_on_clip_end`, true by default, flushes to black at boundaries; `holdLastFrame: true` turns that off. 0.6.0's `scheduler` run had 5 dark frames of 652 too. Its pause measure passes over a single dark frame followed by a new picture, so its reading of no black frames held only for pauses. No seam paused beyond frame spacing: 67 to 126 ms.
- **The continued insert missed its place.** `xc`, inserted before `p2` with `continuity: "previous"` 5.1 s before `p1` ended, waited behind `p2`'s build in flight, and its continued build then took 5.45 s, against 2.1 to 2.2 s for each independent build. It was Ready 2.5 s after `p1` ended, so, as a missed insert does, it aired at the next boundary, after `p2`: behind a clip it did not continue from. No continued join was measured. Every seam's largest picture change was 15 to 27 times the ending clip's own motion.
- **The edit batch landed before its boundary.** Sent 3.16 s before `p3` ended, it took effect 2.12 s later, when `y` was Ready, 1.04 s before the boundary. `w1` was dropped as withdrawn and never started; `y` started 32 ms after `p3` ended.
- **The cut stopped the cut-lane clip too.** The scheduler sent `stop` at 25.452 s, and again at 25.472 s, as soon as the first one's reply came and before H3 reported `long` stopped at 25.491 s. H3's `stop` names no clip and, under autoplay, skips to the next one, as documented, so the second stop cut `cutter` 5 ms after it started, and nothing played after it. Fixed with this record: the engine and the scheduler stop a clip at most once, the twin answers a stop before its effects land, and the check now fails a cut-lane clip that is itself stopped or a cut that sends more than one stop.
- **Position zero.** The schema says position zero is next and the running build is unaffected. The queue read listed it ahead of the running build, and that build took about 4.1 s from submission to `clip_generated`, against 2.19 s for a lone build, as if position zero had gone first.
- **A popped running build.** The schema says it finishes and its result is discarded. The clip queued behind it was Ready 2.09 s after its submission, against a lone build's 2.19 s, so here it did not wait for the discarded build. That sits oddly with the position-zero timing above.
- **Command order, which the schema does not state.** Three times, a `get_queue` sent 1 ms after an enqueue had gone out listed the new clip, and the enqueue's reply came first each time. This deployment applied the two commands in the order they were sent.

## Re-run from main

`scheduler-edits` ran again at commit `aabe52f`, for $0.75, bringing the ledger to $2.25. That commit carries the fixes above: a cut stops a clip once, a continued clip that would miss its place continues from the clip that airs before it, and the session holds the last frame. It ran with published 0.7.0's native library, whose sources are unchanged. `xc` now goes after `p2` and continues from it. It passed:

- **The continued join.** At `p2` to `xc`, the largest picture change was 25.1, or 5.8 times the ending clip's own motion. At the four independent seams it was 48 to 52, or 13 to 18 times. On either side of the continued seam the frames show the same table, glasses and plate, shifted by about 7% of the frame's width. Each independent seam cuts to a new composition. So continuity carried the scene across, but not the exact picture.
- **No black frames.** With the last frame held, all 696 frames were lit, and no seam paused longer than 60 to 170 ms.
- **The continued build.** `xc` was Ready 5.2 s after `p2`, whose build it waited behind, against 2.1 to 2.2 s for each independent build. That matches the first run's 5.45 s. With continued builds measured apart, the independent p95 stayed at 0.441 s per requested second.
- **The edit batch.** It took effect 0.92 s before its boundary, and `w1` never started.
