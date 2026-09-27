| Run      | Check             | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | ----------------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 91a5a6b2 | scheduler         | paid | fail    | 2026-09-27T19:58:10.276Z | $0.750     | $0.750    |
| 4894742d | scheduler-renewal | paid | pass    | 2026-09-27T19:59:55.611Z | $1.500     | $1.500    |

### scheduler: fail (paid, run 91a5a6b2, 2026-09-27T19:58:10.276Z)

- **Environment:** reactor-effect-client 0.6.0, reactor-effect-native 0.6.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** linux workstation, outbound UDP verified by STUN
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.24 s · minted 0.45 s · allocated 0.98 s · clips submitted 4.10 s · autoplay on 9.84 s · boundary 2: move 2500 ms before the end 17.78 s · boundary 3: move 250 ms before the end 25.26 s · boundary 4: pop 1000 ms before the end 29.70 s · boundary 5: pop 250 ms before the end 35.61 s · scheduler observed 37.31 s · closed 37.96 s · trail 38.17 s
- **Connect phases:** described +0.51 s, prepared +0.22 s, registered +0.24 s, offered +0.33 s, answered +0.65 s, ready +0.60 s
- **Builds:** 11 of 11 Ready, 5.167 s each; 2.12 s median between consecutive Ready clips
- **Position zero:** not queued behind the running build
- **Build popped in flight:** was the generation head; never generated or started in the 33.54 s after
- **Boundary 1:** no edit; pause 0.07 s (0 frames, 0 dark); last new frame 1.17 s after clip_finished; clip_started 0.04 s after it, first new frame 1.21 s after that
- **Boundary 2:** move aimed 2.50 s before the end, reply 2.43 s before clip_finished, next clip as intended; pause 0.13 s (0 frames, 0 dark); last new frame 0.11 s after clip_finished; clip_started 0.11 s after it, first new frame 0.13 s after that
- **Boundary 3:** move aimed 0.25 s before the end, reply 0.19 s before clip_finished, next clip as intended; pause 0.05 s (0 frames, 0 dark); last new frame -0.33 s after clip_finished; clip_started 0.03 s after it, first new frame -0.32 s after that
- **Boundary 4:** pop aimed 1.00 s before the end, reply 0.91 s before clip_finished, next clip as intended; pause 0.06 s (0 frames, 0 dark); last new frame -0.88 s after clip_finished; clip_started 0.04 s after it, first new frame -0.86 s after that
- **Boundary 5:** pop aimed 0.25 s before the end, reply 0.16 s before clip_finished, next clip as intended; pause 0.09 s (0 frames, 0 dark); last new frame 0.08 s after clip_finished; clip_started 0.03 s after it, first new frame 0.13 s after that
- **Clip metadata:** clip_queued 12, clip_popped 3, clip_generated 11, clip_started 6, clip_finished 5, clip_moved 2; mismatched none
- **Termination:** confirmed; coordinator terminal +0.86 s after the request; trail CLOSED@+0.86 s
- **Criteria:** ✗ position zero while building · ✓ move 2500 ms before boundary 2 · ✓ pop 1000 ms before boundary 4 · ✓ popped clips never start · ✓ pop the build in flight · ✓ observed clip metadata · ✓ confirmed termination
  - position zero while building: the building clip and position-zero request were not observed in that order

### scheduler-renewal: pass (paid, run 4894742d, 2026-09-27T19:59:55.611Z)

- **Environment:** reactor-effect-client 0.6.0, reactor-effect-native 0.6.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** linux workstation, outbound UDP verified by STUN
- **Cost:** worst case $1.500, estimated $1.500 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.34 s · minted 0.55 s
- **Connect phases:** described +0.29 s, prepared +0.21 s, registered +0.26 s, offered +0.59 s, answered +0.51 s, ready +0.89 s
- **Public scheduler renewal:** continuous constructor; 2 open attempts; monotonic clock; 1 filler requests and 0 filler observations
- **Keyed playback:** qualification-A on 80e81c40-0ff2-4638-9c62-633d577ddc60: Accepted@4.04 s → Building@4.09 s → Ready@6.21 s → Started@6.21 s → Ended@11.34 s; qualification-B on aa852c69-e52d-4827-89b9-73cd0b702f24: Accepted@17.26 s → Building@17.29 s → Ready@19.40 s → Started@19.57 s → Ended@24.70 s
- **Planned switch:** 80e81c40-0ff2-4638-9c62-633d577ddc60 → aa852c69-e52d-4827-89b9-73cd0b702f24: count-complete; 124/124 local final-clip frames; Ended grace 8122/250 ms
- **Logical decoded media:** 247 video frames; attribution complete; boundary gap 8282 ms; audio completeness unverified; no encoded or viewer-output claim
- **Accepted drain:** completed; allocated sources 2 → 2
- **Renewal cleanup:** Continuous; 2 canonical source reports; complete
- **Continuous retention:** keep 1 successes; unresolved limit 2
- **Cleanup summary:** 2 retirements; 1 retained (0 incomplete); 1 omitted complete owned terminations; exhausted false
- **Source 1:** 80e81c40-0ff2-4638-9c62-633d577ddc60; canonical owned termination confirmed; cap expiry 2026-09-27T20:00:46.373Z
- **Source 2:** aa852c69-e52d-4827-89b9-73cd0b702f24; canonical owned termination confirmed; cap expiry 2026-09-27T20:00:59.733Z
- **Criteria:** ✓ public renewal preparation · ✓ keyed playback order · ✓ planned switch · ✓ attributed logical media · ✓ accepted drain · ✓ owned lease cleanup · ✓ bounded allocation
