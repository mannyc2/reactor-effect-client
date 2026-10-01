| Run | Check | Mode | Verdict | Started | Worst case | Estimated |
| --- | ----- | ---- | ------- | ------- | ---------- | --------- |
| 8d283761 | showreel | paid | pass | 2026-10-01T02:01:20.733Z | $2.450 | $1.680 |

### showreel: pass (paid, run 8d283761, 2026-10-01T02:01:20.733Z)

- **Environment:** reactor-effect-client 0.9.0, reactor-effect-native 0.9.0, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit d013d009
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux workstation, no VPN, outbound UDP open
- **Cost:** worst case $2.450, estimated $1.680 at 350 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · opened 0.01 s · scenes submitted 0.02 s · minted 0.23 s · allocated 0.68 s · recording 7.07 s · reel ended 46.96 s · showreel observed 47.19 s · closed 47.84 s · settled 48.09 s
- **Order:** started salt-flat, chrome, canyon, coast, horizon
- **Seam salt-flat to chrome:** pause 89 ms (0 frames, 0 dark); 0 dark frames; join change 66.55 against 1.28 (×51.87)
- **Seam chrome to canyon:** pause 101 ms (0 frames, 0 dark); 0 dark frames; join change 58.75 against 2.48 (×23.71)
- **Seam canyon to coast:** pause 120 ms (0 frames, 0 dark); 0 dark frames; join change 39.12 against 4.95 (×7.9)
- **Seam coast to horizon:** pause 94 ms (0 frames, 0 dark); 0 dark frames; join change 40.81 against 3.98 (×10.26)
- **Scenes:** salt-flat 8 s, chrome 8 s, canyon 8 s, coast 8 s, horizon 8 s; gaps on air 2 ms, 67 ms, 63 ms, 57 ms
- **Readers:** never fell behind
- **Reel:** 958 frames of 1344x768 at 24 fps (39.92 s), 25 repeated, 24 superseded, 0 of another size dropped; sound 3988 blocks at 48000 Hz, 0 ms of silence added; ffmpeg exited 0
- **Files beside the evidence:** reel.mp4 19639.7 KiB, poster.png 442.3 KiB, loop.gif 10102.9 KiB; poster at 19.945 s; loop from 11.945 s for 8 s
- **Termination:** 1f7d4d96-ab51-43b2-8b72-52176276017b confirmed (trail CLOSED)
- **Criteria:** ✓ every scene accepted, built, started and ended · ✓ every seam measured · ✓ no gap on air between scenes · ✓ the reel's readers keep up · ✓ confirmed termination · ✓ the reel was recorded · ✓ the poster and the loop were made after the session closed


**Footage:** the reel, a loop and a poster are attached to the [`media` release](https://github.com/mannyc2/reactor-effect-client/releases/tag/media); the files beside the evidence stay out of Git.
