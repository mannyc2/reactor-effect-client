| Run | Check | Mode | Verdict | Started | Worst case | Estimated |
|---|---|---|---|---|---|---|
| 4c1d9482 | audio | paid | pass | 2026-09-25T00:21:41.330Z | $0.750 | $0.750 |

### audio: pass (paid, run 4c1d9482, 2026-09-25T00:21:41.330Z)

- **Environment:** reactor-effect-client 0.3.1, reactor-effect-native 0.3.1, effect 4.0.0-rc.115, @effect/platform-node 4.0.0-rc.115, native webrtc-7907-a5ddff60-p9 ABI 4, bun 1.4.2, linux x64 6.8.0-101-generic
- **Network:** home server, direct UDP, no VPN
- **Server:** cluster 5a004973-4a49-4f0e-96ac-6cd7637f61eb, zone us-east, version 1.20260924.28429, transport webrtc/1.0
- **Cost:** worst case $0.750, estimated $0.750 at 125 credits/s and 10000 credits/$
- **Timeline:** admitted 0.25 s · minted 0.49 s · allocated 0.92 s · connected 4.07 s · accepted 5.75 s · clip started 8.74 s · observed 14.74 s · closed 15.31 s · trail 15.53 s
- **Connect phases:** described +0.42 s, prepared +0.23 s, registered +0.24 s, offered +0.72 s, answered +0.65 s, ready +0.75 s
- **Clip:** accepted 1.49 s after submit (correlated); generated +2.99 s; started +2.99 s; first clip frame +0.11 s after start; ended +8.12 s; metadata echoed by clip_queued 1, clip_generated 1, queue_update 1, clip_started 1, clip_finished 1; sent 1 image(s) and 1 audio; the clip reports 1 image(s), 1 audio (has_reference_audio true)
- **Video:** 124 frames, 1344x768 BGRA, 24.2 fps, interval p50 41 / p95 44 / max 75 ms, 0 lost in 0 gaps, 123 lit, 124 distinct, metadata on 0
- **Audio:** 963 blocks, 48000 Hz, 1 channel(s), 480 samples a block, peak RMS 0.2463, 0 lost
- **Pressure:** delivered 124 video / 979 audio, dropped 0 / 0, reader overflows 0
- **Network path:** local prflx, RTT median 22 ms, received median 4908.6 kbps over 11 samples
- **Contract:** h3-reference-to-video-turbo-realtime v0.0.0 (documented 0.5.5); reference_audios declared; messages state_update 5, autoplay_accepted 1, queue_update 3, clip_queued 1, clip_generated 1, clip_started 1, clip_finished 1; unknown none; 0 duplicate, 0 stale; diagnostics none
- **Termination:** confirmed; coordinator terminal +0.79 s after the request; trail CLOSED@+0.79 s
- **Criteria:** ✓ correlated acceptance · ✓ lifecycle progression · ✓ live video · ✓ audio when offered · ✓ metadata preserved · ✓ reference audio reported · ✓ ICE pair selected · ✓ confirmed termination
