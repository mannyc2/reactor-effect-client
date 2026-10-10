| Run      | Check     | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | --------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 68595077 | candidate | paid | pass    | 2026-10-10T01:28:22.216Z | $6.300     | $5.075    |

### candidate: pass (paid, run 68595077, 2026-10-10T01:28:22.216Z)

- **Environment:** reactor-effect-client 0.11.0, reactor-effect-native 0.11.0, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit 8b5958d0
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux development host, native WebRTC
- **Cost:** worst case $6.300, estimated $5.075
- **Allocation:** reactor/h3-reference-to-video-turbo-realtime, capped 90 s, 350 credits/s, billed per second, 10000 credits/$
- **Allocation:** reactor/fast-h3, capped 90 s, 350 credits/s, billed per second, 10000 credits/$
- **Candidate reactor/h3-reference-to-video-turbo-realtime:** session 97c4d3b2-5e15-447b-bf90-6a5777597fcd; forecast h3:p1 → h3:i1 → h3:i2 → h3:p2; observed starts h3:firm → h3:p1 → h3:i1 → h3:i2 → h3:p2 → h3:ready → h3:manual; recorded before connect true; Ledger entries after close 0
  - independent end reads: 70.00 s CLOSED
  - h3:group: Aired, 2 played, parts Ended → Ended
  - h3:inserted: Aired, 2 played, parts Ended → Ended
  - firm batch: started 12.92 s, committed 12.92 s, Manual add Ready at start false; Manual build allowed from 54.19 s
  - h3:manual: provider start 64.07 s, first fresh decoded frame 64.07 s, 123 decoded frames during its observed playback; lit and changing
  - h3:firm: provider start 12.92 s, first fresh decoded frame 13.06 s, 357 decoded frames during its observed playback; lit and changing
  - h3:p1: provider start 28.03 s, first fresh decoded frame 28.07 s, 125 decoded frames during its observed playback; lit and changing
  - h3:p2: provider start 43.61 s, first fresh decoded frame 43.63 s, 133 decoded frames during its observed playback; lit and changing
  - h3:i1: provider start 33.27 s, first fresh decoded frame 33.29 s, 123 decoded frames during its observed playback; lit and changing
  - h3:i2: provider start 38.45 s, first fresh decoded frame 38.45 s, 113 decoded frames during its observed playback; lit and changing
  - h3:ready: provider start 58.86 s, first fresh decoded frame 58.91 s, 125 decoded frames during its observed playback; lit and changing
  - readyBy: due 49.77 s, cutoff 56.77 s, Ready 50.99 s, started 58.86 s, missed clip Dropped; filler started 48.77 s, predicted end 58.90 s
- **Candidate reactor/fast-h3:** session 19fe87c6-efda-4622-8422-45f9ba1e0732; forecast fast-h3:p1 → fast-h3:i1 → fast-h3:i2 → fast-h3:p2; observed starts fast-h3:firm → fast-h3:p1 → fast-h3:i1 → fast-h3:i2 → fast-h3:p2 → fast-h3:ready → fast-h3:manual; recorded before connect true; Ledger entries after close 0
  - independent end reads: 146.87 s CLOSED
  - fast-h3:group: Aired, 2 played, parts Ended → Ended
  - fast-h3:inserted: Aired, 2 played, parts Ended → Ended
  - firm batch: started 88.31 s, committed 88.31 s, Manual add Ready at start false; Manual build allowed from 123.53 s
  - fast-h3:manual: provider start 140.93 s, first fresh decoded frame 140.97 s, 123 decoded frames during its observed playback; lit and changing
  - fast-h3:firm: provider start 88.31 s, first fresh decoded frame 88.36 s, 339 decoded frames during its observed playback; lit and changing
  - fast-h3:p1: provider start 103.35 s, first fresh decoded frame 103.35 s, 126 decoded frames during its observed playback; lit and changing
  - fast-h3:p2: provider start 119.28 s, first fresh decoded frame 119.31 s, 122 decoded frames during its observed playback; lit and changing
  - fast-h3:i1: provider start 108.91 s, first fresh decoded frame 108.94 s, 123 decoded frames during its observed playback; lit and changing
  - fast-h3:i2: provider start 114.12 s, first fresh decoded frame 114.14 s, 124 decoded frames during its observed playback; lit and changing
  - fast-h3:ready: provider start 135.37 s, first fresh decoded frame 135.39 s, 127 decoded frames during its observed playback; lit and changing
  - readyBy: due 125.80 s, cutoff 132.80 s, Ready 131.35 s, started 135.37 s, missed clip Dropped; filler started 124.80 s, predicted end 134.92 s
- **Timeline:** admitted 0.00 s · allocated 0.74 s · closed 69.73 s · h3 candidate passed 70.00 s · allocated 70.72 s · closed 146.60 s · fast-h3 candidate passed 146.87 s · settled 147.13 s
- **Liveness:** the runner's 1 s timer fired at most 0.00 s late; 224 session events kept
- **Termination:** 97c4d3b2-5e15-447b-bf90-6a5777597fcd confirmed; 19fe87c6-efda-4622-8422-45f9ba1e0732 confirmed (trail CLOSED)
- **Criteria:** ✓ h3: a firm batch airs before its Manual add is Ready · ✓ h3: the forecast preserves strict places and inserted groups · ✓ h3: groups air all their places in strict order · ✓ h3: decoded video arrives during each ordered clip · ✓ h3: readyBy keeps a Ready clip waiting past its cutoff for filler · ✓ h3: readyBy drops an overdue clip that was not Ready · ✓ h3: readyBy airs after the cutoff once filler ends · ✓ h3: decoded video arrives during every accepted clip · ✓ h3: live decoded video · ✓ h3: the provider reports the uploaded input · ✓ h3: every command has a known outcome · ✓ h3: the source confirms termination · ✓ h3: an independent coordinator read confirms the end · ✓ h3: the Ledger forgets the confirmed session · ✓ h3: the Ledger recorded its session before connection · ✓ fast-h3: a firm batch airs before its Manual add is Ready · ✓ fast-h3: the forecast preserves strict places and inserted groups · ✓ fast-h3: groups air all their places in strict order · ✓ fast-h3: decoded video arrives during each ordered clip · ✓ fast-h3: readyBy keeps a Ready clip waiting past its cutoff for filler · ✓ fast-h3: readyBy drops an overdue clip that was not Ready · ✓ fast-h3: readyBy airs after the cutoff once filler ends · ✓ fast-h3: decoded video arrives during every accepted clip · ✓ fast-h3: live decoded video · ✓ fast-h3: the provider reports the uploaded input · ✓ fast-h3: the continuation reaches the provider · ✓ fast-h3: every command has a known outcome · ✓ fast-h3: the source confirms termination · ✓ fast-h3: an independent coordinator read confirms the end · ✓ fast-h3: the Ledger forgets the confirmed session · ✓ fast-h3: the Ledger recorded its session before connection · ✓ confirmed termination
