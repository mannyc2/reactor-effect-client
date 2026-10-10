| Run      | Check     | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | --------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 4475ab1c | candidate | paid | fail    | 2026-10-09T17:35:38.995Z | $6.300     | $5.460    |

### candidate: fail (paid, run 4475ab1c, 2026-10-09T17:35:38.995Z)

- **Environment:** reactor-effect-client 0.11.0, reactor-effect-native 0.11.0, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit e28d197e
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux development host, native WebRTC
- **Cost:** worst case $6.300, estimated $5.460
- **Allocation:** reactor/h3-reference-to-video-turbo-realtime, capped 90 s, 350 credits/s, billed per second, 10000 credits/$
- **Allocation:** reactor/fast-h3, capped 90 s, 350 credits/s, billed per second, 10000 credits/$
- **Candidate reactor/h3-reference-to-video-turbo-realtime:** session 6b859cb0-4c4b-445a-986c-fe68621e36b4; forecast h3:p1 → h3:i1 → h3:i2 → h3:p2; observed starts h3:firm → h3:p1 → h3:i1 → h3:i2 → h3:p2 → h3:ready → h3:manual; recorded before connect true; Ledger entries after close 0
  - independent end reads: 74.71 s CLOSED
  - h3:group: Aired, 2 played, parts Ended → Ended
  - h3:inserted: Aired, 2 played, parts Ended → Ended
  - firm batch: started 12.87 s, committed 12.87 s, Manual add Ready at start false; Manual build allowed from 53.23 s
  - h3:manual: provider start 69.03 s, first fresh decoded frame 69.04 s, 123 decoded frames during its observed playback; lit and changing
  - h3:firm: provider start 12.87 s, first fresh decoded frame 13.00 s, 356 decoded frames during its observed playback; lit and changing
  - h3:p1: provider start 27.98 s, first fresh decoded frame 27.98 s, 125 decoded frames during its observed playback; lit and changing
  - h3:p2: provider start 43.57 s, first fresh decoded frame 43.58 s, 123 decoded frames during its observed playback; lit and changing
  - h3:i1: provider start 33.23 s, first fresh decoded frame 33.24 s, 123 decoded frames during its observed playback; lit and changing
  - h3:i2: provider start 38.40 s, first fresh decoded frame 38.41 s, 123 decoded frames during its observed playback; lit and changing
  - h3:ready: provider start 63.86 s, first fresh decoded frame 63.96 s, 122 decoded frames during its observed playback; lit and changing
  - readyBy: due 49.74 s, cutoff 56.74 s, Ready 50.95 s, started 63.86 s, missed clip Dropped; filler started 48.74 s, predicted end 63.82 s
- **Candidate reactor/fast-h3:** session f0fbf68c-4206-4b9a-99cd-7d68a546b6b1; forecast fast-h3:p1 → fast-h3:i1 → fast-h3:i2 → fast-h3:p2; observed starts fast-h3:firm → fast-h3:p1 → fast-h3:i1 → fast-h3:i2 → fast-h3:p2 → fast-h3:ready → fast-h3:manual; recorded before connect true; Ledger entries after close unconfirmed
  - independent end reads: none
  - fast-h3:group: Aired, 2 played, parts Ended → Ended
  - fast-h3:inserted: Aired, 2 played, parts Ended → Ended
  - firm batch: started 93.14 s, committed 93.14 s, Manual add Ready at start false; Manual build allowed from 127.64 s
  - fast-h3:manual: provider start 150.39 s, first fresh decoded frame 150.39 s, 105 decoded frames during its observed playback; lit and changing
  - fast-h3:firm: provider start 93.14 s, first fresh decoded frame 93.18 s, 339 decoded frames during its observed playback; lit and changing
  - fast-h3:p1: provider start 108.21 s, first fresh decoded frame 108.21 s, 126 decoded frames during its observed playback; lit and changing
  - fast-h3:p2: provider start 124.17 s, first fresh decoded frame 124.19 s, 122 decoded frames during its observed playback; lit and changing
  - fast-h3:i1: provider start 113.78 s, first fresh decoded frame 113.81 s, 123 decoded frames during its observed playback; lit and changing
  - fast-h3:i2: provider start 119.01 s, first fresh decoded frame 119.02 s, 124 decoded frames during its observed playback; lit and changing
  - fast-h3:ready: provider start 144.69 s, first fresh decoded frame 144.71 s, 127 decoded frames during its observed playback; lit and changing
- **Timeline:** admitted 0.01 s · allocated 0.49 s · closed 74.56 s · h3 candidate passed 74.71 s · allocated 75.09 s · closed 155.14 s · settled 155.28 s
- **Liveness:** the runner's 1 s timer fired at most 0.00 s late; 223 session events kept
- **Termination:** 6b859cb0-4c4b-445a-986c-fe68621e36b4 confirmed; f0fbf68c-4206-4b9a-99cd-7d68a546b6b1 confirmed (trail CLOSED)
- **Criteria:** ✓ h3: a firm batch airs before its Manual add is Ready · ✓ h3: the forecast preserves strict places and inserted groups · ✓ h3: groups air all their places in strict order · ✓ h3: decoded video arrives during each ordered clip · ✓ h3: readyBy keeps a Ready clip waiting past its cutoff for filler · ✓ h3: readyBy drops an overdue clip that was not Ready · ✓ h3: readyBy airs after the cutoff once filler ends · ✓ h3: decoded video arrives during every accepted clip · ✓ h3: live decoded video · ✓ h3: the provider reports the uploaded input · ✓ h3: every command has a known outcome · ✓ h3: the source confirms termination · ✓ h3: an independent coordinator read confirms the end · ✓ h3: the Ledger forgets the confirmed session · ✓ h3: the Ledger recorded its session before connection · ✓ fast-h3: a firm batch airs before its Manual add is Ready · ✓ fast-h3: the forecast preserves strict places and inserted groups · ✓ fast-h3: groups air all their places in strict order · ✓ fast-h3: decoded video arrives during each ordered clip · ✓ fast-h3: readyBy keeps a Ready clip waiting past its cutoff for filler · ✓ fast-h3: readyBy drops an overdue clip that was not Ready · ✓ confirmed termination
  - a step ran past its deadline
