# 0.11.0 candidate attempt

The authorized attempt at `7944b74` stopped at the first H3 session creation with HTTP 402, a known replied refusal. No session was allocated and FastH3 was not attempted. The candidate remains unqualified. The $6.30 reservation stays counted in this ledger; there is no recorded charge estimate or dashboard charge measurement. The allocation rows below describe the planned bounds, not created sessions. A retry requires resolving the refusal and a new authorization.

| Run      | Check     | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | --------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 8de09f45 | candidate | paid | fail    | 2026-10-09T13:03:24.703Z | $6.300     | –         |

### candidate: fail (paid, run 8de09f45, 2026-10-09T13:03:24.703Z)

- **Environment:** reactor-effect-client 0.11.0, reactor-effect-native 0.11.0, effect 4.0.0, @effect/platform-node 4.0.0, bun 1.4.2, linux x64, commit 7944b74e
- **Native addon:** linux-x64-gnu sha256 9390d177, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** Linux development host, native WebRTC
- **Cost:** worst case $6.300, estimated –
- **Allocation:** reactor/h3-reference-to-video-turbo-realtime, capped 90 s, 350 credits/s, billed per second, 10000 credits/$
- **Allocation:** reactor/fast-h3, capped 90 s, 350 credits/s, billed per second, 10000 credits/$
- **Timeline:** admitted 0.00 s · settled 0.29 s
- **Termination:**
- **Criteria:** ✗ confirmed termination
  - Http: create session: HTTP 402 (outcome replied)
  - confirmed termination: no session was recorded
  - incomplete evidence: sessions, candidate
