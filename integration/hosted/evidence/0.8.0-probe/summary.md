| Run      | Check       | Mode | Verdict | Started                  | Worst case | Estimated |
| -------- | ----------- | ---- | ------- | ------------------------ | ---------- | --------- |
| 68039bcf | unconnected | paid | pass    | 2026-09-29T14:00:44.352Z | $3.375     | $0.900    |

### unconnected: pass (paid, run 68039bcf, 2026-09-29T14:00:44.352Z)

- **Environment:** reactor-effect-client 0.7.1, reactor-effect-native 0.7.1, effect 4.0.0-rc.117, @effect/platform-node 4.0.0-rc.117, bun 1.4.2, linux x64, commit c9f3f960
- **Native addon:** linux-x64-gnu sha256 4fdccf92, source 1da0d5fb, webrtc-7907-a5ddff60-p9
- **Network:** maintainer's Linux workstation, network not described
- **Cost:** worst case $3.375, estimated $0.900 at 125 credits/s, billed per second, and 10000 credits/$
- **Timeline:** admitted 0.00 s · minted 0.09 s · minted 0.27 s · create sent 0.27 s · allocated 0.67 s · spent token's first create sent 0.68 s · read ACTIVE 0.86 s · allocated 1.01 s · spent token's second create sent 1.01 s · spent token's second create answered 1.21 s · held 1.39 s · closed 11.78 s · read CLOSED 60.76 s · closed 61.30 s · unconnected observed 61.30 s · settled 61.58 s
- **Unconnected:** 9ccd880c-5c99-4493-9ea6-19c0c98326de requested 2026-09-29T14:00:44.632Z; connectable 0.19 s after allocation; ended by Reactor 60.09 s after allocation, 2026-09-29T14:01:45.121Z
- **Window:** reads until 120.00 s after the request, past the cap and 30 s by 29.60 s from allocation, 29.41 s from ACTIVE and 29.41 s from ready
- **Reads:** ACTIVE from 0.19 s after allocation to 58.09 s after allocation (30 reads) > CLOSED from 60.09 s after allocation to 60.09 s after allocation (1 read)
- **Spent token:** its first create allocated 6572f3f1-5b21-4c2b-9de0-d37d3c6e0748; a second create failed in 0.19 s with Http 403, outcome replied; keys error; codes error (text, 36 chars)
- **Held:** 6572f3f1-5b21-4c2b-9de0-d37d3c6e0748 connectable 0.38 s after allocation; ACTIVE from 0.38 s after allocation to 0.38 s after allocation (1 read)
- **Read at the end:** 200 CLOSED; keys session_id, cluster, zone, state, server_info, model, origin_country, region; codes state CLOSED
- **Termination:** 9ccd880c-5c99-4493-9ea6-19c0c98326de confirmed (trail CLOSED); 6572f3f1-5b21-4c2b-9de0-d37d3c6e0748 confirmed (trail CLOSED)
- **Criteria:** ✓ the watch completed · ✓ the spent token's second create was answered · ✓ confirmed termination

**Billing, for the dashboard:** seconds from the watched session's request, 2026-09-29T14:00:44.632Z. Ready is the first read that found the session connectable; the spent token's sessions are read from its second create's answer, and each is held the time given past its ready, or past a 5 s wait it never became ready in. Each end lies between the two times given: for Reactor's, the last read that found the session running and the first that found it ended; for the key's, its DELETE and the read that confirmed it; and before a DELETE that found no session, its allocation and that DELETE. Fill in the last two columns from the Reactor dashboard.

| Session                              | Made by                        | Requested | Allocated | First ACTIVE read | Ready  | Held               | Ended by | Ended         | Allocated to ended | Ready to ended | Dashboard duration | Dashboard charge |
| ------------------------------------ | ------------------------------ | --------- | --------- | ----------------- | ------ | ------------------ | -------- | ------------- | ------------------ | -------------- | ------------------ | ---------------- |
| 9ccd880c-5c99-4493-9ea6-19c0c98326de | the watched token's create     | 0.00 s    | 0.40 s    | 0.59 s            | 0.59 s | –                  | Reactor  | 58.49–60.49 s | 58.09–60.09 s      | 57.90–59.90 s  | 1m 0s              | not shown        |
| 6572f3f1-5b21-4c2b-9de0-d37d3c6e0748 | the spent token's first create | 0.41 s    | 0.74 s    | 1.12 s            | 1.12 s | 10.00 s past ready | the key  | 11.12–11.51 s | 10.38–10.77 s      | 10.00–10.39 s  | 10 s               | not shown        |

## The dashboard

Read on September 29, 2026, at about 14:15 UTC. The Usage page lists the two sessions, both `CLOSED` on cluster `5a004973-…`, with a duration each and no charge:

- the watched session, 1m 0s, against 58.09–60.09 s from allocation and 57.90–59.90 s from ready;
- the spent token's session, 10 s, against 10.38–10.77 s from allocation and 10.00–10.39 s from ready.

So Reactor meters by the second, not by the started minute: a 10 s session reads 10 s. Whether billing runs from creation or from `ready` stays open, since ready came 0.19 s and 0.38 s after allocation, under the page's one-second resolution. The balance read $9.32, against $10.41 after the 0.8.0-api round, whose charges had not then posted; the $1.09 between them fits neither the published per-second rate ($2.25 for the five sessions of this probe and the 0.8.0-rc `show`) nor a started minute each, so what Reactor charged is still open.
