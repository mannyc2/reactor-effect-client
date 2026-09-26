# Decision for Plan 004: bounded continuous renewal

Planning baseline: remote main `c47e50c784f6889b542cf8ee316067b688ca4737` (0.5.0), September 26, 2026. This is a proposed contract, not implemented behavior. Repository evidence below uses that snapshot, not a moving working tree. Effect finalizer behavior was checked in the installed official `effect@4.0.0-rc.117` source. No implementation, installation, build, test or paid run was performed for this decision.

Recommendation: preserve `make`/`layer` and their lifetime cap and complete cleanup report. Add a deliberately separate `makeContinuous` with an explicit unique-identity precondition and private incarnation fences as specified below. Its engine and retirement mechanism must be shared with existing renewal, not copied. Its cleanup return type must make compacted evidence impossible to mistake for complete history.

**Status: concrete proposed design; API review required before implementation.** The opt-in identity contract has weaker detection of historical adapter misuse than legacy `make`; this must be explicit in docs and review. Do not remove the lifetime identity registry while claiming all of today's runtime checks remain. A stronger reference-bearing API is a concrete deferred alternative if that tradeoff is rejected, not an instruction to ask the user about implementation internals now.

## Bounds

### Current owners and retention

| Owner | Evidence at the baseline | Existing bound or missing bound | Decision |
| --- | --- | --- | --- |
| `slots`, `cleanups`, `seenCleanups` | `renewal.ts:181–185`, `:273–276`, `:537–538` | Successful opens limited to 64 by default, but failed acquisitions can also append cleanup. No independent eviction. | Legacy path unchanged. Continuous path retains only live/retiring slots and bounded retirement records; no process-lifetime strong-reference dedup set. |
| `current`, `replacement`, acquisition locals | `renewal.ts:205–215`, `:484–595` | Main plus one replacement by control flow; not an explicit reservation invariant. | Fixed maximum two source reservations, counting opening, active, recovering and retiring sources. Reserve before `open`; release only after the attempt/retirement finishes. |
| Recovery and acquisition fibers | `renewal.ts:449–457`, `:1082` | `beginRecovery` suppresses parallel recovery per slot; completed fibers are not explicitly retained except in `replacement`. | Keep one recovery per source and one opening. Clear a consumed replacement fiber. Do not fork work after the continuous handle is terminal. |
| Orchestration scope finalizers | `renewal.ts:251`, `:457`, `:623`, `:1082`, `:1142` | Fixed long-lived readers/tick/close plus running fibers. | **Not an established finalizer leak:** rc.117 `Effect.forkIn` removes its scope finalizer when the fiber exits (`effect/src/internal/effect.ts:5555–5562`). Verify this remains true against the implementation version. |
| Source and media scopes | `source-slot.ts:95`, `:123–136`, `:239–269`, `:275–295`; `open-h3.ts:98` | Source scope is closed on retirement; media child scopes on reconnect. Slots retain the closed scope objects today. | Await retirement completion before removing the slot. rc.117 `Scope.fork` removes the parent link when the child closes (`internal/effect.ts:3969–3977`); scope close replaces the finalizer-bearing state (`:3914–3932`). No manual use of private Effect finalizer APIs. |
| Per-slot `submissions`, in-flight gate | `source-slot.ts:74–79`, `:205–232` | At most 4096 registered submissions; completed submissions pruned on registration or completion; cleared after close. | Preserve this bound and committed-outcome guarantees. Keep a separate accounting-completion result: timeout is not successful settlement. |
| Per-slot `accepted` and `started` IDs | `source-slot.ts:78–79`, `:224`, `:248–256` | Grow for the source's entire life. A finite duration alone is not a count bound for arbitrary Source implementations; lifetime may be Infinity. | In continuous mode explicitly bound each set at 4096 distinct IDs per physical source. Reserve accepted-history capacity before dispatch, including in-flight admissions; refuse before dispatch at capacity. Unexpected new Started IDs beyond its bound fail the observation/source explicitly. Never evict dedup IDs and count a repeated Started again. Expose these as continuous-mode limits if a concrete consumer needs different values; initial fixed bound is preferable. |
| H3 source annotation/timing maps | `h3-source.ts:243–248`, `:279–283`, `:424` | Annotations default 2048, allowed up to 16384; times at most twice annotation limit. | Independent adapter bounds remain. A caller selecting H3 limits above the new continuous source-history limit needs an explicit compatible configuration; do not silently weaken either bound. |
| Sequence affinity | `Sequence.ts:178–179`, `:214`, `:238`, `:319–361`; `renewal.ts:1196–1202` | 256 sequences, at most 256 settled plus pending members per sequence. Retire preserves indeterminate history. Release is explicit. | Preserve existing bounds and explicit acknowledgement/release. No automatic history deletion on successful renewal. A caller that never releases sequence history can still reach its documented capacity; continuous renewal does not promise otherwise. |
| Output media queues | `media-buffer.ts:63`, `:68–95` | Private unbounded queues have synchronous admission counters: default 96 video frames and 192000 audio samples. | Preserve independent media bounds and cumulative factual loss counters. Slot deletion must follow aggregation of the retiring owner's loss. |
| Observations and optional renewal reader | `observation.ts:30`, `:45–59`, `:129–147`; `renewal.ts:233` | Default 64 observers, each bounded by count and bytes; onRenewal reader capacity 4096. Overflow is explicit. | Preserve. A callback is not an acknowledged durable cleanup sink; its presence never permits discarding failure evidence. |
| Logical Submission handles retained by callers | `renewal.ts:690–817` | Caller may retain arbitrarily many handles; `activeOwner` and a physical submission can keep a closed Slot alive. | Do not count caller-owned handle quantity as internally bounded. On physical completion cache its immutable Exit/state in the logical handle and detach Slot/physical-operation closures where possible. Repeated submit returns the original outcome. A caller's handle must not keep all earlier slots through an internal registry. |
| Scalars and identity counters | `renewal.ts:207–217`; `source-slot.ts:82–89` | Constant count of counters/references; BigInt submission counter grows logarithmically in bytes. | Keep scalar totals; use bigint/string-encoded counts for lifetime history totals rather than unsafe integer wraparound. This is an object-count/resource-ownership bound, not a constant-bit mathematical guarantee. |

Canonical/custom Source objects and canonical cleanup error payloads have their own data sizes. The proposal bounds retained record counts, live resources and identity sets; it does not assert a fixed RSS byte ceiling for an arbitrary custom Source or arbitrary user error payload. Do not replace canonical reports with truncated error strings to manufacture such a claim.

For the new source-local identity sets, also validate nonempty session/clip identity strings at at most 1024 UTF-16 code units before retaining them. This is a proposed continuous-mode boundary, comfortably above canonical UUID identities; it is not an existing Source constraint. Reject an oversized returned source before registration and close its acquisition; reject oversized accepted/event identities as a source-contract failure without inserting them. Preserve the cleanup report unchanged even when its diagnostic input does not meet an identity-cache bound. The 4096-ID limit is needed because custom/infinite sources have no rate/lifetime-derived upper bound; finite lifetime alone is not its proof.

### Proposed public entry point

Names are proposed API, not existing exports:

```ts
interface ContinuousOptions<R = never> extends Omit<Options<R>, "maxSessions"> {
  // Omitted means no successful-open lifetime cap, only in makeContinuous.
  readonly maxSessions?: number;
  readonly retainedSuccessfulCleanups?: number; // default 64, integer 0..4096
  readonly maxUnresolvedCleanups?: number;      // default 16, integer 2..4096
}

// Engine/Media/sequences reuse the existing public contracts.
// The close result is deliberately different.
interface ContinuousHandleShape extends Omit<HandleShape, "close" | "cleanup"> {
  readonly close: Effect.Effect<CleanupSummary>;
  readonly cleanup: Effect.Effect<Option.Option<CleanupSummary>>;
}
```

`makeContinuous` has the same Effect error/environment requirements as `make`. Do not register the different report behind today's `Handle` service or change `layer` implicitly. Start with the constructor; add a separately named service/layer only if its consumer requires one. Explicit `maxSessions` retains the existing meaning (successful registered source count), safe integer 1..4096. It is never repurposed to mean concurrent sessions. The physical-reservation maximum is fixed at two; this implementation has no reason to admit an arbitrary pool.

Reserve one unresolved-cleanup record before each acquisition, including the initial acquisition. The admission invariant is `retainedUnresolved + outstandingCleanupReservations < maxUnresolvedCleanups` before reserving. Every active/opening/retiring source owns such a reservation until its final record is classified. A complete result releases the reservation; an incomplete result converts it into a retained record. Thus final close has space to report both remaining sources even if each cleanup is incomplete. Admission exhaustion publishes the existing terminal Overflow failure and stops opening; normal close still retires owned sources and returns all retained failures. No automatic retry, acknowledgement or eviction of incomplete records is introduced.

Keep the newest 64 complete records by retirement ordinal (configurable above), evicting only complete older records and incrementing exact omitted counters. Zero retention is allowed and means all complete details are summarized. Incomplete records have no eviction path within this first implementation. Once its failure budget is exhausted, the application reviews the summary and creates a new orchestration deliberately. This avoids a new persistence/acknowledgement service.

## Cleanup predicate

**Source cleanup evidence is necessary but not sufficient for slot eviction.** `SourceSlot.closed` is true for Closing as well as Closed (`renewal-state.ts:46–47`). `source-slot.ts:130` records the source report before committed accounting, affinity retirement and scope closure at lines 132–136. Never prune based on that getter or callback alone.

A slot is releasable only after: source close has returned; committed accounting/join has either completed or recorded its explicit timeout/failure; sequence retirement has finished; source scope closure has finished with its Exit recorded; loss counters have been aggregated; all captured operations are fenced to that retired incarnation; and its cleanup reservation has been converted/released. This is a completion notification, not the start of close. A second closer joins it; it must not treat an in-progress Closing state as completed.

Keep `SourceCleanup.lease` as the unmodified canonical CloseReport and its existing policy results. Add retirement evidence beside it in the new summary; do not manufacture fields on CloseReport. The compactable predicate is the conjunction of:

1. `lease.localClosed === true`, no `localErrors`, no `unresolvedPublications`, no remote error, and every `SourceCleanup.policy` result is Success.
2. Allocation/ownership has one of the following proven forms:
   - `allocation: "none"`: no remote termination obligation. Simulation legitimately includes a session ID with this form (`simulation/_internal/source.ts:545–560`); do not reject it merely for that ID. Reject contradictory evidence that says a remote termination attempt/response occurred while allocation is none.
   - `allocation: "known", ownership: "owned"`: session ID present, `remote.confirmed === true`, and evidence is `absent` or `terminal`. Do not require a successful DELETE status if an independent confirmation proves termination; retain any reported error rather than compacting it.
   - `allocation: "known", ownership: "attached"`: session ID present, local closure proven, and no attempted owned termination. `remote.attempted:false`/`confirmed:false` is expected, not a failed owned cleanup (`session/_internal/cleanup.ts:64–72`). Do not describe it as remotely terminated; count it as a completed local detach.
   - `allocation: "unknown"`, missing ownership for a known allocation, contradictory facts, or any other form: retain detailed incomplete evidence. Do not infer expiry from elapsed time.
3. Local retirement facts explicitly say accounting settled and owned scope closure succeeded; no recorded retirement-policy failure. The current `indeterminate` boolean mixes unknown submission outcomes with join timeout, so it cannot implement this predicate alone. Implementation review found that the original proposed schema had no field preserving unsequenced unknown outcomes. Add nonnegative bigint `retirement.unknownSubmissions`, counting distinct committed unknown submissions once (zero for no-slot attempts). Nonzero rows remain non-compactable. Accounting settled means bookkeeping completed, not that outcomes became known. Retention disposition is incomplete evidence, not a claim that canonical remote cleanup failed; a confirmed lease report remains confirmed. Document that unresolved submission history consumes the same reserved ledger budget and can exhaust it even after confirmed termination. Never fabricate a cleanup error or replay to erase that uncertainty.
4. Sequence retirement has run and no pending affinity mutation remains. Indeterminate sequence history stays in the independently bounded affinity map until explicit acknowledgement and release. Do not delete it with the slot. If retirement itself fails, retain an incomplete cleanup record and the original cause rather than calling it complete.

Evidence: canonical fields are `SessionTypes.ts:108–118`, `coordinator/_internal/client.ts:115–125`; acquisition failures carry their canonical report at `errors.ts:534–572`; `noAcquisition` is valid local completion at `session/_internal/acquire.ts:23–37`. Capture acquisition failure reports even when no SourceSlot exists (`renewal.ts:578–590`). Deduplicate within the acquisition/retirement record by canonical lease reference; repeated close of that record is idempotent. Do not deduplicate unrelated attempts globally just because they share the `noAcquisition` singleton.

Defects in local retirement stay defects. Record factual incomplete status before propagating the original cause when possible; never turn a failed finalizer into a successful cleanup report. A finite local critical section may be masked; committed joins retain their explicit budgets. Arbitrary Source.close and scope finalizers may be uninterruptible and unbounded, so a timeout wrapper cannot establish retirement completion. Keep their reservation/ownership while stalled and prohibit further allocation beyond the two-resource limit; do not declare the Slot releasable or claim bounded whole-handle close. Plan 002 separately bounds scheduler notification, not this cleanup.

## Identity and handles

### Recommended opt-in contract

Today `slots.has(source.id)` rejects reuse of any earlier physical source ID because all earlier slots remain (`renewal.ts:520`). Removing them loses that complete historical check. A bounded ring of IDs, a Bloom filter or a weak reference cannot preserve exact lifetime rejection of arbitrary historical strings. Replacing the map with an unbounded tombstone set merely moves the problem.

Recommend that `makeContinuous` explicitly require each Source adapter to supply globally unique physical session identities for the lifetime of the orchestration, and never reassign a clip ID to a different clip/source during that lifetime. This is a semantic input precondition, not a claim that the SDK can verify arbitrary historical strings with bounded memory. Runtime validation guarantees simultaneously live physical-ID collision detection; historical misuse after eviction may no longer be detected. Do not add a recent-ID cache merely to suggest stronger coverage. Legacy `make` preserves its existing lifetime session-ID check. The separate constructor makes this a deliberate choice, never a changed default. State it prominently beside its `open` option and in migration notes.

Use a private allocator-issued incarnation for safety of already captured work. Generate a monotonic bigint incarnation before each open attempt under the orchestration's existing random namespace. The immutable owner token holds only the physical session ID, orchestration namespace and incarnation; it never owns the Slot. Physical `Session.id` and all public session/clip facts remain unchanged. A live index maps the token to a Slot and a separate at-most-two-entry index maps physical session ID to the live token. Removing the Slot never makes its token live again.

- Route candidates and selected preparations carry the immutable token. At commit, require exact token identity and Active/Ready state, not string equality alone. A captured old preparation cannot bind to a later Slot even if a broken adapter reuses its physical ID. Simultaneously live duplicate physical IDs always refuse.
- Capture an available source token when a source-fenced enqueue starts and revalidate it at commit. Unfenced raw-ID calls first invoked after an adapter violates the uniqueness precondition are outside this guarantee; do not promise otherwise. Existing public raw clip control semantics remain valid for conforming adapters.
- Keep private incarnation owners in `Sequence.makeAffinity` (generic owner and equality support already exist at `Sequence.ts:103–106`, `:180`). Expose public sequence snapshots by projecting the owner token to its physical session ID, retaining existing `Affinity<string>` output shape. No cast of a token to a string. Closed/indeterminate sequence history never rebinds automatically; explicit release retains its existing meaning.
- A logical submission still Unbound intentionally selects the current source when first submitted (`renewal.ts:699–709`). Do not change that into “every prepared handle is permanently bound.” Once preparation selects a physical source, capture its incarnation and revalidate that exact instance at commit. A committed handle preserves its original outcome after retirement; repeated submit never dispatches again.
- Store completed logical handles as a terminal Exit/state independent of the old Slot; clear `activeOwner`/physical-operation references after committed execution finishes, not merely after a caller stops waiting. Do not discard known outcomes to save memory.
- Retired clip ownership that is no longer retained may produce the existing missing-anchor/continuation refusal instead of SessionRetired. It must never dispatch to another conforming source. Explain this diagnostic-detail limit for the opt-in constructor.

API review checkpoint: confirm that this explicit precondition and weaker historical-misuse detection are acceptable for an optional continuous constructor. Implementation can proceed after that review and agreement on the concrete public signatures; no further product choice is needed if this recommendation is accepted. Test both guaranteed behavior (captured-token fencing, active conflicts, conforming unique adapters) and the declared limitation (an evicted historical raw string is not a retained tombstone). Do not turn the limitation into a false positive test asserting full historical rejection.

### Deferred stronger alternative if that tradeoff is rejected

Use public allocator-issued `SourceRef = { orchestrationId, incarnation, sessionId }` and `ClipRef = { source, clipId }` rather than relying on unique raw strings. Add reference-bearing counterparts for source-fenced enqueue, clip moves/removals and affinity anchors. The continuous engine would refuse ambiguous legacy raw-ID controls; legacy `make` would retain them. Engine session/clip facts would expose references without renaming the canonical physical Session. `makeScheduler` would carry them through command queues, indexes and Unknown ownership, and continuous sequence snapshots would expose reference owners. Retired references can then be rejected using the live token index without retaining every historical string.

This stronger alternative requires a separate public-contract PR touching `types.ts`, `renewal.ts`, `routing.ts`, `scheduler.ts`, `scheduler-policy.ts`, public exports, pack consumers and their tests. Do not quietly widen the current retention implementation into it. If review rejects the recommended uniqueness precondition, stop the continuous implementation until these reference signatures are approved; keep the existing finite legacy mode working in the meantime.

## Report schema

Keep today's `CleanupReport = { sessions: SourceCleanup[] }` and codec unchanged. Adding an optional `omitted` field to a report with truncated `sessions` is unsafe: an older Struct decoder can ignore the field and treat the remaining array as complete history.

Propose a separate exported `CleanupSummary` Schema with a new format and **no top-level `sessions` field**:

```ts
{
  format: "reactor-orchestration-cleanup-summary/v1",
  totalRetirements: bigint,
  omittedComplete: {
    noAllocation: bigint,
    ownedTerminated: bigint,
    attachedDetached: bigint
  },
  retained: readonly {
    ordinal: bigint,
    source?: { sessionId: string, incarnation: bigint },
    cleanup?: SourceCleanup, // absent only if close produced no report
    conflictingCleanup?: SourceCleanup,
    retirement: {
      accounting: "settled" | "timed-out" | "not-applicable",
      unknownSubmissions: bigint, // distinct committed unknown outcomes; zero for no-slot attempts
      scope: "closed" | "failed",
      affinity: "retired" | "failed" | "not-applicable",
      // Existing diagnostic error codec; no provider text.
      errors: readonly ReactorError[]
    },
    disposition: "complete" | "incomplete"
  }[],
  exhausted: boolean
}
```

The summary incarnation is factual identity evidence, not a new control API. Use existing bigint JSON codec conventions and nonnegative checks; serialize counts as decimal strings, not lossy JSON numbers. Full in-memory entries preserve the original canonical cleanup object by reference. Rows sort by retirement ordinal, independent of whether they came from success retention or the incomplete ledger. `totalRetirements = retained.length + sum(omittedComplete)` after every completed close. Every incomplete row must be present. An attached detach is never included in owned-termination totals.

No-allocation failed attempts get an ordinal too; an aborted attempt with unknown allocation is an incomplete row, not an omitted success. Acquisition and source-retirement paths must converge on one record per ownership attempt. If the acquired-source close and AcquisitionFailure paths produce conflicting canonical reports, preserve both using `conflictingCleanup`, mark the row incomplete and stop; do not choose the more favorable report. A source close that defects without returning a report leaves `cleanup` absent with explicit failed retirement evidence; it can never satisfy the compactable predicate. Preserve the original defect separately from its diagnostic report. Repeated notifications do not append further copies.

Repeated `close` joins the same completed Exit: successful closure returns the same frozen summary; a defect preserves its original Cause on every close wait. `cleanup` remains None until final retirement bookkeeping finishes, then exposes the factual summary, including incomplete rows if close exited with a defect. Do not turn that summary into a successful close Exit. A still-stalled finalizer leaves close and its reservation pending. Reaching failure-history capacity sets `exhausted:true`; it never hides the cleanup caused by stopping. Do not provide a permissive decoder that silently upgrades old reports to “compacted”; old reports remain explicitly complete legacy reports. If a convenience union reader is needed later, it must discriminate legacy versus summary and return that distinction to callers.

## Compatibility

- `make`, `layer`, `Handle`, `HandleShape`, `CleanupReport` and existing `maxSessions` behavior stay unchanged. The existing `Media.test.ts:285` lifetime-cap test must still pass.
- The new factory and summary codec are additive exports. New continuous callers deliberately change cleanup handling and accept the documented unique-identity precondition. Review those signatures and the weaker historical-misuse detection before publishing; do not claim legacy runtime identity validation in this mode.
- No new generic persistence, log-acknowledgement or retry system. No automatic sequence release, remote retry or inferred remote termination.
- Healthy finite sources with source-local histories below their bounds can renew past 64 acquisitions. An infinite-lifetime/custom source or unreleased sequence history can still hit its independent, explicit resource bound. State these limitations in the continuous API documentation.

## Implementation files

This appendix is the only authorized change from this design pass. Expected later implementation files, after review of the recommended opt-in identity contract:

- `packages/client/src/orchestration/renewal.ts`, `source-slot.ts`, `types.ts`, `routing.ts`, `index.ts`: common mechanism, retirement owner, bounded records, private incarnation routing and the new public constructor/summary.
- `packages/client/src/orchestration/scheduler.ts` and `scheduler-policy.ts` stay out of implementation scope for the recommended design; only the deferred stronger public-reference alternative requires them. Existing scheduler integration is tested with the continuous Engine.
- New `packages/client/src/orchestration/retention.ts`: the concrete reservation/classification owner in parallel packet 004-R, independent of live Slot/Scope objects; no generic cache framework. Its dedicated `Retention.test.ts` fixtures are separate from 004-L's integration suite.
- `packages/client/src/Sequence.ts`: likely no algorithm change because owners are generic. Include only if actual reference integration exposes a missing contract; do not alter explicit history acknowledgement/release.
- Existing `packages/client/test/orchestration/{Renewal,RenewalTrace,LifetimeBoundary,Expiry,Media,Routing,SchedulerRenewal,SchedulerFates}.test.ts`; `SourceFixture.ts` only for the required cleanup/reference barriers. Add `Retention.test.ts` for the concrete bounded record owner and `RenewalRetention.test.ts` for accelerated lifecycle cases, plus `packages/client/test/Evidence.test.ts` for codecs.
- `scripts/pack/node-consumer.mts`, `scripts/pack/browser-consumer.mts` as needed for new public exports; package/root READMEs, orchestration test README, changelog, Plan 004 and index.

List final exact constructor/report/private-owner files and types at API review before implementation. Native/browser transport, H3 protocol and canonical Session allocation/cleanup internals remain outside scope. Read the repository file-structure guide if restored before creating a source/test file. Use Bun, no `any`, the patched Effect compiler, bounded scoped fibers, typed operational failures, preserved defects, and no swallowed finalizer failures.

## Regression matrix

| Case | Required result |
| --- | --- |
| More than 64 accelerated healthy renewals, small retained-success limit | Continuous handle stays active; live/opening/retiring reservations never exceed two; retained successes remain at the limit; summary accounting exactly matches retired attempts. |
| Legacy default and explicit maxSessions | Old default 64/cap semantics and complete report remain; explicit maxSessions: 1 still fails the next open. |
| Clean no-allocation, owned termination, attached detach | Each compacts into its own truthful omitted counter; attach is never claimed remotely terminated. |
| Unknown allocation, unconfirmed owned termination, remote/local/policy errors, unresolved publication | Detailed canonical report remains; no success counter increments. |
| Source report arrives while committed hook/scope finalizer is blocked | Slot and reservations remain until actual retirement completion; join timeout is retained explicitly. |
| Failure ledger near full with current and replacement | Pre-open reservation refusal occurs before allocating; closing both still fits and records their cleanup; no overflow record is discarded. |
| Reconnect/open cycles and parent scope | Completed fiber finalizers and child scope links detach; no retained history of scopes/Slot/media objects. Use scoped lifecycle evidence and, for runtime-only bookkeeping, public Effect scope inspection where supported; never add production debug APIs. |
| Reused physical source ID after history eviction | Private incarnation differs; already selected preparation cannot dispatch to it, and live duplicate physical owners refuse. Document that a new raw-ID command after historical eviction cannot receive a guarantee against an adapter violating the uniqueness precondition. |
| Unbound versus selected versus committed submission | Unbound may choose current at first submit; selected work revalidates exact incarnation; committed/repeated submit returns original result, never fresh dispatch. |
| Caller retains completed submission handle | Result remains readable after eviction; handle does not retain the whole retirement registry or live media owner. |
| Open/sealed/indeterminate/retired sequences | No automatic loss of history; pending settlement and retirement remain correct; explicit acknowledge/release recovers entry capacity. |
| Per-source accepted/Started accounting at boundary | Pre-dispatch accepted capacity includes reservations; repeated Started does not double-count; unseen ID beyond the limit fails explicitly. No ID eviction permits misattribution. |
| Codec and repeated close | Old CleanupReport round-trips unchanged; old codec rejects a summary lacking sessions; summary round-trips failure types and exact bigint counts; repeated successful close yields the same report and repeated defective close preserves its original Cause. |

Use existing `runClock`, `SourceFixture.lifecycle` and gates for deterministic timing/lifetime races. Validate final source changes with focused tests on Node and Bun, then the repository portable and package gates; this design pass ran none. Follow [the parallel schedule](PARALLEL-EXECUTION.md): 004-R starts now; 004-L waits for 003-E, not the full harness. The separate public-renewal qualification plan must be run against the eventual combined implementation revision before making new hosted claims.
