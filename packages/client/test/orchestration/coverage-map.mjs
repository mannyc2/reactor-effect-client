/** Each baseline test's complete assertion list inherits this explicit coverage entry. */
const test = (file, match) => ({ path: `test/orchestration/${file}.test.ts`, match });
const provider = (match) => ({ path: "test/h3/Provider.test.ts", match });
const references = (match) => ({ path: "test/h3/ProviderReferences.test.ts", match });
const entry = (checks, contract = "Preserved with canonical types and explicit ownership.") => ({
  checks,
  contract,
});

export default {
  Adapter: {
    60: entry(
      [
        test("Media", "reconnect reports clips missing"),
        test("H3Source", "explicit source reconnect calls"),
      ],
      "Retained queued identity, one explicit reconnect, missing clips reported Failed, no synthetic Started/Ended, and both full provider reads remain checked.",
    ),
    85: entry(
      [
        test("H3Source", "pop ACK without a named model reply"),
        provider("pop, settings and reset expose actual named replies"),
      ],
      "Replacement: the old fixture returned a valid clip_popped body while retaining its private queue. Named provider replies are authoritative command evidence; ACK alone remains unknown and cannot fabricate removal. Queue observations retain their own freshness contract.",
    ),
    96: entry(
      [test("Renewal", "queued removal and committed work")],
      "Concurrent removal cannot deadlock committed outcome accounting or recovery. It either succeeds before recovery or reports not-submitted/session_recovering and permits an explicit retry afterward. Real H3 removal says generation; it cannot infer the old in_flight build label.",
    ),
    121: entry(
      [
        test("H3Source", "source preparation snapshots metadata"),
        provider("validates image bytes once"),
        test("H3Source", "orchestration establishes autoplay"),
        test("H3Source", "H3 source has no implicit"),
      ],
      "Ordered duplicates, four upload fields, UUID identity, requested seconds, accepted 175/24 length and opaque metadata remain checked. Replacement: autoplay belongs to orchestration; flush is explicit; real H3 queued records have building=None instead of an invented queue-head Building event.",
    ),
    153: entry(
      [
        provider("rejects invalid inputs, oversized metadata, audio and FastH3 fields"),
        test("RequestQueries", "malformed request objects"),
        test("H3Source", "source preparation snapshots metadata"),
      ],
      "Replacement: the supported H3 0.5.5 subset rejects startingFrame/endingFrame before IO instead of uploading unsupported FastH3 inputs. Ordered reference-image deduplication remains supported and checked independently.",
    ),
    166: entry(
      [
        provider("rejects invalid inputs, oversized metadata, audio and FastH3 fields"),
        provider("late metadata evidence can prove acceptance"),
      ],
      "Replacement: unsupported boundary fields never dispatch. Broadcast/metadata acceptance of supported prompt/image input remains covered.",
    ),
    179: entry(
      [
        provider("rejects invalid inputs, oversized metadata, audio and FastH3 fields"),
        test("RequestQueries", "malformed request objects"),
      ],
      "Both parameter values startingFrame and endingFrame are now rejected locally. The old wrong-acceptance/session-retirement scenario cannot arise for an input that is never submitted; zero IO and not-submitted replace those assertions.",
    ),
    192: entry(
      [
        provider("rejects invalid inputs, oversized metadata, audio and FastH3 fields"),
        provider("allows prompt-only input"),
        test("RequestQueries", "malformed request objects"),
        test("H3Source", "deployment duration limits"),
      ],
      "Malformed image bytes, reference counts, metadata and durations remain bounded before dispatch. Replacement: empty references are valid prompt-only input; boundary fields are unsupported rather than image-validated.",
    ),
    214: entry(
      [
        test("Routing", "expired continuation"),
        test("Routing", "missing continuation"),
        test("H3Source", "foreign generation and failure remain visible"),
        provider("forwards valid continuation UUIDs"),
        test("RequestQueries", "malformed request objects"),
      ],
      "Retention and joint ownership belong to orchestration; the portable provider forwards valid UUIDs. The bounded eight-clip hint excludes failures, and missing/expired continuation targets fail locally. Unsupported boundary inputs fail independently.",
    ),
    247: entry(
      [
        test("H3Source", "early observed start and finish survive"),
        provider("early starts and finishes cannot be reversed"),
      ],
      "Observed early Started/Ended survive late acceptance exactly once. Replacement: no fabricated Ready or Building when those lifecycle facts were never reported.",
    ),
    275: entry([
      test("H3Source", "duplicate lifecycle messages emit once"),
      provider("applies each returned model envelope"),
    ]),
    288: entry(
      [
        test("H3Source", "early observed start and finish survive"),
        test("H3Source", "duplicate lifecycle messages emit once"),
        provider("early starts and finishes cannot be reversed"),
      ],
      "Late queued/generated delivery cannot reverse Started/Ended. Replacement: observed start does not synthesize an unreported Ready/Building event or a build-start timestamp.",
    ),
    311: entry([
      provider("an ACK followed by a body with the same request id"),
      provider("late metadata evidence can prove acceptance"),
    ]),
    332: entry(
      [
        test("H3Source", "orchestration establishes autoplay"),
        test("H3Source", "H3 source has no implicit"),
        test("H3Source", "duplicate lifecycle messages emit once"),
      ],
      "Autoplay is established by the explicit orchestration handle before its first admission. H3 provider/source construction does not enable it, and playback requires provider observations.",
    ),
    350: entry(
      [
        test("Simulation", "simulation autoplay consumes clips in order"),
        test("H3Source", "duplicate lifecycle messages emit once"),
        test("H3Source", "foreign generation and failure remain visible"),
        test("H3Source", "orchestration establishes autoplay"),
      ],
      "FIFO, capacity, natural handoff and no implicit play remain checked. Replacement: real H3 building=None; local admission-to-ready timing is Bounded, foreign timing Unknown. Simulation alone exposes actual build starts and Measured timing.",
    ),
    386: entry(
      [test("H3Source", "source stop is faithful")],
      "Observed stop emits Ended(stopped) once and never replays. Replacement: an explicit stop does not mean starvation and does not alter autoplay.",
    ),
    400: entry(
      [
        test("H3Source", "explicit autoplay setup"),
        test("Renewal", "failed and interrupted acquisition"),
      ],
      "All explicit outcome/ACK/timeout setup refusals close the attached lifecycle owner. Failure is canonical AcquisitionFailure/CommandFailure, not a legacy SessionFailed wrapper.",
    ),
    421: entry(
      [
        test("H3Source", "snapshot-only playback changes"),
        test("H3Source", "deployment duration limits"),
      ],
      "Snapshots still expose foreign/known playback and live duration limits. Replacement: start time stays None and no synthetic Started/Ended/Starved is emitted for a missed lifecycle message.",
    ),
    440: entry([
      test("H3Source", "only an observed natural finish"),
      test("H3Source", "orchestration establishes autoplay"),
    ]),
    451: entry([
      test("Renewal", "generation capacity is enforced"),
      test("Routing", "generation order preserves"),
    ]),
    464: entry(
      [
        test("H3Source", "foreign generation and failure remain visible"),
        test("Renewal", "unknown commit outcome remains indeterminate"),
        provider("uses explicit command outcomes"),
      ],
      "Failure reason, failed-clip identity, unknown dispatch, no replay and no invented build remain checked. Dispatch outcome comes from canonical context, not an error-code heuristic.",
    ),
    488: entry(
      [
        provider("uses explicit command outcomes"),
        provider("correlated rejection is replied"),
        provider("late metadata evidence can prove acceptance"),
      ],
      "Canonical replied/not-submitted/unknown replace Rejected/Uncertain wrappers. Correlation and affirmative no-submit evidence remain the basis of each distinction.",
    ),
    514: entry([
      provider("uses explicit command outcomes"),
      test("Renewal", "unknown commit outcome remains indeterminate"),
    ]),
    528: entry([
      test("RequestQueries", "capture detaches and freezes"),
      test("RequestQueries", "malformed request objects"),
      test("Renewal", "logical preparation is inert"),
    ]),
    560: entry(
      [
        test("Media", "terminal media failure is published once"),
        test("Media", "a frame-only terminal failure"),
        test("H3Source", "owned cleanup"),
        test("H3Source", "attached cleanup preserves"),
      ],
      "One terminal publication, stable cause, blocked later dispatch and joined cleanup remain checked. Replacement: only explicitly opted-in owned sources reset; attached sessions preserve canonical cleanup without reset or DELETE.",
    ),
    585: entry(
      [test("H3Source", "owned cleanup")],
      "Opted-in owned reset remains bounded and records its unknown outcome without preventing canonical close. Default/attached cleanup does not reset.",
    ),
    596: entry([
      provider("a malformed correlated acceptance stays unknown"),
      test("Renewal", "unknown commit outcome remains indeterminate"),
    ]),
    607: entry([
      test("H3Source", "source observation fails for malformed"),
      provider("malformed known payloads and missing clip duration"),
    ]),
    616: entry(
      [provider("rejects ${name} before any command")],
      "Replacement strengthens the old missing-name check to structural deployment OpenAPI validation: missing schemas, wrong refs/types/enums/required fields fail before commands.",
    ),
    629: entry(
      [
        test("H3Source", "real H3 removal reports only observed"),
        test("H3Source", "pop ACK without a named model reply"),
        test("Simulation", "actual simulation build ownership"),
        test("Simulation", "simulation renderer hooks receive"),
      ],
      "Real H3 reports generation/ready, never inferred unstarted/in_flight or GPU ownership. Simulation tests actual in_flight removal, discarded completion, remaining queue order and exactly-once renderer release. Unknown IDs stay local not-found; absent command evidence stays unknown.",
    ),
    669: entry([
      test("Renewal", "foreign startup playback and incomplete snapshots"),
      test("Renewal", "pauseAndStop applies explicit policy"),
      test("H3Source", "named mutation acknowledgement"),
    ]),
    689: entry(
      [
        provider("named mutation ACKs do not fabricate payloads"),
        test("H3Source", "named mutation acknowledgement"),
      ],
      "Replacement: a changed full snapshot cannot fabricate the missing named command payload. The mutation outcome remains unknown even when refreshed canvas/autoplay state is independently known; dispatch is never repeated.",
    ),
    730: entry(
      [
        provider("move sends only clip_id and position"),
        provider("pop, settings and reset expose actual named replies"),
        test("H3Source", "pop ACK without a named model reply"),
      ],
      "Replacement: pop/move expose their actual named replies, and authoritative queue projection remains separate. Missing reply is unknown; no fabricated payload or implicit resubmission from a later queue snapshot.",
    ),
    755: entry([
      provider("correlated rejection is replied"),
      provider("named mutation ACKs do not fabricate payloads"),
    ]),
    776: entry(
      [
        provider("interrupting acquisition joins its local reader"),
        provider("cancelling upload prework"),
        test("H3Source", "cancelled source reference prework"),
        test("Renewal", "failed and interrupted acquisition"),
        test("Renewal", "caller cancellation after commit"),
      ],
      "Acquisition and provisional host/upload interruption join resources. Replacement: committed work remains lifecycle-owned through outcome accounting rather than being cancelled with a caller.",
    ),
    808: entry([
      provider("caller cancellation after commit does not cancel"),
      test("Renewal", "caller cancellation after commit keeps"),
    ]),
    838: entry(
      [
        provider("a known acceptance stays known"),
        test("Renewal", "unknown commit outcome remains indeterminate"),
      ],
      "Missing evidence remains unknown through retirement; an already observed acceptance stays known even if the Session ends during reconciliation.",
    ),
    846: entry([
      provider("a malformed correlated acceptance stays unknown"),
      provider("malformed known payloads and missing clip duration"),
      test("Renewal", "unknown commit outcome remains indeterminate"),
    ]),
    852: entry([
      test("H3Source", "one stalled source observer"),
      provider("one slow observer fails with Overflow"),
    ]),
    877: entry([
      provider("foreign clip retention fails visibly"),
      test("H3Source", "orchestration timing retention fails"),
    ]),
  },
  Renewing: {
    34: entry([
      test("Renewal", "failed and interrupted acquisition"),
      test("Renewal", "close joins pending commit accounting"),
    ]),
    56: entry([
      test("Media", "sequence renewal waits for every old video frame"),
      test("Media", "planned handoff retains queued old video"),
    ]),
    108: entry([test("Renewal", "logical preparation is inert")]),
    146: entry(
      [test("Renewal", "concurrent sequence members share their owner")],
      "Both committed members bind one owner before dispatch; final acceptance cannot seal until earlier pending work settles. Physical providers may serialize admission internally, but logical submission order does not depend on that implementation.",
    ),
    179: entry([
      test("Renewal", "cancelling prework releases provisional"),
      test("H3Source", "cancelled source reference prework"),
    ]),
    208: entry([
      test("Renewal", "caller cancellation after commit keeps"),
      test("Renewal", "close joins pending commit accounting"),
    ]),
    234: entry([test("Renewal", "a definitive rejected final member")]),
    271: entry(
      [
        test("Media", "loss prevents a clean handoff"),
        test("Media", "unavailable source pressure stays unknown"),
        test("Media", "reconnect retains known drop evidence"),
      ],
      "No clean Switched claim survives video/audio loss or incomplete frame counts. Replacement: wait before expiry, then explicitly Replaced with loss/incomplete/null evidence at the source deadline; audio always remains unverified, rather than treating loss as an immediate terminal failure.",
    ),
    320: entry([
      test("Media", "unrecoverable media reports each lost clip"),
      test("Renewal", "unknown commit outcome remains indeterminate"),
    ]),
    352: entry([
      test("Renewal", "unknown commit outcome remains indeterminate"),
      test("Media", "result survives handle closure"),
    ]),
  },
  Engine: {
    23: entry(
      [
        test("Simulation", "simulation autoplay consumes clips in order"),
        test("Simulation", "actual simulation build ownership"),
      ],
      "FIFO, playout capacity, natural start intervals and one final Starved remain checked. Replacement: a blocked future build remains queued; an actually running build exposes Some(startedAt) unless explicit unknown timing was requested.",
    ),
    58: entry([test("Simulation", "removing a simulated ready clip")]),
    80: entry([test("Simulation", "failed simulated generation is skipped")]),
  },
  References: {
    21: entry([references("standalone URI loader retains typed malformed-URL")]),
    29: entry(
      [
        references("reference preparation succeeds without writable staging"),
        references("upload failure leaves a closed source reader"),
      ],
      "Replacement removes staging itself: preparation must succeed with all writes/temp creation forbidden, preserving source bytes. Host read/upload failures remain typed; no temporary-directory operation exists whose failure should be required.",
    ),
    50: entry(
      [
        references("concurrent submissions single-flight identical reference uploads"),
        references("identical bytes at different URIs"),
      ],
      "Upload deduplication is content-addressed; URI contents are reread, avoiding the old same-path stale-image cache.",
    ),
    73: entry(
      [
        references("interrupted reference IO closes its stream"),
        references("cancelling after file read while upload waits"),
      ],
      "Replacement: cancellation closes the source reader and never stages a file or dispatches late. Source files remain intact; there is no temporary file to remove.",
    ),
  },
};
