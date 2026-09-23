//! The `Stats` and `MediaSnapshot` responses.

use super::DecimalU64;
use reactor_webrtc::{
    IceCandidatePairState, IceCandidateType, RelayProtocol, StatsReport, StreamKind,
};
use serde::Serialize;
use serde_json::{Value, json};

/// The `Stats` response: an `RTCStatsReport`-shaped array with an entry per
/// inbound and outbound RTP stream and, per ICE candidate pair, the pair and
/// its local candidate. The host reads the pairs to tell an ICE failure from
/// a transport failure above it.
pub(crate) fn stats_json(report: &StatsReport) -> Value {
    let inbound = report.inbound_rtp.iter().map(|entry| {
        json!({
            "id": format!("inbound-rtp-{}", entry.ssrc),
            "type": "inbound-rtp",
            "ssrc": entry.ssrc,
            "kind": stream_kind(entry.kind),
            "packetsReceived": entry.packets_received,
            "bytesReceived": DecimalU64(entry.bytes_received),
            "jitter": entry.jitter_s,
            "packetsLost": entry.packets_lost,
            "nackCount": entry.nack_count,
            "pliCount": entry.pli_count,
            "firCount": entry.fir_count,
            "totalDecodeTime": entry.total_decode_time_s,
            "framesPerSecond": entry.frames_per_second,
            "framesDecoded": entry.frames_decoded,
            "framesDropped": entry.frames_dropped,
            "frameWidth": entry.frame_width,
            "frameHeight": entry.frame_height,
        })
    });
    let outbound = report.outbound_rtp.iter().map(|entry| {
        json!({
            "id": format!("outbound-rtp-{}", entry.ssrc),
            "type": "outbound-rtp",
            "ssrc": entry.ssrc,
            "kind": stream_kind(entry.kind),
            "packetsSent": DecimalU64(entry.packets_sent),
            "bytesSent": DecimalU64(entry.bytes_sent),
            "targetBitrate": entry.target_bitrate_bps,
            "roundTripTime": entry.round_trip_time_s,
            "totalRoundTripTime": entry.total_round_trip_time_s,
            "fractionLost": entry.fraction_lost,
            "packetsLost": entry.packets_lost,
            "retransmittedPacketsSent": DecimalU64(entry.retransmitted_packets_sent),
            "nackCount": entry.nack_count,
            "pliCount": entry.pli_count,
            "firCount": entry.fir_count,
            "framesPerSecond": entry.frames_per_second,
            "framesSent": entry.frames_sent,
            "frameWidth": entry.frame_width,
            "frameHeight": entry.frame_height,
        })
    });
    let pairs = report
        .candidate_pairs
        .iter()
        .enumerate()
        .flat_map(|(index, pair)| {
            let local_id = format!("local-candidate-{index}");
            [
                json!({
                    "id": local_id,
                    "type": "local-candidate",
                    "candidateType": candidate_type(pair.local_candidate_type),
                    "relayProtocol": relay_protocol(pair.local_relay_protocol),
                }),
                json!({
                    "id": format!("candidate-pair-{index}"),
                    "type": "candidate-pair",
                    "state": pair_state(pair.state),
                    "nominated": pair.nominated,
                    "writable": pair.writable,
                    "priority": DecimalU64(pair.priority),
                    "bytesSent": DecimalU64(pair.bytes_sent),
                    "bytesReceived": DecimalU64(pair.bytes_received),
                    "packetsSent": DecimalU64(pair.packets_sent),
                    "packetsReceived": DecimalU64(pair.packets_received),
                    "currentRoundTripTime": pair.current_round_trip_time_s,
                    "totalRoundTripTime": pair.total_round_trip_time_s,
                    "availableOutgoingBitrate": pair.available_outgoing_bitrate_bps,
                    "availableIncomingBitrate": pair.available_incoming_bitrate_bps,
                    "localCandidateId": local_id,
                }),
            ]
        });
    inbound.chain(outbound).chain(pairs).collect()
}

fn stream_kind(kind: StreamKind) -> &'static str {
    match kind {
        StreamKind::Audio => "audio",
        StreamKind::Video => "video",
        StreamKind::Unknown => "unknown",
    }
}

fn pair_state(state: IceCandidatePairState) -> &'static str {
    match state {
        IceCandidatePairState::Waiting => "waiting",
        IceCandidatePairState::InProgress => "in-progress",
        IceCandidatePairState::Failed => "failed",
        IceCandidatePairState::Succeeded => "succeeded",
        IceCandidatePairState::Cancelled => "cancelled",
    }
}

fn candidate_type(kind: IceCandidateType) -> &'static str {
    match kind {
        IceCandidateType::Host => "host",
        IceCandidateType::Srflx => "srflx",
        IceCandidateType::Prflx => "prflx",
        IceCandidateType::Relay => "relay",
        IceCandidateType::Unknown => "unknown",
    }
}

fn relay_protocol(protocol: RelayProtocol) -> &'static str {
    match protocol {
        RelayProtocol::Udp => "udp",
        RelayProtocol::Tcp => "tcp",
        RelayProtocol::Tls => "tls",
        RelayProtocol::NotRelayed => "",
    }
}

/// The `MediaSnapshot` response: what each queue dropped, delivered and still
/// holds.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MediaSnapshot {
    pub(crate) closed: bool,
    pub(crate) queued_control: usize,
    pub(crate) queued_video: usize,
    pub(crate) queued_audio: usize,
    pub(crate) queued_bytes: usize,
    pub(crate) dropped_video: DecimalU64,
    pub(crate) dropped_audio: DecimalU64,
    pub(crate) delivered_video: DecimalU64,
    pub(crate) delivered_audio: DecimalU64,
    /// Always 0: calls wait on the owner thread, never in a native queue.
    pub(crate) pending_requests: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use reactor_webrtc::{IceCandidatePairStats, InboundRtpStats, OutboundRtpStats};

    const BEYOND_DOUBLES: u64 = (1 << 53) + 1;

    fn report() -> StatsReport {
        StatsReport {
            inbound_rtp: vec![InboundRtpStats {
                ssrc: 11,
                kind: StreamKind::Video,
                packets_received: 5,
                bytes_received: BEYOND_DOUBLES,
                jitter_s: 0.25,
                packets_lost: -1,
                nack_count: 2,
                pli_count: 3,
                fir_count: 4,
                total_decode_time_s: 1.5,
                frames_per_second: 24.0,
                frames_decoded: 90,
                frames_dropped: 1,
                frame_width: 64,
                frame_height: 48,
            }],
            outbound_rtp: vec![OutboundRtpStats {
                ssrc: 22,
                kind: StreamKind::Audio,
                packets_sent: 6,
                bytes_sent: 7,
                target_bitrate_bps: 32_000.0,
                round_trip_time_s: 0.5,
                total_round_trip_time_s: 2.0,
                fraction_lost: 0.125,
                packets_lost: 8,
                retransmitted_packets_sent: 9,
                nack_count: 10,
                pli_count: 0,
                fir_count: 0,
                frames_per_second: 0.0,
                frames_sent: 0,
                frame_width: 0,
                frame_height: 0,
            }],
            candidate_pairs: vec![IceCandidatePairStats {
                current_round_trip_time_s: 0.25,
                total_round_trip_time_s: 0.75,
                priority: u64::MAX,
                state: IceCandidatePairState::Succeeded,
                nominated: true,
                writable: true,
                available_outgoing_bitrate_bps: 1_000_000.0,
                available_incoming_bitrate_bps: 0.0,
                bytes_sent: 12,
                bytes_received: 13,
                packets_sent: 14,
                packets_received: 15,
                local_candidate_type: IceCandidateType::Relay,
                local_relay_protocol: RelayProtocol::Tls,
            }],
        }
    }

    // `connectionFailure` and `statsValue` in packages/native/src/_internal/peer.ts
    // read these entries; every counter in its `statsBigInts` travels as a
    // decimal string.
    #[test]
    fn stats_have_the_entries_and_keys_the_host_reads() {
        assert_eq!(
            stats_json(&report()),
            json!([
                {
                    "id": "inbound-rtp-11",
                    "type": "inbound-rtp",
                    "ssrc": 11,
                    "kind": "video",
                    "packetsReceived": 5,
                    "bytesReceived": "9007199254740993",
                    "jitter": 0.25,
                    "packetsLost": -1,
                    "nackCount": 2,
                    "pliCount": 3,
                    "firCount": 4,
                    "totalDecodeTime": 1.5,
                    "framesPerSecond": 24.0,
                    "framesDecoded": 90,
                    "framesDropped": 1,
                    "frameWidth": 64,
                    "frameHeight": 48,
                },
                {
                    "id": "outbound-rtp-22",
                    "type": "outbound-rtp",
                    "ssrc": 22,
                    "kind": "audio",
                    "packetsSent": "6",
                    "bytesSent": "7",
                    "targetBitrate": 32_000.0,
                    "roundTripTime": 0.5,
                    "totalRoundTripTime": 2.0,
                    "fractionLost": 0.125,
                    "packetsLost": 8,
                    "retransmittedPacketsSent": "9",
                    "nackCount": 10,
                    "pliCount": 0,
                    "firCount": 0,
                    "framesPerSecond": 0.0,
                    "framesSent": 0,
                    "frameWidth": 0,
                    "frameHeight": 0,
                },
                {
                    "id": "local-candidate-0",
                    "type": "local-candidate",
                    "candidateType": "relay",
                    "relayProtocol": "tls",
                },
                {
                    "id": "candidate-pair-0",
                    "type": "candidate-pair",
                    "state": "succeeded",
                    "nominated": true,
                    "writable": true,
                    "priority": "18446744073709551615",
                    "bytesSent": "12",
                    "bytesReceived": "13",
                    "packetsSent": "14",
                    "packetsReceived": "15",
                    "currentRoundTripTime": 0.25,
                    "totalRoundTripTime": 0.75,
                    "availableOutgoingBitrate": 1_000_000.0,
                    "availableIncomingBitrate": 0.0,
                    "localCandidateId": "local-candidate-0",
                },
            ])
        );
    }

    #[test]
    fn an_empty_report_is_an_empty_array() {
        assert_eq!(stats_json(&StatsReport::default()), json!([]));
    }

    #[test]
    fn every_pair_state_candidate_type_and_relay_protocol_has_a_stats_name() {
        let states = [
            IceCandidatePairState::Waiting,
            IceCandidatePairState::InProgress,
            IceCandidatePairState::Failed,
            IceCandidatePairState::Succeeded,
            IceCandidatePairState::Cancelled,
        ]
        .map(pair_state);
        assert_eq!(
            states,
            ["waiting", "in-progress", "failed", "succeeded", "cancelled"]
        );
        let types = [
            IceCandidateType::Host,
            IceCandidateType::Srflx,
            IceCandidateType::Prflx,
            IceCandidateType::Relay,
            IceCandidateType::Unknown,
        ]
        .map(candidate_type);
        assert_eq!(types, ["host", "srflx", "prflx", "relay", "unknown"]);
        let protocols = [
            RelayProtocol::Udp,
            RelayProtocol::Tcp,
            RelayProtocol::Tls,
            RelayProtocol::NotRelayed,
        ]
        .map(relay_protocol);
        assert_eq!(protocols, ["udp", "tcp", "tls", ""]);
    }

    #[test]
    fn a_snapshot_has_the_keys_the_host_parses() {
        let snapshot = MediaSnapshot {
            closed: false,
            queued_control: 1,
            queued_video: 2,
            queued_audio: 3,
            queued_bytes: 4,
            dropped_video: DecimalU64(BEYOND_DOUBLES),
            dropped_audio: DecimalU64(5),
            delivered_video: DecimalU64(6),
            delivered_audio: DecimalU64(7),
            pending_requests: 0,
        };
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap(),
            json!({
                "closed": false,
                "queuedControl": 1,
                "queuedVideo": 2,
                "queuedAudio": 3,
                "queuedBytes": 4,
                "droppedVideo": "9007199254740993",
                "droppedAudio": "5",
                "deliveredVideo": "6",
                "deliveredAudio": "7",
                "pendingRequests": 0,
            })
        );
    }
}
