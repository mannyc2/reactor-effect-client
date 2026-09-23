//! Transport events, which the host takes with `reactor_effect_peer_take_event`.

use super::TrackKind;
use crate::abi::Channel;
use crate::error::{BridgeError, FailureClass};
use crate::protocol;
use reactor_webrtc::{IceCandidate, PeerConnectionState};
use serde::Serialize;

/// A transport event for the host.
///
/// On the wire an event is a packet: `[u32 little-endian header length][UTF-8
/// JSON header][payload]`. The header is this enum, tagged by `type`; only a
/// [`Event::Message`] has a payload.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum Event<'a> {
    /// The aggregate connection state changed.
    State { state: &'static str },
    /// A local ICE candidate, or with none, the end of gathering.
    Ice {
        #[serde(skip_serializing_if = "Option::is_none")]
        candidate: Option<LocalCandidate<'a>>,
    },
    /// A bridge data channel opened or closed.
    Channel { channel: Channel, open: bool },
    /// A binary message arrived on a bridge data channel.
    Message {
        channel: Channel,
        /// The message, which travels as the packet payload.
        #[serde(skip)]
        bytes: &'a [u8],
    },
    /// A remote track arrived for a declared receive mapping.
    Track { name: &'a str, mid: &'a str },
    /// That track's decoded media now reaches its queue.
    Decoded {
        kind: TrackKind,
        name: &'a str,
        mid: &'a str,
    },
    /// The connection failed; `status` is the failure class.
    Error { status: i32, message: &'a str },
}

impl<'a> Event<'a> {
    /// A failure of the connection itself, rather than of a host call.
    pub(crate) fn error(class: FailureClass, message: &'a str) -> Self {
        Self::Error {
            status: class.status().code(),
            message,
        }
    }

    /// Frame this event as a packet.
    pub(crate) fn to_packet(&self) -> Result<Vec<u8>, BridgeError> {
        let header = protocol::encode(self)?;
        let header_len = u32::try_from(header.len()).map_err(|error| {
            BridgeError::overflow(format!("event header length does not fit a u32: {error}"))
        })?;
        let payload = match self {
            Self::Message { bytes, .. } => bytes,
            Self::State { .. }
            | Self::Ice { .. }
            | Self::Channel { .. }
            | Self::Track { .. }
            | Self::Decoded { .. }
            | Self::Error { .. } => &[][..],
        };
        let mut packet = Vec::with_capacity(4 + header.len() + payload.len());
        packet.extend_from_slice(&header_len.to_le_bytes());
        packet.extend_from_slice(&header);
        packet.extend_from_slice(payload);
        Ok(packet)
    }
}

/// A local ICE candidate, as the host forwards it to signaling.
#[derive(Debug, Serialize)]
pub(crate) struct LocalCandidate<'a> {
    candidate: &'a str,
    sdp_mid: Option<&'a str>,
    sdp_mline_index: Option<u16>,
}

impl<'a> From<&'a IceCandidate> for LocalCandidate<'a> {
    fn from(candidate: &'a IceCandidate) -> Self {
        Self {
            candidate: &candidate.candidate,
            sdp_mid: candidate.sdp_mid.as_deref(),
            sdp_mline_index: candidate.sdp_mline_index,
        }
    }
}

/// The name of a connection state in `state` events.
pub(crate) fn connection_state(state: PeerConnectionState) -> &'static str {
    match state {
        PeerConnectionState::New => "new",
        PeerConnectionState::Connecting => "connecting",
        PeerConnectionState::Connected => "connected",
        PeerConnectionState::Disconnected => "disconnected",
        PeerConnectionState::Failed => "failed",
        PeerConnectionState::Closed => "closed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::parse_packet;
    use serde_json::{Value, json};

    fn header(event: &Event<'_>) -> Value {
        let packet = event.to_packet().unwrap();
        let (header, payload) = parse_packet(&packet);
        assert!(payload.is_empty(), "only a message carries a payload");
        header
    }

    #[test]
    fn a_packet_is_its_length_prefixed_header_then_the_message_bytes() {
        let payload = [0, 1, 2, 255];
        let event = Event::Message {
            channel: Channel::Data,
            bytes: &payload,
        };
        let packet = event.to_packet().unwrap();
        let header = br#"{"type":"message","channel":"data"}"#;
        assert_eq!(
            packet[..4],
            u32::try_from(header.len()).unwrap().to_le_bytes()
        );
        assert_eq!(packet[4..4 + header.len()], *header);
        assert_eq!(packet[4 + header.len()..], payload);
    }

    // `parseEvent` in packages/native/src/_internal/peer.ts reads these.
    #[test]
    fn each_event_has_the_header_the_host_parses() {
        let candidate = IceCandidate {
            candidate: "candidate:1 1 udp 1 127.0.0.1 9 typ host".into(),
            sdp_mid: Some("0".into()),
            sdp_mline_index: Some(0),
        };
        let cases = [
            (
                Event::State {
                    state: connection_state(PeerConnectionState::Connected),
                },
                json!({ "type": "state", "state": "connected" }),
            ),
            (Event::Ice { candidate: None }, json!({ "type": "ice" })),
            (
                Event::Ice {
                    candidate: Some(LocalCandidate::from(&candidate)),
                },
                json!({
                    "type": "ice",
                    "candidate": {
                        "candidate": "candidate:1 1 udp 1 127.0.0.1 9 typ host",
                        "sdp_mid": "0",
                        "sdp_mline_index": 0,
                    },
                }),
            ),
            (
                Event::Channel {
                    channel: Channel::Control,
                    open: true,
                },
                json!({ "type": "channel", "channel": "control", "open": true }),
            ),
            (
                Event::Track {
                    name: "main",
                    mid: "2",
                },
                json!({ "type": "track", "name": "main", "mid": "2" }),
            ),
            (
                Event::Decoded {
                    kind: TrackKind::Audio,
                    name: "main",
                    mid: "2",
                },
                json!({ "type": "decoded", "kind": "audio", "name": "main", "mid": "2" }),
            ),
            (
                Event::error(FailureClass::Protocol, "undeclared track"),
                json!({ "type": "error", "status": -4, "message": "undeclared track" }),
            ),
        ];
        for (event, expected) in cases {
            assert_eq!(header(&event), expected);
        }
    }

    #[test]
    fn a_candidate_without_a_mid_or_index_sends_nulls() {
        let candidate = IceCandidate {
            candidate: "candidate:2".into(),
            sdp_mid: None,
            sdp_mline_index: None,
        };
        let event = Event::Ice {
            candidate: Some(LocalCandidate::from(&candidate)),
        };
        assert_eq!(
            header(&event)["candidate"],
            json!({ "candidate": "candidate:2", "sdp_mid": null, "sdp_mline_index": null })
        );
    }

    #[test]
    fn every_connection_state_has_a_name_the_host_accepts() {
        let names = [
            PeerConnectionState::New,
            PeerConnectionState::Connecting,
            PeerConnectionState::Connected,
            PeerConnectionState::Disconnected,
            PeerConnectionState::Failed,
            PeerConnectionState::Closed,
        ]
        .map(connection_state);
        assert_eq!(
            names,
            [
                "new",
                "connecting",
                "connected",
                "disconnected",
                "failed",
                "closed"
            ]
        );
    }
}
