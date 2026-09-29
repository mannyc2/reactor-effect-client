//! Transport events, queued by libwebrtc callbacks and taken by the host.

use super::{Channel, TrackKind};
use crate::error::FailureClass;
use crate::sync::QueueItem;
use reactor_webrtc::{IceCandidate, PeerConnectionState};

/// A transport event for the host. Events own their data: a callback copies
/// what libwebrtc lends it, and the host converts an event to a JavaScript
/// value only when it takes it, on its own thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
    /// The aggregate connection state changed.
    State { state: &'static str },
    /// A local ICE candidate, or with none, the end of gathering.
    Ice { candidate: Option<Candidate> },
    /// A bridge data channel opened or closed.
    Channel { channel: Channel, open: bool },
    /// A binary message arrived on a bridge data channel.
    Message { channel: Channel, bytes: Vec<u8> },
    /// A remote track arrived for a declared receive mapping.
    Track { name: String, mid: String },
    /// That track's decoded media now reaches its queue.
    Decoded {
        kind: TrackKind,
        name: String,
        mid: String,
    },
    /// The connection failed.
    Error {
        class: FailureClass,
        message: String,
    },
}

impl Event {
    /// A failure of the connection itself, rather than of a host call.
    pub(crate) fn error(class: FailureClass, message: impl Into<String>) -> Self {
        Self::Error {
            class,
            message: message.into(),
        }
    }
}

/// The bytes an event holds beyond a fixed allowance for its fields, so a
/// queue of events is bounded by what they carry.
const EVENT_OVERHEAD_BYTES: usize = 64;

impl QueueItem for Event {
    fn byte_len(&self) -> usize {
        EVENT_OVERHEAD_BYTES
            + match self {
                Self::State { .. } | Self::Channel { .. } => 0,
                Self::Ice { candidate } => candidate.as_ref().map_or(0, Candidate::byte_len),
                Self::Message { bytes, .. } => bytes.len(),
                Self::Track { name, mid } | Self::Decoded { name, mid, .. } => {
                    name.len() + mid.len()
                }
                Self::Error { message, .. } => message.len(),
            }
    }
}

/// A local ICE candidate, as the host forwards it to signaling.
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(
    clippy::struct_field_names,
    reason = "`candidate` is WebRTC's name for the candidate line"
)]
pub(crate) struct Candidate {
    pub(crate) candidate: String,
    pub(crate) sdp_mid: Option<String>,
    pub(crate) sdp_mline_index: Option<u16>,
}

impl Candidate {
    fn byte_len(&self) -> usize {
        self.candidate.len() + self.sdp_mid.as_ref().map_or(0, String::len)
    }
}

impl From<&IceCandidate> for Candidate {
    fn from(candidate: &IceCandidate) -> Self {
        Self {
            candidate: candidate.candidate.clone(),
            sdp_mid: candidate.sdp_mid.clone(),
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

    #[test]
    fn an_event_counts_what_it_carries() {
        let message = Event::Message {
            channel: Channel::Data,
            bytes: vec![0; 1000],
        };
        assert_eq!(message.byte_len(), EVENT_OVERHEAD_BYTES + 1000);
        let state = Event::State { state: "new" };
        assert_eq!(state.byte_len(), EVENT_OVERHEAD_BYTES);
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
