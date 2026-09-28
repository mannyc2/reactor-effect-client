//! The peer's domain values: call requests and their bounds, the events it
//! reports, and its statistics.

mod event;
mod request;
mod stats;

pub(crate) use event::{Candidate, Event, connection_state};
pub(crate) use request::{
    BitrateRequest, Direction, IceServerSpec, Mapping, PrepareRequest, TrackKind, TrackSpec,
};
pub(crate) use stats::{MediaSnapshot, stats_json};

/// The largest data channel message, in either direction.
pub(crate) const MAX_MESSAGE_BYTES: usize = 256 * 1024;

/// The most a data channel may hold unsent before a send is refused.
pub(crate) const MAX_BUFFERED_SEND_BYTES: u64 = 1024 * 1024;

/// A data channel the bridge owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    Control,
    Data,
}

impl Channel {
    /// The channel's label, which the remote peer matches.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Data => "data",
        }
    }
}
