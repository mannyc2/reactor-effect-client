//! The constants of the C ABI, mirrored from `include/reactor_effect_native.h`.
//!
//! A test parses the header and fails when the two disagree. The host mirrors
//! them too, in `packages/native/src/_internal/bridge.ts`.

use crate::error::BridgeError;
use serde::{Serialize, Serializer};

/// The ABI version the header declares. The host refuses any other.
pub(crate) const ABI_VERSION: u32 = 4;

/// The capacity of `ReactorEffectFailure::message`, in bytes.
pub(crate) const FAILURE_MESSAGE_BYTES: usize = 1020;

/// The smallest response buffer `reactor_effect_peer_call` accepts.
pub(crate) const CALL_BUFFER_MIN: usize = 4 * 1024 * 1024;

/// The largest request `reactor_effect_peer_call` accepts.
pub(crate) const MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// The largest data channel message, in either direction.
pub(crate) const MAX_MESSAGE_BYTES: usize = 256 * 1024;

/// The most a data channel may hold unsent before a send is refused.
pub(crate) const MAX_BUFFERED_SEND_BYTES: u64 = 1024 * 1024;

/// What an entry point reports (`enum ReactorEffectStatus`).
///
/// Non-negative statuses are outcomes. Negative statuses are failure classes,
/// closed for this ABI; the host maps each one to its own error type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum Status {
    Ok = 0,
    /// A take found its queue empty.
    Again = 1,
    /// The required sizes were written and the item stays queued.
    BufferTooSmall = 2,
    /// The peer is fenced or shut down.
    Closed = 3,
    /// An argument or request was rejected.
    InvalidInput = -1,
    /// libwebrtc or the bridge failed in a way it cannot classify.
    Native = -2,
    /// A queue, buffer or message bound was exceeded.
    Overflow = -3,
    /// The remote peer broke the negotiated contract.
    Protocol = -4,
    /// libwebrtc refused to create or apply an SDP.
    SdpRejected = -5,
    /// The data channel is not open.
    ChannelClosed = -6,
}

impl Status {
    /// The C `int` an entry point returns for this status.
    pub(crate) const fn code(self) -> i32 {
        self as i32
    }
}

/// A peer operation run by `reactor_effect_peer_call` (`enum ReactorEffectCall`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    /// Create the connection and its local offer.
    Prepare = 1,
    /// Apply the remote answer.
    Answer = 2,
    /// Pause or resume a declared track.
    Direction = 3,
    /// Cap an outgoing track's send bitrate.
    MaxBitrate = 4,
    /// Read WebRTC statistics.
    Stats = 5,
    /// Read queue pressure.
    MediaSnapshot = 6,
}

impl Operation {
    const ALL: [Self; 6] = [
        Self::Prepare,
        Self::Answer,
        Self::Direction,
        Self::MaxBitrate,
        Self::Stats,
        Self::MediaSnapshot,
    ];
}

impl TryFrom<u32> for Operation {
    type Error = BridgeError;

    fn try_from(code: u32) -> Result<Self, BridgeError> {
        Self::ALL
            .into_iter()
            .find(|operation| *operation as u32 == code)
            .ok_or_else(|| BridgeError::invalid("unknown native call operation"))
    }
}

/// A data channel the bridge owns (`enum ReactorEffectChannel`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    /// Carries Reactor control messages.
    Control = 0,
    /// Carries Reactor data messages.
    Data = 1,
}

impl Channel {
    const ALL: [Self; 2] = [Self::Control, Self::Data];

    /// The channel's SCTP label, which also names it in event headers.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Data => "data",
        }
    }
}

impl TryFrom<u32> for Channel {
    type Error = BridgeError;

    fn try_from(code: u32) -> Result<Self, BridgeError> {
        Self::ALL
            .into_iter()
            .find(|channel| *channel as u32 == code)
            .ok_or_else(|| BridgeError::invalid("unknown data channel"))
    }
}

impl Serialize for Channel {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.label())
    }
}

/// A readiness bit passed to the host's notify callback (`enum ReactorEffectReady`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ready {
    /// The transport event queue received an event.
    Events = 1,
    /// The decoded video queue received a frame.
    Video = 2,
    /// The decoded audio queue received a block.
    Audio = 4,
}

impl Ready {
    /// This readiness as its bit of the callback's mask.
    pub(crate) const fn bit(self) -> u32 {
        self as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const HEADER: &str = include_str!("../include/reactor_effect_native.h");

    /// Every `REACTOR_EFFECT_<NAME> = <value>` enumerator the header declares.
    fn header_enumerators() -> BTreeMap<String, i64> {
        HEADER
            .lines()
            .filter_map(|line| {
                let (name, value) = line
                    .trim()
                    .strip_prefix("REACTOR_EFFECT_")?
                    .split_once(" = ")?;
                let digits: String = value
                    .chars()
                    .take_while(|c| *c == '-' || c.is_ascii_digit())
                    .collect();
                Some((name.to_owned(), digits.parse().ok()?))
            })
            .collect()
    }

    #[test]
    fn the_header_declares_exactly_these_statuses_calls_channels_and_readiness_bits() {
        let statuses = [
            ("OK", Status::Ok),
            ("AGAIN", Status::Again),
            ("BUFFER_TOO_SMALL", Status::BufferTooSmall),
            ("CLOSED", Status::Closed),
            ("INVALID_INPUT", Status::InvalidInput),
            ("NATIVE", Status::Native),
            ("OVERFLOW", Status::Overflow),
            ("PROTOCOL", Status::Protocol),
            ("SDP_REJECTED", Status::SdpRejected),
            ("CHANNEL_CLOSED", Status::ChannelClosed),
        ]
        .map(|(name, status)| (name, i64::from(status.code())));
        let calls = [
            ("PREPARE", Operation::Prepare),
            ("ANSWER", Operation::Answer),
            ("DIRECTION", Operation::Direction),
            ("MAX_BITRATE", Operation::MaxBitrate),
            ("STATS", Operation::Stats),
            ("MEDIA_SNAPSHOT", Operation::MediaSnapshot),
        ]
        .map(|(name, operation)| (name, i64::from(operation as u32)));
        let channels = [("CONTROL", Channel::Control), ("DATA", Channel::Data)]
            .map(|(name, channel)| (name, i64::from(channel as u32)));
        let ready = [
            ("READY_EVENTS", Ready::Events),
            ("READY_VIDEO", Ready::Video),
            ("READY_AUDIO", Ready::Audio),
        ]
        .map(|(name, ready)| (name, i64::from(ready.bit())));

        let expected: BTreeMap<String, i64> = statuses
            .into_iter()
            .chain(calls)
            .chain(channels)
            .chain(ready)
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        assert_eq!(header_enumerators(), expected);
    }

    #[test]
    fn the_header_declares_this_abi_version_and_failure_capacity() {
        assert!(HEADER.contains(&format!("/* ABI {ABI_VERSION}.")));
        assert!(HEADER.contains(&format!("uint8_t message[{FAILURE_MESSAGE_BYTES}];")));
    }

    #[test]
    fn every_call_and_channel_code_parses_back_to_its_variant() {
        for operation in Operation::ALL {
            assert_eq!(Operation::try_from(operation as u32), Ok(operation));
        }
        for channel in Channel::ALL {
            assert_eq!(Channel::try_from(channel as u32), Ok(channel));
        }
    }

    #[test]
    fn unknown_call_and_channel_codes_are_invalid_input() {
        for code in [0, 7, u32::MAX] {
            let error = Operation::try_from(code).unwrap_err();
            assert_eq!(error, BridgeError::invalid("unknown native call operation"));
        }
        for code in [2, u32::MAX] {
            let error = Channel::try_from(code).unwrap_err();
            assert_eq!(error, BridgeError::invalid("unknown data channel"));
        }
    }

    #[test]
    fn channels_serialize_as_their_labels() {
        let labels = serde_json::to_value(Channel::ALL).unwrap();
        assert_eq!(labels, serde_json::json!(["control", "data"]));
    }
}
