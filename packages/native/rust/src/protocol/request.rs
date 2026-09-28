//! Call requests and their bounds.

use crate::error::BridgeError;
use reactor_webrtc::{IceServer, MediaKind, TransceiverDirection};
use std::collections::HashSet;

/// The most ICE servers a prepare request may name.
const MAX_ICE_SERVERS: usize = 64;
/// The most URLs one ICE server may list.
const MAX_ICE_URLS: usize = 16;
/// The longest ICE URL, in bytes.
const MAX_ICE_URL_BYTES: usize = 2048;
/// The most tracks a prepare request may declare.
const MAX_TRACKS: usize = 64;
/// The longest track name, in bytes.
const MAX_TRACK_NAME_BYTES: usize = 256;

/// The media a track carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackKind {
    Video,
    Audio,
}

impl From<TrackKind> for MediaKind {
    fn from(kind: TrackKind) -> Self {
        match kind {
            TrackKind::Video => Self::Video,
            TrackKind::Audio => Self::Audio,
        }
    }
}

/// Which way a track's media flows, seen from the bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// The remote peer sends and the bridge decodes.
    RecvOnly,
    /// The bridge sends.
    SendOnly,
}

impl From<Direction> for TransceiverDirection {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::RecvOnly => Self::RecvOnly,
            Direction::SendOnly => Self::SendOnly,
        }
    }
}

/// A STUN or TURN server.
#[derive(Debug, Clone)]
pub(crate) struct IceServerSpec {
    pub(crate) urls: Vec<String>,
    pub(crate) username: String,
    pub(crate) credential: String,
}

impl From<&IceServerSpec> for IceServer {
    fn from(server: &IceServerSpec) -> Self {
        Self {
            urls: server.urls.clone(),
            username: server.username.clone(),
            password: server.credential.clone(),
        }
    }
}

/// A track to negotiate. Its index in the request identifies it in media items.
#[derive(Debug, Clone)]
pub(crate) struct TrackSpec {
    pub(crate) name: String,
    pub(crate) kind: TrackKind,
    pub(crate) direction: Direction,
}

/// `prepare`: the ICE servers to use and the tracks to negotiate, in order.
#[derive(Debug)]
pub(crate) struct PrepareRequest {
    pub(crate) servers: Vec<IceServerSpec>,
    pub(crate) tracks: Vec<TrackSpec>,
}

impl PrepareRequest {
    /// A request within the bridge's bounds.
    pub(crate) fn new(
        servers: Vec<IceServerSpec>,
        tracks: Vec<TrackSpec>,
    ) -> Result<Self, BridgeError> {
        if servers.len() > MAX_ICE_SERVERS {
            return Err(BridgeError::invalid(format!(
                "at most {MAX_ICE_SERVERS} ICE servers are supported"
            )));
        }
        if tracks.len() > MAX_TRACKS {
            return Err(BridgeError::invalid(format!(
                "at most {MAX_TRACKS} tracks are supported"
            )));
        }
        for server in &servers {
            if !(1..=MAX_ICE_URLS).contains(&server.urls.len()) {
                return Err(BridgeError::invalid(format!(
                    "each ICE server must contain 1..={MAX_ICE_URLS} URLs"
                )));
            }
            if server
                .urls
                .iter()
                .any(|url| url.is_empty() || url.len() > MAX_ICE_URL_BYTES)
            {
                return Err(BridgeError::invalid("invalid ICE URL length"));
            }
        }
        let mut names = HashSet::with_capacity(tracks.len());
        for track in &tracks {
            let name = track.name.as_str();
            if name.is_empty() || name.len() > MAX_TRACK_NAME_BYTES || name.contains('\0') {
                return Err(BridgeError::invalid("invalid track name"));
            }
            if !names.insert(name) {
                return Err(BridgeError::invalid("duplicate track name"));
            }
        }
        Ok(Self { servers, tracks })
    }
}

/// A declared track and the MID libwebrtc gave its transceiver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Mapping {
    pub(crate) name: String,
    pub(crate) kind: TrackKind,
    pub(crate) direction: Direction,
    pub(crate) mid: String,
}

/// `maxBitrate`: cap an outgoing track's send bitrate.
#[derive(Debug)]
pub(crate) struct BitrateRequest {
    pub(crate) name: String,
    /// Always positive, as libwebrtc's `int` requires.
    pub(crate) bits_per_second: i32,
}

impl BitrateRequest {
    pub(crate) fn new(name: String, bits_per_second: u32) -> Result<Self, BridgeError> {
        let bits_per_second = i32::try_from(bits_per_second)
            .ok()
            .filter(|bits| *bits > 0)
            .ok_or_else(|| {
                BridgeError::invalid("bitsPerSecond must be an integer in 1..=2147483647")
            })?;
        Ok(Self {
            name,
            bits_per_second,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FailureClass;

    fn server(urls: usize, url_bytes: usize) -> IceServerSpec {
        IceServerSpec {
            urls: vec!["s".repeat(url_bytes); urls],
            username: "u".into(),
            credential: "c".into(),
        }
    }

    fn track(name: &str) -> TrackSpec {
        TrackSpec {
            name: name.into(),
            kind: TrackKind::Video,
            direction: Direction::RecvOnly,
        }
    }

    fn numbered_tracks(count: usize) -> Vec<TrackSpec> {
        (0..count)
            .map(|index| track(&format!("t{index}")))
            .collect()
    }

    #[test]
    fn prepare_accepts_a_request_at_every_bound() {
        let mut tracks = numbered_tracks(MAX_TRACKS - 1);
        tracks.push(TrackSpec {
            name: "n".repeat(MAX_TRACK_NAME_BYTES),
            kind: TrackKind::Audio,
            direction: Direction::SendOnly,
        });
        let request = PrepareRequest::new(
            vec![server(MAX_ICE_URLS, MAX_ICE_URL_BYTES); MAX_ICE_SERVERS],
            tracks,
        )
        .unwrap();
        assert_eq!(request.servers.len(), MAX_ICE_SERVERS);
        assert_eq!(request.tracks.len(), MAX_TRACKS);
    }

    #[test]
    fn prepare_rejects_a_request_past_any_bound_as_invalid_input() {
        let cases: [(&str, Vec<IceServerSpec>, Vec<TrackSpec>); 10] = [
            (
                "too many servers",
                vec![server(1, 1); MAX_ICE_SERVERS + 1],
                vec![],
            ),
            ("too many tracks", vec![], numbered_tracks(MAX_TRACKS + 1)),
            ("a server without URLs", vec![server(0, 1)], vec![]),
            ("too many URLs", vec![server(MAX_ICE_URLS + 1, 1)], vec![]),
            ("an empty URL", vec![server(1, 0)], vec![]),
            (
                "an overlong URL",
                vec![server(1, MAX_ICE_URL_BYTES + 1)],
                vec![],
            ),
            ("an empty track name", vec![], vec![track("")]),
            (
                "an overlong track name",
                vec![],
                vec![track(&"n".repeat(MAX_TRACK_NAME_BYTES + 1))],
            ),
            ("a NUL in a track name", vec![], vec![track("a\0b")]),
            (
                "a duplicate track name",
                vec![],
                vec![track("same"), track("same")],
            ),
        ];
        for (case, servers, tracks) in cases {
            let error = PrepareRequest::new(servers, tracks).expect_err(case);
            assert_eq!(error.class, FailureClass::InvalidInput, "{case}");
        }
    }

    #[test]
    fn a_bitrate_must_be_a_positive_c_int() {
        let bits = |value: u32| BitrateRequest::new("out".into(), value);
        assert_eq!(bits(1).unwrap().bits_per_second, 1);
        assert_eq!(bits(0x7fff_ffff).unwrap().bits_per_second, i32::MAX);
        for value in [0, 0x8000_0000, u32::MAX] {
            let error = bits(value).expect_err(&value.to_string());
            assert_eq!(error.class, FailureClass::InvalidInput, "{value}");
        }
    }
}
