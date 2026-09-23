//! Call requests, their bounds, and the `Prepare` response.

use super::decode;
use crate::error::BridgeError;
use reactor_webrtc::{IceServer, MediaKind, TransceiverDirection};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// The most ICE servers a prepare request may name.
const MAX_ICE_SERVERS: usize = 64;
/// The most URLs one ICE server may list.
const MAX_ICE_URLS: usize = 16;
/// The longest ICE URL, in bytes.
const MAX_ICE_URL_BYTES: usize = 2048;
/// The most tracks a prepare request may declare. The host checks the same
/// bound on the mapping it receives.
const MAX_TRACKS: usize = 64;
/// The longest track name, in bytes.
const MAX_TRACK_NAME_BYTES: usize = 256;

/// The media a track carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

/// `Prepare`: the ICE servers to use and the tracks to negotiate, in order.
#[derive(Debug, Deserialize)]
pub(crate) struct PrepareRequest {
    pub(crate) servers: Vec<IceServerSpec>,
    pub(crate) tracks: Vec<TrackSpec>,
}

/// A STUN or TURN server.
#[derive(Debug, Deserialize)]
pub(crate) struct IceServerSpec {
    urls: Vec<String>,
    #[serde(default)]
    username: String,
    #[serde(default)]
    credential: String,
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

/// A track to negotiate. Its index in the request identifies it in media
/// headers.
#[derive(Debug, Deserialize)]
pub(crate) struct TrackSpec {
    pub(crate) name: String,
    pub(crate) kind: TrackKind,
    pub(crate) direction: Direction,
}

impl PrepareRequest {
    /// Decode a prepare request and check it against the bridge's bounds.
    pub(crate) fn parse(request: &[u8]) -> Result<Self, BridgeError> {
        let request: Self = decode(request)?;
        request.check_bounds()?;
        Ok(request)
    }

    fn check_bounds(&self) -> Result<(), BridgeError> {
        if self.servers.len() > MAX_ICE_SERVERS {
            return Err(BridgeError::invalid(format!(
                "at most {MAX_ICE_SERVERS} ICE servers are supported"
            )));
        }
        if self.tracks.len() > MAX_TRACKS {
            return Err(BridgeError::invalid(format!(
                "at most {MAX_TRACKS} tracks are supported"
            )));
        }
        for server in &self.servers {
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
        let mut names = HashSet::with_capacity(self.tracks.len());
        for track in &self.tracks {
            let name = track.name.as_str();
            if name.is_empty() || name.len() > MAX_TRACK_NAME_BYTES || name.contains('\0') {
                return Err(BridgeError::invalid("invalid track name"));
            }
            if !names.insert(name) {
                return Err(BridgeError::invalid("duplicate track name"));
            }
        }
        Ok(())
    }
}

/// A declared track and the MID libwebrtc gave its transceiver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Mapping {
    pub(crate) name: String,
    pub(crate) kind: TrackKind,
    pub(crate) direction: Direction,
    pub(crate) mid: String,
}

/// The `Prepare` response: the local offer and each track's mapping, in
/// request order.
#[derive(Debug, Serialize)]
pub(crate) struct PrepareResponse<'a> {
    pub(crate) sdp: &'a str,
    pub(crate) mapping: &'a [Mapping],
}

/// `Direction`: pause or resume a declared track.
#[derive(Debug, Deserialize)]
pub(crate) struct DirectionRequest {
    pub(crate) name: String,
    pub(crate) active: bool,
}

/// `MaxBitrate`: cap an outgoing track's send bitrate.
#[derive(Debug)]
pub(crate) struct BitrateRequest {
    pub(crate) name: String,
    /// Always positive, as libwebrtc's `int` requires.
    pub(crate) bits_per_second: i32,
}

impl BitrateRequest {
    pub(crate) fn parse(request: &[u8]) -> Result<Self, BridgeError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Json {
            name: String,
            bits_per_second: u32,
        }

        let Json {
            name,
            bits_per_second,
        } = decode(request)?;
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
    use serde_json::{Value, json};

    fn server(urls: usize, url_bytes: usize) -> Value {
        json!({ "urls": vec!["s".repeat(url_bytes); urls], "username": "u", "credential": "c" })
    }

    fn track(name: &str) -> Value {
        json!({ "name": name, "kind": "video", "direction": "recvonly" })
    }

    fn prepare(servers: &[Value], tracks: &[Value]) -> Vec<u8> {
        serde_json::to_vec(&json!({ "servers": servers, "tracks": tracks })).unwrap()
    }

    fn numbered_tracks(count: usize) -> Vec<Value> {
        (0..count)
            .map(|index| track(&format!("t{index}")))
            .collect()
    }

    #[test]
    fn prepare_accepts_a_request_at_every_bound() {
        let long_name = "n".repeat(MAX_TRACK_NAME_BYTES);
        let mut tracks = numbered_tracks(MAX_TRACKS - 1);
        tracks.push(json!({ "name": long_name, "kind": "audio", "direction": "sendonly" }));
        let request = prepare(
            &vec![server(MAX_ICE_URLS, MAX_ICE_URL_BYTES); MAX_ICE_SERVERS],
            &tracks,
        );

        let parsed = PrepareRequest::parse(&request).unwrap();
        assert_eq!(parsed.servers.len(), MAX_ICE_SERVERS);
        let last = parsed.tracks.last().unwrap();
        assert_eq!(
            (last.name.len(), last.kind, last.direction),
            (MAX_TRACK_NAME_BYTES, TrackKind::Audio, Direction::SendOnly)
        );
    }

    #[test]
    fn prepare_rejects_a_request_past_any_bound_as_invalid_input() {
        let cases: [(&str, Vec<u8>); 14] = [
            (
                "too many servers",
                prepare(&vec![server(1, 1); MAX_ICE_SERVERS + 1], &[]),
            ),
            (
                "too many tracks",
                prepare(&[], &numbered_tracks(MAX_TRACKS + 1)),
            ),
            ("a server without URLs", prepare(&[server(0, 1)], &[])),
            (
                "too many URLs",
                prepare(&[server(MAX_ICE_URLS + 1, 1)], &[]),
            ),
            ("an empty URL", prepare(&[server(1, 0)], &[])),
            (
                "an overlong URL",
                prepare(&[server(1, MAX_ICE_URL_BYTES + 1)], &[]),
            ),
            ("an empty track name", prepare(&[], &[track("")])),
            (
                "an overlong track name",
                prepare(&[], &[track(&"n".repeat(MAX_TRACK_NAME_BYTES + 1))]),
            ),
            ("a NUL in a track name", prepare(&[], &[track("a\0b")])),
            (
                "a duplicate track name",
                prepare(&[], &[track("same"), track("same")]),
            ),
            (
                "an unknown kind",
                prepare(
                    &[],
                    &[json!({ "name": "t", "kind": "data", "direction": "recvonly" })],
                ),
            ),
            (
                "an unknown direction",
                prepare(
                    &[],
                    &[json!({ "name": "t", "kind": "video", "direction": "sendrecv" })],
                ),
            ),
            ("missing tracks", br#"{"servers":[]}"#.to_vec()),
            ("malformed JSON", b"{".to_vec()),
        ];
        for (case, request) in cases {
            let error = PrepareRequest::parse(&request).expect_err(case);
            assert_eq!(error.class, FailureClass::InvalidInput, "{case}");
        }
    }

    #[test]
    fn ice_credentials_default_to_empty_strings() {
        let request = prepare(&[json!({ "urls": ["stun:example.org"] })], &[]);
        let parsed = PrepareRequest::parse(&request).unwrap();
        let server = IceServer::from(&parsed.servers[0]);
        assert_eq!(server.urls, ["stun:example.org"]);
        assert_eq!(
            (server.username.as_str(), server.password.as_str()),
            ("", "")
        );
    }

    #[test]
    fn a_bitrate_must_be_a_positive_c_int() {
        let parse = |bits: Value| {
            let request = json!({ "name": "out", "bitsPerSecond": bits });
            BitrateRequest::parse(&serde_json::to_vec(&request).unwrap())
        };
        assert_eq!(parse(json!(1)).unwrap().bits_per_second, 1);
        assert_eq!(parse(json!(i32::MAX)).unwrap().bits_per_second, i32::MAX);
        for bits in [
            json!(0),
            json!(-1),
            json!(2_147_483_648_u64),
            json!(1.5),
            json!("1"),
        ] {
            let error = parse(bits.clone()).expect_err(&bits.to_string());
            assert_eq!(error.class, FailureClass::InvalidInput, "{bits}");
        }
    }

    #[test]
    fn mappings_carry_kinds_and_directions_as_the_host_names_them() {
        let mapping = Mapping {
            name: "out".into(),
            kind: TrackKind::Video,
            direction: Direction::SendOnly,
            mid: "1".into(),
        };
        let response = PrepareResponse {
            sdp: "v=0",
            mapping: &[mapping],
        };
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "sdp": "v=0",
                "mapping": [{ "name": "out", "kind": "video", "direction": "sendonly", "mid": "1" }],
            })
        );
    }
}
