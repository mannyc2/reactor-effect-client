//! Test-only libwebrtc far peer standing in for Reactor's media server. It is
//! a Cargo example, so it never enters the staged library or its build
//! identity.
//!
//! Line-delimited JSON on stdin/stdout, one factory for the whole process:
//!
//! ```text
//!   <- {"op":"ready"}
//!   -> {"op":"offer","id":"a","sdp":"..."}   <- {"op":"answer","id":"a","sdp":"..."}
//!   -> {"op":"candidate","id":"a","candidate":"...","sdpMid":"0","sdpMLineIndex":0}
//!   -> {"op":"stats","id":"a"}               <- {"op":"stats","id":"a",...}
//!   -> {"op":"close","id":"a"}               <- {"op":"closed","id":"a"}
//! ```
//!
//! The answer carries the far peer's gathered candidates, as Reactor's does.
//! Once connected, each session sends BGRA video at `--width` x `--height`
//! and `--fps`, every frame carrying a 16-byte metadata `user_data` of
//! `[wall-clock microseconds u64 LE][sequence u64 LE]`, plus 10 ms blocks of
//! 48 kHz mono PCM. Both data channels echo every binary message. Closing
//! stdin ends the process.

mod media;
mod sdp;
mod session;

use media::Shape;
use reactor_webrtc::{IceCandidate, PeerConnectionFactory};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::Session;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::Arc;

/// Distinct pattern frames the video cycles through.
const PATTERN_FRAMES: u32 = 12;

/// A request from the test host.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Request {
    Offer {
        id: String,
        sdp: String,
    },
    Candidate {
        id: String,
        candidate: String,
        #[serde(rename = "sdpMid")]
        sdp_mid: Option<String>,
        #[serde(rename = "sdpMLineIndex")]
        sdp_mline_index: Option<u16>,
    },
    Stats {
        id: String,
    },
    Close {
        id: String,
    },
}

/// A reply to the test host.
#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Reply<'a> {
    Ready,
    Answer { id: &'a str, sdp: &'a str },
    Stats { id: &'a str, stats: Value },
    Closed { id: &'a str },
}

fn reply(reply: &Reply<'_>) {
    let line = serde_json::to_string(reply).expect("a reply is plain JSON data");
    let mut stdout = io::stdout().lock();
    // A host that closed stdout has gone, and there is no one left to tell.
    let _ = writeln!(stdout, "{line}").and_then(|()| stdout.flush());
}

fn main() {
    let shape = Shape::from_args();
    let frames = Arc::new(media::pattern(shape, PATTERN_FRAMES));
    let factory = PeerConnectionFactory::builder()
        .with_synthetic_adm()
        .build()
        .expect("far peer factory");
    let mut sessions = HashMap::new();
    reply(&Reply::Ready);
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        // A line that is not a request is not for this peer.
        let Ok(request) = serde_json::from_str::<Request>(&line) else {
            continue;
        };
        match request {
            Request::Offer { id, sdp } => {
                let (session, answer) = Session::open(&factory, sdp, Arc::clone(&frames), shape);
                sessions.insert(id.clone(), session);
                reply(&Reply::Answer {
                    id: &id,
                    sdp: &answer,
                });
            }
            Request::Candidate {
                id,
                candidate,
                sdp_mid,
                sdp_mline_index,
            } => {
                if let Some(session) = sessions.get(&id)
                    && !candidate.is_empty()
                {
                    session.add_candidate(&IceCandidate {
                        candidate,
                        sdp_mid,
                        sdp_mline_index,
                    });
                }
            }
            Request::Stats { id } => {
                let stats = sessions.get(&id).map_or(Value::Null, Session::stats);
                reply(&Reply::Stats { id: &id, stats });
            }
            Request::Close { id } => {
                if let Some(session) = sessions.remove(&id) {
                    session.close();
                }
                reply(&Reply::Closed { id: &id });
            }
        }
    }
    for session in sessions.into_values() {
        session.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn requests_parse_from_the_lines_the_host_writes() {
        let candidate: Request = serde_json::from_str(
            r#"{"op":"candidate","id":"a","candidate":"candidate:1","sdpMid":"0","sdpMLineIndex":2}"#,
        )
        .unwrap();
        assert!(matches!(
            candidate,
            Request::Candidate { ref id, ref sdp_mid, sdp_mline_index: Some(2), .. }
                if id == "a" && sdp_mid.as_deref() == Some("0")
        ));
        // JSON.stringify omits an undefined sdpMid.
        let bare: Request =
            serde_json::from_str(r#"{"op":"candidate","id":"a","candidate":"c"}"#).unwrap();
        assert!(matches!(
            bare,
            Request::Candidate {
                sdp_mid: None,
                sdp_mline_index: None,
                ..
            }
        ));
        assert!(serde_json::from_str::<Request>(r#"{"op":"unknown","id":"a"}"#).is_err());
    }

    #[test]
    fn replies_serialize_to_the_lines_the_host_reads() {
        let cases = [
            (Reply::Ready, json!({ "op": "ready" })),
            (
                Reply::Answer {
                    id: "a",
                    sdp: "v=0",
                },
                json!({ "op": "answer", "id": "a", "sdp": "v=0" }),
            ),
            (
                Reply::Stats {
                    id: "a",
                    stats: Value::Null,
                },
                json!({ "op": "stats", "id": "a", "stats": null }),
            ),
            (
                Reply::Closed { id: "a" },
                json!({ "op": "closed", "id": "a" }),
            ),
        ];
        for (reply, expected) in cases {
            assert_eq!(serde_json::to_value(&reply).unwrap(), expected);
        }
    }
}
