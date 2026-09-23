use super::*;
use crate::protocol::TrackKind;
use crate::sync::Taken;
use serde_json::{Value, json};

fn owner() -> Owner {
    Owner::new(Arc::new(Shared::new()))
}

fn prepare_request(tracks: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({ "servers": [], "tracks": tracks })).unwrap()
}

/// An owner prepared with one receive and one send track.
fn prepared() -> Owner {
    let mut owner = owner();
    owner
        .prepare(&prepare_request(&json!([
            { "name": "in", "kind": "video", "direction": "recvonly" },
            { "name": "out", "kind": "video", "direction": "sendonly" },
        ])))
        .expect("prepare");
    owner
}

#[test]
fn prepare_offers_every_track_and_maps_each_to_its_mid_in_request_order() {
    let mut owner = owner();
    let response = owner
        .prepare(&prepare_request(&json!([
            { "name": "video", "kind": "video", "direction": "recvonly" },
            { "name": "audio", "kind": "audio", "direction": "recvonly" },
            { "name": "camera", "kind": "video", "direction": "sendonly" },
        ])))
        .expect("prepare");
    let response: Value = serde_json::from_slice(&response).unwrap();
    let sdp = response["sdp"].as_str().unwrap();
    assert!(sdp.contains("m=video") && sdp.contains("m=audio"), "{sdp}");
    let mapping: Vec<Mapping> = serde_json::from_value(response["mapping"].clone()).unwrap();
    let declared: Vec<_> = mapping
        .iter()
        .map(|entry| (entry.name.as_str(), entry.kind, entry.direction))
        .collect();
    assert_eq!(
        declared,
        [
            ("video", TrackKind::Video, Direction::RecvOnly),
            ("audio", TrackKind::Audio, Direction::RecvOnly),
            ("camera", TrackKind::Video, Direction::SendOnly),
        ]
    );
    for entry in &mapping {
        assert!(sdp.contains(&format!("a=mid:{}", entry.mid)), "{entry:?}");
    }
    owner.shutdown();
}

#[test]
fn a_peer_prepares_once() {
    let mut owner = prepared();
    let error = owner.prepare(&prepare_request(&json!([]))).unwrap_err();
    assert_eq!(
        error,
        BridgeError::invalid("native peer is already prepared")
    );
    owner.shutdown();
}

#[test]
fn a_rejected_answer_is_classified_as_sdp_rejected() {
    let mut owner = prepared();
    let error = owner
        .answer(b"v=0\r\nthis is not an answer\r\n")
        .expect_err("libwebrtc must reject a malformed answer");
    assert_eq!(error.class, FailureClass::SdpRejected);
    assert!(
        error.message.starts_with("set_remote_description: "),
        "{error}"
    );
    owner.shutdown();
}

#[test]
fn an_empty_or_non_utf8_answer_is_invalid_input() {
    let mut owner = prepared();
    assert_eq!(
        owner.answer(b"").unwrap_err(),
        BridgeError::invalid("answer SDP is empty")
    );
    assert_eq!(
        owner.answer(&[0xff, 0xfe]).unwrap_err(),
        BridgeError::invalid("answer SDP is not UTF-8")
    );
    owner.shutdown();
}

#[test]
fn before_prepare_each_call_fails_with_its_class() {
    let mut owner = owner();
    assert_eq!(owner.answer(b"v=0").unwrap_err(), BridgeError::closed());
    assert_eq!(owner.stats().unwrap_err(), BridgeError::closed());
    let error = owner
        .set_direction(br#"{"name":"out","active":false}"#)
        .unwrap_err();
    assert_eq!(error, BridgeError::invalid("unknown track: out"));
    let error = owner.send(Channel::Data, b"early").unwrap_err();
    assert_eq!(
        error,
        BridgeError::new(FailureClass::ChannelClosed, "data channel is not open")
    );
    owner.shutdown();
}

#[test]
fn a_declared_track_pauses_resumes_and_caps_its_bitrate() {
    let mut owner = prepared();
    for request in [
        br#"{"name":"out","active":false}"#.as_slice(),
        br#"{"name":"out","active":true}"#,
        br#"{"name":"in","active":false}"#,
        br#"{"name":"in","active":true}"#,
    ] {
        let response = owner.set_direction(request).expect("direction");
        assert_eq!(response, b"{}");
    }
    let response = owner
        .set_max_bitrate(br#"{"name":"out","bitsPerSecond":2000000}"#)
        .expect("bitrate");
    assert_eq!(response, b"{}");
    owner.shutdown();
}

#[test]
fn only_a_declared_sending_track_takes_a_bitrate() {
    let mut owner = prepared();
    assert_eq!(
        owner
            .set_max_bitrate(br#"{"name":"in","bitsPerSecond":1000}"#)
            .unwrap_err(),
        BridgeError::invalid("in is not an outgoing track")
    );
    assert_eq!(
        owner
            .set_max_bitrate(br#"{"name":"missing","bitsPerSecond":1000}"#)
            .unwrap_err(),
        BridgeError::invalid("unknown track: missing")
    );
    assert_eq!(
        owner
            .set_direction(br#"{"name":"missing","active":true}"#)
            .unwrap_err(),
        BridgeError::invalid("unknown track: missing")
    );
    owner.shutdown();
}

#[test]
fn a_prepared_peer_reports_stats_before_it_connects() {
    let mut owner = prepared();
    let stats: Value = serde_json::from_slice(&owner.stats().expect("stats")).unwrap();
    assert!(stats.is_array(), "{stats}");
    owner.shutdown();
}

#[test]
fn commands_queued_behind_close_are_refused() {
    let mut owner = prepared();
    owner.shared.close();
    let refused = owner.unless_closed(|_| -> Result<(), BridgeError> {
        unreachable!("a closed peer runs no command")
    });
    assert_eq!(refused, Err(BridgeError::closed()));
    owner.shutdown();
}

#[test]
fn shutdown_closes_every_queue() {
    let shared = Arc::new(Shared::new());
    let mut owner = Owner::new(Arc::clone(&shared));
    owner
        .prepare(&prepare_request(&json!([])))
        .expect("prepare");
    owner.shutdown();
    assert_eq!(shared.events.take(|_| true), Taken::Closed);
    assert_eq!(shared.video.take(|_| true), Taken::Closed);
    assert_eq!(shared.audio.take(|_| true), Taken::Closed);
}
