use super::*;
use crate::protocol::{TrackKind, TrackSpec};
use crate::sync::Taken;

fn owner() -> Owner {
    Owner::new(Arc::new(Shared::new()))
}

fn request(tracks: &[(&str, TrackKind, Direction)]) -> PrepareRequest {
    let tracks = tracks
        .iter()
        .map(|&(name, kind, direction)| TrackSpec {
            name: name.into(),
            kind,
            direction,
        })
        .collect();
    PrepareRequest::new(vec![], tracks).expect("a request within the bounds")
}

/// An owner prepared with one receive and one send track.
fn prepared() -> Owner {
    let mut owner = owner();
    owner
        .prepare(request(&[
            ("in", TrackKind::Video, Direction::RecvOnly),
            ("out", TrackKind::Video, Direction::SendOnly),
        ]))
        .expect("prepare");
    owner
}

fn bitrate(name: &str, bits: u32) -> BitrateRequest {
    BitrateRequest::new(name.into(), bits).expect("a valid bitrate")
}

#[test]
fn prepare_offers_every_track_and_maps_each_to_its_mid_in_request_order() {
    let mut owner = owner();
    let prepared = owner
        .prepare(request(&[
            ("video", TrackKind::Video, Direction::RecvOnly),
            ("audio", TrackKind::Audio, Direction::RecvOnly),
            ("camera", TrackKind::Video, Direction::SendOnly),
        ]))
        .expect("prepare");
    let sdp = &prepared.sdp;
    assert!(sdp.contains("m=video") && sdp.contains("m=audio"), "{sdp}");
    let declared: Vec<_> = prepared
        .mapping
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
    for entry in &prepared.mapping {
        assert!(sdp.contains(&format!("a=mid:{}", entry.mid)), "{entry:?}");
    }
    owner.shutdown();
}

#[test]
fn a_peer_prepares_once() {
    let mut owner = prepared();
    let error = owner.prepare(request(&[])).unwrap_err();
    assert_eq!(
        error,
        BridgeError::invalid("native peer is already prepared")
    );
    owner.shutdown();
}

#[test]
fn a_rejected_answer_is_classified_as_sdp_rejected() {
    let owner = prepared();
    let error = owner
        .answer("v=0\r\nthis is not an answer\r\n".into())
        .expect_err("libwebrtc must reject a malformed answer");
    assert_eq!(error.class, FailureClass::SdpRejected);
    assert!(
        error.message.starts_with("set_remote_description: "),
        "{error}"
    );
    assert_eq!(
        owner.answer(String::new()).unwrap_err(),
        BridgeError::invalid("answer SDP is empty")
    );
    owner.shutdown();
}

#[test]
fn before_prepare_each_call_fails_with_its_class() {
    let owner = owner();
    assert_eq!(
        owner.answer("v=0".into()).unwrap_err(),
        BridgeError::closed()
    );
    assert_eq!(owner.stats().unwrap_err(), BridgeError::closed());
    assert_eq!(
        owner.set_direction("out", false).unwrap_err(),
        BridgeError::invalid("unknown track: out")
    );
    assert_eq!(
        owner.send(Channel::Data, b"early").unwrap_err(),
        BridgeError::new(FailureClass::ChannelClosed, "data channel is not open")
    );
    owner.shutdown();
}

#[test]
fn a_declared_track_pauses_resumes_and_caps_its_bitrate() {
    let owner = prepared();
    for (name, active) in [("out", false), ("out", true), ("in", false), ("in", true)] {
        owner.set_direction(name, active).expect("direction");
    }
    owner
        .set_max_bitrate(&bitrate("out", 2_000_000))
        .expect("bitrate");
    assert_eq!(
        owner.set_max_bitrate(&bitrate("in", 1000)).unwrap_err(),
        BridgeError::invalid("in is not an outgoing track")
    );
    assert_eq!(
        owner
            .set_max_bitrate(&bitrate("missing", 1000))
            .unwrap_err(),
        BridgeError::invalid("unknown track: missing")
    );
    owner.shutdown();
}

#[test]
fn a_prepared_peer_reports_stats_before_it_connects() {
    let owner = prepared();
    let stats = owner.stats().expect("stats");
    assert!(stats.is_array(), "{stats}");
    owner.shutdown();
}

#[test]
fn commands_queued_behind_close_are_refused() {
    let mut owner = prepared();
    owner.shared.close();
    let refused = owner
        .unless_closed(|_| -> Result<(), BridgeError> { panic!("a closed peer runs no command") });
    assert_eq!(refused, Err(BridgeError::closed()));
    owner.shutdown();
}

#[test]
fn shutdown_closes_every_queue() {
    let shared = Arc::new(Shared::new());
    let mut owner = Owner::new(Arc::clone(&shared));
    owner.prepare(request(&[])).expect("prepare");
    owner.shutdown();
    assert_eq!(shared.events.take(|_| true), Taken::Closed);
    assert_eq!(shared.video.take(|_| true), Taken::Closed);
    assert_eq!(shared.audio.take(|_| true), Taken::Closed);
}
