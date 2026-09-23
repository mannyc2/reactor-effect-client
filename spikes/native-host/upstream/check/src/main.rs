//! Exercises the two draft reactor-webrtc patches: a typed RTCErrorType on SDP
//! rejection, the remote transceiver MID in on_track_with_mid, and
//! RTCIceConnectionState. Prints one JSON-ish line per fact and exits nonzero
//! on a mismatch.
use reactor_webrtc::{
    Error, IceCandidate, IceConnectionState, MediaKind, PeerConnectionFactory,
    PeerConnectionObserver, RtcConfiguration, RtcErrorKind, SdpType, SessionDescription,
    TransceiverDirection,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn main() {
    let factory = PeerConnectionFactory::builder().with_synthetic_adm().build().unwrap();

    // 1. A syntactically broken remote offer is a typed SDP rejection.
    let pc = factory
        .create_peer_connection(&RtcConfiguration::default(), PeerConnectionObserver::new())
        .unwrap();
    let err = pc
        .set_remote_description(&SessionDescription { kind: SdpType::Offer, sdp: "v=0\r\nnot sdp\r\n".into() })
        .unwrap_err();
    println!("broken offer -> {err:?}");
    assert!(matches!(
        err,
        Error::Rtc { operation: "set_remote_description", kind: RtcErrorKind::SyntaxError | RtcErrorKind::InvalidParameter, .. }
    ), "expected a typed SDP rejection");
    // An answer in the wrong signaling state is InvalidState, not a string.
    let offer_pc = factory
        .create_peer_connection(&RtcConfiguration::default(), PeerConnectionObserver::new())
        .unwrap();
    offer_pc.add_transceiver(MediaKind::Video, TransceiverDirection::RecvOnly).unwrap();
    let offer = offer_pc.create_offer().unwrap();
    let err = pc
        .set_remote_description(&SessionDescription { kind: SdpType::Answer, sdp: offer.sdp.clone() })
        .unwrap_err();
    println!("answer in stable state -> {err:?}");

    // 2. Loopback: MIDs on remote tracks, ICE connection states.
    let mids = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
    let ice_states = Arc::new(Mutex::new(Vec::<IceConnectionState>::new()));
    let off_ice = Arc::new(Mutex::new(VecDeque::<IceCandidate>::new()));
    let ans_ice = Arc::new(Mutex::new(VecDeque::<IceCandidate>::new()));
    let tracks = Arc::new(Mutex::new(Vec::new()));
    let off = factory
        .create_peer_connection(
            &RtcConfiguration::default(),
            PeerConnectionObserver::new()
                .on_ice_candidate({ let q = off_ice.clone(); move |c| q.lock().unwrap().push_back(c) })
                .on_ice_connection_state_change({ let s = ice_states.clone(); move |st| s.lock().unwrap().push(st) })
                .on_track_with_mid({
                    let m = mids.clone();
                    let t = tracks.clone();
                    move |track, mid| {
                        m.lock().unwrap().push((format!("{:?}", track.kind()), mid));
                        t.lock().unwrap().push(track);
                    }
                }),
        )
        .unwrap();
    for kind in [MediaKind::Video, MediaKind::Video, MediaKind::Audio] {
        off.add_transceiver(kind, TransceiverDirection::RecvOnly).unwrap();
    }
    let _dc = off.create_data_channel("control").unwrap();
    let offer = off.create_offer().unwrap();
    off.set_local_description(&offer).unwrap();
    let ans = factory
        .create_peer_connection(
            &RtcConfiguration::default(),
            PeerConnectionObserver::new().on_ice_candidate({ let q = ans_ice.clone(); move |c| q.lock().unwrap().push_back(c) }),
        )
        .unwrap();
    ans.set_remote_description(&offer).unwrap();
    let v1 = factory.create_video_track("v1").unwrap();
    let v2 = factory.create_video_track("v2").unwrap();
    let a1 = factory.create_audio_track("a1").unwrap();
    let mut vids = vec![&v1, &v2].into_iter();
    for t in ans.transceivers() {
        match t.kind() {
            MediaKind::Video => t.set_track(vids.next().unwrap()).unwrap(),
            MediaKind::Audio => t.set_track(&a1).unwrap(),
            _ => continue,
        }
        t.set_direction(TransceiverDirection::SendOnly).unwrap();
    }
    let answer = ans.create_answer().unwrap();
    ans.set_local_description(&answer).unwrap();
    off.set_remote_description(&answer).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        while let Some(c) = off_ice.lock().unwrap().pop_front() { let _ = ans.add_ice_candidate(&c); }
        while let Some(c) = ans_ice.lock().unwrap().pop_front() { let _ = off.add_ice_candidate(&c); }
        if ice_states.lock().unwrap().iter().any(|s| matches!(s, IceConnectionState::Connected | IceConnectionState::Completed)) {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let mids = mids.lock().unwrap().clone();
    println!("remote tracks with mid -> {mids:?}");
    println!("ice connection states -> {:?}", ice_states.lock().unwrap());
    assert_eq!(mids.len(), 3, "three remote tracks");
    assert!(mids.iter().all(|(_, mid)| mid.is_some()), "every remote track carries its MID");
    let mut distinct: Vec<_> = mids.iter().map(|(_, m)| m.clone()).collect();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), 3, "same-kind tracks are distinguishable");
    println!("ok");
}
