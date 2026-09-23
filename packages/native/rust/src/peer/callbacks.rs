//! libwebrtc callbacks. Each runs on a libwebrtc thread, is admitted through
//! the peer's callback gate, and only copies into the shared queues: none
//! calls or waits on the host.

use super::Shared;
use crate::abi::{Channel, MAX_MESSAGE_BYTES};
use crate::error::{BridgeError, Classify, FailureClass};
use crate::protocol::{Event, LocalCandidate, connection_state};
use reactor_webrtc::{
    DataChannel, DataChannelState, IceGatheringState, PeerConnection, PeerConnectionObserver,
};
use std::sync::Arc;

/// The connection's observer: state changes, local ICE candidates and remote
/// tracks become events.
pub(crate) fn observer(shared: &Arc<Shared>) -> PeerConnectionObserver {
    PeerConnectionObserver::new()
        .on_connection_state_change({
            let shared = Arc::clone(shared);
            move |state| {
                shared.admit(|| {
                    shared.emit(&Event::State {
                        state: connection_state(state),
                    });
                });
            }
        })
        .on_ice_gathering_change({
            let shared = Arc::clone(shared);
            move |state| {
                shared.admit(|| {
                    if state == IceGatheringState::Complete {
                        shared.emit(&Event::Ice { candidate: None });
                    }
                });
            }
        })
        .on_ice_candidate({
            let shared = Arc::clone(shared);
            move |candidate| {
                shared.admit(|| {
                    shared.emit(&Event::Ice {
                        candidate: Some(LocalCandidate::from(&candidate)),
                    });
                });
            }
        })
        .on_track({
            let shared = Arc::clone(shared);
            move |track| shared.admit(|| shared.accept_remote(track))
        })
}

/// Create one of the bridge's data channels and turn its messages and state
/// changes into events.
pub(crate) fn open_channel(
    peer: &PeerConnection,
    channel: Channel,
    shared: &Arc<Shared>,
) -> Result<DataChannel, BridgeError> {
    let mut data_channel = peer
        .create_data_channel(channel.label())
        .classify(FailureClass::Native, "create_data_channel")?;
    data_channel.on_message({
        let shared = Arc::clone(shared);
        move |bytes, binary| shared.admit(|| receive(&shared, channel, bytes, binary))
    });
    data_channel.on_state_change({
        let shared = Arc::clone(shared);
        move |state| {
            shared.admit(|| {
                let open = match state {
                    DataChannelState::Open => true,
                    DataChannelState::Closed => false,
                    DataChannelState::Connecting | DataChannelState::Closing => return,
                };
                shared.emit(&Event::Channel { channel, open });
            });
        }
    });
    Ok(data_channel)
}

/// Pass a message to the host. The bridge's channels carry binary messages
/// within the local bound; anything else fails the connection.
fn receive(shared: &Shared, channel: Channel, bytes: &[u8], binary: bool) {
    let label = channel.label();
    if !binary {
        shared.emit_error(
            FailureClass::Protocol,
            &format!("{label} data channel delivered a nonbinary message"),
        );
    } else if bytes.len() > MAX_MESSAGE_BYTES {
        shared.emit_error(
            FailureClass::Overflow,
            &format!("{label} data channel message exceeds local bound"),
        );
    } else {
        shared.emit(&Event::Message { channel, bytes });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::Status;
    use crate::sync::Taken;
    use crate::test_support::parse_packet;
    use serde_json::{Value, json};

    fn next_event(shared: &Shared) -> (Value, Vec<u8>) {
        let Taken::Item(packet) = shared.events.take(|_| true) else {
            panic!("an event must be queued");
        };
        let (header, payload) = parse_packet(&packet);
        (header, payload.to_vec())
    }

    #[test]
    fn a_binary_message_within_the_bound_reaches_the_host_unchanged() {
        let shared = Shared::new();
        let message = vec![7; MAX_MESSAGE_BYTES];
        receive(&shared, Channel::Control, &message, true);
        let (header, payload) = next_event(&shared);
        assert_eq!(header, json!({ "type": "message", "channel": "control" }));
        assert_eq!(payload, message);
    }

    #[test]
    fn a_text_or_oversized_message_fails_the_connection() {
        let shared = Shared::new();
        receive(&shared, Channel::Data, b"text", false);
        receive(
            &shared,
            Channel::Data,
            &vec![0; MAX_MESSAGE_BYTES + 1],
            true,
        );
        let (text, _) = next_event(&shared);
        assert_eq!(text["type"], "error");
        assert_eq!(text["status"], Status::Protocol.code());
        let (oversized, _) = next_event(&shared);
        assert_eq!(oversized["status"], Status::Overflow.code());
    }
}
