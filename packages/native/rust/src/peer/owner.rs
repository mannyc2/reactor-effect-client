//! The owner thread: the one thread that holds a peer's libwebrtc objects.

use super::{Shared, callbacks};
use crate::abi::{Channel, MAX_BUFFERED_SEND_BYTES, Operation};
use crate::error::{BridgeError, Classify, FailureClass};
use crate::protocol::{
    self, BitrateRequest, Direction, DirectionRequest, Mapping, PrepareRequest, PrepareResponse,
    stats_json,
};
use crate::sync::lock;
use reactor_webrtc::{
    DataChannel, DataChannelState, PeerConnection, PeerConnectionFactory, RtcConfiguration,
    SdpType, SessionDescription, Transceiver, TransceiverDirection,
};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};

#[cfg(test)]
mod tests;

/// Where the owner thread sends one command's result.
pub(crate) type Reply<T> = SyncSender<Result<T, BridgeError>>;

/// Work for the owner thread, which runs commands one at a time.
pub(crate) enum Command {
    Call {
        operation: Operation,
        request: Vec<u8>,
        reply: Reply<Vec<u8>>,
    },
    Send {
        channel: Channel,
        bytes: Vec<u8>,
        reply: Reply<()>,
    },
    Shutdown,
}

/// libwebrtc's threads are process-global, so reactor-webrtc requires one
/// factory per process. Every peer shares this one; it is created on first
/// use and never destroyed. A `Mutex` rather than a `OnceLock` lets a failed
/// creation be retried.
static FACTORY: Mutex<Option<&'static PeerConnectionFactory>> = Mutex::new(None);

/// The process's libwebrtc factory.
pub(crate) fn factory() -> Result<&'static PeerConnectionFactory, BridgeError> {
    let mut slot = lock(&FACTORY);
    if let Some(factory) = *slot {
        return Ok(factory);
    }
    let factory = PeerConnectionFactory::builder()
        .with_synthetic_adm()
        .build()
        .classify(FailureClass::Native, "create_factory")?;
    let factory: &'static PeerConnectionFactory = Box::leak(Box::new(factory));
    *slot = Some(factory);
    Ok(factory)
}

/// A negotiated connection and the libwebrtc objects created with it.
struct Connection {
    peer: PeerConnection,
    control: DataChannel,
    data: DataChannel,
    tracks: HashMap<String, Track>,
}

impl Connection {
    fn channel(&self, channel: Channel) -> &DataChannel {
        match channel {
            Channel::Control => &self.control,
            Channel::Data => &self.data,
        }
    }
}

/// A declared track's transceiver.
struct Track {
    direction: Direction,
    transceiver: Transceiver,
}

/// The owner thread's state.
pub(crate) struct Owner {
    shared: Arc<Shared>,
    /// Set by `Prepare`.
    connection: Option<Connection>,
}

impl Owner {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            connection: None,
        }
    }

    /// Run commands until shutdown, then release the libwebrtc objects and
    /// wait for every admitted callback to return.
    pub(crate) fn run(mut self, commands: &Receiver<Command>) {
        while let Ok(command) = commands.recv() {
            match command {
                Command::Call {
                    operation,
                    request,
                    reply,
                } => {
                    let result = self.unless_closed(|owner| owner.call(operation, &request));
                    // The peer waits for every reply; only a panic unwinding
                    // it can have dropped the receiver.
                    let _ = reply.send(result);
                }
                Command::Send {
                    channel,
                    bytes,
                    reply,
                } => {
                    let result = self.unless_closed(|owner| owner.send(channel, &bytes));
                    let _ = reply.send(result);
                }
                Command::Shutdown => break,
            }
        }
        self.shutdown();
    }

    /// Run a command, unless the peer closed while it waited in the queue.
    fn unless_closed<T>(
        &mut self,
        command: impl FnOnce(&mut Self) -> Result<T, BridgeError>,
    ) -> Result<T, BridgeError> {
        if self.shared.gate.is_open() {
            command(self)
        } else {
            Err(BridgeError::closed())
        }
    }

    fn call(&mut self, operation: Operation, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        match operation {
            Operation::Prepare => self.prepare(request),
            Operation::Answer => self.answer(request),
            Operation::Direction => self.set_direction(request),
            Operation::MaxBitrate => self.set_max_bitrate(request),
            Operation::Stats => self.stats(),
            // The peer answers snapshots itself so they never wait behind a
            // blocking call; answering one here as well keeps this total.
            Operation::MediaSnapshot => protocol::encode(&self.shared.snapshot()),
        }
    }

    /// Create the connection, its data channels and a transceiver per
    /// declared track, and return the local offer with each track's MID.
    fn prepare(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        if self.connection.is_some() {
            return Err(BridgeError::invalid("native peer is already prepared"));
        }
        let request = PrepareRequest::parse(request)?;
        let config = RtcConfiguration {
            ice_servers: request.servers.iter().map(Into::into).collect(),
            ..RtcConfiguration::default()
        };
        let peer = factory()?
            .create_peer_connection(&config, callbacks::observer(&self.shared))
            .classify(FailureClass::Native, "create_peer_connection")?;
        let control = callbacks::open_channel(&peer, Channel::Control, &self.shared)?;
        let data = callbacks::open_channel(&peer, Channel::Data, &self.shared)?;
        let transceivers = request
            .tracks
            .iter()
            .map(|track| {
                peer.add_transceiver(track.kind.into(), track.direction.into())
                    .classify(FailureClass::Native, "add_transceiver")
            })
            .collect::<Result<Vec<_>, _>>()?;

        let offer = peer
            .create_offer()
            .classify(FailureClass::SdpRejected, "create_offer")?;
        peer.set_local_description(&offer)
            .classify(FailureClass::SdpRejected, "set_local_description")?;

        // Transceivers have their MIDs once the local description is set.
        let mapping = request
            .tracks
            .iter()
            .zip(&transceivers)
            .map(|(track, transceiver)| {
                let mid = transceiver.mid().ok_or_else(|| {
                    BridgeError::new(
                        FailureClass::Native,
                        format!("missing MID after local description: {}", track.name),
                    )
                })?;
                Ok(Mapping {
                    name: track.name.clone(),
                    kind: track.kind,
                    direction: track.direction,
                    mid,
                })
            })
            .collect::<Result<Vec<_>, BridgeError>>()?;
        let response = protocol::encode(&PrepareResponse {
            sdp: &offer.sdp,
            mapping: &mapping,
        })?;
        self.shared.set_bindings(&mapping);

        let tracks = request
            .tracks
            .into_iter()
            .zip(transceivers)
            .map(|(track, transceiver)| {
                let track_state = Track {
                    direction: track.direction,
                    transceiver,
                };
                (track.name, track_state)
            })
            .collect();
        self.connection = Some(Connection {
            peer,
            control,
            data,
            tracks,
        });
        Ok(response)
    }

    /// Apply the remote answer, which carries the remote peer's candidates.
    fn answer(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        let sdp = std::str::from_utf8(request)
            .map_err(|_| BridgeError::invalid("answer SDP is not UTF-8"))?;
        if sdp.is_empty() {
            return Err(BridgeError::invalid("answer SDP is empty"));
        }
        let answer = SessionDescription {
            kind: SdpType::Answer,
            sdp: sdp.to_owned(),
        };
        self.connection()?
            .peer
            .set_remote_description(&answer)
            .classify(FailureClass::SdpRejected, "set_remote_description")?;
        Ok(protocol::empty_response())
    }

    /// Pause a declared track, or resume it in its declared direction.
    fn set_direction(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        let request: DirectionRequest = protocol::decode(request)?;
        let track = self.track(&request.name)?;
        let direction = if request.active {
            track.direction.into()
        } else {
            TransceiverDirection::Inactive
        };
        track
            .transceiver
            .set_direction(direction)
            .classify(FailureClass::Native, "set_direction")?;
        Ok(protocol::empty_response())
    }

    /// Cap an outgoing track's send bitrate.
    fn set_max_bitrate(&mut self, request: &[u8]) -> Result<Vec<u8>, BridgeError> {
        let request = BitrateRequest::parse(request)?;
        let track = self.track(&request.name)?;
        if track.direction != Direction::SendOnly {
            return Err(BridgeError::invalid(format!(
                "{} is not an outgoing track",
                request.name
            )));
        }
        track
            .transceiver
            .set_send_bitrate(None, Some(request.bits_per_second))
            .classify(FailureClass::Native, "set_send_bitrate")?;
        Ok(protocol::empty_response())
    }

    fn stats(&mut self) -> Result<Vec<u8>, BridgeError> {
        let report = self
            .connection()?
            .peer
            .get_stats()
            .classify(FailureClass::Native, "get_stats")?;
        protocol::encode(&stats_json(&report))
    }

    /// Send one binary message on an open bridge channel, unless it would
    /// push the channel's unsent bytes past their bound.
    fn send(&mut self, channel: Channel, bytes: &[u8]) -> Result<(), BridgeError> {
        let open = self
            .connection
            .as_ref()
            .map(|connection| connection.channel(channel))
            .filter(|data_channel| data_channel.state() == DataChannelState::Open);
        let Some(data_channel) = open else {
            return Err(BridgeError::new(
                FailureClass::ChannelClosed,
                format!("{} channel is not open", channel.label()),
            ));
        };
        let buffered = data_channel.buffered_amount();
        if buffered.saturating_add(bytes.len() as u64) > MAX_BUFFERED_SEND_BYTES {
            return Err(BridgeError::overflow(
                "native data channel buffered amount bound exceeded",
            ));
        }
        data_channel
            .send(bytes, true)
            .classify(FailureClass::Native, "send")
    }

    fn connection(&self) -> Result<&Connection, BridgeError> {
        self.connection.as_ref().ok_or_else(BridgeError::closed)
    }

    fn track(&self, name: &str) -> Result<&Track, BridgeError> {
        self.connection
            .as_ref()
            .and_then(|connection| connection.tracks.get(name))
            .ok_or_else(|| BridgeError::invalid(format!("unknown track: {name}")))
    }

    /// Release the libwebrtc objects children first, wait for every admitted
    /// callback to return, then close the queues.
    fn shutdown(self) {
        if let Some(Connection {
            peer,
            control,
            data,
            tracks,
        }) = self.connection
        {
            drop((control, data));
            self.shared.release_remote_tracks();
            drop(tracks);
            drop(peer);
        }
        self.shared.gate.wait_idle();
        self.shared.close_queues();
    }
}
