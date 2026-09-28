//! The owner thread: the one thread that holds a peer's libwebrtc objects.

use super::{Shared, callbacks};
use crate::error::{BridgeError, Classify, FailureClass};
use crate::protocol::{
    BitrateRequest, Channel, Direction, MAX_BUFFERED_SEND_BYTES, Mapping, PrepareRequest,
    stats_json,
};
use crate::sync::lock;
use reactor_webrtc::{
    DataChannel, DataChannelState, PeerConnection, PeerConnectionFactory, RtcConfiguration,
    SdpType, SessionDescription, Transceiver, TransceiverDirection,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

#[cfg(test)]
mod tests;

/// Where a command's result goes. It runs exactly once, on the owner thread,
/// and must not wait: the addon's completion only queues a promise settlement.
pub(crate) type Done<T> = Box<dyn FnOnce(Result<T, BridgeError>) + Send>;

/// A prepared connection's local offer and each declared track's mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Prepared {
    pub(crate) sdp: String,
    pub(crate) mapping: Vec<Mapping>,
}

/// Work for the owner thread, which runs commands one at a time.
pub(crate) enum Command {
    Prepare {
        request: PrepareRequest,
        done: Done<Prepared>,
    },
    Answer {
        sdp: String,
        done: Done<()>,
    },
    Direction {
        name: String,
        active: bool,
        done: Done<()>,
    },
    MaxBitrate {
        request: BitrateRequest,
        done: Done<()>,
    },
    Stats {
        done: Done<Value>,
    },
    Send {
        channel: Channel,
        bytes: Vec<u8>,
        done: Done<()>,
    },
    Shutdown,
}

impl Command {
    /// Refuse the command, which then never reaches the owner thread.
    pub(crate) fn refuse(self, error: BridgeError) {
        match self {
            Self::Prepare { done, .. } => done(Err(error)),
            Self::Stats { done } => done(Err(error)),
            Self::Answer { done, .. }
            | Self::Direction { done, .. }
            | Self::MaxBitrate { done, .. }
            | Self::Send { done, .. } => done(Err(error)),
            Self::Shutdown => {}
        }
    }
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
            if !matches!(command, Command::Shutdown) {
                // The peer counted it on submission; it is answered below.
                self.shared.in_flight.fetch_sub(1, Ordering::AcqRel);
            }
            match command {
                Command::Prepare { request, done } => {
                    done(self.unless_closed(|owner| owner.prepare(request)));
                }
                Command::Answer { sdp, done } => {
                    done(self.unless_closed(|owner| owner.answer(sdp)));
                }
                Command::Direction { name, active, done } => {
                    done(self.unless_closed(|owner| owner.set_direction(&name, active)));
                }
                Command::MaxBitrate { request, done } => {
                    done(self.unless_closed(|owner| owner.set_max_bitrate(&request)));
                }
                Command::Stats { done } => done(self.unless_closed(|owner| owner.stats())),
                Command::Send {
                    channel,
                    bytes,
                    done,
                } => done(self.unless_closed(|owner| owner.send(channel, &bytes))),
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

    /// Create the connection, its data channels and a transceiver per
    /// declared track, and return the local offer with each track's MID.
    fn prepare(&mut self, request: PrepareRequest) -> Result<Prepared, BridgeError> {
        if self.connection.is_some() {
            return Err(BridgeError::invalid("native peer is already prepared"));
        }
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
        Ok(Prepared {
            sdp: offer.sdp,
            mapping,
        })
    }

    /// Apply the remote answer, which carries the remote peer's candidates.
    fn answer(&self, sdp: String) -> Result<(), BridgeError> {
        if sdp.is_empty() {
            return Err(BridgeError::invalid("answer SDP is empty"));
        }
        let answer = SessionDescription {
            kind: SdpType::Answer,
            sdp,
        };
        self.connection()?
            .peer
            .set_remote_description(&answer)
            .classify(FailureClass::SdpRejected, "set_remote_description")
    }

    /// Pause a declared track, or resume it in its declared direction.
    fn set_direction(&self, name: &str, active: bool) -> Result<(), BridgeError> {
        let track = self.track(name)?;
        let direction = if active {
            track.direction.into()
        } else {
            TransceiverDirection::Inactive
        };
        track
            .transceiver
            .set_direction(direction)
            .classify(FailureClass::Native, "set_direction")
    }

    /// Cap an outgoing track's send bitrate.
    fn set_max_bitrate(&self, request: &BitrateRequest) -> Result<(), BridgeError> {
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
            .classify(FailureClass::Native, "set_send_bitrate")
    }

    fn stats(&self) -> Result<Value, BridgeError> {
        let report = self
            .connection()?
            .peer
            .get_stats()
            .classify(FailureClass::Native, "get_stats")?;
        Ok(stats_json(&report))
    }

    /// Send one binary message on an open bridge channel, unless it would
    /// push the channel's unsent bytes past their bound.
    fn send(&self, channel: Channel, bytes: &[u8]) -> Result<(), BridgeError> {
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
