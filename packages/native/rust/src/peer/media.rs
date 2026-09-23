//! Decoded media: the items the media queues hold, and the sinks that copy
//! each frame libwebrtc decodes into them.

use super::Shared;
use crate::ffi::{ReactorEffectAudioHeader, ReactorEffectVideoHeader};
use crate::protocol::TrackKind;
use crate::sync::QueueItem;
use reactor_webrtc::{AudioFrame, RemoteTrack, VideoFrame};
use std::sync::Arc;

/// One decoded BGRA frame and its sender metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VideoItem {
    /// The track's index in the prepare request.
    pub(crate) track: u32,
    pub(crate) width: u32,
    pub(crate) height: u32,
    /// 0 when the sender supplied none.
    pub(crate) frame_id: u64,
    /// The sender's capture time; 0 when absent.
    pub(crate) timestamp_us: u64,
    pub(crate) bgra: Vec<u8>,
    pub(crate) metadata: Vec<u8>,
}

impl VideoItem {
    /// Copy a frame that libwebrtc lends only for the duration of its
    /// callback. This is the one native copy of each frame.
    fn copy(track: u32, frame: VideoFrame<'_>) -> Self {
        let (frame_id, timestamp_us, metadata) = frame
            .metadata
            .map(|metadata| {
                (
                    metadata.frame_id,
                    metadata.capture_time_us,
                    metadata.user_data,
                )
            })
            .unwrap_or_default();
        Self {
            track,
            width: frame.width,
            height: frame.height,
            frame_id,
            timestamp_us,
            bgra: frame.bgra.to_vec(),
            metadata,
        }
    }

    pub(crate) fn header(&self) -> ReactorEffectVideoHeader {
        ReactorEffectVideoHeader {
            width: self.width,
            height: self.height,
            data_len: queued_len(self.bgra.len()),
            metadata_len: queued_len(self.metadata.len()),
            frame_id: self.frame_id,
            timestamp_us: self.timestamp_us,
            track: self.track,
            reserved: 0,
        }
    }
}

impl QueueItem for VideoItem {
    fn byte_len(&self) -> usize {
        self.bgra.len() + self.metadata.len()
    }
}

/// One block of decoded, interleaved PCM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudioItem {
    /// The track's index in the prepare request.
    pub(crate) track: u32,
    pub(crate) sample_rate: u32,
    pub(crate) channels: u32,
    pub(crate) pcm: Vec<i16>,
}

impl AudioItem {
    /// Copy a block that libwebrtc lends only for the duration of its callback.
    fn copy(track: u32, frame: &AudioFrame<'_>) -> Self {
        Self {
            track,
            sample_rate: frame.sample_rate,
            channels: frame.channels,
            pcm: frame.pcm.to_vec(),
        }
    }

    pub(crate) fn header(&self) -> ReactorEffectAudioHeader {
        ReactorEffectAudioHeader {
            sample_rate: self.sample_rate,
            channels: self.channels,
            samples: queued_len(self.pcm.len()),
            track: self.track,
        }
    }
}

impl QueueItem for AudioItem {
    fn byte_len(&self) -> usize {
        size_of_val(self.pcm.as_slice())
    }
}

/// A length for a C header. Only queued items get headers, and every queue's
/// byte bound is far below 4 GiB.
fn queued_len(len: usize) -> u32 {
    u32::try_from(len).expect("a queued item is smaller than its queue's byte bound")
}

pub(crate) fn kind_of(track: &RemoteTrack) -> TrackKind {
    match track {
        RemoteTrack::Video(_) => TrackKind::Video,
        RemoteTrack::Audio(_) => TrackKind::Audio,
    }
}

/// Copy a remote track's decoded frames into the media queues, labelled with
/// the declared track's index.
pub(crate) fn route(track: &RemoteTrack, index: u32, shared: &Arc<Shared>) {
    let shared = Arc::clone(shared);
    match track {
        RemoteTrack::Video(video) => video.on_frame(move |frame| {
            shared.admit(|| shared.push_video(VideoItem::copy(index, frame)));
        }),
        RemoteTrack::Audio(audio) => audio.on_frame(move |frame| {
            shared.admit(|| shared.push_audio(AudioItem::copy(index, &frame)));
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_items_count_their_payload_bytes() {
        let video = VideoItem {
            track: 0,
            width: 2,
            height: 1,
            frame_id: 0,
            timestamp_us: 0,
            bgra: vec![0; 8],
            metadata: vec![0; 3],
        };
        let audio = AudioItem {
            track: 1,
            sample_rate: 48_000,
            channels: 2,
            pcm: vec![0; 480],
        };
        assert_eq!(video.byte_len(), 11);
        assert_eq!(audio.byte_len(), 960, "samples are two bytes each");
    }

    #[test]
    fn headers_describe_the_queued_payload() {
        let video = VideoItem {
            track: 3,
            width: 2,
            height: 1,
            frame_id: u64::MAX,
            timestamp_us: 9_007_199_254_740_993,
            bgra: vec![0x21; 8],
            metadata: b"meta".to_vec(),
        };
        assert_eq!(
            video.header(),
            ReactorEffectVideoHeader {
                width: 2,
                height: 1,
                data_len: 8,
                metadata_len: 4,
                frame_id: u64::MAX,
                timestamp_us: 9_007_199_254_740_993,
                track: 3,
                reserved: 0,
            }
        );
        let audio = AudioItem {
            track: 1,
            sample_rate: 48_000,
            channels: 2,
            pcm: vec![0; 960],
        };
        assert_eq!(
            audio.header(),
            ReactorEffectAudioHeader {
                sample_rate: 48_000,
                channels: 2,
                samples: 960,
                track: 1,
            }
        );
    }
}
