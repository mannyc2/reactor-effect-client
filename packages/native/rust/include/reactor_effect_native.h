#ifndef REACTOR_EFFECT_NATIVE_H
#define REACTOR_EFFECT_NATIVE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ABI 3. Every supported host is little-endian; headers are native-endian. */

typedef struct ReactorEffectPeer ReactorEffectPeer;

/*
 * Non-negative statuses are outcomes. Negative statuses are failure classes;
 * the set is closed for this ABI, and a host maps each class to its own error
 * type. Diagnostic text travels beside a class in ReactorEffectFailure.
 *
 * A misaligned pointer argument fails with INVALID_INPUT without being used.
 * A request over 1 MiB or a message over 256 KiB fails with OVERFLOW without
 * being read.
 */
enum ReactorEffectStatus {
  REACTOR_EFFECT_OK = 0,
  REACTOR_EFFECT_AGAIN = 1,            /* take: the queue is empty */
  REACTOR_EFFECT_BUFFER_TOO_SMALL = 2, /* sizes written; the item stays queued */
  REACTOR_EFFECT_CLOSED = 3,           /* the peer is fenced or shut down */
  REACTOR_EFFECT_INVALID_INPUT = -1,   /* an argument or request was rejected */
  REACTOR_EFFECT_NATIVE = -2,          /* an unclassified libwebrtc or bridge failure */
  REACTOR_EFFECT_OVERFLOW = -3,        /* a queue, buffer or message bound was exceeded */
  REACTOR_EFFECT_PROTOCOL = -4,        /* the remote peer broke the negotiated contract */
  REACTOR_EFFECT_SDP_REJECTED = -5,    /* libwebrtc refused to create or apply an SDP */
  REACTOR_EFFECT_CHANNEL_CLOSED = -6   /* the data channel is not open */
};

enum ReactorEffectCall {
  REACTOR_EFFECT_PREPARE = 1,
  REACTOR_EFFECT_ANSWER = 2,
  REACTOR_EFFECT_DIRECTION = 3,
  REACTOR_EFFECT_MAX_BITRATE = 4,
  REACTOR_EFFECT_STATS = 5,
  REACTOR_EFFECT_MEDIA_SNAPSHOT = 6
};

enum ReactorEffectChannel {
  REACTOR_EFFECT_CONTROL = 0,
  REACTOR_EFFECT_DATA = 1
};

/* Readiness bits passed to ReactorEffectNotify. */
enum ReactorEffectReady {
  REACTOR_EFFECT_READY_EVENTS = 1,
  REACTOR_EFFECT_READY_VIDEO = 2,
  REACTOR_EFFECT_READY_AUDIO = 4
};

/* Diagnostic text for a failure status. Never match on it: the status is the class. */
typedef struct ReactorEffectFailure {
  uint32_t message_len;
  uint8_t message[1020]; /* UTF-8, truncated on a character boundary */
} ReactorEffectFailure;

typedef struct ReactorEffectVideoHeader {
  uint32_t width;
  uint32_t height;
  uint32_t data_len;     /* BGRA bytes: width * height * 4 */
  uint32_t metadata_len; /* frame-metadata user data bytes */
  uint64_t frame_id;     /* 0 when the sender supplied none */
  uint64_t timestamp_us; /* sender capture time; 0 when absent */
  uint32_t track;        /* index into the prepare request's tracks */
  uint32_t reserved;
} ReactorEffectVideoHeader; /* 40 bytes */

typedef struct ReactorEffectAudioHeader {
  uint32_t sample_rate;
  uint32_t channels;
  uint32_t samples; /* interleaved int16_t samples: frames * channels */
  uint32_t track;   /* index into the prepare request's tracks */
} ReactorEffectAudioHeader; /* 16 bytes */

/*
 * Runs on the peer's notifier thread with the readiness bits of every queue
 * that received an item since the previous call; a host must drain each named
 * queue until AGAIN. The callback may call the take functions. libwebrtc
 * threads never run it and never wait for it: while it has not returned, they
 * keep queueing and the bounded queues count what they evict.
 */
typedef void (*ReactorEffectNotify)(uint32_t ready);

uint32_t reactor_effect_abi_version(void);
/* Static source/build identity of this loaded image; never free the pointer. */
const char *reactor_effect_build_identity(void);

/*
 * notify may be NULL: no notifier thread runs and the host polls the take
 * functions itself. Every peer shares one process-wide libwebrtc factory,
 * created on the first prepare and never destroyed.
 */
ReactorEffectPeer *reactor_effect_peer_create(ReactorEffectNotify notify);

/*
 * request is UTF-8. On OK the response is UTF-8 JSON and response_cap must be
 * at least 4 MiB (BUFFER_TOO_SMALL reports that size). Calls are serialized by
 * the native peer owner and may block on libwebrtc. failure may be NULL.
 */
int reactor_effect_peer_call(
    ReactorEffectPeer *peer,
    uint32_t operation,
    const uint8_t *request,
    size_t request_len,
    uint8_t *response,
    size_t response_cap,
    size_t *response_len,
    ReactorEffectFailure *failure);

int reactor_effect_peer_send(
    ReactorEffectPeer *peer,
    uint32_t channel,
    const uint8_t *data,
    size_t data_len,
    ReactorEffectFailure *failure);

/*
 * Nonblocking takes. Each copies its queue's oldest item into caller memory
 * once and removes it. BUFFER_TOO_SMALL writes the required size (out_len or
 * the header) and keeps the item at the front; AGAIN means the queue is empty.
 * Events are [u32 little-endian header length][UTF-8 JSON header][payload].
 */
int reactor_effect_peer_take_event(
    ReactorEffectPeer *peer,
    uint8_t *out,
    size_t out_cap,
    size_t *out_len);

int reactor_effect_peer_take_video(
    ReactorEffectPeer *peer,
    ReactorEffectVideoHeader *header,
    uint8_t *bgra,
    size_t bgra_cap,
    uint8_t *metadata,
    size_t metadata_cap);

/* pcm_cap counts int16_t samples, not bytes. */
int reactor_effect_peer_take_audio(
    ReactorEffectPeer *peer,
    ReactorEffectAudioHeader *header,
    int16_t *pcm,
    size_t pcm_cap);

/* Immediate admission fence. No callback/event can be admitted after return. */
void reactor_effect_peer_close(ReactorEffectPeer *peer);

/*
 * Drops channels/tracks/PeerConnection on the owner thread, waits for all
 * native callback guards to leave, then joins that owner and the notifier
 * thread. The notifier may be waiting for the host to run ReactorEffectNotify,
 * so never call this from the thread that runs that callback. Safe to call
 * more than once. failure may be NULL.
 */
int reactor_effect_peer_shutdown(ReactorEffectPeer *peer, ReactorEffectFailure *failure);

/*
 * Performs shutdown if needed (so the same thread rule applies), then frees
 * the opaque handle. The host must first join EVERY foreign call using this
 * handle, including work still queued in its FFI executor. Native owner
 * shutdown alone does not establish that.
 */
void reactor_effect_peer_destroy(ReactorEffectPeer *peer);

#ifdef __cplusplus
}
#endif

#endif
