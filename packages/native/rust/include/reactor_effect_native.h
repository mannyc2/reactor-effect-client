#ifndef REACTOR_EFFECT_NATIVE_H
#define REACTOR_EFFECT_NATIVE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ReactorEffectPeer ReactorEffectPeer;

enum ReactorEffectStatus {
  REACTOR_EFFECT_OK = 0,
  REACTOR_EFFECT_AGAIN = 1,
  REACTOR_EFFECT_BUFFER_TOO_SMALL = 2,
  REACTOR_EFFECT_CLOSED = 3,
  REACTOR_EFFECT_INVALID = -1,
  REACTOR_EFFECT_NATIVE = -2,
  REACTOR_EFFECT_OVERFLOW = -3
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

uint32_t reactor_effect_abi_version(void);
/* Static source/build identity of this loaded image; never free the pointer. */
const char *reactor_effect_build_identity(void);

ReactorEffectPeer *reactor_effect_peer_create(void);

/*
 * request is UTF-8. The response is UTF-8 JSON on both success and failure.
 * Calls are serialized by the native peer owner and may block on libwebrtc.
 */
int reactor_effect_peer_call(
    ReactorEffectPeer *peer,
    uint32_t operation,
    const uint8_t *request,
    size_t request_len,
    uint8_t *response,
    size_t response_cap,
    size_t *response_len);

int reactor_effect_peer_send(
    ReactorEffectPeer *peer,
    uint32_t channel,
    const uint8_t *data,
    size_t data_len,
    uint8_t *error,
    size_t error_cap,
    size_t *error_len);

/*
 * Poll packets are [u32 little-endian header length][UTF-8 JSON header][payload].
 * A zero-capacity call returns BUFFER_TOO_SMALL with the required packet size and
 * retains that exact packet until copy or close. Retained packets count toward
 * item and byte bounds and cannot be evicted by producer pressure. Each queue
 * permits one reader across probe/copy; the host must serialize that pair.
 * timeout_ms == 0 is a nonblocking poll.
 */
int reactor_effect_peer_poll_event(
    ReactorEffectPeer *peer,
    uint32_t timeout_ms,
    uint8_t *out,
    size_t out_cap,
    size_t *out_len);

int reactor_effect_peer_poll_video(
    ReactorEffectPeer *peer,
    uint32_t timeout_ms,
    uint8_t *out,
    size_t out_cap,
    size_t *out_len);

int reactor_effect_peer_poll_audio(
    ReactorEffectPeer *peer,
    uint32_t timeout_ms,
    uint8_t *out,
    size_t out_cap,
    size_t *out_len);

/* Immediate admission fence. No callback/event can be admitted after return. */
void reactor_effect_peer_close(ReactorEffectPeer *peer);

/*
 * Drops channels/tracks/PeerConnection on the owner thread, waits for all native
 * callback guards to leave, then joins that owner. Safe to call more than once.
 */
int reactor_effect_peer_shutdown(
    ReactorEffectPeer *peer,
    uint8_t *error,
    size_t error_cap,
    size_t *error_len);

/*
 * Performs shutdown if needed, then frees the opaque handle. The host must
 * first join EVERY foreign call using this handle, including work still queued
 * in its FFI executor. Native owner shutdown alone does not establish that.
 */
void reactor_effect_peer_destroy(ReactorEffectPeer *peer);

#ifdef __cplusplus
}
#endif

#endif
