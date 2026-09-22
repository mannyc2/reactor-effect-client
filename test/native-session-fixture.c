#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

enum {
  STATUS_OK = 0,
  STATUS_AGAIN = 1,
  STATUS_BUFFER_TOO_SMALL = 2,
  STATUS_CLOSED = 3,
  STATUS_INVALID = -1
};

enum {
  CALL_PREPARE = 1,
  CALL_ANSWER = 2,
  CALL_DIRECTION = 3,
  CALL_MAX_BITRATE = 4,
  CALL_STATS = 5,
  CALL_MEDIA_SNAPSHOT = 6
};

typedef struct FixturePeer {
  atomic_int answered;
  atomic_int closed;
  atomic_int media_ready;
  atomic_int event_index;
  atomic_int video_sent;
  atomic_int audio_sent;
  atomic_int active_sends;
} FixturePeer;

static const char *PREPARED =
  "{\"sdp\":\"fixture native offer\",\"mapping\":["
  "{\"name\":\"main_video\",\"kind\":\"video\",\"direction\":\"recvonly\",\"mid\":\"0\"},"
  "{\"name\":\"main_audio\",\"kind\":\"audio\",\"direction\":\"recvonly\",\"mid\":\"1\"},"
  "{\"name\":\"input_audio\",\"kind\":\"audio\",\"direction\":\"sendonly\",\"mid\":\"2\"}]}";

static const char *SNAPSHOT =
  "{\"closed\":false,\"queuedControl\":0,\"queuedVideo\":0,\"queuedAudio\":0,"
  "\"queuedBytes\":0,\"droppedVideo\":\"0\",\"droppedAudio\":\"0\","
  "\"pendingRequests\":0,\"deliveredVideo\":\"0\",\"deliveredAudio\":\"0\"}";

static int copy_bytes(const uint8_t *bytes, size_t len, uint8_t *out, size_t cap, size_t *out_len) {
  if (out_len == NULL) return STATUS_INVALID;
  *out_len = len;
  if (cap < len || (len != 0 && out == NULL)) return STATUS_BUFFER_TOO_SMALL;
  if (len != 0) memcpy(out, bytes, len);
  return STATUS_OK;
}

static int copy_text(const char *text, uint8_t *out, size_t cap, size_t *out_len) {
  return copy_bytes((const uint8_t *)text, strlen(text), out, cap, out_len);
}

static size_t make_packet(const char *header, const uint8_t *payload, size_t payload_len, uint8_t *out, size_t cap) {
  const size_t header_len = strlen(header);
  const size_t needed = 4 + header_len + payload_len;
  if (out == NULL || cap < needed) return needed;
  const uint32_t n = (uint32_t)header_len;
  out[0] = (uint8_t)(n & 0xff);
  out[1] = (uint8_t)((n >> 8) & 0xff);
  out[2] = (uint8_t)((n >> 16) & 0xff);
  out[3] = (uint8_t)((n >> 24) & 0xff);
  memcpy(out + 4, header, header_len);
  if (payload_len != 0) memcpy(out + 4 + header_len, payload, payload_len);
  return needed;
}

static int poll_packet(FixturePeer *peer, const char *header, const uint8_t *payload, size_t payload_len,
                       uint8_t *out, size_t cap, size_t *out_len, atomic_int *sent) {
  if (peer == NULL || out_len == NULL) return STATUS_INVALID;
  if (atomic_load(&peer->closed)) { *out_len = 0; return STATUS_CLOSED; }
  if (sent != NULL && atomic_load(sent)) { *out_len = 0; return STATUS_AGAIN; }
  const size_t needed = make_packet(header, payload, payload_len, NULL, 0);
  *out_len = needed;
  if (cap < needed || out == NULL) return STATUS_BUFFER_TOO_SMALL;
  make_packet(header, payload, payload_len, out, cap);
  if (sent != NULL) atomic_store(sent, 1);
  return STATUS_OK;
}

uint32_t reactor_effect_abi_version(void) { return 1; }

void *reactor_effect_peer_create(void) {
  return calloc(1, sizeof(FixturePeer));
}

int reactor_effect_peer_call(void *raw, uint32_t operation, const uint8_t *request, size_t request_len,
                             uint8_t *response, size_t response_cap, size_t *response_len) {
  (void)request;
  (void)request_len;
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL || response_len == NULL) return STATUS_INVALID;
  if (atomic_load(&peer->closed)) return copy_text("{\"code\":\"Closed\",\"message\":\"fixture closed\"}", response, response_cap, response_len) == STATUS_OK ? STATUS_CLOSED : STATUS_BUFFER_TOO_SMALL;
  switch (operation) {
    case CALL_PREPARE: return copy_text(PREPARED, response, response_cap, response_len);
    case CALL_ANSWER:
      atomic_store(&peer->answered, 1);
      return copy_text("{}", response, response_cap, response_len);
    case CALL_DIRECTION:
    case CALL_MAX_BITRATE:
      return copy_text("{}", response, response_cap, response_len);
    case CALL_STATS:
      return copy_text("[]", response, response_cap, response_len);
    case CALL_MEDIA_SNAPSHOT:
      atomic_store(&peer->media_ready, 1);
      return copy_text(SNAPSHOT, response, response_cap, response_len);
    default:
      return copy_text("{\"code\":\"InvalidInput\",\"message\":\"unknown fixture call\"}", response, response_cap, response_len) == STATUS_OK ? STATUS_INVALID : STATUS_BUFFER_TOO_SMALL;
  }
}

int reactor_effect_peer_send(void *raw, uint32_t channel, const uint8_t *data, size_t data_len,
                             uint8_t *error, size_t error_cap, size_t *error_len) {
  (void)data;
  (void)data_len;
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL || error_len == NULL) return STATUS_INVALID;
  if (atomic_load(&peer->closed)) return copy_text("{\"code\":\"Closed\",\"message\":\"fixture closed\"}", error, error_cap, error_len) == STATUS_OK ? STATUS_CLOSED : STATUS_BUFFER_TOO_SMALL;
  if (channel == 1) {
    atomic_fetch_add(&peer->active_sends, 1);
    usleep(50000);
    atomic_fetch_sub(&peer->active_sends, 1);
  }
  *error_len = 0;
  return STATUS_OK;
}

int reactor_effect_peer_poll_event(void *raw, uint32_t timeout_ms, uint8_t *out, size_t cap, size_t *out_len) {
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL || out_len == NULL) return STATUS_INVALID;
  if (atomic_load(&peer->closed)) { *out_len = 0; return STATUS_CLOSED; }
  if (!atomic_load(&peer->answered)) {
    if (timeout_ms != 0) usleep((useconds_t)(timeout_ms > 5 ? 5000 : timeout_ms * 1000));
    *out_len = 0;
    return STATUS_AGAIN;
  }
  const char *events[] = {
    "{\"type\":\"state\",\"state\":\"connected\"}",
    "{\"type\":\"channel\",\"channel\":\"control\",\"open\":true}",
    "{\"type\":\"channel\",\"channel\":\"data\",\"open\":true}"
  };
  const int index = atomic_load(&peer->event_index);
  if (index >= 3) { *out_len = 0; return STATUS_AGAIN; }
  const size_t needed = make_packet(events[index], NULL, 0, NULL, 0);
  *out_len = needed;
  if (cap < needed || out == NULL) return STATUS_BUFFER_TOO_SMALL;
  make_packet(events[index], NULL, 0, out, cap);
  atomic_fetch_add(&peer->event_index, 1);
  return STATUS_OK;
}

int reactor_effect_peer_poll_video(void *raw, uint32_t timeout_ms, uint8_t *out, size_t cap, size_t *out_len) {
  (void)timeout_ms;
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL || out_len == NULL) return STATUS_INVALID;
  if (!atomic_load(&peer->media_ready)) { *out_len = 0; return atomic_load(&peer->closed) ? STATUS_CLOSED : STATUS_AGAIN; }
  static const uint8_t payload[] = {1, 2, 3, 4, 9, 8, 7};
  return poll_packet(peer,
    "{\"type\":\"video\",\"format\":\"BGRA\",\"track\":\"main_video\",\"width\":1,\"height\":1,"
    "\"dataLength\":4,\"metadataLength\":3,\"frameId\":\"18446744073709551615\",\"timestampMicros\":\"9007199254740993\"}",
    payload, sizeof(payload), out, cap, out_len, &peer->video_sent);
}

int reactor_effect_peer_poll_audio(void *raw, uint32_t timeout_ms, uint8_t *out, size_t cap, size_t *out_len) {
  (void)timeout_ms;
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL || out_len == NULL) return STATUS_INVALID;
  if (!atomic_load(&peer->media_ready)) { *out_len = 0; return atomic_load(&peer->closed) ? STATUS_CLOSED : STATUS_AGAIN; }
  static const uint8_t payload[] = {1, 0, 0xfe, 0xff, 0x2c, 0x01, 0x70, 0xfe};
  return poll_packet(peer,
    "{\"type\":\"audio\",\"format\":\"s16le\",\"track\":\"main_audio\",\"sampleRate\":48000,\"channels\":2,\"samples\":4}",
    payload, sizeof(payload), out, cap, out_len, &peer->audio_sent);
}

void reactor_effect_peer_close(void *raw) {
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer != NULL) atomic_store(&peer->closed, 1);
}

int reactor_effect_peer_shutdown(void *raw, uint8_t *error, size_t error_cap, size_t *error_len) {
  (void)error;
  (void)error_cap;
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL || error_len == NULL) return STATUS_INVALID;
  atomic_store(&peer->closed, 1);
  while (atomic_load(&peer->active_sends) != 0) usleep(1000);
  *error_len = 0;
  return STATUS_OK;
}

void reactor_effect_peer_destroy(void *raw) {
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL) return;
  atomic_store(&peer->closed, 1);
  while (atomic_load(&peer->active_sends) != 0) usleep(1000);
  free(peer);
}
