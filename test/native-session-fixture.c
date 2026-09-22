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
  atomic_int destroyed;
  uint8_t retained[1024];
  size_t retained_len;
  uint8_t queued[2][1024];
  size_t queued_len[2];
  size_t queue_count;
} FixturePeer;

/* Explicit test controls. Tombstones let the harness detect unsafe destruction
 * order without deliberately dereferencing freed memory or claiming corruption. */
static FixturePeer *controlled_peer;
static atomic_int lifetime_enabled, lifetime_expected, lifetime_entered, lifetime_completed;
static atomic_int lifetime_release_others, lifetime_release_held, lifetime_shutdowns;
static atomic_int lifetime_destroyed, lifetime_destroy_pending, lifetime_late, lifetime_changed_input;
static int packet_enabled, packet_first_size, packet_evictions;
static int media_fault;

void fixture_media_fault(int enabled) { media_fault = enabled; }

void fixture_lifetime_begin(int expected) {
  atomic_store(&lifetime_expected, expected);
  atomic_store(&lifetime_entered, 0);
  atomic_store(&lifetime_completed, 0);
  atomic_store(&lifetime_release_others, 0);
  atomic_store(&lifetime_release_held, 0);
  atomic_store(&lifetime_shutdowns, 0);
  atomic_store(&lifetime_destroyed, 0);
  atomic_store(&lifetime_destroy_pending, 0);
  atomic_store(&lifetime_late, 0);
  atomic_store(&lifetime_changed_input, 0);
  atomic_store(&lifetime_enabled, 1);
}

int fixture_lifetime_stat(int which) {
  switch (which) {
    case 0: return atomic_load(&lifetime_entered);
    case 1: return atomic_load(&lifetime_completed);
    case 2: return atomic_load(&lifetime_shutdowns);
    case 3: return atomic_load(&lifetime_destroyed);
    case 4: return atomic_load(&lifetime_destroy_pending);
    case 5: return atomic_load(&lifetime_late);
    case 6: return atomic_load(&lifetime_changed_input);
    default: return -1;
  }
}

void fixture_lifetime_release(int which) {
  if (which == 0 || which == 2) atomic_store(&lifetime_release_others, 1);
  if (which == 1 || which == 2) atomic_store(&lifetime_release_held, 1);
}

int fixture_lifetime_dispose(void) {
  if (atomic_load(&lifetime_completed) != atomic_load(&lifetime_expected)) return -1;
  free(controlled_peer);
  controlled_peer = NULL;
  atomic_store(&lifetime_enabled, 0);
  return 0;
}

void fixture_packet_begin(int first_size) { packet_enabled = 1; packet_first_size = first_size; packet_evictions = 0; }
int fixture_packet_evictions(void) { return packet_evictions; }

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

void fixture_packet_push(int size, int value) {
  FixturePeer *peer = controlled_peer;
  if (peer == NULL || size < 1 || size > 512) return;
  while (peer->queue_count + (peer->retained_len != 0) >= 2 && peer->queue_count != 0) {
    if (peer->queue_count == 2) {
      memcpy(peer->queued[0], peer->queued[1], peer->queued_len[1]);
      peer->queued_len[0] = peer->queued_len[1];
    }
    peer->queue_count--;
    packet_evictions++;
  }
  uint8_t payload[512];
  memset(payload, value, (size_t)size);
  const size_t index = peer->queue_count++;
  peer->queued_len[index] = make_packet("{\"type\":\"fixture\"}", payload, (size_t)size, peer->queued[index], 1024);
}

static int fixture_packet_poll(FixturePeer *peer, uint8_t *out, size_t cap, size_t *out_len) {
  if (atomic_load(&peer->closed)) { *out_len = 0; return STATUS_CLOSED; }
  if (peer->retained_len == 0 && peer->queue_count != 0) {
    peer->retained_len = peer->queued_len[0];
    memcpy(peer->retained, peer->queued[0], peer->retained_len);
    if (peer->queue_count == 2) {
      memcpy(peer->queued[0], peer->queued[1], peer->queued_len[1]);
      peer->queued_len[0] = peer->queued_len[1];
    }
    peer->queue_count--;
  }
  if (peer->retained_len == 0) { *out_len = 0; return STATUS_AGAIN; }
  const int status = copy_bytes(peer->retained, peer->retained_len, out, cap, out_len);
  if (status == STATUS_OK) peer->retained_len = 0;
  return status;
}

uint32_t reactor_effect_abi_version(void) { return 2; }
#ifndef REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY
#define REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY "explicit-native-test-fixture"
#endif
const char *reactor_effect_build_identity(void) { return REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY; }

void *reactor_effect_peer_create(void) {
  FixturePeer *peer = calloc(1, sizeof(FixturePeer));
  if (atomic_load(&lifetime_enabled) || packet_enabled) controlled_peer = peer;
  if (packet_enabled) {
    fixture_packet_push(packet_first_size, 1);
    fixture_packet_push(7, 2);
  }
  return peer;
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
  if (atomic_load(&lifetime_enabled)) {
    const int ordinal = atomic_fetch_add(&lifetime_entered, 1) + 1;
    if (atomic_load(&peer->destroyed)) atomic_fetch_add(&lifetime_late, 1);
    atomic_fetch_add(&peer->active_sends, 1);
    while (!(ordinal == 1 ? atomic_load(&lifetime_release_held) : atomic_load(&lifetime_release_others))) usleep(1000);
    if (atomic_load(&peer->destroyed)) atomic_fetch_add(&lifetime_late, 1);
    if (data_len != 2 || (unsigned)data[0] + (unsigned)data[1] != 255) atomic_fetch_add(&lifetime_changed_input, 1);
    atomic_fetch_sub(&peer->active_sends, 1);
    atomic_fetch_add(&lifetime_completed, 1);
    *error_len = 0;
    return STATUS_OK;
  }
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
  if (packet_enabled) return fixture_packet_poll(peer, out, cap, out_len);
  if (!atomic_load(&peer->media_ready)) { *out_len = 0; return atomic_load(&peer->closed) ? STATUS_CLOSED : STATUS_AGAIN; }
  static const uint8_t payload[] = {1, 2, 3, 4, 9, 8, 7};
  if (media_fault) return poll_packet(peer,
    "{\"type\":\"video\",\"format\":\"invalid\",\"track\":\"main_video\"}",
    payload, sizeof(payload), out, cap, out_len, &peer->video_sent);
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
  if (atomic_load(&lifetime_enabled)) {
    atomic_fetch_add(&lifetime_shutdowns, 1);
    *error_len = 0;
    return STATUS_OK;
  }
  while (atomic_load(&peer->active_sends) != 0) usleep(1000);
  *error_len = 0;
  return STATUS_OK;
}

void reactor_effect_peer_destroy(void *raw) {
  FixturePeer *peer = (FixturePeer *)raw;
  if (peer == NULL) return;
  atomic_store(&peer->closed, 1);
  if (atomic_load(&lifetime_enabled)) {
    if (atomic_load(&peer->active_sends) != 0 || atomic_load(&lifetime_entered) != atomic_load(&lifetime_expected)) {
      atomic_fetch_add(&lifetime_destroy_pending, 1);
    }
    atomic_store(&peer->destroyed, 1);
    atomic_fetch_add(&lifetime_destroyed, 1);
    return; /* fixture_lifetime_dispose frees after all blocked calls complete. */
  }
  while (atomic_load(&peer->active_sends) != 0) usleep(1000);
  free(peer);
}
