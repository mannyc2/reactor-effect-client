#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* The real header: every function below must match its ABI 3 prototype. */
#include "reactor_effect_native.h"

/* The layouts the host decodes, also asserted against the Rust definitions. */
_Static_assert(sizeof(ReactorEffectVideoHeader) == 40, "video header layout");
_Static_assert(sizeof(ReactorEffectAudioHeader) == 16, "audio header layout");
_Static_assert(sizeof(ReactorEffectFailure) == 1024, "failure layout");

/* A scripted stand-in for the bridge, without libwebrtc, so tests control
 * timing, failures and handle lifetime. Like the bridge, it runs one notifier
 * thread per peer and joins it in shutdown. */
struct ReactorEffectPeer {
  atomic_int answered;
  atomic_int closed;
  atomic_int media_ready;
  atomic_int event_index;
  atomic_int video_sent;
  atomic_int audio_sent;
  atomic_int active_sends;
  atomic_int destroyed;
  ReactorEffectNotify notify;
  pthread_t notifier;
  int notifying;
  pthread_mutex_t lock;
  pthread_cond_t wake;
  uint32_t ready;
  int stopping;
};

/* Explicit test controls. Tombstones let the harness detect unsafe destruction
 * order without deliberately dereferencing freed memory or claiming corruption. */
static ReactorEffectPeer *controlled_peer;
static atomic_int lifetime_enabled, lifetime_expected, lifetime_entered, lifetime_completed;
static atomic_int lifetime_release_others, lifetime_release_held, lifetime_shutdowns;
static atomic_int lifetime_destroyed, lifetime_destroy_pending, lifetime_late, lifetime_changed_input;
static atomic_int notifier_joins;
static int media_fault;
/* A shutdown join held open, as a wedged native owner would leave it. */
static atomic_int shutdown_held, shutdowns_entered, peers_destroyed;

void fixture_media_fault(int enabled) { media_fault = enabled; }
void fixture_shutdown_hold(int held) { atomic_store(&shutdown_held, held); }

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
    case 7: return atomic_load(&notifier_joins);
    case 8: return atomic_load(&shutdowns_entered);
    case 9: return atomic_load(&peers_destroyed);
    default: return -1;
  }
}

void fixture_lifetime_release(int which) {
  if (which == 0 || which == 2) atomic_store(&lifetime_release_others, 1);
  if (which == 1 || which == 2) atomic_store(&lifetime_release_held, 1);
}

int fixture_lifetime_dispose(void) {
  if (atomic_load(&lifetime_completed) != atomic_load(&lifetime_expected)) return -1;
  pthread_mutex_destroy(&controlled_peer->lock);
  pthread_cond_destroy(&controlled_peer->wake);
  free(controlled_peer);
  controlled_peer = NULL;
  atomic_store(&lifetime_enabled, 0);
  return 0;
}

static const char *PREPARED =
  "{\"sdp\":\"fixture native offer\",\"mapping\":["
  "{\"name\":\"main_video\",\"kind\":\"video\",\"direction\":\"recvonly\",\"mid\":\"0\"},"
  "{\"name\":\"main_audio\",\"kind\":\"audio\",\"direction\":\"recvonly\",\"mid\":\"1\"},"
  "{\"name\":\"input_audio\",\"kind\":\"audio\",\"direction\":\"sendonly\",\"mid\":\"2\"}]}";

static const char *SNAPSHOT =
  "{\"closed\":false,\"queuedControl\":0,\"queuedVideo\":0,\"queuedAudio\":0,"
  "\"queuedBytes\":0,\"droppedVideo\":\"0\",\"droppedAudio\":\"0\","
  "\"pendingRequests\":0,\"deliveredVideo\":\"0\",\"deliveredAudio\":\"0\"}";

static void *notifier_main(void *raw) {
  ReactorEffectPeer *peer = (ReactorEffectPeer *)raw;
  pthread_mutex_lock(&peer->lock);
  for (;;) {
    while (peer->ready == 0 && !peer->stopping) pthread_cond_wait(&peer->wake, &peer->lock);
    if (peer->stopping) break;
    const uint32_t ready = peer->ready;
    peer->ready = 0;
    pthread_mutex_unlock(&peer->lock);
    peer->notify(ready);
    pthread_mutex_lock(&peer->lock);
  }
  pthread_mutex_unlock(&peer->lock);
  return NULL;
}

static void signal_ready(ReactorEffectPeer *peer, uint32_t bits) {
  pthread_mutex_lock(&peer->lock);
  if (!peer->stopping) {
    peer->ready |= bits;
    pthread_cond_signal(&peer->wake);
  }
  pthread_mutex_unlock(&peer->lock);
}

static void stop_notifier(ReactorEffectPeer *peer) {
  pthread_mutex_lock(&peer->lock);
  peer->stopping = 1;
  pthread_cond_broadcast(&peer->wake);
  pthread_mutex_unlock(&peer->lock);
}

static void join_notifier(ReactorEffectPeer *peer) {
  stop_notifier(peer);
  if (peer->notifying) {
    pthread_join(peer->notifier, NULL);
    peer->notifying = 0;
    atomic_fetch_add(&notifier_joins, 1);
  }
}

static int fail(ReactorEffectFailure *failure, int status, const char *message) {
  if (failure != NULL) {
    const size_t length = strlen(message);
    memcpy(failure->message, message, length);
    failure->message_len = (uint32_t)length;
  }
  return status;
}

static int copy_text(const char *text, uint8_t *out, size_t cap, size_t *out_len) {
  const size_t length = strlen(text);
  *out_len = length;
  if (cap < length || out == NULL) return REACTOR_EFFECT_BUFFER_TOO_SMALL;
  memcpy(out, text, length);
  return REACTOR_EFFECT_OK;
}

static size_t make_packet(const char *header, uint8_t *out, size_t cap) {
  const size_t header_len = strlen(header);
  const size_t needed = 4 + header_len;
  if (out == NULL || cap < needed) return needed;
  const uint32_t n = (uint32_t)header_len;
  out[0] = (uint8_t)(n & 0xff);
  out[1] = (uint8_t)((n >> 8) & 0xff);
  out[2] = (uint8_t)((n >> 16) & 0xff);
  out[3] = (uint8_t)((n >> 24) & 0xff);
  memcpy(out + 4, header, header_len);
  return needed;
}

uint32_t reactor_effect_abi_version(void) { return 3; }
#ifndef REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY
#define REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY "explicit-native-test-fixture"
#endif
const char *reactor_effect_build_identity(void) { return REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY; }

ReactorEffectPeer *reactor_effect_peer_create(ReactorEffectNotify notify) {
  ReactorEffectPeer *peer = calloc(1, sizeof(ReactorEffectPeer));
  if (peer == NULL) return NULL;
  pthread_mutex_init(&peer->lock, NULL);
  pthread_cond_init(&peer->wake, NULL);
  peer->notify = notify;
  if (notify != NULL) peer->notifying = pthread_create(&peer->notifier, NULL, notifier_main, peer) == 0;
  if (atomic_load(&lifetime_enabled)) controlled_peer = peer;
  return peer;
}

int reactor_effect_peer_call(ReactorEffectPeer *peer, uint32_t operation, const uint8_t *request,
                             size_t request_len, uint8_t *response, size_t response_cap,
                             size_t *response_len, ReactorEffectFailure *failure) {
  (void)request;
  (void)request_len;
  if (peer == NULL || response_len == NULL) return REACTOR_EFFECT_INVALID_INPUT;
  *response_len = 0;
  if (atomic_load(&peer->closed)) return fail(failure, REACTOR_EFFECT_CLOSED, "fixture closed");
  switch (operation) {
    case REACTOR_EFFECT_PREPARE: return copy_text(PREPARED, response, response_cap, response_len);
    case REACTOR_EFFECT_ANSWER:
      atomic_store(&peer->answered, 1);
      signal_ready(peer, REACTOR_EFFECT_READY_EVENTS);
      return copy_text("{}", response, response_cap, response_len);
    case REACTOR_EFFECT_DIRECTION:
    case REACTOR_EFFECT_MAX_BITRATE:
      return copy_text("{}", response, response_cap, response_len);
    case REACTOR_EFFECT_STATS:
      return copy_text("[]", response, response_cap, response_len);
    case REACTOR_EFFECT_MEDIA_SNAPSHOT:
      /* An explicit test gate: readers subscribe before media is released. */
      atomic_store(&peer->media_ready, 1);
      signal_ready(peer, REACTOR_EFFECT_READY_VIDEO | REACTOR_EFFECT_READY_AUDIO);
      return copy_text(SNAPSHOT, response, response_cap, response_len);
    default:
      return fail(failure, REACTOR_EFFECT_INVALID_INPUT, "unknown fixture call");
  }
}

int reactor_effect_peer_send(ReactorEffectPeer *peer, uint32_t channel, const uint8_t *data,
                             size_t data_len, ReactorEffectFailure *failure) {
  if (peer == NULL) return REACTOR_EFFECT_INVALID_INPUT;
  if (atomic_load(&lifetime_enabled)) {
    const int ordinal = atomic_fetch_add(&lifetime_entered, 1) + 1;
    if (atomic_load(&peer->destroyed)) atomic_fetch_add(&lifetime_late, 1);
    atomic_fetch_add(&peer->active_sends, 1);
    while (!(ordinal == 1 ? atomic_load(&lifetime_release_held) : atomic_load(&lifetime_release_others))) usleep(1000);
    if (atomic_load(&peer->destroyed)) atomic_fetch_add(&lifetime_late, 1);
    if (data_len != 2 || (unsigned)data[0] + (unsigned)data[1] != 255) atomic_fetch_add(&lifetime_changed_input, 1);
    atomic_fetch_sub(&peer->active_sends, 1);
    atomic_fetch_add(&lifetime_completed, 1);
    return REACTOR_EFFECT_OK;
  }
  if (atomic_load(&peer->closed)) return fail(failure, REACTOR_EFFECT_CLOSED, "fixture closed");
  if (channel == REACTOR_EFFECT_DATA) {
    atomic_fetch_add(&peer->active_sends, 1);
    usleep(50000);
    atomic_fetch_sub(&peer->active_sends, 1);
  }
  return REACTOR_EFFECT_OK;
}

int reactor_effect_peer_take_event(ReactorEffectPeer *peer, uint8_t *out, size_t out_cap,
                                   size_t *out_len) {
  if (peer == NULL || out_len == NULL || (out_cap != 0 && out == NULL)) return REACTOR_EFFECT_INVALID_INPUT;
  *out_len = 0;
  if (atomic_load(&peer->closed)) return REACTOR_EFFECT_CLOSED;
  static const char *events[] = {
    "{\"type\":\"state\",\"state\":\"connected\"}",
    "{\"type\":\"channel\",\"channel\":\"control\",\"open\":true}",
    "{\"type\":\"channel\",\"channel\":\"data\",\"open\":true}"
  };
  const int index = atomic_load(&peer->event_index);
  if (!atomic_load(&peer->answered) || index >= 3) return REACTOR_EFFECT_AGAIN;
  *out_len = make_packet(events[index], NULL, 0);
  if (out_cap < *out_len) return REACTOR_EFFECT_BUFFER_TOO_SMALL;
  make_packet(events[index], out, out_cap);
  atomic_fetch_add(&peer->event_index, 1);
  return REACTOR_EFFECT_OK;
}

int reactor_effect_peer_take_video(ReactorEffectPeer *peer, ReactorEffectVideoHeader *header,
                                   uint8_t *bgra, size_t bgra_cap, uint8_t *metadata,
                                   size_t metadata_cap) {
  if (peer == NULL || header == NULL) return REACTOR_EFFECT_INVALID_INPUT;
  if (atomic_load(&peer->closed)) return REACTOR_EFFECT_CLOSED;
  if (!atomic_load(&peer->media_ready) || atomic_load(&peer->video_sent)) return REACTOR_EFFECT_AGAIN;
  static const uint8_t pixel[] = {1, 2, 3, 4}, trailer[] = {9, 8, 7};
  /* A fault names main_audio's index, which is not a video receive track. */
  *header = (ReactorEffectVideoHeader){
    .width = 1,
    .height = 1,
    .data_len = sizeof(pixel),
    .metadata_len = sizeof(trailer),
    .frame_id = UINT64_MAX,
    .timestamp_us = 9007199254740993ULL,
    .track = media_fault ? 1 : 0,
  };
  if (bgra == NULL || bgra_cap < sizeof(pixel) || metadata == NULL || metadata_cap < sizeof(trailer))
    return REACTOR_EFFECT_BUFFER_TOO_SMALL;
  memcpy(bgra, pixel, sizeof(pixel));
  memcpy(metadata, trailer, sizeof(trailer));
  atomic_store(&peer->video_sent, 1);
  return REACTOR_EFFECT_OK;
}

int reactor_effect_peer_take_audio(ReactorEffectPeer *peer, ReactorEffectAudioHeader *header,
                                   int16_t *pcm, size_t pcm_cap) {
  if (peer == NULL || header == NULL) return REACTOR_EFFECT_INVALID_INPUT;
  if (atomic_load(&peer->closed)) return REACTOR_EFFECT_CLOSED;
  if (!atomic_load(&peer->media_ready) || atomic_load(&peer->audio_sent)) return REACTOR_EFFECT_AGAIN;
  static const int16_t samples[] = {1, -2, 300, -400};
  *header = (ReactorEffectAudioHeader){
    .sample_rate = 48000,
    .channels = 2,
    .samples = 4,
    .track = 1,
  };
  if (pcm == NULL || pcm_cap < 4) return REACTOR_EFFECT_BUFFER_TOO_SMALL;
  memcpy(pcm, samples, sizeof(samples));
  atomic_store(&peer->audio_sent, 1);
  return REACTOR_EFFECT_OK;
}

void reactor_effect_peer_close(ReactorEffectPeer *peer) {
  if (peer != NULL) atomic_store(&peer->closed, 1);
}

int reactor_effect_peer_shutdown(ReactorEffectPeer *peer, ReactorEffectFailure *failure) {
  (void)failure;
  if (peer == NULL) return REACTOR_EFFECT_INVALID_INPUT;
  atomic_fetch_add(&shutdowns_entered, 1);
  while (atomic_load(&shutdown_held)) usleep(1000);
  atomic_store(&peer->closed, 1);
  join_notifier(peer);
  if (atomic_load(&lifetime_enabled)) {
    atomic_fetch_add(&lifetime_shutdowns, 1);
    return REACTOR_EFFECT_OK;
  }
  while (atomic_load(&peer->active_sends) != 0) usleep(1000);
  return REACTOR_EFFECT_OK;
}

void reactor_effect_peer_destroy(ReactorEffectPeer *peer) {
  if (peer == NULL) return;
  atomic_fetch_add(&peers_destroyed, 1);
  atomic_store(&peer->closed, 1);
  join_notifier(peer);
  if (atomic_load(&lifetime_enabled)) {
    if (atomic_load(&peer->active_sends) != 0 || atomic_load(&lifetime_entered) != atomic_load(&lifetime_expected)) {
      atomic_fetch_add(&lifetime_destroy_pending, 1);
    }
    atomic_store(&peer->destroyed, 1);
    atomic_fetch_add(&lifetime_destroyed, 1);
    return; /* fixture_lifetime_dispose frees after all blocked calls complete. */
  }
  while (atomic_load(&peer->active_sends) != 0) usleep(1000);
  pthread_mutex_destroy(&peer->lock);
  pthread_cond_destroy(&peer->wake);
  free(peer);
}
