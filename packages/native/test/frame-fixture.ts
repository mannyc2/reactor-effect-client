import { compileLibrary } from "./support.js";

/**
 * A minimal ABI 3 library whose video queue the test scripts frame by frame.
 * take_video copies a preallocated source into caller memory with one memcpy,
 * as the Rust bridge's `OutSlice::copy_from` does, so the host's allocation
 * and copy behaviour is the only JavaScript-side variable.
 */
const source = String.raw`
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "reactor_effect_native.h"

struct ReactorEffectPeer { int closed; };
typedef struct { uint32_t width, height, metadata; } Spec;
static Spec queue[256];
static unsigned head, tail;
static uint64_t sequence;
static uint8_t *pixels;
static size_t pixels_cap;

void fixture_push(uint32_t width, uint32_t height, uint32_t metadata, uint32_t count) {
  const size_t size = (size_t)width * height * 4;
  if (size > pixels_cap) {
    pixels = realloc(pixels, size);
    for (size_t i = pixels_cap; i < size; i++) pixels[i] = (uint8_t)(i * 7);
    pixels_cap = size;
  }
  for (uint32_t i = 0; i < count; i++) queue[tail++ % 256] = (Spec){width, height, metadata};
}

uint32_t reactor_effect_abi_version(void) { return 3; }
const char *reactor_effect_build_identity(void) { return "frame-allocation-fixture"; }
ReactorEffectPeer *reactor_effect_peer_create(ReactorEffectNotify notify) {
  (void)notify;
  return calloc(1, sizeof(ReactorEffectPeer));
}
int reactor_effect_peer_call(ReactorEffectPeer *peer, uint32_t operation, const uint8_t *request,
                             size_t request_len, uint8_t *response, size_t response_cap,
                             size_t *response_len, ReactorEffectFailure *failure) {
  (void)peer; (void)operation; (void)request; (void)request_len; (void)response;
  (void)response_cap; (void)failure;
  if (response_len != NULL) *response_len = 0;
  return REACTOR_EFFECT_INVALID_INPUT;
}
int reactor_effect_peer_send(ReactorEffectPeer *peer, uint32_t channel, const uint8_t *data,
                             size_t data_len, ReactorEffectFailure *failure) {
  (void)peer; (void)channel; (void)data; (void)data_len; (void)failure;
  return REACTOR_EFFECT_INVALID_INPUT;
}
int reactor_effect_peer_take_event(ReactorEffectPeer *peer, uint8_t *out, size_t out_cap,
                                   size_t *out_len) {
  (void)out; (void)out_cap;
  if (out_len != NULL) *out_len = 0;
  return peer->closed ? REACTOR_EFFECT_CLOSED : REACTOR_EFFECT_AGAIN;
}
int reactor_effect_peer_take_video(ReactorEffectPeer *peer, ReactorEffectVideoHeader *header,
                                   uint8_t *bgra, size_t bgra_cap, uint8_t *metadata,
                                   size_t metadata_cap) {
  if (peer == NULL || header == NULL) return REACTOR_EFFECT_INVALID_INPUT;
  if (peer->closed) return REACTOR_EFFECT_CLOSED;
  if (head == tail) return REACTOR_EFFECT_AGAIN;
  const Spec spec = queue[head % 256];
  const size_t size = (size_t)spec.width * spec.height * 4;
  *header = (ReactorEffectVideoHeader){
    .width = spec.width, .height = spec.height,
    .data_len = (uint32_t)size, .metadata_len = spec.metadata,
    .frame_id = sequence + 1, .timestamp_us = 1000 * (sequence + 1), .track = 0,
  };
  if (bgra == NULL || bgra_cap < size || (spec.metadata != 0 && (metadata == NULL || metadata_cap < spec.metadata)))
    return REACTOR_EFFECT_BUFFER_TOO_SMALL;
  memcpy(bgra, pixels, size);
  bgra[0] = (uint8_t)(sequence + 1);
  if (spec.metadata != 0) memset(metadata, (int)(0xa0 + sequence), spec.metadata);
  sequence++;
  head++;
  return REACTOR_EFFECT_OK;
}
int reactor_effect_peer_take_audio(ReactorEffectPeer *peer, ReactorEffectAudioHeader *header,
                                   int16_t *pcm, size_t pcm_cap) {
  (void)header; (void)pcm; (void)pcm_cap;
  return peer->closed ? REACTOR_EFFECT_CLOSED : REACTOR_EFFECT_AGAIN;
}
void reactor_effect_peer_close(ReactorEffectPeer *peer) { if (peer != NULL) peer->closed = 1; }
int reactor_effect_peer_shutdown(ReactorEffectPeer *peer, ReactorEffectFailure *failure) {
  (void)failure;
  if (peer != NULL) peer->closed = 1;
  return REACTOR_EFFECT_OK;
}
void reactor_effect_peer_destroy(ReactorEffectPeer *peer) { free(peer); }
`;

export const compileFrameFixture = (): { readonly directory: string; readonly path: string } =>
  compileLibrary(source, "frames");

/** The byte the fixture writes at pixel offset `index` of the frame with 1-based `frameId`. */
export const expectedPixel = (frameId: number, index: number): number =>
  index === 0 ? frameId & 0xff : (index * 7) & 0xff;
