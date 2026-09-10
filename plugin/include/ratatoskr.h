/* Ratatoskr media receive core, C ABI.
 *
 * The OBS plugin calls these and implements no transport of its own. The plugin
 * this replaces hand-rolled CultNet RUDP in C++ and could not receive upstream
 * fixes; if you find yourself adding a sequence number or an ACK mask on the C
 * side, it belongs in the Rust core or in CultLib, not here.
 *
 * Hand-written rather than generated, because the surface is meant to stay
 * small enough to read in one sitting. If it outgrows that, the split is wrong.
 */

#ifndef RATATOSKR_H
#define RATATOSKR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RATATOSKR_OK 0
#define RATATOSKR_NONE 1
#define RATATOSKR_ERR_ARGUMENT (-1)
#define RATATOSKR_ERR_OPEN (-2)
#define RATATOSKR_ERR_POLL (-3)
#define RATATOSKR_ERR_BUFFER_TOO_SMALL (-4)

/* What a payload is, so a caller can route it without parsing anything. A
 * video payload is a whole access unit (Annex B for H.264/H.265): chunking and
 * parity are the transport's business and never cross this boundary. */
#define RATATOSKR_KIND_VIDEO 0
#define RATATOSKR_KIND_AUDIO 1

typedef struct RatatoskrHandle RatatoskrHandle;

/* Opens a media receiver. Release with ratatoskr_receiver_close exactly once. */
int ratatoskr_receiver_open(const char *bind,
                            const char *runtime_id,
                            unsigned int connection_id,
                            RatatoskrHandle **out_handle);

/* Drains the transport. Returns payloads now waiting, or a negative error. */
int ratatoskr_receiver_poll(RatatoskrHandle *handle);

/* Takes the next payload. RATATOSKR_NONE when empty. On
 * RATATOSKR_ERR_BUFFER_TOO_SMALL the required size is written to out_len and
 * the payload stays queued rather than being discarded. */
int ratatoskr_receiver_next_payload(RatatoskrHandle *handle,
                                    uint8_t *buffer,
                                    size_t capacity,
                                    size_t *out_len,
                                    int *out_kind);

/* Payloads that arrived on the media channel and did not decode. Non-zero means
 * the producer and this build disagree about the envelope. */
uint64_t ratatoskr_receiver_undecodable(RatatoskrHandle *handle);

/* The port actually bound; 0 if unavailable. */
uint16_t ratatoskr_receiver_local_port(RatatoskrHandle *handle);

/* Payloads and bytes actually delivered, never what was requested. Either out
 * pointer may be NULL. */
void ratatoskr_receiver_delivered(RatatoskrHandle *handle,
                                  uint64_t *out_payloads,
                                  uint64_t *out_bytes);

/* What became of the video frames: completed, chunks given back by parity, and
 * frames given up on (aged out or evicted). Any out pointer may be NULL. */
void ratatoskr_receiver_video_stats(RatatoskrHandle *handle,
                                    uint64_t *out_completed,
                                    uint64_t *out_repaired_chunks,
                                    uint64_t *out_given_up);

/* What was said back to the producer: records sent, chunks asked for again,
 * keyframes requested, and records the transport refused (usually: no producer
 * attached). Any out pointer may be NULL. */
void ratatoskr_receiver_feedback_stats(RatatoskrHandle *handle,
                                       uint64_t *out_sent,
                                       uint64_t *out_chunks_requested,
                                       uint64_t *out_keyframes_requested,
                                       uint64_t *out_not_sent);

/* Last error on this thread. Returns the full length; truncates to capacity. */
size_t ratatoskr_last_error(char *buffer, size_t capacity);

void ratatoskr_receiver_close(RatatoskrHandle *handle);

#ifdef __cplusplus
}
#endif

#endif /* RATATOSKR_H */
