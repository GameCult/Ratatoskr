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
#define RATATOSKR_ERR_CATALOG (-5)
#define RATATOSKR_ERR_REQUEST (-6)

/* What a payload is, so a caller can route it without parsing anything. A
 * video payload is a whole access unit (Annex B for H.264/H.265): chunking and
 * parity are the transport's business and never cross this boundary. */
#define RATATOSKR_KIND_VIDEO 0
#define RATATOSKR_KIND_AUDIO 1

/* ---- receiving ---- */

typedef struct RatatoskrHandle RatatoskrHandle;

/* Dials the producer at `producer` ("host:port": the advertised media_endpoint)
 * and keeps dialling until it answers, so it may be opened before the
 * producer has started. This host admits nothing inbound. `video_relay` may
 * be NULL, or a "host:port" that every whole video access unit is also
 * copied to as a raw byte stream over UDP, for a local decoder that reads one
 * (OBS's own ffmpeg source does). Release with ratatoskr_receiver_close
 * exactly once. */
int ratatoskr_receiver_open(const char *producer,
                            const char *runtime_id,
                            unsigned int connection_id,
                            const char *video_relay,
                            RatatoskrHandle **out_handle);

/* Drains the transport, gives up on frames that are too old, and tells the
 * producer what was lost. Returns payloads now waiting, or a negative error. */
int ratatoskr_receiver_poll(RatatoskrHandle *handle);

/* Takes the next payload. RATATOSKR_NONE when empty. On
 * RATATOSKR_ERR_BUFFER_TOO_SMALL the required size is written to out_len and
 * the payload stays queued rather than being discarded. out_pts_ns receives
 * the producer's presentation time in nanoseconds; it may be NULL. */
int ratatoskr_receiver_next_payload(RatatoskrHandle *handle,
                                    uint8_t *buffer,
                                    size_t capacity,
                                    size_t *out_len,
                                    int *out_kind,
                                    int64_t *out_pts_ns);

/* Payloads that arrived on the media channel and did not decode. Non-zero means
 * the producer and this build disagree about the envelope. */
uint64_t ratatoskr_receiver_undecodable(RatatoskrHandle *handle);

/* 1 while the producer has answered and not since gone quiet, else 0. */
int ratatoskr_receiver_attached(RatatoskrHandle *handle);

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

void ratatoskr_receiver_close(RatatoskrHandle *handle);

/* A free loopback UDP port for a local decoder to listen on. Bound and
 * released; use it promptly. 0 on failure. */
uint16_t ratatoskr_free_udp_port(void);

/* ---- discovering and asking ---- */

typedef struct RatatoskrCatalog RatatoskrCatalog;

/* One stream a producer advertises. Every pointer is owned by the catalog and
 * valid until ratatoskr_catalog_close. Parallel arrays pair ids with labels. */
typedef struct RatatoskrStreamInfo {
    const char *stream_id;
    const char *producer_id;
    const char *label;
    const char *state; /* "available", "streaming", "unavailable" */
    size_t video_source_count;
    const char *const *video_source_ids;
    const char *const *video_source_labels;
    size_t audio_source_count;
    const char *const *audio_source_ids;
    const char *const *audio_source_labels;
    size_t video_codec_count;
    const char *const *video_codecs;
    size_t audio_codec_count;
    const char *const *audio_codecs;
    uint32_t audio_sample_rate;
    uint32_t audio_channels;
    uint32_t default_video_bitrate_kbps;
    uint32_t default_latency_budget_ms;
    uint32_t media_packet_bytes;
    const char *media_endpoint;   /* "host:port" the producer serves from */
    uint32_t media_connection_id; /* dial media_endpoint with this */
} RatatoskrStreamInfo;

/* Pulls every advertised stream from Odin ("rudp://host:port" or "host:port").
 * Blocks for the pull, seconds at most: call from a worker, never a render
 * thread. state_dir holds a small working store per runtime id. */
int ratatoskr_catalog_pull(const char *odin,
                           const char *runtime_id,
                           const char *state_dir,
                           RatatoskrCatalog **out_catalog);

size_t ratatoskr_catalog_count(const RatatoskrCatalog *catalog);

/* Advertisements present but malformed and left out of the count. */
size_t ratatoskr_catalog_rejected(const RatatoskrCatalog *catalog);

int ratatoskr_catalog_stream(const RatatoskrCatalog *catalog,
                             size_t index,
                             RatatoskrStreamInfo *out_info);

/* Asks the producer of stream `index` to serve it. This receiver is known to
 * the producer by the catalog's runtime id; it dials media_endpoint itself.
 * Empty source ids mean none of that kind; zero bitrate or latency means the
 * producer's default. Blocks for the publish. */
int ratatoskr_request_start(const RatatoskrCatalog *catalog,
                            size_t index,
                            const char *video_source_id,
                            const char *audio_source_id,
                            const char *video_codec,
                            const char *audio_codec,
                            unsigned int video_bitrate_kbps,
                            unsigned int latency_budget_ms);

/* Asks the producer of stream `index` to stop serving it to this receiver. */
int ratatoskr_request_stop(const RatatoskrCatalog *catalog, size_t index);

/* The producer's current answer for stream `index`, copied into the buffers
 * (NUL-terminated, truncated to capacity). RATATOSKR_NONE when the mesh holds
 * no request from this receiver for it. Blocks for the pull. */
int ratatoskr_request_state(const RatatoskrCatalog *catalog,
                            size_t index,
                            char *state,
                            size_t state_capacity,
                            char *detail,
                            size_t detail_capacity);

void ratatoskr_catalog_close(RatatoskrCatalog *catalog);

/* Last error on this thread. Returns the full length; truncates to capacity. */
size_t ratatoskr_last_error(char *buffer, size_t capacity);

#ifdef __cplusplus
}
#endif

#endif /* RATATOSKR_H */
