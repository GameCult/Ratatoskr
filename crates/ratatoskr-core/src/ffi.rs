//! The C ABI the OBS plugin calls.
//!
//! Kept deliberately small. Every capability exposed here is one the plugin
//! would otherwise implement itself in C++, and the last plugin implemented
//! roughly 2,500 lines that way. If this surface starts growing toward the
//! shape of a transport library, the split is in the wrong place.
//!
//! Ownership: a handle from `ratatoskr_receiver_open` must be released with
//! `ratatoskr_receiver_close` exactly once. Payload bytes are copied into
//! caller memory; nothing returned here needs freeing by the caller.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::{CStr, CString, c_char, c_int, c_uint};
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::ptr;

use chrono::{SecondsFormat, Utc};
use cultnet_rs::GameCultMediaStreamAdvertisementRecord;

use crate::catalog::{CatalogOptions, pull_catalog, pull_request_state, publish_request, start_request, stop_request};

use crate::feedback::FeedbackOptions;
use crate::receiver::{MediaEvent, RatatoskrReceiver, ReceiverOptions};
use crate::video::VideoAssemblerOptions;

pub const RATATOSKR_OK: c_int = 0;
pub const RATATOSKR_NONE: c_int = 1;
pub const RATATOSKR_ERR_ARGUMENT: c_int = -1;
pub const RATATOSKR_ERR_OPEN: c_int = -2;
pub const RATATOSKR_ERR_POLL: c_int = -3;
pub const RATATOSKR_ERR_BUFFER_TOO_SMALL: c_int = -4;
pub const RATATOSKR_ERR_CATALOG: c_int = -5;
pub const RATATOSKR_ERR_REQUEST: c_int = -6;

/// What a payload is, so a caller can route it without parsing anything.
/// A video payload is a whole access unit: chunking and parity are the
/// transport's business and never cross this boundary.
pub const RATATOSKR_KIND_VIDEO: c_int = 0;
pub const RATATOSKR_KIND_AUDIO: c_int = 1;

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

fn set_last_error(message: impl Into<String>) {
    LAST_ERROR.with(|slot| *slot.borrow_mut() = message.into());
}

/// Opaque to C. Holds the receiver plus payloads drained but not yet taken,
/// so a caller can poll on one cadence and consume on another.
pub struct RatatoskrHandle {
    receiver: RatatoskrReceiver,
    /// (kind, presentation time in nanoseconds, payload)
    pending: VecDeque<(c_int, i64, Vec<u8>)>,
    attached: Option<SocketAddr>,
}

fn pts_nanos(pts_ticks: i64, num: u32, den: u32) -> i64 {
    if den == 0 {
        return 0;
    }
    ((pts_ticks as i128 * num as i128 * 1_000_000_000) / den as i128) as i64
}

/// # Safety
/// `bind` and `runtime_id` must be valid NUL-terminated UTF-8; `video_relay`
/// may be NULL or a `host:port` to copy whole video access units to.
/// `out_handle` must be a valid pointer to write one handle into.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_open(
    bind: *const c_char,
    runtime_id: *const c_char,
    connection_id: c_uint,
    video_relay: *const c_char,
    out_handle: *mut *mut RatatoskrHandle,
) -> c_int {
    if bind.is_null() || runtime_id.is_null() || out_handle.is_null() {
        set_last_error("null argument");
        return RATATOSKR_ERR_ARGUMENT;
    }
    let (bind, runtime_id) = unsafe { (CStr::from_ptr(bind), CStr::from_ptr(runtime_id)) };
    let video_relay = if video_relay.is_null() {
        None
    } else {
        match unsafe { CStr::from_ptr(video_relay) }.to_str().ok().and_then(|text| text.parse::<SocketAddr>().ok()) {
            Some(target) => Some(target),
            None => {
                set_last_error("video relay is not a socket address");
                return RATATOSKR_ERR_ARGUMENT;
            }
        }
    };
    let Ok(bind) = bind.to_str().map(str::to_owned) else {
        set_last_error("bind address is not valid UTF-8");
        return RATATOSKR_ERR_ARGUMENT;
    };
    let Ok(bind) = bind.parse::<SocketAddr>() else {
        set_last_error(format!("bind address {bind} is not a socket address"));
        return RATATOSKR_ERR_ARGUMENT;
    };
    let Ok(runtime_id) = runtime_id.to_str().map(str::to_owned) else {
        set_last_error("runtime id is not valid UTF-8");
        return RATATOSKR_ERR_ARGUMENT;
    };

    match RatatoskrReceiver::open(ReceiverOptions {
        bind,
        runtime_id,
        connection_id: connection_id as u32,
        video: VideoAssemblerOptions::default(),
        feedback: FeedbackOptions::default(),
        video_relay,
    }) {
        Ok(receiver) => {
            let handle = Box::new(RatatoskrHandle {
                receiver,
                pending: VecDeque::new(),
                attached: None,
            });
            unsafe { *out_handle = Box::into_raw(handle) };
            RATATOSKR_OK
        }
        Err(error) => {
            set_last_error(format!("{error:#}"));
            RATATOSKR_ERR_OPEN
        }
    }
}

/// Drains the transport into the handle's queue. Returns the number of payloads
/// now waiting, or a negative error code.
///
/// # Safety
/// `handle` must come from `ratatoskr_receiver_open` and not yet be closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_poll(handle: *mut RatatoskrHandle) -> c_int {
    let Some(handle) = (unsafe { handle.as_mut() }) else {
        set_last_error("null handle");
        return RATATOSKR_ERR_ARGUMENT;
    };
    match handle.receiver.poll() {
        Ok(events) => {
            for event in events {
                match event {
                    MediaEvent::VideoFrame { frame } => {
                        let pts = pts_nanos(frame.pts_ticks, frame.timebase_num, frame.timebase_den);
                        handle.pending.push_back((RATATOSKR_KIND_VIDEO, pts, frame.bytes));
                    }
                    MediaEvent::Audio { record } => {
                        if !record.payload.is_empty() {
                            let pts = pts_nanos(record.pts_ticks, record.timebase_num, record.timebase_den);
                            handle.pending.push_back((RATATOSKR_KIND_AUDIO, pts, record.payload));
                        }
                    }
                    // Given-up frames and feedback carry no media for a
                    // renderer; the counters say they happened.
                    MediaEvent::VideoFrameExpired { .. }
                    | MediaEvent::Feedback { .. }
                    | MediaEvent::FeedbackSent { .. } => {}
                    MediaEvent::Undecodable { reason, .. }
                    | MediaEvent::Rejected { reason }
                    | MediaEvent::FeedbackNotSent { reason } => set_last_error(reason),
                    MediaEvent::ProducerAttached { remote } => handle.attached = Some(remote),
                    MediaEvent::ProducerDetached { .. } => handle.attached = None,
                }
            }
            c_int::try_from(handle.pending.len()).unwrap_or(c_int::MAX)
        }
        Err(error) => {
            set_last_error(format!("{error:#}"));
            RATATOSKR_ERR_POLL
        }
    }
}

/// Copies the next waiting payload into `buffer`.
///
/// Returns `RATATOSKR_OK` and writes `out_len` when one was taken,
/// `RATATOSKR_NONE` when the queue is empty, or
/// `RATATOSKR_ERR_BUFFER_TOO_SMALL` with the required size in `out_len`, in
/// which case the payload stays queued rather than being dropped.
///
/// # Safety
/// `handle` must be live; `buffer` must be writable for `capacity` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_next_payload(
    handle: *mut RatatoskrHandle,
    buffer: *mut u8,
    capacity: usize,
    out_len: *mut usize,
    out_kind: *mut c_int,
    out_pts_ns: *mut i64,
) -> c_int {
    let Some(handle) = (unsafe { handle.as_mut() }) else {
        set_last_error("null handle");
        return RATATOSKR_ERR_ARGUMENT;
    };
    if out_len.is_null() {
        set_last_error("null out_len");
        return RATATOSKR_ERR_ARGUMENT;
    }
    let Some((kind, pts, payload)) = handle.pending.front() else {
        unsafe { *out_len = 0 };
        return RATATOSKR_NONE;
    };
    let needed = payload.len();
    unsafe { *out_len = needed };
    if !out_kind.is_null() {
        unsafe { *out_kind = *kind };
    }
    if !out_pts_ns.is_null() {
        unsafe { *out_pts_ns = *pts };
    }
    if needed > capacity || buffer.is_null() {
        set_last_error(format!("buffer of {capacity} too small for payload of {needed}"));
        return RATATOSKR_ERR_BUFFER_TOO_SMALL;
    }
    let (_, _, payload) = handle.pending.pop_front().expect("front checked above");
    unsafe { ptr::copy_nonoverlapping(payload.as_ptr(), buffer, needed) };
    RATATOSKR_OK
}

/// Payloads that arrived on the media channel and did not decode. Non-zero
/// means the producer and this build disagree about the envelope.
///
/// # Safety
/// `handle` must be live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_undecodable(handle: *mut RatatoskrHandle) -> u64 {
    unsafe { handle.as_ref() }
        .map(|handle| handle.receiver.undecodable())
        .unwrap_or(0)
}

/// The port actually bound, which differs from the requested one when 0 was
/// passed. Returns 0 if unavailable.
///
/// # Safety
/// `handle` must be live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_local_port(handle: *mut RatatoskrHandle) -> u16 {
    unsafe { handle.as_ref() }
        .and_then(|handle| handle.receiver.local_addr().ok())
        .map(|addr| addr.port())
        .unwrap_or(0)
}

/// Payloads delivered and their total bytes, both observed rather than
/// requested. Either out pointer may be null.
///
/// # Safety
/// `handle` must be live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_delivered(
    handle: *mut RatatoskrHandle,
    out_payloads: *mut u64,
    out_bytes: *mut u64,
) {
    let Some(handle) = (unsafe { handle.as_ref() }) else {
        return;
    };
    let (payloads, bytes) = handle.receiver.delivered();
    if !out_payloads.is_null() {
        unsafe { *out_payloads = payloads };
    }
    if !out_bytes.is_null() {
        unsafe { *out_bytes = bytes };
    }
}

/// What became of the video frames: completed, chunks given back by parity,
/// and frames given up on (aged out or evicted). Any out pointer may be null.
///
/// # Safety
/// `handle` must be live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_video_stats(
    handle: *mut RatatoskrHandle,
    out_completed: *mut u64,
    out_repaired_chunks: *mut u64,
    out_given_up: *mut u64,
) {
    let Some(handle) = (unsafe { handle.as_ref() }) else {
        return;
    };
    let stats = handle.receiver.video_stats();
    if !out_completed.is_null() {
        unsafe { *out_completed = stats.completed };
    }
    if !out_repaired_chunks.is_null() {
        unsafe { *out_repaired_chunks = stats.repaired_chunks };
    }
    if !out_given_up.is_null() {
        unsafe { *out_given_up = stats.expired + stats.evicted };
    }
}

/// What was said back to the producer: records sent, chunks asked for again,
/// keyframes requested, and records the transport refused. Any out pointer
/// may be null.
///
/// # Safety
/// `handle` must be live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_feedback_stats(
    handle: *mut RatatoskrHandle,
    out_sent: *mut u64,
    out_chunks_requested: *mut u64,
    out_keyframes_requested: *mut u64,
    out_not_sent: *mut u64,
) {
    let Some(handle) = (unsafe { handle.as_ref() }) else {
        return;
    };
    let stats = handle.receiver.feedback_stats();
    for (out, value) in [
        (out_sent, stats.sent),
        (out_chunks_requested, stats.chunks_requested),
        (out_keyframes_requested, stats.keyframes_requested),
        (out_not_sent, stats.not_sent),
    ] {
        if !out.is_null() {
            unsafe { *out = value };
        }
    }
}

/// A free loopback UDP port for a local decoder to listen on. Bound and
/// released; the caller must use it promptly.
#[unsafe(no_mangle)]
pub extern "C" fn ratatoskr_free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .and_then(|socket| socket.local_addr())
        .map(|addr| addr.port())
        .unwrap_or(0)
}

/// One stream a producer advertises, as C sees it. Every pointer is owned by
/// the catalog handle and valid until `ratatoskr_catalog_close`.
#[repr(C)]
pub struct RatatoskrStreamInfo {
    pub stream_id: *const c_char,
    pub producer_id: *const c_char,
    pub label: *const c_char,
    pub state: *const c_char,
    pub video_source_count: usize,
    pub video_source_ids: *const *const c_char,
    pub video_source_labels: *const *const c_char,
    pub audio_source_count: usize,
    pub audio_source_ids: *const *const c_char,
    pub audio_source_labels: *const *const c_char,
    pub video_codec_count: usize,
    pub video_codecs: *const *const c_char,
    pub audio_codec_count: usize,
    pub audio_codecs: *const *const c_char,
    pub audio_sample_rate: u32,
    pub audio_channels: u32,
    pub default_video_bitrate_kbps: u32,
    pub default_latency_budget_ms: u32,
    pub media_packet_bytes: u32,
    pub media_connection_id: u32,
}

struct StreamStrings {
    scalars: [CString; 4],
    /// Owns the bytes `list_pointers` point into; never read directly.
    _owned_lists: [Vec<CString>; 6],
    list_pointers: [Vec<*const c_char>; 6],
}

/// Opaque to C: the pulled advertisements plus the C strings that view them.
pub struct RatatoskrCatalog {
    options: CatalogOptions,
    streams: Vec<GameCultMediaStreamAdvertisementRecord>,
    strings: Vec<StreamStrings>,
    rejected: usize,
}

fn c_string(value: &str) -> CString {
    CString::new(value.replace('\0', "")).expect("nul stripped")
}

fn c_list(values: &[String]) -> (Vec<CString>, Vec<*const c_char>) {
    let owned = values.iter().map(|value| c_string(value)).collect::<Vec<_>>();
    let pointers = owned.iter().map(|value| value.as_ptr()).collect();
    (owned, pointers)
}

fn read_c_str<'a>(pointer: *const c_char, what: &str) -> Option<&'a str> {
    if pointer.is_null() {
        set_last_error(format!("{what} is null"));
        return None;
    }
    match unsafe { CStr::from_ptr(pointer) }.to_str() {
        Ok(text) => Some(text),
        Err(_) => {
            set_last_error(format!("{what} is not valid UTF-8"));
            None
        }
    }
}

fn catalog_options(odin: *const c_char, runtime_id: *const c_char, state_dir: *const c_char) -> Option<CatalogOptions> {
    Some(CatalogOptions {
        odin: read_c_str(odin, "odin endpoint")?.to_string(),
        runtime_id: read_c_str(runtime_id, "runtime id")?.to_string(),
        state_dir: PathBuf::from(read_c_str(state_dir, "state dir")?),
    })
}

/// Pulls every advertised stream from Odin. Blocks for the pull (seconds at
/// most); call it from a worker, not a render thread.
///
/// # Safety
/// The strings must be valid NUL-terminated UTF-8; `out` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_catalog_pull(
    odin: *const c_char,
    runtime_id: *const c_char,
    state_dir: *const c_char,
    out: *mut *mut RatatoskrCatalog,
) -> c_int {
    if out.is_null() {
        set_last_error("null out pointer");
        return RATATOSKR_ERR_ARGUMENT;
    }
    let Some(options) = catalog_options(odin, runtime_id, state_dir) else {
        return RATATOSKR_ERR_ARGUMENT;
    };
    let pulled = match pull_catalog(&options) {
        Ok(pulled) => pulled,
        Err(error) => {
            set_last_error(format!("{error:#}"));
            return RATATOSKR_ERR_CATALOG;
        }
    };
    let strings = pulled
        .streams
        .iter()
        .map(|stream| {
            let lists = [
                c_list(&stream.video_source_ids),
                c_list(&stream.video_source_labels),
                c_list(&stream.audio_source_ids),
                c_list(&stream.audio_source_labels),
                c_list(&stream.video_codecs),
                c_list(&stream.audio_codecs),
            ];
            let [a, b, c, d, e, f] = lists;
            StreamStrings {
                scalars: [
                    c_string(&stream.stream_id),
                    c_string(&stream.producer_id),
                    c_string(&stream.label),
                    c_string(&stream.state),
                ],
                _owned_lists: [a.0, b.0, c.0, d.0, e.0, f.0],
                list_pointers: [a.1, b.1, c.1, d.1, e.1, f.1],
            }
        })
        .collect();
    let catalog = Box::new(RatatoskrCatalog {
        options,
        rejected: pulled.rejected.len(),
        streams: pulled.streams,
        strings,
    });
    unsafe { *out = Box::into_raw(catalog) };
    RATATOSKR_OK
}

/// # Safety
/// `catalog` must be live or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_catalog_count(catalog: *const RatatoskrCatalog) -> usize {
    unsafe { catalog.as_ref() }.map_or(0, |catalog| catalog.streams.len())
}

/// Advertisements that were present but malformed and left out.
///
/// # Safety
/// `catalog` must be live or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_catalog_rejected(catalog: *const RatatoskrCatalog) -> usize {
    unsafe { catalog.as_ref() }.map_or(0, |catalog| catalog.rejected)
}

/// # Safety
/// `catalog` must be live; `out` must be writable. Pointers written into
/// `out` are owned by the catalog.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_catalog_stream(
    catalog: *const RatatoskrCatalog,
    index: usize,
    out: *mut RatatoskrStreamInfo,
) -> c_int {
    let Some(catalog) = (unsafe { catalog.as_ref() }) else {
        set_last_error("null catalog");
        return RATATOSKR_ERR_ARGUMENT;
    };
    let (Some(stream), Some(strings)) = (catalog.streams.get(index), catalog.strings.get(index)) else {
        set_last_error(format!("stream index {index} out of range"));
        return RATATOSKR_ERR_ARGUMENT;
    };
    if out.is_null() {
        set_last_error("null out pointer");
        return RATATOSKR_ERR_ARGUMENT;
    }
    let info = RatatoskrStreamInfo {
        stream_id: strings.scalars[0].as_ptr(),
        producer_id: strings.scalars[1].as_ptr(),
        label: strings.scalars[2].as_ptr(),
        state: strings.scalars[3].as_ptr(),
        video_source_count: strings.list_pointers[0].len(),
        video_source_ids: strings.list_pointers[0].as_ptr(),
        video_source_labels: strings.list_pointers[1].as_ptr(),
        audio_source_count: strings.list_pointers[2].len(),
        audio_source_ids: strings.list_pointers[2].as_ptr(),
        audio_source_labels: strings.list_pointers[3].as_ptr(),
        video_codec_count: strings.list_pointers[4].len(),
        video_codecs: strings.list_pointers[4].as_ptr(),
        audio_codec_count: strings.list_pointers[5].len(),
        audio_codecs: strings.list_pointers[5].as_ptr(),
        audio_sample_rate: stream.audio_sample_rate,
        audio_channels: stream.audio_channels,
        default_video_bitrate_kbps: stream.default_video_bitrate_kbps,
        default_latency_budget_ms: stream.default_latency_budget_ms,
        media_packet_bytes: stream.media_packet_bytes,
        media_connection_id: stream.media_connection_id,
    };
    unsafe { ptr::write(out, info) };
    RATATOSKR_OK
}

/// Asks the producer of stream `index` to start serving it to
/// `receiver_endpoint`. Empty source ids mean "none of that kind"; a zero
/// bitrate or latency means the producer's default. Blocks for the publish.
///
/// # Safety
/// `catalog` must be live; strings must be valid NUL-terminated UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_request_start(
    catalog: *const RatatoskrCatalog,
    index: usize,
    receiver_endpoint: *const c_char,
    video_source_id: *const c_char,
    audio_source_id: *const c_char,
    video_codec: *const c_char,
    audio_codec: *const c_char,
    video_bitrate_kbps: c_uint,
    latency_budget_ms: c_uint,
) -> c_int {
    let Some(catalog) = (unsafe { catalog.as_ref() }) else {
        set_last_error("null catalog");
        return RATATOSKR_ERR_ARGUMENT;
    };
    let Some(stream) = catalog.streams.get(index) else {
        set_last_error(format!("stream index {index} out of range"));
        return RATATOSKR_ERR_ARGUMENT;
    };
    let (Some(endpoint), Some(video_source), Some(audio_source), Some(video_codec), Some(audio_codec)) = (
        read_c_str(receiver_endpoint, "receiver endpoint"),
        read_c_str(video_source_id, "video source id"),
        read_c_str(audio_source_id, "audio source id"),
        read_c_str(video_codec, "video codec"),
        read_c_str(audio_codec, "audio codec"),
    ) else {
        return RATATOSKR_ERR_ARGUMENT;
    };
    let Ok(endpoint) = endpoint.parse::<SocketAddr>() else {
        set_last_error(format!("receiver endpoint {endpoint} is not host:port"));
        return RATATOSKR_ERR_ARGUMENT;
    };
    let observed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let request = start_request(
        stream,
        &catalog.options.runtime_id,
        endpoint,
        video_source,
        audio_source,
        video_codec,
        audio_codec,
        video_bitrate_kbps as u32,
        latency_budget_ms as u32,
        &observed_at,
    );
    match publish_request(&catalog.options, &request) {
        Ok(()) => RATATOSKR_OK,
        Err(error) => {
            set_last_error(format!("{error:#}"));
            RATATOSKR_ERR_REQUEST
        }
    }
}

/// Asks the producer of stream `index` to stop serving it to this receiver.
///
/// # Safety
/// `catalog` must be live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_request_stop(catalog: *const RatatoskrCatalog, index: usize) -> c_int {
    let Some(catalog) = (unsafe { catalog.as_ref() }) else {
        set_last_error("null catalog");
        return RATATOSKR_ERR_ARGUMENT;
    };
    let Some(stream) = catalog.streams.get(index) else {
        set_last_error(format!("stream index {index} out of range"));
        return RATATOSKR_ERR_ARGUMENT;
    };
    let observed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let request = stop_request(stream, &catalog.options.runtime_id, &observed_at);
    match publish_request(&catalog.options, &request) {
        Ok(()) => RATATOSKR_OK,
        Err(error) => {
            set_last_error(format!("{error:#}"));
            RATATOSKR_ERR_REQUEST
        }
    }
}

/// The producer's current answer for stream `index`: its state and detail,
/// copied into the buffers (truncated to capacity, always NUL-terminated).
/// Returns RATATOSKR_NONE when the mesh holds no request from this receiver.
///
/// # Safety
/// `catalog` must be live; buffers must be writable for their capacities.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_request_state(
    catalog: *const RatatoskrCatalog,
    index: usize,
    state: *mut c_char,
    state_capacity: usize,
    detail: *mut c_char,
    detail_capacity: usize,
) -> c_int {
    let Some(catalog) = (unsafe { catalog.as_ref() }) else {
        set_last_error("null catalog");
        return RATATOSKR_ERR_ARGUMENT;
    };
    let Some(stream) = catalog.streams.get(index) else {
        set_last_error(format!("stream index {index} out of range"));
        return RATATOSKR_ERR_ARGUMENT;
    };
    let held = match pull_request_state(&catalog.options, &stream.stream_id) {
        Ok(Some(held)) => held,
        Ok(None) => return RATATOSKR_NONE,
        Err(error) => {
            set_last_error(format!("{error:#}"));
            return RATATOSKR_ERR_CATALOG;
        }
    };
    copy_c(&held.state, state, state_capacity);
    copy_c(&held.detail, detail, detail_capacity);
    RATATOSKR_OK
}

fn copy_c(value: &str, buffer: *mut c_char, capacity: usize) {
    if buffer.is_null() || capacity == 0 {
        return;
    }
    let bytes = value.as_bytes();
    let writable = bytes.len().min(capacity - 1);
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), writable);
        *buffer.add(writable) = 0;
    }
}

/// # Safety
/// `catalog` must come from `ratatoskr_catalog_pull` and be closed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_catalog_close(catalog: *mut RatatoskrCatalog) {
    if !catalog.is_null() {
        drop(unsafe { Box::from_raw(catalog) });
    }
}

/// Last error on this thread, valid until the next call that fails. Never null.
#[unsafe(no_mangle)]
pub extern "C" fn ratatoskr_last_error(buffer: *mut c_char, capacity: usize) -> usize {
    LAST_ERROR.with(|slot| {
        let message = slot.borrow();
        let bytes = message.as_bytes();
        let writable = bytes.len().min(capacity.saturating_sub(1));
        if !buffer.is_null() && capacity > 0 {
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), writable);
                *buffer.add(writable) = 0;
            }
        }
        bytes.len()
    })
}

/// # Safety
/// `handle` must come from `ratatoskr_receiver_open` and be closed exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_close(handle: *mut RatatoskrHandle) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn open() -> *mut RatatoskrHandle {
        let bind = CString::new("127.0.0.1:0").expect("cstring");
        let id = CString::new("ratatoskr-ffi-test").expect("cstring");
        let mut handle: *mut RatatoskrHandle = ptr::null_mut();
        let rc = unsafe {
            ratatoskr_receiver_open(bind.as_ptr(), id.as_ptr(), 0x0BE0_0001, ptr::null(), &mut handle)
        };
        assert_eq!(rc, RATATOSKR_OK);
        assert!(!handle.is_null());
        handle
    }

    #[test]
    fn open_poll_close_round_trips() {
        let handle = open();
        assert!(unsafe { ratatoskr_receiver_local_port(handle) } != 0);
        assert_eq!(unsafe { ratatoskr_receiver_poll(handle) }, 0);
        unsafe { ratatoskr_receiver_close(handle) };
    }

    #[test]
    fn taking_from_an_empty_queue_reports_none_rather_than_failing() {
        let handle = open();
        let mut out_len = usize::MAX;
        let mut buffer = [0u8; 16];
        let mut kind = -99;
        let rc = unsafe {
            ratatoskr_receiver_next_payload(
                handle,
                buffer.as_mut_ptr(),
                buffer.len(),
                &mut out_len,
                &mut kind,
                ptr::null_mut(),
            )
        };
        assert_eq!(rc, RATATOSKR_NONE);
        assert_eq!(out_len, 0);
        unsafe { ratatoskr_receiver_close(handle) };
    }

    /// A short buffer must not consume the payload. Dropping it here would lose
    /// media the transport had already delivered intact.
    #[test]
    fn a_too_small_buffer_reports_the_size_and_keeps_the_payload() {
        let handle = open();
        unsafe { (*handle).pending.push_back((RATATOSKR_KIND_AUDIO, 0, vec![7u8; 64])) };

        let mut out_len = 0usize;
        let mut kind = -99;
        let mut small = [0u8; 8];
        let rc = unsafe {
            ratatoskr_receiver_next_payload(
                handle,
                small.as_mut_ptr(),
                small.len(),
                &mut out_len,
                &mut kind,
                ptr::null_mut(),
            )
        };
        assert_eq!(rc, RATATOSKR_ERR_BUFFER_TOO_SMALL);
        assert_eq!(out_len, 64, "the caller is told what it needs");
        assert_eq!(kind, RATATOSKR_KIND_AUDIO, "and what it is, before taking it");

        let mut big = [0u8; 64];
        let rc = unsafe {
            ratatoskr_receiver_next_payload(
                handle,
                big.as_mut_ptr(),
                big.len(),
                &mut out_len,
                &mut kind,
                ptr::null_mut(),
            )
        };
        assert_eq!(rc, RATATOSKR_OK, "the payload survived the short read");
        assert_eq!(out_len, 64);
        assert_eq!(kind, RATATOSKR_KIND_AUDIO);
        assert_eq!(big, [7u8; 64]);
        unsafe { ratatoskr_receiver_close(handle) };
    }

    #[test]
    fn null_arguments_are_refused_rather_than_dereferenced() {
        let mut handle: *mut RatatoskrHandle = ptr::null_mut();
        let id = CString::new("x").expect("cstring");
        assert_eq!(
            unsafe { ratatoskr_receiver_open(ptr::null(), id.as_ptr(), 1, ptr::null(), &mut handle) },
            RATATOSKR_ERR_ARGUMENT
        );
        assert_eq!(
            unsafe { ratatoskr_receiver_poll(ptr::null_mut()) },
            RATATOSKR_ERR_ARGUMENT
        );
        assert_eq!(unsafe { ratatoskr_receiver_local_port(ptr::null_mut()) }, 0);
        unsafe { ratatoskr_receiver_close(ptr::null_mut()) };
    }

    #[test]
    fn a_failure_leaves_a_readable_message() {
        let bad = CString::new("not-an-address").expect("cstring");
        let id = CString::new("x").expect("cstring");
        let mut handle: *mut RatatoskrHandle = ptr::null_mut();
        let rc = unsafe { ratatoskr_receiver_open(bad.as_ptr(), id.as_ptr(), 1, ptr::null(), &mut handle) };
        assert_eq!(rc, RATATOSKR_ERR_ARGUMENT);

        let mut buffer = [0i8; 256];
        let len = ratatoskr_last_error(buffer.as_mut_ptr(), buffer.len());
        assert!(len > 0, "a failure must say why");
        let message = unsafe { CStr::from_ptr(buffer.as_ptr()) }
            .to_str()
            .expect("utf8");
        assert!(message.contains("not-an-address"), "got {message:?}");
    }
}
