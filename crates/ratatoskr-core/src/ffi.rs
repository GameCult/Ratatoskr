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
use std::ffi::{CStr, c_char, c_int, c_uint};
use std::net::SocketAddr;
use std::ptr;

use crate::feedback::FeedbackOptions;
use crate::receiver::{MediaEvent, RatatoskrReceiver, ReceiverOptions};
use crate::video::VideoAssemblerOptions;

pub const RATATOSKR_OK: c_int = 0;
pub const RATATOSKR_NONE: c_int = 1;
pub const RATATOSKR_ERR_ARGUMENT: c_int = -1;
pub const RATATOSKR_ERR_OPEN: c_int = -2;
pub const RATATOSKR_ERR_POLL: c_int = -3;
pub const RATATOSKR_ERR_BUFFER_TOO_SMALL: c_int = -4;

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
    pending: VecDeque<(c_int, Vec<u8>)>,
    attached: Option<SocketAddr>,
}

/// # Safety
/// `bind` and `runtime_id` must be valid NUL-terminated UTF-8. `out_handle`
/// must be a valid pointer to write one handle into.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ratatoskr_receiver_open(
    bind: *const c_char,
    runtime_id: *const c_char,
    connection_id: c_uint,
    out_handle: *mut *mut RatatoskrHandle,
) -> c_int {
    if bind.is_null() || runtime_id.is_null() || out_handle.is_null() {
        set_last_error("null argument");
        return RATATOSKR_ERR_ARGUMENT;
    }
    let (bind, runtime_id) = unsafe { (CStr::from_ptr(bind), CStr::from_ptr(runtime_id)) };
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
                        handle.pending.push_back((RATATOSKR_KIND_VIDEO, frame.bytes));
                    }
                    MediaEvent::Audio { record } => {
                        if !record.payload.is_empty() {
                            handle.pending.push_back((RATATOSKR_KIND_AUDIO, record.payload));
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
) -> c_int {
    let Some(handle) = (unsafe { handle.as_mut() }) else {
        set_last_error("null handle");
        return RATATOSKR_ERR_ARGUMENT;
    };
    if out_len.is_null() {
        set_last_error("null out_len");
        return RATATOSKR_ERR_ARGUMENT;
    }
    let Some((kind, payload)) = handle.pending.front() else {
        unsafe { *out_len = 0 };
        return RATATOSKR_NONE;
    };
    let needed = payload.len();
    unsafe { *out_len = needed };
    if !out_kind.is_null() {
        unsafe { *out_kind = *kind };
    }
    if needed > capacity || buffer.is_null() {
        set_last_error(format!("buffer of {capacity} too small for payload of {needed}"));
        return RATATOSKR_ERR_BUFFER_TOO_SMALL;
    }
    let (_, payload) = handle.pending.pop_front().expect("front checked above");
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
            ratatoskr_receiver_open(bind.as_ptr(), id.as_ptr(), 0x0BE0_0001, &mut handle)
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
        unsafe { (*handle).pending.push_back((RATATOSKR_KIND_AUDIO, vec![7u8; 64])) };

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
            unsafe { ratatoskr_receiver_open(ptr::null(), id.as_ptr(), 1, &mut handle) },
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
        let rc = unsafe { ratatoskr_receiver_open(bad.as_ptr(), id.as_ptr(), 1, &mut handle) };
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
