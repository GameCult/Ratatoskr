//! Ratatoskr carries CultMesh media streams to a renderer and owns none of them.
//!
//! It exists because the OBS plugin it replaces hand-rolled CultNet RUDP —
//! acknowledgement, fragment reassembly, sockets, URL parsing, wire framing and
//! two erasure codes — in 973 lines of C++ headers that linked no CultLib. Every
//! CultNet fix from August and September reached `cultnet-rs` and `cultnet-ts`
//! and could not reach it, so the receiving end of a protocol drifted away from
//! the sending end while both were being maintained.
//!
//! The rule that follows from that: **this crate implements no transport.**
//! It consumes `cultnet-rs`. If something here starts to look like a sequence
//! number, an ACK mask or a retransmit timer, it belongs upstream in CultLib
//! where both ends of the conversation can share it.
//!
//! # Authority
//!
//! - **Owner:** Ratatoskr owns turning a CultMesh media stream into decoded
//!   frames a renderer can present, and the lifetime of that subscription.
//! - **Inputs:** typed media records over a CultNet RUDP media channel.
//! - **Outputs:** ordered, reassembled media events, plus the receiver feedback
//!   a producer needs to adapt.
//! - **Not Ratatoskr's:** the media contract (CultLib), discovery and rendezvous
//!   (Odin), what gets captured (Muninn), how it is presented (the renderer).
//!
//! Ratatoskr is a consumer of a general contract. It is not the contract, and it
//! has no opinion about who produced the stream.

pub mod ffi;
mod receiver;

pub use receiver::{MediaEvent, RatatoskrReceiver, ReceiverOptions};

/// The CultNet channel media rides. Named here once so nothing downstream
/// guesses it from a string literal.
pub const MEDIA_CHANNEL: &str = "media";
