//! The subscription: bind, accept a producer, hand back media events.
//!
//! Everything below the events is `cultnet-rs`. This file has no sequence
//! numbers, no acknowledgement state and no retransmit timers, and it must not
//! grow any — see the crate docs for why.

use std::net::{SocketAddr, UdpSocket};

use anyhow::{Context, Result};
use cultnet_rs::{
    CultNetRudpServerHub, CultNetRudpServerHubOptions, CultNetRudpServerEvent,
    CultNetTransportFrame, GameCultMediaWireRecord, decode_media_wire_record,
};

use crate::MEDIA_CHANNEL;

#[derive(Clone, Debug)]
pub struct ReceiverOptions {
    /// Where to listen for the producer.
    pub bind: SocketAddr,
    /// Identifies this receiver to the producer and in CultNet logs.
    pub runtime_id: String,
    /// Must match the producer's media connection id.
    pub connection_id: u32,
}

/// What the renderer is given. Deliberately not the wire record: a renderer
/// should not have to know how a frame was chunked to present it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MediaEvent {
    /// A producer attached.
    ProducerAttached { remote: SocketAddr },
    /// A media record arrived and decoded.
    Record { record: Box<GameCultMediaWireRecord> },
    /// A media payload arrived but is not a media record this build understands.
    ///
    /// Surfaced rather than dropped: a consumer that silently discards what it
    /// cannot parse gives a producer no way to learn that its stream is
    /// unreadable, which is how a receiver and a sender drift apart.
    Undecodable { bytes: Vec<u8>, reason: String },
    /// A producer detached, gracefully or otherwise.
    ProducerDetached { remote: SocketAddr },
}

pub struct RatatoskrReceiver {
    hub: CultNetRudpServerHub,
    payloads: u64,
    payload_bytes: u64,
    undecodable: u64,
}

impl RatatoskrReceiver {
    pub fn open(options: ReceiverOptions) -> Result<Self> {
        let socket = UdpSocket::bind(options.bind)
            .with_context(|| format!("binding Ratatoskr media receiver to {}", options.bind))?;
        socket
            .set_nonblocking(true)
            .context("setting Ratatoskr media receiver nonblocking")?;

        let hub = CultNetRudpServerHub::new(CultNetRudpServerHubOptions::new(
            options.runtime_id,
            socket,
            options.connection_id,
        ))
        .context("opening Ratatoskr CultNet RUDP receiver")?;

        Ok(Self {
            hub,
            payloads: 0,
            payload_bytes: 0,
            undecodable: 0,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.hub.local_addr()
    }

    /// Drains what has arrived. Never blocks; a caller drives this from its own
    /// loop so the renderer keeps its own cadence.
    pub fn poll(&mut self) -> Result<Vec<MediaEvent>> {
        let mut events = Vec::new();
        while let Some(event) = self.hub.receive_event_once()? {
            match event {
                CultNetRudpServerEvent::Connected { session } => {
                    events.push(MediaEvent::ProducerAttached {
                        remote: session.remote_addr,
                    });
                }
                CultNetRudpServerEvent::Disconnected { session, .. } => {
                    events.push(MediaEvent::ProducerDetached {
                        remote: session.remote_addr,
                    });
                }
                CultNetRudpServerEvent::Frame { frame, .. } => {
                    if let Some(payload) = media_payload(frame) {
                        self.payloads += 1;
                        self.payload_bytes += payload.len() as u64;
                        events.push(match decode_media_wire_record(&payload) {
                            Ok(record) => MediaEvent::Record {
                                record: Box::new(record),
                            },
                            Err(error) => {
                                self.undecodable += 1;
                                MediaEvent::Undecodable {
                                    bytes: payload,
                                    reason: format!("{error:#}"),
                                }
                            }
                        });
                    }
                }
                _ => {}
            }
        }
        Ok(events)
    }

    /// Payloads admitted and their total size. Counted from what was actually
    /// delivered, never from what was requested — a health signal derived from
    /// configuration is how the last generation reported a healthy audio lane
    /// into silence.
    pub fn delivered(&self) -> (u64, u64) {
        (self.payloads, self.payload_bytes)
    }

    /// Payloads that arrived on the media channel and did not decode. A
    /// non-zero count here means the producer and this build disagree about the
    /// envelope, which is worth knowing loudly.
    pub fn undecodable(&self) -> u64 {
        self.undecodable
    }
}

/// Frames on other channels belong to other conversations.
fn media_payload(frame: CultNetTransportFrame) -> Option<Vec<u8>> {
    (frame.channel_id == MEDIA_CHANNEL && !frame.payload.is_empty()).then_some(frame.payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback() -> ReceiverOptions {
        ReceiverOptions {
            bind: "127.0.0.1:0".parse().expect("parses"),
            runtime_id: "ratatoskr-test".to_string(),
            connection_id: 0x0BE0_0001,
        }
    }

    #[test]
    fn a_receiver_binds_and_reports_where() {
        let receiver = RatatoskrReceiver::open(loopback()).expect("opens");
        let addr = receiver.local_addr().expect("has an address");
        assert_ne!(addr.port(), 0, "an ephemeral bind must resolve to a real port");
    }

    #[test]
    fn polling_an_idle_receiver_yields_nothing_and_does_not_block() {
        let mut receiver = RatatoskrReceiver::open(loopback()).expect("opens");
        assert!(receiver.poll().expect("polls").is_empty());
        assert_eq!(receiver.delivered(), (0, 0));
    }

    #[test]
    fn frames_from_other_channels_are_not_media() {
        assert_eq!(
            media_payload(CultNetTransportFrame {
                channel_id: "schema".to_string(),
                payload: vec![1, 2, 3],
            }),
            None
        );
        assert_eq!(
            media_payload(CultNetTransportFrame {
                channel_id: MEDIA_CHANNEL.to_string(),
                payload: vec![1, 2, 3],
            }),
            Some(vec![1, 2, 3])
        );
    }

    /// An empty payload is not a delivery. Counting it would inflate the one
    /// signal a producer uses to decide whether the stream is alive.
    #[test]
    fn an_empty_media_frame_is_not_a_payload() {
        assert_eq!(
            media_payload(CultNetTransportFrame {
                channel_id: MEDIA_CHANNEL.to_string(),
                payload: Vec::new(),
            }),
            None
        );
    }
}
