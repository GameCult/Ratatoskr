//! The subscription: bind, accept a producer, hand back media events.
//!
//! Everything below the events is `cultnet-rs`. This file has no sequence
//! numbers, no acknowledgement state and no retransmit timers, and it must not
//! grow any — see the crate docs for why.

use std::net::{SocketAddr, UdpSocket};
use std::time::Instant;

use anyhow::{Context, Result};
use cultnet_rs::{
    CultNetRudpServerHub, CultNetRudpServerHubOptions, CultNetRudpServerEvent,
    CultNetTransportFrame, GameCultMediaAudioPacketRecord, GameCultMediaReceiverFeedbackRecord,
    GameCultMediaWireRecord, decode_media_wire_record,
};

use crate::MEDIA_CHANNEL;
use crate::video::{ExpiredFrame, VideoAssembler, VideoAssemblerOptions, VideoFrame, VideoStats};

#[derive(Clone, Debug)]
pub struct ReceiverOptions {
    /// Where to listen for the producer.
    pub bind: SocketAddr,
    /// Identifies this receiver to the producer and in CultNet logs.
    pub runtime_id: String,
    /// Must match the producer's media connection id.
    pub connection_id: u32,
    /// How incomplete video frames are waited for and given up on.
    pub video: VideoAssemblerOptions,
}

/// What the renderer is given. Deliberately not the wire record: a renderer
/// should not have to know how a frame was chunked to present it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MediaEvent {
    /// A producer attached.
    ProducerAttached { remote: SocketAddr },
    /// A whole video access unit, reassembled and, if needed, repaired.
    VideoFrame { frame: Box<VideoFrame> },
    /// An incomplete video frame was given up on. What it lacked is named so
    /// a receiver can tell the producer.
    VideoFrameExpired { frame: ExpiredFrame },
    /// An audio packet. Audio is not chunked on the wire, so this is the
    /// record as sent.
    Audio { record: Box<GameCultMediaAudioPacketRecord> },
    /// Feedback flows the other way; a producer echoing it here is unusual
    /// but not an error.
    Feedback { record: Box<GameCultMediaReceiverFeedbackRecord> },
    /// A media payload arrived but is not a media record this build understands.
    ///
    /// Surfaced rather than dropped: a consumer that silently discards what it
    /// cannot parse gives a producer no way to learn that its stream is
    /// unreadable, which is how a receiver and a sender drift apart.
    Undecodable { bytes: Vec<u8>, reason: String },
    /// A record decoded but contradicted the frame it belongs to.
    Rejected { reason: String },
    /// A producer detached, gracefully or otherwise.
    ProducerDetached { remote: SocketAddr },
}

pub struct RatatoskrReceiver {
    hub: CultNetRudpServerHub,
    video: VideoAssembler,
    payloads: u64,
    payload_bytes: u64,
    undecodable: u64,
    rejected: u64,
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
            video: VideoAssembler::new(options.video),
            payloads: 0,
            payload_bytes: 0,
            undecodable: 0,
            rejected: 0,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.hub.local_addr()
    }

    /// Drains what has arrived. Never blocks; a caller drives this from its own
    /// loop so the renderer keeps its own cadence.
    pub fn poll(&mut self) -> Result<Vec<MediaEvent>> {
        let now = Instant::now();
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
                        match decode_media_wire_record(&payload) {
                            Ok(record) => events.extend(self.admit(record, now)),
                            Err(error) => {
                                self.undecodable += 1;
                                events.push(MediaEvent::Undecodable {
                                    bytes: payload,
                                    reason: format!("{error:#}"),
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        events.extend(
            self.video
                .expire(now)
                .into_iter()
                .map(|frame| MediaEvent::VideoFrameExpired { frame }),
        );
        Ok(events)
    }

    fn admit(&mut self, record: GameCultMediaWireRecord, now: Instant) -> Option<MediaEvent> {
        let assembled = match record {
            GameCultMediaWireRecord::Video(record) => self.video.insert_chunk(record, now),
            GameCultMediaWireRecord::VideoParity(record) => self.video.insert_parity(record, now),
            GameCultMediaWireRecord::Audio(record) => {
                return Some(MediaEvent::Audio {
                    record: Box::new(record),
                });
            }
            GameCultMediaWireRecord::Feedback(record) => {
                return Some(MediaEvent::Feedback {
                    record: Box::new(record),
                });
            }
        };
        match assembled {
            Ok(Some(frame)) => Some(MediaEvent::VideoFrame {
                frame: Box::new(frame),
            }),
            Ok(None) => None,
            Err(error) => {
                self.rejected += 1;
                Some(MediaEvent::Rejected {
                    reason: format!("{error:#}"),
                })
            }
        }
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

    /// Records that decoded but contradicted their frame.
    pub fn rejected(&self) -> u64 {
        self.rejected
    }

    /// What became of the video frames: completed, repaired, given up on.
    pub fn video_stats(&self) -> VideoStats {
        self.video.stats()
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
            video: VideoAssemblerOptions::default(),
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
        assert_eq!(receiver.video_stats(), VideoStats::default());
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

    /// The receiver hands a renderer whole frames, never chunks: the wire's
    /// chunking is the transport's business and stops here.
    #[test]
    fn video_records_become_frames_and_audio_stays_as_sent() {
        let mut receiver = RatatoskrReceiver::open(loopback()).expect("opens");
        let now = Instant::now();
        let chunk = |index: u16, payload: Vec<u8>| GameCultMediaWireRecord::Video(
            cultnet_rs::GameCultMediaVideoAccessUnitRecord {
                stream_id: "s".into(),
                session_id: "x".into(),
                frame_id: 1,
                codec: "h264".into(),
                pts_ticks: 0,
                duration_ticks: 3_000,
                timebase_num: 1,
                timebase_den: 90_000,
                keyframe: true,
                dependency_frame_id: None,
                deadline_ticks: 1_800,
                chunk_index: index,
                chunk_count: 2,
                payload,
            },
        );
        assert_eq!(receiver.admit(chunk(0, vec![1, 2]), now), None);
        let Some(MediaEvent::VideoFrame { frame }) = receiver.admit(chunk(1, vec![3]), now) else {
            panic!("second chunk completes the frame");
        };
        assert_eq!(frame.bytes, vec![1, 2, 3]);
        assert_eq!(receiver.video_stats().completed, 1);

        let audio = GameCultMediaWireRecord::Audio(GameCultMediaAudioPacketRecord {
            stream_id: "s".into(),
            session_id: "x".into(),
            packet_id: 7,
            codec: "opus".into(),
            pts_ticks: 0,
            duration_ticks: 960,
            timebase_num: 1,
            timebase_den: 48_000,
            deadline_ticks: 960,
            payload: vec![9, 9, 9],
        });
        let Some(MediaEvent::Audio { record }) = receiver.admit(audio, now) else {
            panic!("audio passes through");
        };
        assert_eq!(record.payload, vec![9, 9, 9]);
    }
}
