//! The subscription: bind, accept a producer, hand back media events, and
//! say back what was lost.
//!
//! Everything below the events is `cultnet-rs`. This file has no sequence
//! numbers, no acknowledgement state and no retransmit timers, and it must not
//! grow any — see the crate docs for why.

use std::net::{SocketAddr, UdpSocket};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use cultnet_rs::{
    CultNetRudpServerEvent, CultNetRudpServerHub, CultNetRudpServerHubOptions,
    CultNetRudpServerSessionContext, CultNetTransportFrame, GameCultMediaAudioPacketRecord,
    GameCultMediaReceiverFeedbackRecord, GameCultMediaWireRecord, MediaWireProvenance,
    decode_media_wire_record, encode_media_wire_record,
};

use crate::MEDIA_CHANNEL;
use crate::feedback::{FeedbackComposer, FeedbackOptions, FeedbackStats};
use crate::video::{ExpiredFrame, VideoAssembler, VideoAssemblerOptions, VideoFrame, VideoStats};

/// The producer name Ratatoskr puts on the envelopes it emits. The envelope
/// takes it as a parameter so no consumer's name is baked into the contract.
pub const RATATOSKR_MEDIA_PRODUCER: &str = "ratatoskr";

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
    /// How often the producer is told.
    pub feedback: FeedbackOptions,
    /// Where to copy each whole video access unit as UDP datagrams, for a
    /// decoder that reads a raw H.264/H.265 byte stream from a local socket
    /// (OBS's own ffmpeg source does). `None` keeps frames in-process.
    pub video_relay: Option<SocketAddr>,
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
    /// Feedback this receiver handed to the transport for the producer.
    FeedbackSent { record: Box<GameCultMediaReceiverFeedbackRecord> },
    /// Feedback that could not be handed to the transport, usually because no
    /// producer is attached to tell.
    FeedbackNotSent { reason: String },
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
    relay: Option<(UdpSocket, SocketAddr)>,
    relayed_frames: u64,
    runtime_id: String,
    producer: Option<CultNetRudpServerSessionContext>,
    video: VideoAssembler,
    feedback: FeedbackComposer,
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
            options.runtime_id.clone(),
            socket,
            options.connection_id,
        ))
        .context("opening Ratatoskr CultNet RUDP receiver")?;

        let relay = match options.video_relay {
            Some(target) => {
                let socket = UdpSocket::bind("127.0.0.1:0").context("binding the video relay socket")?;
                Some((socket, target))
            }
            None => None,
        };

        Ok(Self {
            hub,
            relay,
            relayed_frames: 0,
            feedback: FeedbackComposer::new(options.runtime_id.clone(), options.feedback),
            runtime_id: options.runtime_id,
            producer: None,
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

    /// Drains what has arrived, gives up on what is too old, and tells the
    /// producer what is wanted. Never blocks; a caller drives this from its
    /// own loop so the renderer keeps its own cadence.
    pub fn poll(&mut self) -> Result<Vec<MediaEvent>> {
        let now = Instant::now();
        let mut events = Vec::new();
        while let Some(event) = self.hub.receive_event_once()? {
            match event {
                CultNetRudpServerEvent::Connected { session } => {
                    events.push(MediaEvent::ProducerAttached {
                        remote: session.remote_addr,
                    });
                    self.producer = Some(session);
                }
                CultNetRudpServerEvent::Disconnected { session, .. } => {
                    events.push(MediaEvent::ProducerDetached {
                        remote: session.remote_addr,
                    });
                    if self.producer.as_ref().is_some_and(|producer| {
                        producer.session_generation == session.session_generation
                    }) {
                        self.producer = None;
                    }
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
        self.hub.poll_resends()?;

        let repairs = self.video.due_repairs(now);
        let expired = self.video.expire(now);
        events.extend(
            expired
                .iter()
                .cloned()
                .map(|frame| MediaEvent::VideoFrameExpired { frame }),
        );
        if !repairs.is_empty() || !expired.is_empty() {
            let observed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            let records = self.feedback.compose(
                &repairs,
                &expired,
                self.video.highest_delivered_frame_id(),
                now,
                &observed_at,
            )?;
            for record in records {
                events.push(self.send_feedback(record, &observed_at));
            }
        }
        Ok(events)
    }

    fn send_feedback(
        &mut self,
        record: GameCultMediaReceiverFeedbackRecord,
        stored_at: &str,
    ) -> MediaEvent {
        let Some(producer) = self.producer.as_ref() else {
            self.feedback.record_not_sent();
            return MediaEvent::FeedbackNotSent {
                reason: "no producer is attached".to_string(),
            };
        };
        let wire = GameCultMediaWireRecord::Feedback(record);
        let encoded = encode_media_wire_record(
            &wire,
            MediaWireProvenance {
                stored_at,
                runtime_id: &self.runtime_id,
                role: "receiver",
                producer: RATATOSKR_MEDIA_PRODUCER,
            },
        )
        .and_then(|payload| self.hub.send(producer, MEDIA_CHANNEL, payload));
        let GameCultMediaWireRecord::Feedback(record) = wire else {
            unreachable!("constructed above");
        };
        match encoded {
            Ok(()) => {
                self.feedback.record_sent();
                MediaEvent::FeedbackSent {
                    record: Box::new(record),
                }
            }
            Err(error) => {
                self.feedback.record_not_sent();
                MediaEvent::FeedbackNotSent {
                    reason: format!("{error:#}"),
                }
            }
        }
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
            Ok(Some(frame)) => {
                self.relay_video(&frame.bytes);
                Some(MediaEvent::VideoFrame {
                    frame: Box::new(frame),
                })
            }
            Ok(None) => None,
            Err(error) => {
                self.rejected += 1;
                Some(MediaEvent::Rejected {
                    reason: format!("{error:#}"),
                })
            }
        }
    }

    /// A whole access unit as a raw byte stream over loopback. Datagrams stay
    /// under the UDP limit; a raw H.264 demuxer reads the stream, not the
    /// packetisation, and loopback keeps them in order.
    fn relay_video(&mut self, bytes: &[u8]) {
        let Some((socket, target)) = self.relay.as_ref() else {
            return;
        };
        const DATAGRAM: usize = 60_000;
        let mut ok = true;
        for piece in bytes.chunks(DATAGRAM) {
            if socket.send_to(piece, target).is_err() {
                ok = false;
                break;
            }
        }
        if ok {
            self.relayed_frames += 1;
        }
    }

    /// Whole frames copied to the relay. Zero with no relay configured.
    pub fn relayed_frames(&self) -> u64 {
        self.relayed_frames
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

    /// What was said back to the producer.
    pub fn feedback_stats(&self) -> FeedbackStats {
        self.feedback.stats()
    }
}

/// Frames on other channels belong to other conversations.
fn media_payload(frame: CultNetTransportFrame) -> Option<Vec<u8>> {
    (frame.channel_id == MEDIA_CHANNEL && !frame.payload.is_empty()).then_some(frame.payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use cultnet_rs::{
        CultNetRudpSocketTransportConnection, CultNetRudpSocketTransportOptions,
        CultNetTransportDelivery, GameCultMediaVideoAccessUnitRecord,
    };

    const CONNECTION: u32 = 0x0BE0_0001;

    fn loopback() -> ReceiverOptions {
        ReceiverOptions {
            bind: "127.0.0.1:0".parse().expect("parses"),
            runtime_id: "ratatoskr-test".to_string(),
            connection_id: CONNECTION,
            video: VideoAssemblerOptions::default(),
            feedback: FeedbackOptions::default(),
            video_relay: None,
        }
    }

    fn chunk(frame_id: u64, index: u16, count: u16, payload: Vec<u8>) -> GameCultMediaWireRecord {
        GameCultMediaWireRecord::Video(GameCultMediaVideoAccessUnitRecord {
            stream_id: "s".into(),
            session_id: "x".into(),
            frame_id,
            codec: "h264".into(),
            pts_ticks: 0,
            duration_ticks: 3_000,
            timebase_num: 1,
            timebase_den: 90_000,
            keyframe: true,
            dependency_frame_id: None,
            deadline_ticks: 1_800,
            chunk_index: index,
            chunk_count: count,
            payload,
        })
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
        assert_eq!(receiver.feedback_stats(), FeedbackStats::default());
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
        assert_eq!(receiver.admit(chunk(1, 0, 2, vec![1, 2]), now), None);
        let Some(MediaEvent::VideoFrame { frame }) = receiver.admit(chunk(1, 1, 2, vec![3]), now) else {
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

    /// The relay hands a local decoder the frame exactly as reassembled.
    #[test]
    fn a_relayed_frame_arrives_whole_on_loopback() {
        let listener = UdpSocket::bind("127.0.0.1:0").expect("binds");
        listener.set_read_timeout(Some(Duration::from_secs(2))).expect("timeout");
        let mut receiver = RatatoskrReceiver::open(ReceiverOptions {
            video_relay: Some(listener.local_addr().expect("addr")),
            ..loopback()
        })
        .expect("opens");
        let now = Instant::now();
        receiver.admit(chunk(1, 0, 2, vec![1, 2]), now);
        receiver.admit(chunk(1, 1, 2, vec![3]), now);
        let mut buffer = [0u8; 16];
        let (len, _) = listener.recv_from(&mut buffer).expect("the frame was relayed");
        assert_eq!(&buffer[..len], &[1, 2, 3]);
        assert_eq!(receiver.relayed_frames(), 1);
    }

    /// Feedback with nobody to send it to is reported, not lost silently.
    #[test]
    fn feedback_without_a_producer_is_reported_not_sent() {
        let mut receiver = RatatoskrReceiver::open(ReceiverOptions {
            video: VideoAssemblerOptions {
                repair_first_wait: Duration::ZERO,
                ..Default::default()
            },
            ..loopback()
        })
        .expect("opens");
        assert_eq!(receiver.admit(chunk(1, 0, 3, vec![1]), Instant::now()), None);
        let events = receiver.poll().expect("polls");
        assert!(
            matches!(events.as_slice(), [MediaEvent::FeedbackNotSent { reason }] if reason.contains("no producer")),
            "got {events:?}"
        );
        assert_eq!(receiver.feedback_stats().not_sent, 1);
    }

    /// The whole return path over real sockets: a producer connects, loses a
    /// chunk on the way, and is told which one — then, when the frame is given
    /// up on, that it was late and that a keyframe is wanted.
    #[test]
    fn a_producer_is_told_what_was_lost() -> Result<()> {
        let mut receiver = RatatoskrReceiver::open(ReceiverOptions {
            video: VideoAssemblerOptions {
                max_frame_age: Duration::from_millis(150),
                repair_first_wait: Duration::from_millis(20),
                repair_retry_wait: Duration::from_millis(20),
                repair_max_requests: 3,
                ..Default::default()
            },
            ..loopback()
        })?;
        let receiver_addr = receiver.local_addr()?;

        let socket = UdpSocket::bind("127.0.0.1:0")?;
        socket.set_nonblocking(true)?;
        let mut options = CultNetRudpSocketTransportOptions::client("producer-test", socket, receiver_addr, CONNECTION);
        options.media_delivery = Some(CultNetTransportDelivery::Unreliable);
        let mut producer = CultNetRudpSocketTransportConnection::new(options)?;
        producer.connect(b"test".to_vec())?;

        fn pump(
            receiver: &mut RatatoskrReceiver,
            producer: &mut CultNetRudpSocketTransportConnection,
            feedback: &mut Vec<GameCultMediaReceiverFeedbackRecord>,
        ) -> Result<Vec<MediaEvent>> {
            let events = receiver.poll()?;
            producer.poll_resends()?;
            while let Some(frame) = producer.receive_once()? {
                if frame.channel_id == MEDIA_CHANNEL {
                    if let Ok(GameCultMediaWireRecord::Feedback(record)) = decode_media_wire_record(&frame.payload) {
                        feedback.push(record);
                    }
                }
            }
            Ok(events)
        }
        let mut feedback = Vec::new();

        let deadline = Instant::now() + Duration::from_secs(3);
        while !producer.connected() {
            pump(&mut receiver, &mut producer, &mut feedback)?;
            assert!(Instant::now() < deadline, "producer never connected");
            std::thread::sleep(Duration::from_millis(2));
        }

        // Three chunks; the middle one never leaves.
        let provenance = MediaWireProvenance { stored_at: "t", runtime_id: "producer-test", role: "producer", producer: "test" };
        for index in [0u16, 2] {
            let record = chunk(9, index, 3, vec![index as u8; 4]);
            producer.send(MEDIA_CHANNEL, encode_media_wire_record(&record, provenance.clone())?)?;
        }

        let deadline = Instant::now() + Duration::from_secs(3);
        let mut expired = false;
        while Instant::now() < deadline {
            for event in pump(&mut receiver, &mut producer, &mut feedback)? {
                if let MediaEvent::VideoFrameExpired { frame } = event {
                    assert_eq!(frame.key.frame_id, 9);
                    expired = true;
                }
            }
            if expired && feedback.iter().any(|record| !record.late_frame_ids.is_empty()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }

        let repairs = feedback.iter().filter(|record| !record.missing_video_chunk_keys.is_empty()).collect::<Vec<_>>();
        assert!(!repairs.is_empty(), "the producer was never asked for the missing chunk; feedback: {feedback:?}");
        for record in &repairs {
            assert_eq!(record.missing_video_chunk_keys, vec!["9:1"]);
            assert_eq!(record.receiver_id, "ratatoskr-test");
            assert_eq!(record.stream_id, "s");
        }
        assert!(repairs.iter().all(|record| !record.requested_keyframe), "a repair request is not a keyframe request");

        let late = feedback.iter().find(|record| !record.late_frame_ids.is_empty()).expect("the frame was reported late");
        assert_eq!(late.late_frame_ids, vec![9]);
        assert!(late.requested_keyframe, "a lost frame asks for a keyframe");

        let stats = receiver.feedback_stats();
        assert!(stats.sent >= 2, "{stats:?}");
        assert_eq!(stats.keyframes_requested, 1);
        assert_eq!(receiver.video_stats().expired, 1);
        Ok(())
    }
}
