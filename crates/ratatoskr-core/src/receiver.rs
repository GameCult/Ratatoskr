//! The subscription: dial the producer, hand back media events, and say back
//! what was lost.
//!
//! The consumer dials. A producer listens on the endpoint it advertises and
//! this receiver connects to it, so the machine running the renderer admits
//! nothing inbound: no firewall rule, no port forward. The producer is the
//! host doing the serving, and one open port there is the ordinary ask.
//!
//! Everything below the events is `cultnet-rs`. This file has no sequence
//! numbers, no acknowledgement state and no retransmit timers, and it must not
//! grow any — see the crate docs for why.

use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use cultnet_rs::{
    CultNetRudpSocketTransportConnection, CultNetRudpSocketTransportOptions, CultNetTransportFrame,
    GAMECULT_MEDIA_AUDIO_CHANNEL, GameCultMediaAudioPacketRecord,
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
    /// The producer's advertised media endpoint, to dial.
    pub producer: SocketAddr,
    /// Identifies this receiver to the producer: it is the connect payload the
    /// producer sees, the `receiver_id` on feedback, and the CultNet runtime id.
    pub runtime_id: String,
    /// Must match the producer's advertised media connection id.
    pub connection_id: u32,
    /// How incomplete video frames are waited for and given up on.
    pub video: VideoAssemblerOptions,
    /// How often the producer is told.
    pub feedback: FeedbackOptions,
    /// Where to copy each whole video access unit as UDP datagrams, for a
    /// decoder that reads a raw H.264/H.265 byte stream from a local socket
    /// (OBS's own ffmpeg source does). `None` keeps frames in-process.
    pub video_relay: Option<SocketAddr>,
    /// How long an unanswered dial is left before dialling afresh. A producer
    /// is usually still spinning up when the first dial goes out.
    pub redial_after: Duration,
    /// How long an attached producer may be silent before it is presumed gone
    /// and dialled afresh. The receiver pings at a third of this.
    pub silence_timeout: Duration,
}

impl ReceiverOptions {
    pub fn new(producer: SocketAddr, runtime_id: impl Into<String>, connection_id: u32) -> Self {
        Self {
            producer,
            runtime_id: runtime_id.into(),
            connection_id,
            video: VideoAssemblerOptions::default(),
            feedback: FeedbackOptions::default(),
            video_relay: None,
            redial_after: Duration::from_secs(1),
            silence_timeout: Duration::from_secs(4),
        }
    }
}

/// What the renderer is given. Deliberately not the wire record: a renderer
/// should not have to know how a frame was chunked to present it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MediaEvent {
    /// The producer answered the dial.
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
    /// Feedback that could not be handed to the transport, usually because
    /// the producer has not answered the dial yet.
    FeedbackNotSent { reason: String },
    /// A media payload arrived but is not a media record this build understands.
    ///
    /// Surfaced rather than dropped: a consumer that silently discards what it
    /// cannot parse gives a producer no way to learn that its stream is
    /// unreadable, which is how a receiver and a sender drift apart.
    Undecodable { bytes: Vec<u8>, reason: String },
    /// A record decoded but contradicted the frame it belongs to.
    Rejected { reason: String },
    /// The producer went away, by saying so or by silence. The receiver dials
    /// again on its own; this is so a renderer can say why the picture froze.
    ProducerDetached { remote: SocketAddr, reason: String },
}

pub struct RatatoskrReceiver {
    producer: SocketAddr,
    runtime_id: String,
    connection_id: u32,
    redial_after: Duration,
    silence_timeout: Duration,
    transport: CultNetRudpSocketTransportConnection,
    dialled_at: Instant,
    pinged_at: Instant,
    attached: bool,
    redials: u64,
    relay: Option<(UdpSocket, SocketAddr)>,
    relayed_frames: u64,
    video: VideoAssembler,
    feedback: FeedbackComposer,
    payloads: u64,
    payload_bytes: u64,
    undecodable: u64,
    rejected: u64,
}

impl RatatoskrReceiver {
    /// Dials the producer. Returns once the dial is sent, not once it is
    /// answered: attachment is reported by [`MediaEvent::ProducerAttached`]
    /// from [`poll`](Self::poll), and the dial is repeated until then.
    pub fn open(options: ReceiverOptions) -> Result<Self> {
        let transport = dial(options.producer, &options.runtime_id, options.connection_id)?;
        let relay = match options.video_relay {
            Some(target) => {
                let socket = UdpSocket::bind("127.0.0.1:0").context("binding the video relay socket")?;
                Some((socket, target))
            }
            None => None,
        };
        let now = Instant::now();
        Ok(Self {
            producer: options.producer,
            connection_id: options.connection_id,
            redial_after: options.redial_after,
            silence_timeout: options.silence_timeout,
            transport,
            dialled_at: now,
            pinged_at: now,
            attached: false,
            redials: 0,
            relay,
            relayed_frames: 0,
            feedback: FeedbackComposer::new(options.runtime_id.clone(), options.feedback),
            runtime_id: options.runtime_id,
            video: VideoAssembler::new(options.video),
            payloads: 0,
            payload_bytes: 0,
            undecodable: 0,
            rejected: 0,
        })
    }

    /// Where the dial goes.
    pub fn producer(&self) -> SocketAddr {
        self.producer
    }

    /// Whether the producer has answered and not since gone quiet.
    pub fn attached(&self) -> bool {
        self.attached
    }

    /// Dials made after the first, whether because nobody answered or because
    /// an attached producer went away.
    pub fn redials(&self) -> u64 {
        self.redials
    }

    /// Drains what has arrived, gives up on what is too old, tells the
    /// producer what is wanted, and keeps the dial alive. Never blocks; a
    /// caller drives this from its own loop so the renderer keeps its own
    /// cadence.
    pub fn poll(&mut self) -> Result<Vec<MediaEvent>> {
        let now = Instant::now();
        let mut events = Vec::new();
        while let Some(frame) = self.transport.receive_once()? {
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
        self.transport.poll_resends()?;
        events.extend(self.keep_dialling(now)?);

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

    /// The attach/detach state machine. The transport knows whether its
    /// session is up; this decides what to do about it.
    fn keep_dialling(&mut self, now: Instant) -> Result<Option<MediaEvent>> {
        if !self.attached {
            if self.transport.connected() {
                self.attached = true;
                self.pinged_at = now;
                return Ok(Some(MediaEvent::ProducerAttached { remote: self.producer }));
            }
            if now.duration_since(self.dialled_at) >= self.redial_after {
                self.redial(now)?;
            }
            return Ok(None);
        }
        let reason = if let Some(reason) = self.transport.disconnect_reason() {
            Some(format!("producer disconnected: {}", String::from_utf8_lossy(reason)))
        } else if self.transport.check_timeout(self.silence_timeout.as_millis() as u64) {
            Some(format!("silent for {:?}", self.silence_timeout))
        } else {
            None
        };
        if let Some(reason) = reason {
            self.attached = false;
            self.redial(now)?;
            return Ok(Some(MediaEvent::ProducerDetached { remote: self.producer, reason }));
        }
        if now.duration_since(self.pinged_at) >= self.silence_timeout / 3 {
            self.pinged_at = now;
            self.transport.ping(Vec::new())?;
        }
        Ok(None)
    }

    fn redial(&mut self, now: Instant) -> Result<()> {
        self.transport = dial(self.producer, &self.runtime_id, self.connection_id)?;
        self.dialled_at = now;
        self.redials += 1;
        Ok(())
    }

    fn send_feedback(
        &mut self,
        record: GameCultMediaReceiverFeedbackRecord,
        stored_at: &str,
    ) -> MediaEvent {
        if !self.attached {
            self.feedback.record_not_sent();
            return MediaEvent::FeedbackNotSent {
                reason: "the producer has not answered".to_string(),
            };
        }
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
        .and_then(|payload| self.transport.send(MEDIA_CHANNEL, payload));
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

    /// Payloads that arrived on the media channels and did not decode. A
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

/// A fresh client session towards the producer. A new socket each time: a
/// session that has been given up on takes its sequence state with it.
fn dial(producer: SocketAddr, runtime_id: &str, connection_id: u32) -> Result<CultNetRudpSocketTransportConnection> {
    let bind = if producer.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
    let socket = UdpSocket::bind(bind).with_context(|| format!("binding a socket to dial {producer}"))?;
    socket
        .set_nonblocking(true)
        .context("setting the Ratatoskr media socket nonblocking")?;
    let mut transport = CultNetRudpSocketTransportConnection::new(CultNetRudpSocketTransportOptions::client(
        runtime_id,
        socket,
        producer,
        connection_id,
    ))
    .context("opening the Ratatoskr CultNet RUDP media client")?;
    transport
        .connect(runtime_id.as_bytes().to_vec())
        .with_context(|| format!("dialling media producer {producer}"))?;
    Ok(transport)
}

/// Frames on other channels belong to other conversations. Video and feedback
/// ride the media channel; audio has its own so it can be reliable.
fn media_payload(frame: CultNetTransportFrame) -> Option<Vec<u8>> {
    let media = frame.channel_id == MEDIA_CHANNEL || frame.channel_id == GAMECULT_MEDIA_AUDIO_CHANNEL;
    (media && !frame.payload.is_empty()).then_some(frame.payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    use cultnet_rs::{
        CultNetRudpServerEvent, CultNetRudpServerHub, CultNetRudpServerHubOptions,
        CultNetTransportDelivery, GameCultMediaVideoAccessUnitRecord,
    };

    const CONNECTION: u32 = 0x0BE0_0001;

    /// An address nobody answers on.
    fn dead_producer() -> SocketAddr {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("binds");
        socket.local_addr().expect("addr")
    }

    fn towards(producer: SocketAddr) -> ReceiverOptions {
        ReceiverOptions::new(producer, "ratatoskr-test", CONNECTION)
    }

    fn producer_hub() -> CultNetRudpServerHub {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("binds");
        socket.set_nonblocking(true).expect("nonblocking");
        let mut options = CultNetRudpServerHubOptions::new("producer-test", socket, CONNECTION);
        options.media_delivery = Some(CultNetTransportDelivery::Unreliable);
        CultNetRudpServerHub::new(options).expect("hub")
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
    fn a_receiver_dials_and_says_where() {
        let producer = dead_producer();
        let receiver = RatatoskrReceiver::open(towards(producer)).expect("opens");
        assert_eq!(receiver.producer(), producer);
        assert!(!receiver.attached(), "nobody has answered");
    }

    #[test]
    fn polling_an_unanswered_receiver_yields_nothing_and_does_not_block() {
        let mut receiver = RatatoskrReceiver::open(towards(dead_producer())).expect("opens");
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
        for channel in [MEDIA_CHANNEL, GAMECULT_MEDIA_AUDIO_CHANNEL] {
            assert_eq!(
                media_payload(CultNetTransportFrame {
                    channel_id: channel.to_string(),
                    payload: vec![1, 2, 3],
                }),
                Some(vec![1, 2, 3]),
                "{channel}"
            );
        }
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
        let mut receiver = RatatoskrReceiver::open(towards(dead_producer())).expect("opens");
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
            ..towards(dead_producer())
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
            ..towards(dead_producer())
        })
        .expect("opens");
        assert_eq!(receiver.admit(chunk(1, 0, 3, vec![1]), Instant::now()), None);
        let events = receiver.poll().expect("polls");
        assert!(
            matches!(events.as_slice(), [MediaEvent::FeedbackNotSent { reason }] if reason.contains("not answered")),
            "got {events:?}"
        );
        assert_eq!(receiver.feedback_stats().not_sent, 1);
    }

    /// A producer that is not up yet is dialled again, not given up on: the
    /// request that spins it up and the dial that reaches it race by design.
    #[test]
    fn a_receiver_keeps_dialling_until_the_producer_answers() -> Result<()> {
        let producer = dead_producer();
        let mut receiver = RatatoskrReceiver::open(ReceiverOptions {
            redial_after: Duration::from_millis(30),
            ..towards(producer)
        })?;
        std::thread::sleep(Duration::from_millis(100));
        assert!(receiver.poll()?.is_empty());
        assert!(receiver.redials() >= 1, "the unanswered dial was repeated");

        let socket = UdpSocket::bind(producer).context("rebinding the producer's port")?;
        socket.set_nonblocking(true)?;
        let mut hub = CultNetRudpServerHub::new(CultNetRudpServerHubOptions::new("producer-test", socket, CONNECTION))?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut connect_payload = None;
        loop {
            while let Some(event) = hub.receive_event_once()? {
                if let CultNetRudpServerEvent::Connected { session } = event {
                    connect_payload = Some(session.connect_payload);
                }
            }
            hub.poll_resends()?;
            let events = receiver.poll()?;
            if events.iter().any(|event| matches!(event, MediaEvent::ProducerAttached { remote } if *remote == producer)) {
                break;
            }
            assert!(Instant::now() < deadline, "the producer never attached; redials={}", receiver.redials());
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(receiver.attached());
        assert_eq!(connect_payload.as_deref(), Some(b"ratatoskr-test".as_slice()), "the producer learns who dialled");
        Ok(())
    }

    /// The whole return path over real sockets: the receiver dials a producer
    /// hub, a chunk is lost on the way, and the producer is told which one —
    /// then, when the frame is given up on, that it was late and that a
    /// keyframe is wanted.
    #[test]
    fn a_producer_is_told_what_was_lost() -> Result<()> {
        let mut hub = producer_hub();
        let producer = hub.local_addr()?;
        let mut receiver = RatatoskrReceiver::open(ReceiverOptions {
            video: VideoAssemblerOptions {
                max_frame_age: Duration::from_millis(150),
                repair_first_wait: Duration::from_millis(20),
                repair_retry_wait: Duration::from_millis(20),
                repair_max_requests: 3,
                ..Default::default()
            },
            ..towards(producer)
        })?;

        fn pump(
            receiver: &mut RatatoskrReceiver,
            hub: &mut CultNetRudpServerHub,
            feedback: &mut Vec<GameCultMediaReceiverFeedbackRecord>,
        ) -> Result<Vec<MediaEvent>> {
            while let Some(event) = hub.receive_event_once()? {
                if let CultNetRudpServerEvent::Frame { frame, .. } = event {
                    if frame.channel_id == MEDIA_CHANNEL {
                        if let Ok(GameCultMediaWireRecord::Feedback(record)) = decode_media_wire_record(&frame.payload) {
                            feedback.push(record);
                        }
                    }
                }
            }
            hub.poll_resends()?;
            receiver.poll()
        }
        let mut feedback = Vec::new();

        let deadline = Instant::now() + Duration::from_secs(3);
        while !receiver.attached() {
            pump(&mut receiver, &mut hub, &mut feedback)?;
            assert!(Instant::now() < deadline, "the producer never answered");
            std::thread::sleep(Duration::from_millis(2));
        }
        let session = hub.sessions().into_iter().next().expect("the receiver is a session");

        // Three chunks; the middle one never leaves.
        let provenance = MediaWireProvenance { stored_at: "t", runtime_id: "producer-test", role: "producer", producer: "test" };
        for index in [0u16, 2] {
            let record = chunk(9, index, 3, vec![index as u8; 4]);
            hub.send(&session, MEDIA_CHANNEL, encode_media_wire_record(&record, provenance.clone())?)?;
        }

        let deadline = Instant::now() + Duration::from_secs(3);
        let mut expired = false;
        while Instant::now() < deadline {
            for event in pump(&mut receiver, &mut hub, &mut feedback)? {
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

    /// Audio on its own channel is media too, and a producer that goes silent
    /// is noticed and dialled again rather than waited on forever.
    #[test]
    fn audio_arrives_on_its_channel_and_silence_is_a_detach() -> Result<()> {
        let mut hub = producer_hub();
        let producer = hub.local_addr()?;
        let mut receiver = RatatoskrReceiver::open(ReceiverOptions {
            silence_timeout: Duration::from_millis(300),
            redial_after: Duration::from_millis(50),
            ..towards(producer)
        })?;
        let deadline = Instant::now() + Duration::from_secs(3);
        while !receiver.attached() {
            while hub.receive_event_once()?.is_some() {}
            hub.poll_resends()?;
            receiver.poll()?;
            assert!(Instant::now() < deadline, "never attached");
            std::thread::sleep(Duration::from_millis(2));
        }
        let session = hub.sessions().into_iter().next().expect("session");
        let audio = GameCultMediaWireRecord::Audio(GameCultMediaAudioPacketRecord {
            stream_id: "s".into(),
            session_id: "x".into(),
            packet_id: 1,
            codec: "pcm-f32le-interleaved".into(),
            pts_ticks: 0,
            duration_ticks: 480,
            timebase_num: 1,
            timebase_den: 48_000,
            deadline_ticks: 480,
            payload: vec![4; 16],
        });
        let provenance = MediaWireProvenance { stored_at: "t", runtime_id: "producer-test", role: "producer", producer: "test" };
        hub.send(&session, GAMECULT_MEDIA_AUDIO_CHANNEL, encode_media_wire_record(&audio, provenance)?)?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut heard = false;
        while !heard {
            while hub.receive_event_once()?.is_some() {}
            hub.poll_resends()?;
            heard = receiver.poll()?.iter().any(|event| matches!(event, MediaEvent::Audio { record } if record.payload == vec![4; 16]));
            assert!(Instant::now() < deadline, "audio never arrived");
            std::thread::sleep(Duration::from_millis(2));
        }

        // The producer stops answering: no pumps, no pongs.
        drop(hub);
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let events = receiver.poll()?;
            if let Some(MediaEvent::ProducerDetached { reason, .. }) = events.iter().find(|event| matches!(event, MediaEvent::ProducerDetached { .. })) {
                assert!(reason.contains("silent"), "{reason}");
                break;
            }
            assert!(Instant::now() < deadline, "silence was never noticed");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!receiver.attached());
        assert!(receiver.redials() >= 1, "and it dialled again");
        Ok(())
    }
}
