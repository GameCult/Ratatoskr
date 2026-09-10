//! What the receiver says back, and when.
//!
//! The record is the contract's (`gamecult.media_receiver_feedback`) and its
//! shape is CultLib's `build_receiver_feedback`. This module owns only the
//! receiver-side policy of what goes in one: repair requests for frames still
//! waiting, late reports for frames given up on, and a keyframe request when a
//! reference is gone — no more often than a producer can usefully answer.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use cultnet_rs::{GameCultMediaReceiverFeedbackRecord, ReceiverFeedbackOptions, build_receiver_feedback};

use crate::video::{ExpiredFrame, RepairRequest};

#[derive(Clone, Debug)]
pub struct FeedbackOptions {
    /// A producer answers a keyframe request with an IDR, which costs it
    /// bandwidth for a whole GOP. One request per cooldown is enough to
    /// resynchronise; more just describes the same loss louder.
    pub keyframe_request_cooldown: Duration,
}

impl Default for FeedbackOptions {
    fn default() -> Self {
        Self {
            keyframe_request_cooldown: Duration::from_millis(500),
        }
    }
}

/// Counted from what was said, not what was configured.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedbackStats {
    /// Records handed to the transport.
    pub sent: u64,
    /// Chunk keys asked for again across all records.
    pub chunks_requested: u64,
    /// Records that carried a keyframe request.
    pub keyframes_requested: u64,
    /// Records the transport refused, usually because no producer is attached.
    pub not_sent: u64,
}

pub struct FeedbackComposer {
    receiver_id: String,
    options: FeedbackOptions,
    last_keyframe_request: Option<Instant>,
    stats: FeedbackStats,
}

impl FeedbackComposer {
    pub fn new(receiver_id: impl Into<String>, options: FeedbackOptions) -> Self {
        Self {
            receiver_id: receiver_id.into(),
            options,
            last_keyframe_request: None,
            stats: FeedbackStats::default(),
        }
    }

    /// One record per stream and session that has something to say. Repairs
    /// name the chunks still wanted; expiries name the frames given up on and,
    /// once per cooldown, ask for a keyframe, since whatever depended on them
    /// cannot be decoded either.
    pub fn compose(
        &mut self,
        repairs: &[RepairRequest],
        expired: &[ExpiredFrame],
        highest_decodable_frame_id: Option<u64>,
        now: Instant,
        observed_at: &str,
    ) -> Result<Vec<GameCultMediaReceiverFeedbackRecord>> {
        let mut by_stream: BTreeMap<(String, String), (Vec<String>, Vec<u64>)> = BTreeMap::new();
        for repair in repairs {
            let entry = by_stream
                .entry((repair.key.stream_id.clone(), repair.key.session_id.clone()))
                .or_default();
            entry.0.extend(repair.missing_chunk_keys.iter().cloned());
        }
        for frame in expired {
            let entry = by_stream
                .entry((frame.key.stream_id.clone(), frame.key.session_id.clone()))
                .or_default();
            entry.1.push(frame.key.frame_id);
        }

        let mut records = Vec::with_capacity(by_stream.len());
        for ((stream_id, session_id), (missing_video_chunk_keys, late_frame_ids)) in by_stream {
            let lost_a_reference = !late_frame_ids.is_empty();
            let requested_keyframe = lost_a_reference && self.keyframe_request_due(now);
            let record = build_receiver_feedback(ReceiverFeedbackOptions {
                stream_id: &stream_id,
                session_id: &session_id,
                receiver_id: &self.receiver_id,
                highest_decodable_frame_id,
                missing_frame_ids: Vec::new(),
                missing_video_chunk_keys,
                late_frame_ids,
                requested_keyframe,
                // The producer reads neither today. Zero is "not measured",
                // which is the truth; a plausible number would not be.
                jitter_us: 0,
                decode_queue_us: 0,
                observed_at,
            })?;
            if record.requested_keyframe {
                self.last_keyframe_request = Some(now);
                self.stats.keyframes_requested += 1;
            }
            self.stats.chunks_requested += record.missing_video_chunk_keys.len() as u64;
            records.push(record);
        }
        Ok(records)
    }

    pub fn record_sent(&mut self) {
        self.stats.sent += 1;
    }

    pub fn record_not_sent(&mut self) {
        self.stats.not_sent += 1;
    }

    pub fn stats(&self) -> FeedbackStats {
        self.stats
    }

    fn keyframe_request_due(&self, now: Instant) -> bool {
        self.last_keyframe_request
            .is_none_or(|last| now.duration_since(last) >= self.options.keyframe_request_cooldown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::video::{ExpiryReason, FrameKey};

    fn key(frame_id: u64) -> FrameKey {
        FrameKey {
            stream_id: "s".into(),
            session_id: "x".into(),
            frame_id,
        }
    }

    #[test]
    fn repairs_ask_for_chunks_without_asking_for_a_keyframe() -> Result<()> {
        let mut composer = FeedbackComposer::new("starfire.obs", FeedbackOptions::default());
        let repairs = vec![RepairRequest {
            key: key(9),
            missing_chunk_keys: vec!["9:2".into(), "9:1".into()],
        }];
        let records = composer.compose(&repairs, &[], Some(8), Instant::now(), "2026-09-10T00:00:00Z")?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].missing_video_chunk_keys, vec!["9:1", "9:2"]);
        assert!(records[0].late_frame_ids.is_empty());
        assert!(!records[0].requested_keyframe);
        assert_eq!(records[0].highest_decodable_frame_id, Some(8));
        assert_eq!(records[0].receiver_id, "starfire.obs");
        assert_eq!(composer.stats().chunks_requested, 2);
        Ok(())
    }

    #[test]
    fn a_lost_frame_asks_for_a_keyframe_once_per_cooldown() -> Result<()> {
        let start = Instant::now();
        let mut composer = FeedbackComposer::new("starfire.obs", FeedbackOptions { keyframe_request_cooldown: Duration::from_millis(100) });
        let lost = |frame_id| ExpiredFrame {
            key: key(frame_id),
            keyframe: false,
            missing_chunk_keys: vec![format!("{frame_id}:1")],
            reason: ExpiryReason::Aged,
        };

        let first = composer.compose(&[], &[lost(9)], Some(8), start, "t")?;
        assert!(first[0].requested_keyframe);
        assert_eq!(first[0].late_frame_ids, vec![9]);

        let second = composer.compose(&[], &[lost(10)], Some(8), start + Duration::from_millis(50), "t")?;
        assert!(!second[0].requested_keyframe, "still inside the cooldown");
        assert_eq!(second[0].late_frame_ids, vec![10]);

        let third = composer.compose(&[], &[lost(11)], Some(8), start + Duration::from_millis(150), "t")?;
        assert!(third[0].requested_keyframe);
        assert_eq!(composer.stats().keyframes_requested, 2);
        Ok(())
    }

    #[test]
    fn nothing_to_say_yields_no_record() -> Result<()> {
        let mut composer = FeedbackComposer::new("r", FeedbackOptions::default());
        assert!(composer.compose(&[], &[], None, Instant::now(), "t")?.is_empty());
        Ok(())
    }

    #[test]
    fn streams_are_reported_separately() -> Result<()> {
        let mut composer = FeedbackComposer::new("r", FeedbackOptions::default());
        let other = FrameKey { stream_id: "other".into(), session_id: "x".into(), frame_id: 3 };
        let repairs = vec![
            RepairRequest { key: key(9), missing_chunk_keys: vec!["9:1".into()] },
            RepairRequest { key: other, missing_chunk_keys: vec!["3:0".into()] },
        ];
        let records = composer.compose(&repairs, &[], None, Instant::now(), "t")?;
        assert_eq!(records.len(), 2);
        assert_eq!(records.iter().map(|r| r.stream_id.as_str()).collect::<Vec<_>>(), vec!["other", "s"]);
        Ok(())
    }
}
