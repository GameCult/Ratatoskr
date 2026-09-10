//! Video frame reassembly: chunks and parity in, whole access units out.
//!
//! A producer splits each access unit into `chunk_count` chunks that fit one
//! datagram and, for frames of more than one chunk, adds XOR parity: shard `s`
//! of `parity_count` is the XOR of every chunk whose index is `s` modulo
//! `parity_count`. Each stripe can therefore give back exactly one lost chunk.
//! That is the whole erasure code, and it is the contract's, not this
//! crate's: `GameCultMediaVideoParityShardRecord` in CultLib says which chunks
//! a shard covers and how long each is, and this module only follows it.
//!
//! What this module decides, and the producer does not:
//!
//! - when an incomplete frame is given up on (age on the receiver's clock,
//!   or pressure from newer frames — never a producer-side deadline, which is
//!   in a clock the receiver does not share);
//! - that a frame completed once is complete forever, so late chunks for it
//!   are discarded rather than reopening it;
//! - that after any frame is lost the renderer is handed nothing until the
//!   next keyframe, because a decoder fed a frame whose reference is missing
//!   produces garbage that looks like a stream.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use cultnet_rs::{
    GameCultMediaVideoAccessUnitRecord, GameCultMediaVideoParityShardRecord,
    validate_video_parity_record, validate_video_record, video_chunk_feedback_key,
};

/// A complete access unit, ready for a decoder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VideoFrame {
    pub stream_id: String,
    pub session_id: String,
    pub frame_id: u64,
    pub codec: String,
    pub pts_ticks: i64,
    pub duration_ticks: u32,
    pub timebase_num: u32,
    pub timebase_den: u32,
    pub keyframe: bool,
    pub dependency_frame_id: Option<u64>,
    /// The access unit as the producer emitted it (Annex B for H.264/H.265).
    pub bytes: Vec<u8>,
    /// Chunks that arrived as parity rather than as themselves.
    pub repaired_chunks: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FrameKey {
    pub stream_id: String,
    pub session_id: String,
    pub frame_id: u64,
}

/// Why an incomplete frame was given up on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryReason {
    /// Older than `max_frame_age` on the receiver's clock.
    Aged,
    /// Pushed out by newer frames when `max_pending_frames` was reached.
    Evicted,
}

/// An incomplete frame that was given up on. Carries what a producer would
/// need to repair or skip it; emitting that as feedback is the receiver's job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredFrame {
    pub key: FrameKey,
    pub keyframe: bool,
    pub missing_chunk_keys: Vec<String>,
    pub reason: ExpiryReason,
}

#[derive(Clone, Debug)]
pub struct VideoAssemblerOptions {
    /// How long an incomplete frame may wait for its remaining chunks or
    /// parity. Measured on this clock from the first chunk seen.
    pub max_frame_age: Duration,
    /// Incomplete frames held at once. The oldest is evicted to admit a newer
    /// one; a bound that errors instead of evicting is a fuse, not a limit.
    pub max_pending_frames: usize,
    /// Completed frames remembered so their late chunks are recognised as late.
    pub remembered_frames: usize,
}

impl Default for VideoAssemblerOptions {
    fn default() -> Self {
        Self {
            // The July 2026 acceptance ledger's recovery budget: a lost frame
            // is repaired or replaced within a quarter second, or not at all.
            max_frame_age: Duration::from_millis(250),
            max_pending_frames: 64,
            remembered_frames: 256,
        }
    }
}

/// Counted from what happened, never from configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VideoStats {
    pub completed: u64,
    pub repaired_chunks: u64,
    pub expired: u64,
    pub evicted: u64,
    /// Chunks or parity for frames already completed or given up on.
    pub late_discarded: u64,
    /// Complete frames withheld because a reference was lost and no keyframe
    /// has arrived since.
    pub awaiting_keyframe_discarded: u64,
}

/// The metadata every chunk and shard of one frame must agree on.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Meta {
    codec: String,
    pts_ticks: i64,
    duration_ticks: u32,
    timebase_num: u32,
    timebase_den: u32,
    keyframe: bool,
    dependency_frame_id: Option<u64>,
    deadline_ticks: i64,
    chunk_count: u16,
}

impl Meta {
    fn from_chunk(record: &GameCultMediaVideoAccessUnitRecord) -> Self {
        Self {
            codec: record.codec.clone(),
            pts_ticks: record.pts_ticks,
            duration_ticks: record.duration_ticks,
            timebase_num: record.timebase_num,
            timebase_den: record.timebase_den,
            keyframe: record.keyframe,
            dependency_frame_id: record.dependency_frame_id,
            deadline_ticks: record.deadline_ticks,
            chunk_count: record.chunk_count,
        }
    }

    fn from_parity(record: &GameCultMediaVideoParityShardRecord) -> Self {
        Self {
            codec: record.codec.clone(),
            pts_ticks: record.pts_ticks,
            duration_ticks: record.duration_ticks,
            timebase_num: record.timebase_num,
            timebase_den: record.timebase_den,
            keyframe: record.keyframe,
            dependency_frame_id: record.dependency_frame_id,
            deadline_ticks: record.deadline_ticks,
            chunk_count: record.chunk_count,
        }
    }
}

struct ParityShard {
    payload: Vec<u8>,
    parity_count: u16,
    chunk_payload_bytes: u32,
    last_chunk_payload_bytes: u32,
}

struct Assembly {
    meta: Meta,
    chunks: BTreeMap<u16, Vec<u8>>,
    parity: BTreeMap<u16, ParityShard>,
    first_seen: Instant,
    repaired: u16,
}

impl Assembly {
    fn new(meta: Meta, now: Instant) -> Self {
        Self {
            meta,
            chunks: BTreeMap::new(),
            parity: BTreeMap::new(),
            first_seen: now,
            repaired: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.chunks.len() == self.meta.chunk_count as usize
    }

    fn missing(&self) -> impl Iterator<Item = u16> + '_ {
        (0..self.meta.chunk_count).filter(|index| !self.chunks.contains_key(index))
    }

    fn missing_chunk_keys(&self, frame_id: u64) -> Vec<String> {
        self.missing()
            .map(|index| video_chunk_feedback_key(frame_id, index))
            .collect()
    }

    /// Recovers every chunk the parity on hand can give back. One pass may
    /// unlock nothing, or several stripes at once; it never needs a second
    /// pass because stripes are independent.
    fn repair(&mut self) -> bool {
        let mut recovered_any = false;
        let stripes = self.parity.keys().copied().collect::<Vec<_>>();
        for stripe in stripes {
            let Some(shard) = self.parity.get(&stripe) else { continue };
            if shard.parity_count == 0 || stripe >= shard.parity_count {
                continue;
            }
            let members = (stripe..self.meta.chunk_count)
                .step_by(shard.parity_count as usize)
                .collect::<Vec<_>>();
            let missing = members
                .iter()
                .copied()
                .filter(|index| !self.chunks.contains_key(index))
                .collect::<Vec<_>>();
            let [lost] = missing.as_slice() else {
                continue; // nothing to do, or more than XOR can give back
            };
            let length = if *lost == self.meta.chunk_count - 1 {
                shard.last_chunk_payload_bytes
            } else {
                shard.chunk_payload_bytes
            } as usize;
            if length == 0 || length > shard.payload.len() {
                continue;
            }
            let mut recovered = shard.payload[..length].to_vec();
            for index in &members {
                if index == lost {
                    continue;
                }
                let Some(present) = self.chunks.get(index) else { continue };
                for (offset, byte) in present.iter().take(length).enumerate() {
                    recovered[offset] ^= byte;
                }
            }
            self.chunks.insert(*lost, recovered);
            self.repaired += 1;
            recovered_any = true;
        }
        recovered_any
    }

    fn into_frame(self, key: &FrameKey) -> VideoFrame {
        let mut bytes = Vec::with_capacity(self.chunks.values().map(Vec::len).sum());
        for chunk in self.chunks.values() {
            bytes.extend_from_slice(chunk);
        }
        VideoFrame {
            stream_id: key.stream_id.clone(),
            session_id: key.session_id.clone(),
            frame_id: key.frame_id,
            codec: self.meta.codec,
            pts_ticks: self.meta.pts_ticks,
            duration_ticks: self.meta.duration_ticks,
            timebase_num: self.meta.timebase_num,
            timebase_den: self.meta.timebase_den,
            keyframe: self.meta.keyframe,
            dependency_frame_id: self.meta.dependency_frame_id,
            bytes,
            repaired_chunks: self.repaired,
        }
    }
}

pub struct VideoAssembler {
    options: VideoAssemblerOptions,
    pending: BTreeMap<FrameKey, Assembly>,
    remembered: VecDeque<FrameKey>,
    waiting_for_keyframe: bool,
    stats: VideoStats,
}

impl Default for VideoAssembler {
    fn default() -> Self {
        Self::new(VideoAssemblerOptions::default())
    }
}

impl VideoAssembler {
    pub fn new(options: VideoAssemblerOptions) -> Self {
        Self {
            options,
            pending: BTreeMap::new(),
            remembered: VecDeque::new(),
            // A decoder has nothing to build on until the first keyframe.
            waiting_for_keyframe: true,
            stats: VideoStats::default(),
        }
    }

    /// Admits one chunk. Returns the frame if this chunk completed it.
    pub fn insert_chunk(
        &mut self,
        record: GameCultMediaVideoAccessUnitRecord,
        now: Instant,
    ) -> Result<Option<VideoFrame>> {
        validate_video_record(&record)?;
        let key = FrameKey {
            stream_id: record.stream_id.clone(),
            session_id: record.session_id.clone(),
            frame_id: record.frame_id,
        };
        let meta = Meta::from_chunk(&record);
        let Some(assembly) = self.admit(&key, meta, now)? else {
            return Ok(None);
        };
        if assembly.chunks.contains_key(&record.chunk_index) {
            // A duplicate is not damage. The producer resent, or the network
            // did; either way the chunk we hold is the same bytes.
            return Ok(None);
        }
        assembly.chunks.insert(record.chunk_index, record.payload);
        Ok(self.try_finish(&key))
    }

    /// Admits one parity shard. Returns the frame if the shard let it complete.
    pub fn insert_parity(
        &mut self,
        record: GameCultMediaVideoParityShardRecord,
        now: Instant,
    ) -> Result<Option<VideoFrame>> {
        validate_video_parity_record(&record)?;
        let key = FrameKey {
            stream_id: record.stream_id.clone(),
            session_id: record.session_id.clone(),
            frame_id: record.frame_id,
        };
        let meta = Meta::from_parity(&record);
        let Some(assembly) = self.admit(&key, meta, now)? else {
            return Ok(None);
        };
        assembly.parity.insert(
            record.parity_index,
            ParityShard {
                payload: record.payload,
                parity_count: record.parity_count,
                chunk_payload_bytes: record.chunk_payload_bytes,
                last_chunk_payload_bytes: record.last_chunk_payload_bytes,
            },
        );
        Ok(self.try_finish(&key))
    }

    /// Gives up on frames older than `max_frame_age`. Call from the receive
    /// loop; the assembler has no clock of its own.
    pub fn expire(&mut self, now: Instant) -> Vec<ExpiredFrame> {
        let aged = self
            .pending
            .iter()
            .filter(|(_, assembly)| now.duration_since(assembly.first_seen) >= self.options.max_frame_age)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        aged.into_iter()
            .filter_map(|key| self.give_up(&key, ExpiryReason::Aged))
            .collect()
    }

    pub fn pending_frames(&self) -> usize {
        self.pending.len()
    }

    pub fn missing_chunk_keys(&self, key: &FrameKey) -> Vec<String> {
        self.pending
            .get(key)
            .map(|assembly| assembly.missing_chunk_keys(key.frame_id))
            .unwrap_or_default()
    }

    pub fn waiting_for_keyframe(&self) -> bool {
        self.waiting_for_keyframe
    }

    pub fn stats(&self) -> VideoStats {
        self.stats
    }

    /// Finds or opens the assembly for `key`, refusing late arrivals and
    /// metadata that contradicts what the frame already said about itself.
    /// Returns `None` when the record was discarded rather than refused.
    fn admit(&mut self, key: &FrameKey, meta: Meta, now: Instant) -> Result<Option<&mut Assembly>> {
        if self.remembered.contains(key) {
            self.stats.late_discarded += 1;
            return Ok(None);
        }
        if !self.pending.contains_key(key) {
            while self.pending.len() >= self.options.max_pending_frames {
                let stalest = self
                    .pending
                    .iter()
                    .min_by_key(|(_, assembly)| assembly.first_seen)
                    .map(|(key, _)| key.clone())
                    .expect("bound reached implies a pending frame");
                self.give_up(&stalest, ExpiryReason::Evicted);
            }
            self.pending.insert(key.clone(), Assembly::new(meta, now));
            return Ok(self.pending.get_mut(key));
        }
        let assembly = self.pending.get_mut(key).expect("checked above");
        if assembly.meta != meta {
            return Err(anyhow!(
                "frame {} received mixed media metadata",
                key.frame_id
            ));
        }
        Ok(Some(assembly))
    }

    fn try_finish(&mut self, key: &FrameKey) -> Option<VideoFrame> {
        let assembly = self.pending.get_mut(key)?;
        if !assembly.is_complete() {
            assembly.repair();
        }
        if !assembly.is_complete() {
            return None;
        }
        let assembly = self.pending.remove(key)?;
        self.remember(key.clone());
        self.stats.completed += 1;
        self.stats.repaired_chunks += u64::from(assembly.repaired);
        let frame = assembly.into_frame(key);
        if self.waiting_for_keyframe {
            if !frame.keyframe {
                self.stats.awaiting_keyframe_discarded += 1;
                return None;
            }
            self.waiting_for_keyframe = false;
        }
        Some(frame)
    }

    fn give_up(&mut self, key: &FrameKey, reason: ExpiryReason) -> Option<ExpiredFrame> {
        let assembly = self.pending.remove(key)?;
        self.remember(key.clone());
        match reason {
            ExpiryReason::Aged => self.stats.expired += 1,
            ExpiryReason::Evicted => self.stats.evicted += 1,
        }
        // Whatever depended on this frame cannot be decoded either.
        self.waiting_for_keyframe = true;
        Some(ExpiredFrame {
            key: key.clone(),
            keyframe: assembly.meta.keyframe,
            missing_chunk_keys: assembly.missing_chunk_keys(key.frame_id),
            reason,
        })
    }

    fn remember(&mut self, key: FrameKey) {
        self.remembered.push_back(key);
        while self.remembered.len() > self.options.remembered_frames {
            self.remembered.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM: &str = "muninn.raven.av.rudp";
    const SESSION: &str = "session-1";
    const STRIPES: u16 = 16;

    /// Splits a frame the way the producer does: even chunks of
    /// `max_payload_bytes`, the last one shorter.
    fn chunks(frame_id: u64, keyframe: bool, bytes: &[u8], max_payload_bytes: usize) -> Vec<GameCultMediaVideoAccessUnitRecord> {
        let pieces = bytes.chunks(max_payload_bytes).collect::<Vec<_>>();
        pieces
            .iter()
            .enumerate()
            .map(|(index, piece)| GameCultMediaVideoAccessUnitRecord {
                stream_id: STREAM.to_string(),
                session_id: SESSION.to_string(),
                frame_id,
                codec: "h264".to_string(),
                pts_ticks: 27_000 + frame_id as i64 * 3_000,
                duration_ticks: 3_000,
                timebase_num: 1,
                timebase_den: 90_000,
                keyframe,
                dependency_frame_id: (!keyframe).then(|| frame_id - 1),
                deadline_ticks: 28_800 + frame_id as i64 * 3_000,
                chunk_index: index as u16,
                chunk_count: pieces.len() as u16,
                payload: piece.to_vec(),
            })
            .collect()
    }

    /// XOR stripe parity as the producer builds it.
    fn parity(chunks: &[GameCultMediaVideoAccessUnitRecord]) -> Vec<GameCultMediaVideoParityShardRecord> {
        let first = &chunks[0];
        let parity_count = first.chunk_count.min(STRIPES);
        let chunk_payload_bytes = chunks.iter().map(|c| c.payload.len()).max().unwrap() as u32;
        let last_chunk_payload_bytes = chunks.last().unwrap().payload.len() as u32;
        (0..parity_count)
            .map(|stripe| {
                let members = chunks.iter().filter(|c| c.chunk_index % parity_count == stripe);
                let length = members.clone().map(|c| c.payload.len()).max().unwrap();
                let mut payload = vec![0u8; length];
                for member in members {
                    for (offset, byte) in member.payload.iter().enumerate() {
                        payload[offset] ^= byte;
                    }
                }
                GameCultMediaVideoParityShardRecord {
                    stream_id: first.stream_id.clone(),
                    session_id: first.session_id.clone(),
                    frame_id: first.frame_id,
                    codec: first.codec.clone(),
                    pts_ticks: first.pts_ticks,
                    duration_ticks: first.duration_ticks,
                    timebase_num: first.timebase_num,
                    timebase_den: first.timebase_den,
                    keyframe: first.keyframe,
                    dependency_frame_id: first.dependency_frame_id,
                    deadline_ticks: first.deadline_ticks,
                    chunk_count: first.chunk_count,
                    parity_index: stripe,
                    parity_count,
                    chunk_payload_bytes,
                    last_chunk_payload_bytes,
                    payload,
                }
            })
            .collect()
    }

    fn key(frame_id: u64) -> FrameKey {
        FrameKey { stream_id: STREAM.to_string(), session_id: SESSION.to_string(), frame_id }
    }

    #[test]
    fn reassembles_chunks_arriving_out_of_order() -> Result<()> {
        let now = Instant::now();
        let records = chunks(9, true, &[1, 2, 3, 4, 5], 2);
        let mut assembler = VideoAssembler::default();

        assert!(assembler.insert_chunk(records[2].clone(), now)?.is_none());
        assert_eq!(assembler.missing_chunk_keys(&key(9)), vec![video_chunk_feedback_key(9, 0), video_chunk_feedback_key(9, 1)]);
        assert!(assembler.insert_chunk(records[0].clone(), now)?.is_none());
        let frame = assembler.insert_chunk(records[1].clone(), now)?.expect("completes");

        assert_eq!(frame.bytes, vec![1, 2, 3, 4, 5]);
        assert!(frame.keyframe);
        assert_eq!(frame.repaired_chunks, 0);
        assert_eq!(assembler.pending_frames(), 0);
        assert_eq!(assembler.stats().completed, 1);
        Ok(())
    }

    #[test]
    fn interleaved_frames_are_kept_apart() -> Result<()> {
        let now = Instant::now();
        let one = chunks(9, true, &[1, 2, 3, 4, 5], 2);
        let two = chunks(10, false, &[6, 7, 8, 9], 2);
        let mut assembler = VideoAssembler::default();

        assembler.insert_chunk(one[0].clone(), now)?;
        assembler.insert_chunk(two[1].clone(), now)?;
        assert_eq!(assembler.pending_frames(), 2);
        assembler.insert_chunk(one[2].clone(), now)?;
        let frame = assembler.insert_chunk(one[1].clone(), now)?.expect("frame one completes");
        assert_eq!(frame.frame_id, 9);
        assert_eq!(assembler.pending_frames(), 1);
        let frame = assembler.insert_chunk(two[0].clone(), now)?.expect("frame two completes after the keyframe");
        assert_eq!(frame.bytes, vec![6, 7, 8, 9]);
        Ok(())
    }

    #[test]
    fn parity_gives_back_one_lost_chunk_per_stripe() -> Result<()> {
        let now = Instant::now();
        let bytes = (1_u8..=40).collect::<Vec<_>>();
        let records = chunks(9, true, &bytes, 2);
        let shards = parity(&records);
        assert_eq!(records.len(), 20);
        assert_eq!(shards.len(), 16);
        let mut assembler = VideoAssembler::default();

        // Lose the whole tail: four chunks in four different stripes.
        for record in &records[..16] {
            assert!(assembler.insert_chunk(record.clone(), now)?.is_none());
        }
        let mut frame = None;
        for shard in shards {
            if let Some(done) = assembler.insert_parity(shard, now)? {
                frame = Some(done);
            }
        }
        let frame = frame.expect("parity completes the frame");
        assert_eq!(frame.bytes, bytes);
        assert_eq!(frame.repaired_chunks, 4);
        assert_eq!(assembler.stats().repaired_chunks, 4);
        Ok(())
    }

    #[test]
    fn two_losses_in_one_stripe_are_beyond_repair() -> Result<()> {
        let now = Instant::now();
        let bytes = (1_u8..=40).collect::<Vec<_>>();
        let records = chunks(9, true, &bytes, 2);
        let mut assembler = VideoAssembler::default();

        // Chunks 0 and 16 share stripe 0.
        for record in records.iter().filter(|r| r.chunk_index != 0 && r.chunk_index != 16) {
            assembler.insert_chunk(record.clone(), now)?;
        }
        for shard in parity(&records) {
            assert!(assembler.insert_parity(shard, now)?.is_none());
        }
        assert_eq!(assembler.pending_frames(), 1);
        assert_eq!(assembler.missing_chunk_keys(&key(9)), vec![video_chunk_feedback_key(9, 0), video_chunk_feedback_key(9, 16)]);
        Ok(())
    }

    #[test]
    fn contradicting_metadata_is_refused() -> Result<()> {
        let now = Instant::now();
        let mut records = chunks(9, false, &[1, 2, 3, 4], 2);
        let mut assembler = VideoAssembler::default();
        assembler.insert_chunk(records[0].clone(), now)?;
        records[1].pts_ticks += 1;

        let error = assembler.insert_chunk(records[1].clone(), now).unwrap_err();
        assert!(error.to_string().contains("mixed media metadata"), "{error}");
        Ok(())
    }

    #[test]
    fn an_aged_frame_is_given_up_with_its_missing_chunks_named() -> Result<()> {
        let start = Instant::now();
        let one = chunks(9, true, &[1, 2, 3, 4, 5], 2);
        let two = chunks(10, false, &[6, 7, 8, 9], 2);
        let mut assembler = VideoAssembler::new(VideoAssemblerOptions { max_frame_age: Duration::from_millis(100), ..Default::default() });

        assembler.insert_chunk(one[0].clone(), start)?;
        assembler.insert_chunk(two[0].clone(), start + Duration::from_millis(60))?;
        let expired = assembler.expire(start + Duration::from_millis(120));

        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key, key(9));
        assert_eq!(expired[0].reason, ExpiryReason::Aged);
        assert!(expired[0].keyframe);
        assert_eq!(expired[0].missing_chunk_keys, vec![video_chunk_feedback_key(9, 1), video_chunk_feedback_key(9, 2)]);
        assert_eq!(assembler.pending_frames(), 1);
        assert_eq!(assembler.stats().expired, 1);
        Ok(())
    }

    /// After a loss the decoder has no reference until the next keyframe, so
    /// completing a non-key frame is not the same as being able to show it.
    #[test]
    fn nothing_is_delivered_between_a_loss_and_the_next_keyframe() -> Result<()> {
        let now = Instant::now();
        let mut assembler = VideoAssembler::new(VideoAssemblerOptions { max_frame_age: Duration::ZERO, ..Default::default() });

        // Before any keyframe at all, a complete P-frame is withheld.
        let early = chunks(8, false, &[9, 9], 2);
        assert!(assembler.insert_chunk(early[0].clone(), now)?.is_none());
        assert_eq!(assembler.stats().awaiting_keyframe_discarded, 1);

        let key9 = chunks(9, true, &[1, 2], 2);
        assert!(assembler.insert_chunk(key9[0].clone(), now)?.is_some());
        assert!(!assembler.waiting_for_keyframe());

        // Lose frame 10; frame 11 completes but depends on it.
        let ten = chunks(10, false, &[3, 4, 5, 6], 2);
        assembler.insert_chunk(ten[0].clone(), now)?;
        assert_eq!(assembler.expire(now).len(), 1);
        assert!(assembler.waiting_for_keyframe());
        let eleven = chunks(11, false, &[7, 8], 2);
        assert!(assembler.insert_chunk(eleven[0].clone(), now)?.is_none());
        assert_eq!(assembler.stats().awaiting_keyframe_discarded, 2);

        let key12 = chunks(12, true, &[1, 1], 2);
        assert!(assembler.insert_chunk(key12[0].clone(), now)?.is_some());
        assert!(!assembler.waiting_for_keyframe());
        Ok(())
    }

    #[test]
    fn a_late_chunk_for_a_finished_frame_does_not_reopen_it() -> Result<()> {
        let now = Instant::now();
        let records = chunks(9, true, &[1, 2, 3], 2);
        let mut assembler = VideoAssembler::default();
        assembler.insert_chunk(records[0].clone(), now)?;
        assert!(assembler.insert_chunk(records[1].clone(), now)?.is_some());

        assert!(assembler.insert_chunk(records[1].clone(), now)?.is_none());
        assert_eq!(assembler.pending_frames(), 0);
        assert_eq!(assembler.stats().late_discarded, 1);

        // The same holds for a frame that was given up on.
        let ten = chunks(10, false, &[4, 5, 6], 2);
        assembler.insert_chunk(ten[0].clone(), now)?;
        assembler.expire(now + Duration::from_secs(1));
        assert!(assembler.insert_chunk(ten[1].clone(), now)?.is_none());
        assert_eq!(assembler.pending_frames(), 0);
        assert_eq!(assembler.stats().late_discarded, 2);
        Ok(())
    }

    #[test]
    fn a_duplicate_chunk_is_not_damage() -> Result<()> {
        let now = Instant::now();
        let records = chunks(9, true, &[1, 2, 3], 2);
        let mut assembler = VideoAssembler::default();
        assembler.insert_chunk(records[0].clone(), now)?;
        assert!(assembler.insert_chunk(records[0].clone(), now)?.is_none());
        let frame = assembler.insert_chunk(records[1].clone(), now)?.expect("completes");
        assert_eq!(frame.bytes, vec![1, 2, 3]);
        Ok(())
    }

    /// The pending set is a bound, not a fuse: reaching it evicts the oldest
    /// frame and keeps receiving.
    #[test]
    fn the_pending_bound_evicts_the_oldest_frame() -> Result<()> {
        let start = Instant::now();
        let mut assembler = VideoAssembler::new(VideoAssemblerOptions { max_pending_frames: 2, ..Default::default() });
        for frame_id in 1..=3_u64 {
            let records = chunks(frame_id, true, &[1, 2, 3], 2);
            assembler.insert_chunk(records[0].clone(), start + Duration::from_millis(frame_id))?;
        }
        assert_eq!(assembler.pending_frames(), 2);
        assert!(assembler.missing_chunk_keys(&key(1)).is_empty(), "frame 1 was evicted");
        assert!(!assembler.missing_chunk_keys(&key(3)).is_empty());
        assert_eq!(assembler.stats().evicted, 1);
        Ok(())
    }
}
