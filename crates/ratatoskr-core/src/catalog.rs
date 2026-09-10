//! What streams exist and asking for one: the CultMesh side of the squirrel.
//!
//! Producers advertise `gamecult.media_stream_advertisement` through Odin;
//! a consumer pulls those to build a picker and publishes a
//! `gamecult.media_stream_request` naming the stream, the sources, the codecs
//! and the endpoint the producer should dial. The producer answers on the
//! same request key. None of this touches the media channel, and none of it
//! names Muninn: the picker shows whatever advertises.
//!
//! Everything below is `cultmesh-rs`. A node here is a small local CultCache
//! store used as the working memory for one pull or one publish; it is not a
//! long-lived mesh participant and holds no authority.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cultmesh_rs::{
    CultMesh, CultMeshNodeOptions, CultMeshRudpDocumentPublishOptions, CultMeshRudpSnapshotOptions,
    cultmesh_documents,
};
use cultnet_rs::{
    GAMECULT_MEDIA_STREAM_ADVERTISEMENT_SCHEMA, GAMECULT_MEDIA_STREAM_REQUEST_SCHEMA,
    GameCultMediaStreamAdvertisementRecord, GameCultMediaStreamRequestRecord,
    media_stream_request_key, validate_media_stream_advertisement, validate_media_stream_request,
};

cultmesh_documents!(RatatoskrDocuments {
    GameCultMediaStreamAdvertisementRecord => GAMECULT_MEDIA_STREAM_ADVERTISEMENT_SCHEMA,
    GameCultMediaStreamRequestRecord => GAMECULT_MEDIA_STREAM_REQUEST_SCHEMA,
});

#[derive(Clone, Debug)]
pub struct CatalogOptions {
    /// Odin's RUDP catalog endpoint, as `rudp://host:port` or `host:port`.
    pub odin: String,
    /// How this consumer names itself to Odin and in request records.
    pub runtime_id: String,
    /// Where the working store lives. One file per consumer runtime.
    pub state_dir: PathBuf,
}

impl CatalogOptions {
    fn target(&self) -> Result<SocketAddr> {
        CultMesh::resolve_rudp_endpoint(&self.odin)
            .with_context(|| format!("resolving Odin catalog endpoint {}", self.odin))
    }

    fn store_path(&self) -> Result<PathBuf> {
        std::fs::create_dir_all(&self.state_dir)
            .with_context(|| format!("creating {}", self.state_dir.display()))?;
        Ok(self.state_dir.join(format!("{}.catalog.cc", self.runtime_id)))
    }
}

/// Every stream currently advertised through Odin, validated. A malformed
/// advertisement is skipped and returned separately rather than hiding the
/// good ones behind an error.
pub struct CatalogPull {
    pub streams: Vec<GameCultMediaStreamAdvertisementRecord>,
    pub rejected: Vec<(String, String)>,
}

pub fn pull_catalog(options: &CatalogOptions) -> Result<CatalogPull> {
    let target = options.target()?;
    let mut node = CultMesh::create_node(
        options.store_path()?,
        RatatoskrDocuments,
        CultMeshNodeOptions {
            runtime_id: options.runtime_id.clone(),
            pull_on_start: false,
        },
    )
    .context("opening Ratatoskr catalog store")?;
    node.pull_rudp_catalog_snapshot(CultMeshRudpSnapshotOptions {
        schema_ids: Some(vec![GAMECULT_MEDIA_STREAM_ADVERTISEMENT_SCHEMA.to_string()]),
        ..CultMeshRudpSnapshotOptions::odin(target, options.runtime_id.clone())
    })
    .with_context(|| format!("pulling media stream advertisements from Odin at {target}"))?;

    let mut streams = Vec::new();
    let mut rejected = Vec::new();
    for (key, record) in node.get_all_with_keys::<GameCultMediaStreamAdvertisementRecord>()? {
        match validate_media_stream_advertisement(&record) {
            Ok(()) => streams.push(record),
            Err(error) => rejected.push((key, format!("{error:#}"))),
        }
    }
    streams.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    Ok(CatalogPull { streams, rejected })
}

/// Publishes a request to Odin under its stream/receiver key, superseding
/// any earlier request from the same receiver for the same stream.
pub fn publish_request(
    options: &CatalogOptions,
    request: &GameCultMediaStreamRequestRecord,
) -> Result<()> {
    validate_media_stream_request(request)?;
    let target = options.target()?;
    let node = CultMesh::create_node(
        options.store_path()?,
        RatatoskrDocuments,
        CultMeshNodeOptions {
            runtime_id: options.runtime_id.clone(),
            pull_on_start: false,
        },
    )
    .context("opening Ratatoskr catalog store")?;
    node.publish_document_to_rudp_catalog(
        media_stream_request_key(&request.stream_id, &request.receiver_id),
        request,
        CultMeshRudpDocumentPublishOptions {
            target,
            runtime_id: options.runtime_id.clone(),
            source_role: Some("media-stream-consumer".to_string()),
            tags: vec!["gamecult.media-stream-request".to_string()],
            ..CultMeshRudpDocumentPublishOptions::default()
        },
    )
    .with_context(|| format!("publishing media stream request {} to Odin", request.request_id))
}

/// The producer's answer, if it has written one yet.
pub fn pull_request_state(
    options: &CatalogOptions,
    stream_id: &str,
) -> Result<Option<GameCultMediaStreamRequestRecord>> {
    let target = options.target()?;
    let key = media_stream_request_key(stream_id, &options.runtime_id);
    let mut node = CultMesh::create_node(
        options.store_path()?,
        RatatoskrDocuments,
        CultMeshNodeOptions {
            runtime_id: options.runtime_id.clone(),
            pull_on_start: false,
        },
    )?;
    node.pull_rudp_catalog_snapshot(CultMeshRudpSnapshotOptions {
        schema_ids: Some(vec![GAMECULT_MEDIA_STREAM_REQUEST_SCHEMA.to_string()]),
        record_keys: Some(vec![key.clone()]),
        ..CultMeshRudpSnapshotOptions::odin(target, options.runtime_id.clone())
    })?;
    node.get::<GameCultMediaStreamRequestRecord>(&key)
}

/// A start request as the OBS source would issue it. `receiver_endpoint` is
/// where this consumer's media receiver listens; the producer dials it with
/// the advertised connection id.
#[allow(clippy::too_many_arguments)]
pub fn start_request(
    advertisement: &GameCultMediaStreamAdvertisementRecord,
    receiver_id: &str,
    receiver_endpoint: SocketAddr,
    video_source_id: &str,
    audio_source_id: &str,
    video_codec: &str,
    audio_codec: &str,
    video_bitrate_kbps: u32,
    latency_budget_ms: u32,
    observed_at: &str,
) -> GameCultMediaStreamRequestRecord {
    GameCultMediaStreamRequestRecord {
        request_id: format!("{}:{}:{}", advertisement.stream_id, receiver_id, observed_at),
        stream_id: advertisement.stream_id.clone(),
        producer_id: advertisement.producer_id.clone(),
        receiver_id: receiver_id.to_string(),
        receiver_endpoint: receiver_endpoint.to_string(),
        action: "start".to_string(),
        state: "pending".to_string(),
        video_source_id: video_source_id.to_string(),
        audio_source_id: audio_source_id.to_string(),
        video_codec: if video_source_id.is_empty() { String::new() } else { video_codec.to_string() },
        audio_codec: if audio_source_id.is_empty() { String::new() } else { audio_codec.to_string() },
        video_bitrate_kbps: if video_bitrate_kbps == 0 { advertisement.default_video_bitrate_kbps } else { video_bitrate_kbps },
        latency_budget_ms: if latency_budget_ms == 0 { advertisement.default_latency_budget_ms } else { latency_budget_ms },
        media_packet_bytes: advertisement.media_packet_bytes,
        detail: String::new(),
        updated_at: observed_at.to_string(),
    }
}

pub fn stop_request(
    advertisement: &GameCultMediaStreamAdvertisementRecord,
    receiver_id: &str,
    observed_at: &str,
) -> GameCultMediaStreamRequestRecord {
    GameCultMediaStreamRequestRecord {
        request_id: format!("{}:{}:{}", advertisement.stream_id, receiver_id, observed_at),
        stream_id: advertisement.stream_id.clone(),
        producer_id: advertisement.producer_id.clone(),
        receiver_id: receiver_id.to_string(),
        receiver_endpoint: String::new(),
        action: "stop".to_string(),
        state: "pending".to_string(),
        video_source_id: String::new(),
        audio_source_id: String::new(),
        video_codec: String::new(),
        audio_codec: String::new(),
        video_bitrate_kbps: 0,
        latency_budget_ms: 0,
        media_packet_bytes: 0,
        detail: String::new(),
        updated_at: observed_at.to_string(),
    }
}

/// The working store is per consumer; nothing else should read it.
pub fn store_path_for(state_dir: &Path, runtime_id: &str) -> PathBuf {
    state_dir.join(format!("{runtime_id}.catalog.cc"))
}
