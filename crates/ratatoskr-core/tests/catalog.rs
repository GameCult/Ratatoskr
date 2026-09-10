//! The mesh side over real sockets: an Odin-shaped RUDP catalog server on
//! loopback that persists whatever is published and serves it back, so a
//! pull sees an advertisement and a producer's answer to a request.

use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use cultmesh_rs::{
    CultMeshRudpDocumentServer, CultMeshRudpDocumentServerOptions, CultMeshRudpRawDocumentReceipt,
    CultMeshRudpRawDocumentSink, CultMeshRudpServerClock, CultMeshRudpSnapshotQuery,
    CultMeshRudpSnapshotSource,
};
use cultnet_rs::{
    CultNetRawDocumentRecord, CultNetRawPayloadEncoding, GAMECULT_MEDIA_STREAM_ADVERTISEMENT_SCHEMA,
    GameCultMediaStreamAdvertisementRecord,
};
use ratatoskr_core::{CatalogOptions, pull_catalog, pull_request_state, publish_request, start_request, stop_request};

/// Odin's behaviour in miniature: keep every well-formed document by key and
/// answer snapshots by schema and key.
#[derive(Clone, Default)]
struct Store(Arc<Mutex<Vec<CultNetRawDocumentRecord>>>);

impl CultMeshRudpRawDocumentSink for Store {
    fn accept_raw_document(&mut self, receipt: CultMeshRudpRawDocumentReceipt) -> Result<()> {
        let mut documents = self.0.lock().unwrap();
        documents.retain(|existing| {
            !(existing.schema_id == receipt.document.schema_id
                && existing.record_key == receipt.document.record_key)
        });
        documents.push(receipt.document);
        Ok(())
    }
}

impl CultMeshRudpSnapshotSource for Store {
    fn raw_snapshot(&mut self, query: &CultMeshRudpSnapshotQuery) -> Result<Vec<CultNetRawDocumentRecord>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|document| {
                query.schema_ids.as_ref().is_none_or(|ids| ids.contains(&document.schema_id))
                    && query.record_keys.as_ref().is_none_or(|keys| keys.contains(&document.record_key))
            })
            .cloned()
            .collect())
    }
}

#[derive(Clone)]
struct WallClock(Instant);

impl CultMeshRudpServerClock for WallClock {
    fn now_unix_millis(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }
    fn now_monotonic_millis(&self) -> u64 {
        self.0.elapsed().as_millis() as u64
    }
}

struct Odin {
    addr: String,
    store: Store,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Odin {
    fn start() -> Result<Self> {
        let store = Store::default();
        let mut server = CultMeshRudpDocumentServer::new(
            UdpSocket::bind("127.0.0.1:0")?,
            store.clone(),
            store.clone(),
            WallClock(Instant::now()),
            CultMeshRudpDocumentServerOptions {
                resend_delay: Duration::from_millis(15),
                ..Default::default()
            },
        )?;
        let addr = server.local_addr()?.to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            let mut last_maintain = Instant::now();
            while !stopping.load(Ordering::Relaxed) {
                let _ = server.poll_once();
                if last_maintain.elapsed() >= Duration::from_millis(15) {
                    let _ = server.maintain();
                    last_maintain = Instant::now();
                }
                thread::sleep(Duration::from_millis(1));
            }
        });
        Ok(Self { addr, store, stop, thread: Some(thread) })
    }

    fn seed(&self, record: &GameCultMediaStreamAdvertisementRecord) -> Result<()> {
        self.store.0.lock().unwrap().push(CultNetRawDocumentRecord {
            schema_id: GAMECULT_MEDIA_STREAM_ADVERTISEMENT_SCHEMA.into(),
            record_key: record.stream_id.clone(),
            stored_at: "2026-09-10T00:00:00Z".into(),
            payload_encoding: CultNetRawPayloadEncoding::Messagepack,
            payload: rmp_serde::to_vec(record)?,
            source_runtime_id: Some(record.producer_id.clone()),
            source_agent_id: None,
            source_role: Some("media-stream-producer".into()),
            tags: None,
        });
        Ok(())
    }
}

impl Drop for Odin {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn advertisement(stream_id: &str) -> GameCultMediaStreamAdvertisementRecord {
    GameCultMediaStreamAdvertisementRecord {
        stream_id: stream_id.into(),
        producer_id: "raven".into(),
        label: "Raven desktop".into(),
        state: "available".into(),
        video_source_ids: vec!["display:0".into()],
        video_source_labels: vec!["Display 1".into()],
        audio_source_ids: vec!["wasapi-loopback:Realtek".into()],
        audio_source_labels: vec!["Realtek loopback".into()],
        video_codecs: vec!["h264".into()],
        audio_codecs: vec!["pcm-f32le-interleaved".into()],
        audio_sample_rate: 48_000,
        audio_channels: 2,
        default_video_bitrate_kbps: 12_000,
        default_latency_budget_ms: 250,
        media_packet_bytes: 848,
        media_connection_id: 0x6d75_0001,
        updated_at: "2026-09-10T00:00:00Z".into(),
    }
}

fn options(odin: &Odin, runtime_id: &str) -> CatalogOptions {
    CatalogOptions {
        odin: format!("rudp://{}", odin.addr),
        runtime_id: runtime_id.into(),
        state_dir: std::env::temp_dir().join(format!("ratatoskr-catalog-test-{}", std::process::id())),
    }
}

#[test]
fn the_picker_sees_what_advertises_and_a_request_comes_back_answered() -> Result<()> {
    let odin = Odin::start()?;
    odin.seed(&advertisement("muninn.raven.av.rudp"))?;
    odin.seed(&advertisement("other.host.av"))?;
    let mut broken = advertisement("broken");
    broken.media_connection_id = 0;
    odin.seed(&broken)?;
    let options = options(&odin, "starfire.obs");

    let pulled = pull_catalog(&options)?;
    assert_eq!(
        pulled.streams.iter().map(|s| s.stream_id.as_str()).collect::<Vec<_>>(),
        vec!["muninn.raven.av.rudp", "other.host.av"],
        "sorted, and the malformed one is kept out of the picker"
    );
    assert_eq!(pulled.rejected.len(), 1);
    assert_eq!(pulled.rejected[0].0, "broken");
    assert_eq!(pulled.streams[0].audio_channels, 2);

    // Ask for it, then read back what the mesh holds under the request key.
    let request = start_request(
        &pulled.streams[0],
        "starfire.obs",
        "192.168.178.146:5204".parse()?,
        "display:0",
        "wasapi-loopback:Realtek",
        "h264",
        "pcm-f32le-interleaved",
        0,
        0,
        "2026-09-10T00:00:01Z",
    );
    assert_eq!(request.video_bitrate_kbps, 12_000, "zero means the producer's default");
    publish_request(&options, &request)?;
    let held = pull_request_state(&options, "muninn.raven.av.rudp")?.expect("the mesh holds the request");
    assert_eq!(held, request);
    assert_eq!(held.state, "pending");

    // A newer request for the same stream from the same receiver replaces it.
    let stop = stop_request(&pulled.streams[0], "starfire.obs", "2026-09-10T00:00:02Z");
    publish_request(&options, &stop)?;
    let held = pull_request_state(&options, "muninn.raven.av.rudp")?.expect("still one document");
    assert_eq!(held.action, "stop");
    assert_eq!(odin.store.0.lock().unwrap().iter().filter(|d| d.schema_id.contains("request")).count(), 1);
    Ok(())
}

#[test]
fn a_request_that_names_a_source_without_a_codec_is_refused_before_it_leaves() -> Result<()> {
    let odin = Odin::start()?;
    let options = options(&odin, "starfire.obs");
    let mut request = start_request(&advertisement("s"), "starfire.obs", "127.0.0.1:1".parse()?, "display:0", "", "h264", "", 0, 0, "t");
    request.video_codec.clear();
    let error = publish_request(&options, &request).unwrap_err();
    assert!(error.to_string().contains("codec"), "{error}");
    assert!(odin.store.0.lock().unwrap().is_empty(), "nothing reached the mesh");
    Ok(())
}
