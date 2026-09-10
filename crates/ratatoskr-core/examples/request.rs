//! Ask a producer for a stream, or to stop, or read its answer — without OBS.
//!
//! ```text
//! cargo run --example request -- <odin> <stream-id> start [video-source] [audio-source]
//! cargo run --example request -- <odin> <stream-id> stop
//! cargo run --example request -- <odin> <stream-id> state
//! ```
//!
//! `<odin>` is `rudp://host:port` or `host:port`. The receiver id is
//! `ratatoskr-request-probe` unless `RATATOSKR_RECEIVER_ID` says otherwise;
//! codecs are the first the advertisement offers; bitrate and latency are the
//! producer's defaults. This is the same request path the OBS source uses.

use chrono::{SecondsFormat, Utc};
use ratatoskr_core::{CatalogOptions, pull_catalog, pull_request_state, publish_request, start_request, stop_request};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [odin, stream_id, action, rest @ ..] = args.as_slice() else {
        anyhow::bail!("usage: request <odin> <stream-id> start [video-source] [audio-source] | stop | state");
    };
    let options = CatalogOptions {
        odin: odin.clone(),
        runtime_id: std::env::var("RATATOSKR_RECEIVER_ID").unwrap_or_else(|_| "ratatoskr-request-probe".to_string()),
        state_dir: std::env::temp_dir().join("ratatoskr-request-probe"),
    };
    if action == "state" {
        return match pull_request_state(&options, stream_id)? {
            Some(held) => {
                println!("{} {} state={} detail={:?} updated={}", held.request_id, held.action, held.state, held.detail, held.updated_at);
                Ok(())
            }
            None => {
                println!("no request from {} for {stream_id}", options.runtime_id);
                Ok(())
            }
        };
    }
    let pulled = pull_catalog(&options)?;
    let Some(advertisement) = pulled.streams.iter().find(|stream| &stream.stream_id == stream_id) else {
        anyhow::bail!(
            "{odin} does not advertise {stream_id}; it advertises {:?}",
            pulled.streams.iter().map(|stream| stream.stream_id.as_str()).collect::<Vec<_>>()
        );
    };
    let observed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let request = match action.as_str() {
        "start" => {
            let video = rest.first().cloned().or_else(|| advertisement.video_source_ids.first().cloned()).unwrap_or_default();
            let audio = rest.get(1).cloned().or_else(|| advertisement.audio_source_ids.first().cloned()).unwrap_or_default();
            let video_codec = advertisement.video_codecs.first().cloned().unwrap_or_default();
            let audio_codec = advertisement.audio_codecs.first().cloned().unwrap_or_default();
            start_request(advertisement, &options.runtime_id, &video, &audio, &video_codec, &audio_codec, 0, 0, &observed_at)
        }
        "stop" => stop_request(advertisement, &options.runtime_id, &observed_at),
        other => anyhow::bail!("unknown action {other}; use start, stop or state"),
    };
    publish_request(&options, &request)?;
    println!(
        "published {} {} for {} as {}; dial {} conn {:#x} once the producer answers running",
        request.action, request.request_id, stream_id, options.runtime_id, advertisement.media_endpoint, advertisement.media_connection_id
    );
    Ok(())
}
