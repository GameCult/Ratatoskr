//! What Odin advertises, from the consumer's side of the fence.
//!
//! ```text
//! cargo run --example catalog -- rudp://10.77.0.1:17871 [receiver-id]
//! ```
//!
//! Pulls every `gamecult.media_stream_advertisement` the given Odin holds and
//! prints the picker's view of it: stream, producer, state, where to dial,
//! sources and codecs. Malformed advertisements are listed with the reason.
//! This is the field probe for "the plugin shows nothing": it uses exactly
//! the code path the OBS source uses and nothing else.

use ratatoskr_core::{CatalogOptions, pull_catalog};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let odin = args.next().unwrap_or_else(|| "rudp://10.77.0.1:17871".to_string());
    let runtime_id = args.next().unwrap_or_else(|| "ratatoskr-catalog-probe".to_string());
    let state_dir = std::env::temp_dir().join("ratatoskr-catalog-probe");
    let pulled = pull_catalog(&CatalogOptions {
        odin: odin.clone(),
        runtime_id,
        state_dir,
    })?;
    println!("{odin}: {} stream(s), {} malformed", pulled.streams.len(), pulled.rejected.len());
    for stream in &pulled.streams {
        println!(
            "- {} by {} [{}] dial {} conn {:#x}\n    video {:?} {:?}\n    audio {:?} {:?} {} Hz x{}\n    defaults {} kbps, {} ms, packet {} B, updated {}",
            stream.stream_id,
            stream.producer_id,
            stream.state,
            stream.media_endpoint,
            stream.media_connection_id,
            stream.video_source_ids,
            stream.video_codecs,
            stream.audio_source_ids,
            stream.audio_codecs,
            stream.audio_sample_rate,
            stream.audio_channels,
            stream.default_video_bitrate_kbps,
            stream.default_latency_budget_ms,
            stream.media_packet_bytes,
            stream.updated_at,
        );
    }
    for (key, reason) in &pulled.rejected {
        println!("! {key}: {reason}");
    }
    Ok(())
}
