//! Dial a producer and say what arrives, without OBS.
//!
//! ```text
//! cargo run --example receive -- <media-endpoint> <connection-id> [seconds] [receiver-id]
//! ```
//!
//! `<media-endpoint>` and `<connection-id>` are what the advertisement says
//! (`dial … conn …` in the `catalog` example). Prints attachment, then one
//! line per second: whole video frames completed, chunks repaired, frames
//! given up on, audio packets, bytes, and what was sent back. This is the
//! whole receive path the OBS source runs, minus the renderer, so a working
//! line here and a black source in OBS is a plugin question, not a pipe one.

use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};

use ratatoskr_core::{MediaEvent, RatatoskrReceiver, ReceiverOptions};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [endpoint, connection, rest @ ..] = args.as_slice() else {
        anyhow::bail!("usage: receive <media-endpoint> <connection-id> [seconds] [receiver-id]");
    };
    let seconds: u64 = rest.first().map(|s| s.parse()).transpose()?.unwrap_or(15);
    let receiver_id = rest.get(1).cloned().unwrap_or_else(|| "ratatoskr-receive-probe".to_string());
    let connection_id = u32::from_str_radix(connection.trim_start_matches("0x"), 16)
        .or_else(|_| connection.parse::<u32>())?;
    let producer = endpoint
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("{endpoint} resolves to nothing"))?;

    let mut receiver = RatatoskrReceiver::open(ReceiverOptions::new(producer, receiver_id, connection_id))?;
    println!("dialling {producer} conn {connection_id:#x} for {seconds}s");
    let started = Instant::now();
    let mut next_line = started + Duration::from_secs(1);
    let (mut frames, mut audio, mut keyframes) = (0u64, 0u64, 0u64);
    while started.elapsed() < Duration::from_secs(seconds) {
        for event in receiver.poll()? {
            match event {
                MediaEvent::ProducerAttached { remote } => println!("attached to {remote}"),
                MediaEvent::ProducerDetached { remote, reason } => println!("detached from {remote}: {reason}"),
                MediaEvent::VideoFrame { frame } => {
                    frames += 1;
                    if frame.keyframe {
                        keyframes += 1;
                    }
                }
                MediaEvent::Audio { .. } => audio += 1,
                MediaEvent::Undecodable { reason, .. } => println!("undecodable: {reason}"),
                MediaEvent::Rejected { reason } => println!("rejected: {reason}"),
                _ => {}
            }
        }
        if Instant::now() >= next_line {
            let (payloads, bytes) = receiver.delivered();
            let video = receiver.video_stats();
            let feedback = receiver.feedback_stats();
            let transport = receiver.transport_stats();
            println!(
                "{:>3}s attached={} frames={frames} (key {keyframes}) audio={audio} payloads={payloads} bytes={bytes} repaired={} expired={} redials={} fed_back={} repairs_asked={} keyframes_asked={} | wire_frames={} wire_bytes={} evicted_sets={} undecodable={} rejected={}",
                started.elapsed().as_secs(),
                receiver.attached(),
                video.repaired_chunks,
                video.expired,
                receiver.redials(),
                feedback.sent,
                feedback.chunks_requested,
                feedback.keyframes_requested,
                transport.frames_received,
                transport.bytes_received,
                transport.fragment_sets_evicted,
                receiver.undecodable(),
                receiver.rejected(),
            );
            next_line += Duration::from_secs(1);
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}
