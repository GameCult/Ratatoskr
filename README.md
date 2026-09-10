# Ratatoskr

**Carries CultMesh media streams to a renderer, and owns none of them.**

The squirrel runs up and down Yggdrasil carrying messages between the eagle at
the crown and the serpent at the root. It repeats what it is given. It is not
the eagle, it is not the serpent, and it has no opinion about the argument.

Ratatoskr subscribes to a CultMesh media stream, hands decoded frames to a
renderer, and reports back what actually arrived. Its first lowering target is
OBS.

---

## Why it exists

The receiver it replaces lived in Mimir as a 6,515-line OBS plugin that
hand-rolled CultNet RUDP in C++: acknowledgement, fragment reassembly, sockets,
`cultmesh://` URL parsing, wire framing, and two erasure codes, in 973 lines of
headers that linked no CultLib.

That made the receiving end of a protocol a separate implementation from the
sending end, in a different language, in a different repository, maintained by
nobody in particular. Every CultNet fix from August and September 2026 — split
reliable and lossy sequence domains, bounded reliable windows, ACK-horizon
binding, cumulative acknowledgement — reached `cultnet-rs` and `cultnet-ts` and
could not reach it.

So the rule this repository exists to hold:

> **Ratatoskr implements no transport.** It consumes CultNet. If something here
> starts to look like a sequence number, an ACK mask, or a retransmit timer, it
> belongs upstream in CultLib where both ends of the conversation can share it.

## Authority

- **Owner:** Ratatoskr owns turning a CultMesh media stream into frames a
  renderer can present, and the lifetime of that subscription.
- **Inputs:** typed media records over a CultNet RUDP media channel.
- **Outputs:** ordered, reassembled media payloads, and the receiver feedback a
  producer needs in order to adapt.
- **Not Ratatoskr's:** the media contract (CultLib), discovery and rendezvous
  (Odin), what gets captured (Muninn), how frames are presented (the renderer).

Ratatoskr is one consumer of a general contract. It is not the contract, and it
does not know or care which producer it is attached to.

## Shape

```
crates/ratatoskr-core/     Rust. Everything below the OBS callbacks.
  src/receiver.rs          Subscription over cultnet-rs. No transport of its own.
  src/video.rs             Chunks and parity in, whole access units out.
  src/feedback.rs          What the receiver says back, and how often.
  src/catalog.rs           What advertises through Odin; asking for a stream.
  src/ffi.rs               The C ABI, deliberately small.
plugin/
  include/ratatoskr.h      Hand-written header. Small enough to read in one sitting.
  src/ratatoskr_source.c   The OBS source. Owns the OBS lifecycle and nothing else.
  tests/abi_smoke.c        Loads the cdylib and checks the header tells the truth.
  CMakeLists.txt           find_package(libobs) + the core static library.
```

The core builds as `rlib`, `cdylib` and `staticlib`, following the
`muninn-move-tracker` precedent already in the estate. The plugin links the
staticlib under MSVC; `abi_smoke.c` loads the cdylib dynamically so the ABI can
be checked with any compiler.

## Status

The OBS source exists and installs; it has not yet carried a live stream.

**In OBS:** *Sources → Add → CultMesh Media Stream*. Its properties name an
Odin endpoint, pull every `gamecult.media_stream_advertisement` Odin holds,
and offer each stream's video sources, audio sources and codecs from the
advertisement itself, plus bitrate and latency budget (zero means the
producer's default). Selecting and activating publishes a
`gamecult.media_stream_request` naming this receiver's endpoint; the producer
dials it with the advertised connection id and answers on the same request
key. Nothing in the properties comes from configuration or from Muninn: the
picker shows whatever advertises.

Video reaches OBS the way the previous plugin proved in the field: the core
relays each whole access unit as a raw byte stream to a loopback UDP port and
the source draws a private `ffmpeg_source` child reading it, so OBS's own
decoder decodes. Audio is PCM and goes straight to `obs_source_output_audio`
with the producer's presentation time.

**Building the plugin** needs a libobs to link against. There is no SDK
download for Windows; `.sdk/` (git-ignored) holds an obs-studio checkout at
the installed version, the matching obs-deps, and a libobs-only build:

```
cmake -S .sdk/obs-studio -B .sdk/obs-build -G "Visual Studio 17 2022" -A x64   -DCMAKE_PREFIX_PATH=.sdk/obs-deps-<version>-x64 -DENABLE_FRONTEND=OFF   -DENABLE_PLUGINS=OFF -DENABLE_SCRIPTING=OFF -DENABLE_BROWSER=OFF
cmake --build .sdk/obs-build --config Release --target libobs
cargo build --release -p ratatoskr-core
cmake -S plugin -B plugin/build -G "Visual Studio 17 2022" -A x64   -DCMAKE_MODULE_PATH=.sdk/obs-studio/cmake/finders   -DCMAKE_PREFIX_PATH=".sdk/obs-build/libobs;.sdk/obs-build/deps/w32-pthreads;.sdk/obs-deps-<version>-x64"
cmake --build plugin/build --config Release
cmake --install plugin/build --config Release --prefix %APPDATA%/obs-studio/plugins/ratatoskr
```

The core opens a CultNet media subscription, drains it, decodes the
CultMesh media envelope, reassembles video, and hands whole access units and
audio packets across the ABI with a kind discriminator so a caller routes them
without parsing records itself. Tests sit on both sides of that boundary,
including a loopback Odin-shaped catalog server for the discovery path.

Video reassembly follows the contract's erasure code and adds only receiver
policy. The producer splits each access unit into datagram-sized chunks and,
for frames of more than one chunk, XOR stripe parity: shard `s` of
`parity_count` covers every chunk whose index is `s` modulo `parity_count`, so
each stripe gives back exactly one lost chunk. What the contract does not say
and this crate decides: an incomplete frame is given up on after 250 ms on the
receiver's clock, or sooner if 64 newer frames are pending — the bound evicts,
it never errors; a completed frame is complete forever, so late chunks are
discarded rather than reopening it; and after any loss nothing reaches the
renderer until the next keyframe, because a decoder fed a frame whose reference
is missing produces garbage that looks like a stream. Every one of those is a
counter in `VideoStats`, observed rather than configured.

Audio is not chunked on the wire and carries no parity, so audio packets pass
through as sent. The previous receiver's GF(256) audio erasure code had no
producer; it is not missing here, it never existed on this contract.

The return path is `gamecult.media_receiver_feedback`, built by CultLib's
`build_receiver_feedback` so both ends agree on its shape, and sent back down
the producer's own session on the media channel. A frame still waiting is
asked for by chunk after 64 ms, then every 32 ms, three times — the previous
receiver's field-tested cadence — and a repair request never asks for a
keyframe, because it exists so that one is not needed. A frame given up on is
reported late and, once per 500 ms, a keyframe is requested, since whatever
depended on it cannot be decoded either. `highest_decodable_frame_id` is the
newest frame actually handed to the renderer. `jitter_us` and
`decode_queue_us` are sent as zero: the producer reads neither, and "not
measured" is the truth where a plausible number would not be. With no
producer attached, feedback is reported as not sent and counted, never
dropped on the floor.

A payload that arrives on the media channel and fails to decode is surfaced as
`Undecodable` and counted, never silently dropped. A consumer that discards what
it cannot parse gives a producer no way to learn its stream is unreadable, which
is how the last receiver and sender drifted apart without either noticing.

What is not here yet:

- **A live frame.** No stream has crossed Raven → Starfire through this path.
  It needs Muninn on Raven running a build that advertises and answers
  (`b679fc2` or later) and Starfire admitting inbound UDP on the receiver's
  port.
- **Opus.** The request carries `audio_codec`; the only producer today emits
  `pcm-f32le-interleaved`. When Opus lands, the audio path here grows a
  decoder or a second `ffmpeg_source` child.

## A note on health signals

`delivered()` counts payloads and bytes that actually arrived. Nothing in this
repository may report stream health from configuration.

The previous generation published *"typed video/audio access units are
publishing over CultNet RUDP media"* from a match on two boolean options. It
said that continuously while the audio lane degraded to silence, because the
string was derived from what had been requested rather than from anything
observed. That is the specific mistake this line exists to prevent.
