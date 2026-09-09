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
crates/ratatoskr-core/     Rust. The transport-facing half.
  src/receiver.rs          Subscription over cultnet-rs. No transport of its own.
  src/ffi.rs               The C ABI, deliberately small.
plugin/
  include/ratatoskr.h      Hand-written header. Small enough to read in one sitting.
  tests/abi_smoke.c        Loads the cdylib and checks the header tells the truth.
```

The core builds as `rlib`, `cdylib` and `staticlib`, following the
`muninn-move-tracker` precedent already in the estate. The plugin links the
staticlib under MSVC; `abi_smoke.c` loads the cdylib dynamically so the ABI can
be checked with any compiler.

## Status

Early. The core opens a CultNet media subscription, drains it, and hands
payloads across the ABI, with tests on both sides of that boundary. What is not
here yet:

- **Frame reassembly and FEC.** The media records carry chunking and parity
  (`gamecult.media_video_access_unit`, `gamecult.media_video_parity_shard.v2`).
  A dead Rust implementation of exactly this survives in Muninn's
  `media_packetizer.rs`, kept because it is the reference the C++ receiver
  drifted away from. It is the natural seed for this half and should move here.
- **The wire envelope.** `MuninnMediaWireRecord` and `encode_media_wire_record`
  still live in Muninn. Both ends need them, so they belong in CultLib beside
  the media records rather than in either end. Until that move, this repo cannot
  decode a real Muninn stream.
- **Receiver feedback.** `gamecult.media_receiver_feedback` is the return path a
  producer adapts to. Nothing here emits it yet.
- **The OBS plugin itself.** Roughly 1,200 lines of genuine OBS work — source
  registration, the ffmpeg audio decode child, the program texture source, the
  stem IPC — is worth porting from the Mimir plugin rather than rewriting. The
  ~2,500 lines of transport around it is not.

## A note on health signals

`delivered()` counts payloads and bytes that actually arrived. Nothing in this
repository may report stream health from configuration.

The previous generation published *"typed video/audio access units are
publishing over CultNet RUDP media"* from a match on two boolean options. It
said that continuously while the audio lane degraded to silence, because the
string was derived from what had been requested rather than from anything
observed. That is the specific mistake this line exists to prevent.
