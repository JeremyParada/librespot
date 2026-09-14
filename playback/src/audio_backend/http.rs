//! Serves the decoded stream over HTTP as LPCM/WAV, for Google Cast receivers.
//!
//! A Cast group disables Spotify's own crossfade, because the client stops decoding and
//! Spotify hands the tracks to the group one by one. Feeding the group a single
//! never-ending stream instead means it never sees a track transition, so whatever
//! mixing the player does (crossfade included) survives.
//!
//! ```text
//! librespot --backend http --device 0.0.0.0:8321 --format S16 --crossfade 8
//! ```
//!
//! Two things make this different from the `pipe` backend, and both are load-bearing:
//!
//! 1. **It keeps its own clock.** A sound card paces the player by consuming at 44100 Hz;
//!    a socket does not pace anything. Without a clock here the player decodes whole
//!    tracks in seconds and skips through them with nothing audible. So the sender thread
//!    releases exactly one chunk per chunk-duration, and the bounded channel back to
//!    `write_bytes` blocks the player once it runs ahead.
//!
//! 2. **It never goes quiet.** On pause the player stops calling `write_bytes`. If the
//!    stream dried up, the receiver's buffer would drain and it would end the session,
//!    with no way back on resume. So the sender emits silence whenever no audio is ready.

use super::{Open, Sink, SinkAsBytes, SinkError, SinkResult};
use crate::config::AudioFormat;
use crate::convert::Converter;
use crate::decoder::AudioPacket;

use std::io::Write;
use std::process::exit;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use thiserror::Error;

const RATE: u32 = 44100;
const CHANNELS: u16 = 2;
const BITS: u16 = 16;
const FRAME: usize = (CHANNELS as usize) * (BITS as usize / 8);

/// A multiple of 1152 frames, small enough that a pause is noticed promptly.
const CHUNK_FRAMES: usize = 1152 * 4;
const CHUNK_BYTES: usize = CHUNK_FRAMES * FRAME;

/// How far the player may run ahead before `write_bytes` blocks. This is the
/// backpressure that keeps decoding at real time.
const AHEAD_CHUNKS: usize = 8;

/// Dropped if a client falls this far behind; a stalled socket must not stall the clock.
const CLIENT_BACKLOG: usize = 64;

const DEFAULT_ADDR: &str = "0.0.0.0:8321";

#[derive(Debug, Error)]
enum HttpError {
    #[error("<HttpSink> Only S16 is supported, got {0:?}")]
    UnsupportedFormat(AudioFormat),

    #[error("<HttpSink> Can Not Bind To {addr}, {e}")]
    BindFailure { addr: String, e: std::io::Error },

    #[error("<HttpSink> The Sender Thread Is Gone")]
    SenderGone,
}

impl From<HttpError> for SinkError {
    fn from(e: HttpError) -> SinkError {
        let es = e.to_string();
        match e {
            HttpError::UnsupportedFormat(_) => SinkError::InvalidParams(es),
            HttpError::BindFailure { .. } => SinkError::ConnectionRefused(es),
            HttpError::SenderGone => SinkError::NotConnected(es),
        }
    }
}

/// One chunk of PCM, shared between every listener rather than copied per client.
type Chunk = Arc<Vec<u8>>;
type Clients = Arc<Mutex<Vec<SyncSender<Chunk>>>>;

/// The set of connected listeners. Publishing never blocks: a client that cannot keep
/// up is dropped rather than allowed to hold back the clock for everyone else.
#[derive(Clone, Default)]
struct Hub(Clients);

impl Hub {
    fn subscribe(&self) -> Receiver<Chunk> {
        let (tx, rx) = sync_channel(CLIENT_BACKLOG);
        if let Ok(mut clients) = self.0.lock() {
            clients.push(tx);
        }
        rx
    }

    fn publish(&self, chunk: Chunk) {
        if let Ok(mut clients) = self.0.lock() {
            clients.retain(|tx| !matches!(tx.try_send(chunk.clone()), Err(TrySendError::Full(_))));
        }
    }
}

/// RIFF header with "endless" sizes, which is how a WAV of unknown length is served.
fn wav_header() -> Vec<u8> {
    let byte_rate = RATE * (CHANNELS as u32) * (BITS as u32 / 8);
    let mut h = Vec::with_capacity(44);
    h.extend_from_slice(b"RIFF");
    h.extend_from_slice(&u32::MAX.to_le_bytes());
    h.extend_from_slice(b"WAVEfmt ");
    h.extend_from_slice(&16u32.to_le_bytes()); // PCM fmt chunk size
    h.extend_from_slice(&1u16.to_le_bytes()); // PCM, uncompressed
    h.extend_from_slice(&CHANNELS.to_le_bytes());
    h.extend_from_slice(&RATE.to_le_bytes());
    h.extend_from_slice(&byte_rate.to_le_bytes());
    h.extend_from_slice(&(CHANNELS * BITS / 8).to_le_bytes()); // block align
    h.extend_from_slice(&BITS.to_le_bytes());
    h.extend_from_slice(b"data");
    h.extend_from_slice(&u32::MAX.to_le_bytes());
    h
}

/// Releases one chunk per chunk-duration, filling with silence when the player has
/// nothing ready. The schedule is absolute: if an iteration overruns, the next wait is
/// shorter and the loop catches up instead of drifting slow forever.
fn run_sender(rx: Receiver<Vec<u8>>, hub: Hub) {
    let period = Duration::from_micros(CHUNK_FRAMES as u64 * 1_000_000 / RATE as u64);
    let silence = Arc::new(vec![0u8; CHUNK_BYTES]);
    let mut next = Instant::now();

    loop {
        next += period;
        let wait = next.saturating_duration_since(Instant::now());
        let chunk = match rx.recv_timeout(wait) {
            Ok(pcm) => Arc::new(pcm),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => silence.clone(),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        };
        hub.publish(chunk);
        if let Some(rest) = next.checked_duration_since(Instant::now()) {
            thread::sleep(rest);
        }
    }
}

fn run_server(listener: std::net::TcpListener, hub: Hub) {
    for stream in listener.incoming().flatten() {
        let hub = hub.clone();
        thread::spawn(move || serve_client(stream, hub));
    }
}

fn serve_client(mut stream: std::net::TcpStream, hub: Hub) {
    // The body is delimited by the connection close, as live streams are served; a
    // Content-Length would be a lie and makes Cast receivers try to seek.
    let head = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: audio/wav\r\n",
        "Cache-Control: no-cache, no-store\r\n",
        "Connection: close\r\n\r\n"
    );
    if stream.write_all(head.as_bytes()).is_err() || stream.write_all(&wav_header()).is_err() {
        return;
    }
    let rx = hub.subscribe();
    while let Ok(chunk) = rx.recv() {
        if stream.write_all(&chunk).is_err() {
            return;
        }
    }
}

pub struct HttpSink {
    /// Used by the `sink_as_bytes!` macro to convert packets.
    format: AudioFormat,
    tx: Option<SyncSender<Vec<u8>>>,
    /// `write_bytes` is handed arbitrary lengths; the clock wants fixed chunks.
    pending: Vec<u8>,
}

impl Open for HttpSink {
    /// Binds here rather than in `start()`, which the player only calls once playback
    /// begins. A Cast receiver has to be pointed at the URL *before* anything plays, so
    /// the stream has to exist from the moment the backend opens -- serving silence
    /// until there is audio.
    fn open(device: Option<String>, format: AudioFormat) -> Self {
        let addr = device.unwrap_or_else(|| DEFAULT_ADDR.to_string());

        if format != AudioFormat::S16 {
            error!("{}", HttpError::UnsupportedFormat(format));
            exit(1);
        }

        let listener = match std::net::TcpListener::bind(&addr) {
            Ok(l) => l,
            Err(e) => {
                error!(
                    "{}",
                    HttpError::BindFailure {
                        addr: addr.clone(),
                        e
                    }
                );
                exit(1);
            }
        };

        let hub = Hub::default();
        let (tx, rx) = sync_channel(AHEAD_CHUNKS);

        thread::spawn({
            let hub = hub.clone();
            move || run_sender(rx, hub)
        });
        thread::spawn({
            let hub = hub.clone();
            move || run_server(listener, hub)
        });

        info!("Using HttpSink with format: {format:?}, serving WAV on http://{addr}/");

        Self {
            format,
            tx: Some(tx),
            pending: Vec::with_capacity(CHUNK_BYTES * 2),
        }
    }
}

impl Sink for HttpSink {
    fn start(&mut self) -> SinkResult<()> {
        // Nothing to do: the server is already up from `open()` and the sender has been
        // filling the stream with silence in the meantime.
        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        // Deliberately keeps serving. The sender switches to silence on its own, and
        // tearing the stream down on pause is exactly what would end the Cast session.
        self.pending.clear();
        Ok(())
    }

    sink_as_bytes!();
}

impl SinkAsBytes for HttpSink {
    fn write_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        let tx = self.tx.as_ref().ok_or(HttpError::SenderGone)?;
        self.pending.extend_from_slice(data);
        while self.pending.len() >= CHUNK_BYTES {
            let rest = self.pending.split_off(CHUNK_BYTES);
            let chunk = std::mem::replace(&mut self.pending, rest);
            // Blocks once the player is AHEAD_CHUNKS ahead. That block is the whole
            // point: it is what holds decoding to real time.
            tx.send(chunk).map_err(|_| HttpError::SenderGone)?;
        }
        Ok(())
    }
}

impl HttpSink {
    pub const NAME: &'static str = "http";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_header_describes_cd_quality_pcm() {
        let h = wav_header();
        assert_eq!(h.len(), 44);
        assert_eq!(&h[0..4], b"RIFF");
        assert_eq!(&h[8..12], b"WAVE");
        assert_eq!(&h[36..40], b"data");
        // Sizes must be "endless": a real size would make the receiver stop early.
        assert_eq!(&h[4..8], &u32::MAX.to_le_bytes());
        assert_eq!(&h[40..44], &u32::MAX.to_le_bytes());
        assert_eq!(u32::from_le_bytes(h[24..28].try_into().unwrap()), RATE);
        assert_eq!(u32::from_le_bytes(h[28..32].try_into().unwrap()), 176_400);
    }

    #[test]
    fn a_slow_client_is_dropped_rather_than_stalling_the_clock() {
        let hub = Hub::default();
        let _rx = hub.subscribe();
        // One chunk past the backlog is enough to evict it.
        for _ in 0..CLIENT_BACKLOG + 1 {
            hub.publish(Arc::new(vec![0u8; 4]));
        }
        assert_eq!(hub.0.lock().unwrap().len(), 0);
    }

    #[test]
    fn the_sender_emits_silence_when_the_player_has_nothing() {
        let (_tx, rx) = sync_channel::<Vec<u8>>(1);
        let hub = Hub::default();
        let out = hub.subscribe();
        thread::spawn({
            let hub = hub.clone();
            move || run_sender(rx, hub)
        });
        // Nothing is ever sent, so everything that arrives must be silence -- and it
        // must arrive, because a dry stream ends the Cast session.
        let chunk = out
            .recv_timeout(Duration::from_secs(2))
            .expect("the stream must not dry up while paused");
        assert_eq!(chunk.len(), CHUNK_BYTES);
        assert!(chunk.iter().all(|&b| b == 0));
    }
}
