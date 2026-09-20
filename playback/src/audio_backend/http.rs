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
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, OnceLock};
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

/// How often the accept loop looks at its stop flag. Only bounds shutdown latency.
const ACCEPT_POLL: Duration = Duration::from_millis(100);

/// How long a bind keeps trying. Covers a previous instance on its way out, and no more:
/// past this, the port really does belong to somebody else.
const BIND_TIMEOUT: Duration = Duration::from_secs(3);

/// When the player last produced audio, as milliseconds since the process started.
///
/// Exposed so a caster can release the Cast device once nothing has played for a while:
/// holding a speaker group keeps it busy for everyone else in the house and pushes
/// 1.4 Mbps of silence across the network for no reason.
static LAST_AUDIO_MS: AtomicU64 = AtomicU64::new(0);

fn epoch() -> &'static Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now)
}

fn mark_audio() {
    // Never store 0: that is the "nothing has played yet" sentinel, and audio arriving
    // within the first millisecond would otherwise read as never having played at all.
    let ms = (epoch().elapsed().as_millis() as u64).max(1);
    LAST_AUDIO_MS.store(ms, Ordering::Relaxed);
}

/// The port the stream is on, or 0 when nothing is being served.
static SERVING: AtomicU16 = AtomicU16::new(0);

/// The port the stream is actually being served on.
///
/// A host can be told the bridge started before this backend has even tried to bind,
/// because the player builds its sink on its own thread; a failure there kills that
/// thread and leaves the host claiming to play. This is the proof that it is not.
pub fn serving_port() -> Option<u16> {
    match SERVING.load(Ordering::Relaxed) {
        0 => None,
        port => Some(port),
    }
}

/// How many clients are pulling the stream right now.
static LISTENERS: AtomicUsize = AtomicUsize::new(0);

/// Number of clients currently being served.
///
/// A caster uses this to tell a live session from a dead one: a Cast receiver that goes
/// away stops reading, and on some devices nothing else says so.
pub fn listeners() -> usize {
    LISTENERS.load(Ordering::Relaxed)
}

/// Counts one listener for as long as it lives, however the serving loop ends.
struct Listener;

impl Listener {
    fn new() -> Self {
        LISTENERS.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        LISTENERS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// How long the player has produced nothing, or `None` if it never has.
///
/// "Never played" has to stay distinguishable from "played a moment ago": a caster
/// waiting for something to play and a caster deciding when to let the device go want
/// opposite answers for it, and collapsing both into a duration gets one of them wrong.
pub fn idle_for() -> Option<Duration> {
    match LAST_AUDIO_MS.load(Ordering::Relaxed) {
        0 => None,
        // saturating: the clock reading and the stored mark are taken at different
        // instants, so "now" can legitimately be the earlier of the two.
        last => Some(Duration::from_millis(
            (epoch().elapsed().as_millis() as u64).saturating_sub(last),
        )),
    }
}

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
            Ok(pcm) => {
                mark_audio();
                Arc::new(pcm)
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => silence.clone(),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        };
        hub.publish(chunk);
        if let Some(rest) = next.checked_duration_since(Instant::now()) {
            thread::sleep(rest);
        }
    }
}

/// Binds, giving a previous instance a moment to let go of the port.
///
/// Stopping does not free the port instantly: the accept loop only notices on its next
/// poll. A host that stops and starts back to back would otherwise hit its own listener
/// on the way out, which reads like "the port is taken by something else" and is not.
fn bind_with_retry(addr: &str) -> std::io::Result<std::net::TcpListener> {
    let deadline = Instant::now() + BIND_TIMEOUT;
    loop {
        match std::net::TcpListener::bind(addr) {
            Ok(listener) => return Ok(listener),
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => thread::sleep(ACCEPT_POLL),
        }
    }
}

/// Accepts until `stop` is set, then lets the listener close.
///
/// Polled rather than blocked on `accept`, because closing a listener another thread is
/// parked inside needs the raw fd. The wait only delays shutdown: a Cast receiver
/// connects once, at the start, and is never waiting on this.
fn run_server(listener: std::net::TcpListener, hub: Hub, stop: Arc<AtomicBool>) {
    if let Err(e) = listener.set_nonblocking(true) {
        error!("<HttpSink> could not poll the listener, it will stay up: {e}");
        for stream in listener.incoming().flatten() {
            let hub = hub.clone();
            thread::spawn(move || serve_client(stream, hub));
        }
        return;
    }

    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                // Back to blocking: only accepting is polled, serving is not.
                let _ = stream.set_nonblocking(false);
                let hub = hub.clone();
                thread::spawn(move || serve_client(stream, hub));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL);
            }
            Err(e) => {
                error!("<HttpSink> stopped accepting connections: {e}");
                return;
            }
        }
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
    let _counted = Listener::new();
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
    /// Tells the accept loop to let go of the port when this sink is dropped.
    stop: Arc<AtomicBool>,
}

impl Drop for HttpSink {
    /// Frees the port. Without this the listener outlives the sink, and an embedder that
    /// stops and starts again finds its own address already in use.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        SERVING.store(0, Ordering::Relaxed);
    }
}

impl Open for HttpSink {
    /// Binds here rather than in `start()`, which the player only calls once playback
    /// begins. A Cast receiver has to be pointed at the URL *before* anything plays, so
    /// the stream has to exist from the moment the backend opens -- serving silence
    /// until there is audio.
    fn open(device: Option<String>, format: AudioFormat) -> Self {
        let addr = device.unwrap_or_else(|| DEFAULT_ADDR.to_string());

        // Panics instead of exiting: this backend also runs inside someone else's
        // process, where `exit` runs the whole program's static destructors and brings
        // the host down with it -- on Android as a SIGABRT in an unrelated thread.
        if format != AudioFormat::S16 {
            let e = HttpError::UnsupportedFormat(format);
            error!("{e}");
            panic!("{e}");
        }

        let listener = match bind_with_retry(&addr) {
            Ok(l) => l,
            Err(e) => {
                let e = HttpError::BindFailure {
                    addr: addr.clone(),
                    e,
                };
                error!("{e}");
                panic!("{e}");
            }
        };

        // Announced from the address the OS settled on, so a `:0` bind reports the port
        // it was actually given rather than zero.
        match listener.local_addr() {
            Ok(bound) => SERVING.store(bound.port(), Ordering::Relaxed),
            Err(e) => error!("<HttpSink> bound but cannot name the port: {e}"),
        }

        let _ = epoch();
        let hub = Hub::default();
        let (tx, rx) = sync_channel(AHEAD_CHUNKS);

        thread::spawn({
            let hub = hub.clone();
            move || run_sender(rx, hub)
        });
        let stop = Arc::new(AtomicBool::new(false));
        thread::spawn({
            let hub = hub.clone();
            let stop = stop.clone();
            move || run_server(listener, hub, stop)
        });

        info!("Using HttpSink with format: {format:?}, serving WAV on http://{addr}/");

        Self {
            format,
            tx: Some(tx),
            pending: Vec::with_capacity(CHUNK_BYTES * 2),
            stop,
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

    /// The bug this guards: the listener used to live in a detached thread that outlived
    /// the sink, so the port stayed bound. A CLI never notices -- it is exiting anyway --
    /// but an embedder that stops and starts again cannot bind its own address.
    #[test]
    fn dropping_the_sink_frees_the_port() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        let addr = probe.local_addr().expect("the bound address").to_string();
        drop(probe);

        let sink = HttpSink::open(Some(addr.clone()), AudioFormat::S16);
        assert!(
            std::net::TcpListener::bind(&addr).is_err(),
            "the sink should be holding the port while it is alive"
        );
        drop(sink);

        // Up to ACCEPT_POLL to notice, plus room for a slow machine.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if std::net::TcpListener::bind(&addr).is_ok() {
                return;
            }
            thread::sleep(ACCEPT_POLL);
        }
        panic!("the port was still bound {addr} after the sink was dropped");
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
    fn never_played_is_not_the_same_as_played_just_now() {
        // Collapsing these two into one duration is what made the caster grab the
        // speakers at startup before anything had played.
        assert_eq!(idle_for(), None, "nothing has played yet");

        mark_audio();
        assert!(
            idle_for().is_some_and(|d| d < Duration::from_secs(1)),
            "just-marked audio must read as active"
        );
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
