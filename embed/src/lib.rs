//! Runs librespot inside a host application instead of as a process to exec.
//!
//! On Android there is no binary to run: an app loads a `cdylib` and calls into it. That
//! rules out the usual "ship the executable and spawn it" approach, which also fights the
//! platform -- executing binaries out of an APK is something Android keeps tightening, and
//! a child process is awkward for a foreground service to own.
//!
//! The platform-specific part is deliberately thin. Everything here is ordinary Rust that
//! runs and can be tested on a desktop; only the JNI shim knows Android exists. Getting
//! the wiring wrong on a laptop costs seconds, getting it wrong over `adb` costs an
//! afternoon.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::{self, JoinHandle};

use librespot_connect::{ConnectConfig, Spirc};
use librespot_core::{Session, SessionConfig, cache::Cache};
use librespot_playback::audio_backend;
use librespot_playback::config::{AudioFormat, PlayerConfig};
use librespot_playback::mixer::softmixer::SoftMixer;
use librespot_playback::mixer::{Mixer, MixerConfig};
use librespot_playback::player::Player;
use log::{error, info};
use tokio::sync::oneshot;

/// The exact scope list the CLI requests. Trimming it to what seems necessary gets
/// the whole request rejected with `invalid_scope`, not a narrower grant.
const OAUTH_SCOPES: &[&str] = &[
    "app-remote-control",
    "playlist-modify",
    "playlist-modify-private",
    "playlist-modify-public",
    "playlist-read",
    "playlist-read-collaborative",
    "playlist-read-private",
    "streaming",
    "ugc-image-upload",
    "user-follow-modify",
    "user-follow-read",
    "user-library-modify",
    "user-library-read",
    "user-modify",
    "user-modify-playback-state",
    "user-modify-private",
    "user-personalized",
    "user-read-birthdate",
    "user-read-currently-playing",
    "user-read-email",
    "user-read-play-history",
    "user-read-playback-position",
    "user-read-playback-state",
    "user-read-private",
    "user-read-recently-played",
    "user-top-read",
];

/// librespot's own client id, from librespot-core/src/config.rs where it is `pub(crate)`.
///
/// `SessionConfig::default()` picks one per operating system, which on Android means the
/// official Spotify app's id -- and that client does not carry the scopes librespot asks
/// for, so device auth is refused outright with `invalid_scope`. Using the same id the
/// desktop CLI uses, on every host, keeps minting and spending credentials consistent.
const CLIENT_ID: &str = "65b708073fc0480ea92a077233ca87bd";

/// One-time setup every entry point needs.
///
/// More than one rustls crypto provider ends up in the tree -- librespot brings one and
/// the caster another -- and rustls refuses to guess between them, panicking on first use
/// deep inside whatever happened to call it. Choosing once, here, turns that into a
/// decision instead of a crash. Every public entry point must call this: the sign-in path
/// reaches TLS without going anywhere near `start`.
fn init() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        // Announce the same platform the credentials were minted for. CLIENT_ID below is
        // the desktop CLI's, and login5 denies a stored credential whose client id does
        // not match the platform asking -- BAD_REQUEST, right after the access point has
        // already said the session is fine.
        librespot_core::config::set_os("linux");
    });
}

/// A session config that does not vary with the host operating system.
fn session_config() -> SessionConfig {
    SessionConfig {
        client_id: CLIENT_ID.to_string(),
        ..Default::default()
    }
}

/// 0 idle, 1 waiting for the user, 2 done, 3 failed.
static AUTH_STATE: AtomicU8 = AtomicU8::new(0);

pub const AUTH_IDLE: u8 = 0;
pub const AUTH_PENDING: u8 = 1;
pub const AUTH_DONE: u8 = 2;
pub const AUTH_FAILED: u8 = 3;

/// What to show the person so they can approve this device.
#[derive(Debug, Clone)]
pub struct DeviceAuth {
    pub code: String,
    pub url: String,
}

/// Progress of the sign-in started by [`begin_device_auth`].
pub fn auth_status() -> u8 {
    AUTH_STATE.load(Ordering::Relaxed)
}

/// Starts a device sign-in and returns the code to display.
///
/// Minting the credentials here rather than copying them from a desktop is the whole
/// point: stored credentials are tied to the client id that obtained them, and that id
/// is chosen per operating system. Credentials made on Windows authenticate the session
/// on Android and are then rejected by `login5`, which Spirc calls immediately after --
/// an error that reads like a wrong password and is nothing of the sort.
pub fn begin_device_auth(config: Config) -> Result<DeviceAuth, Error> {
    use librespot_oauth::DeviceAuthClientBuilder;

    init();

    let client = DeviceAuthClientBuilder::new(CLIENT_ID, OAUTH_SCOPES.to_vec())
        .build()
        .map_err(|e| Error::Session(e.to_string()))?;

    let auth = client
        .request_device_code()
        .map_err(|e| Error::Session(e.to_string()))?;
    let shown = DeviceAuth {
        code: auth.user_code().to_string(),
        url: auth.url().to_string(),
    };

    AUTH_STATE.store(AUTH_PENDING, Ordering::Relaxed);
    thread::spawn(move || {
        // Polling blocks until the person approves or the code expires.
        let token = match client.poll_for_token(&auth) {
            Ok(t) => t,
            Err(e) => {
                error!("device auth failed: {e}");
                AUTH_STATE.store(AUTH_FAILED, Ordering::Relaxed);
                return;
            }
        };

        // Connecting once with the access token is what makes the session write reusable
        // credentials into the cache; the token itself is short-lived and no use later.
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                error!("could not start the runtime: {e}");
                AUTH_STATE.store(AUTH_FAILED, Ordering::Relaxed);
                return;
            }
        };
        let ok = runtime.block_on(async move {
            let cache = match Cache::new(
                Some(config.cache_dir.as_path()),
                Some(config.cache_dir.as_path()),
                None,
                None,
            ) {
                Ok(c) => c,
                Err(e) => {
                    error!("cache: {e}");
                    return false;
                }
            };
            let session = Session::new(session_config(), Some(cache));
            match session
                .connect(
                    librespot_core::authentication::Credentials::with_access_token(
                        token.access_token,
                    ),
                    true,
                )
                .await
            {
                Ok(()) => {
                    session.shutdown();
                    true
                }
                Err(e) => {
                    error!("could not store credentials: {e}");
                    false
                }
            }
        });

        AUTH_STATE.store(if ok { AUTH_DONE } else { AUTH_FAILED }, Ordering::Relaxed);
        if ok {
            info!("device authorised; credentials stored");
        }
    });

    Ok(shown)
}

#[cfg(target_os = "android")]
mod jni_bridge;

/// Everything the host has to decide. Kept small on purpose: anything with a sane default
/// is not worth a knob the app has to thread through JNI.
#[derive(Debug, Clone)]
pub struct Config {
    /// Shown in Spotify's device list.
    pub device_name: String,
    /// Where the http backend listens, e.g. `0.0.0.0:8321`.
    pub bind: String,
    /// Exact name of the Cast device or group to drive, if any.
    pub cast_to: Option<String>,
    /// Seconds of overlap between tracks; 0 disables it.
    pub crossfade_secs: u64,
    /// Crossfade consecutive tracks of the same album too, instead of leaving them
    /// gapless.
    pub crossfade_albums: bool,
    /// Directory holding the credentials written by a previous OAuth sign-in.
    pub cache_dir: PathBuf,
}

impl Config {
    pub fn new(device_name: impl Into<String>, cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            device_name: device_name.into(),
            bind: "0.0.0.0:8321".to_string(),
            cast_to: None,
            crossfade_secs: 0,
            crossfade_albums: false,
            cache_dir: cache_dir.into(),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    /// No cached credentials. The host has to run an OAuth sign-in first; this API
    /// deliberately cannot do it, because it must never block on a browser.
    NoCredentials,
    Cache(String),
    Session(String),
    Backend(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoCredentials => write!(
                f,
                "no cached credentials; sign in once with --enable-device-auth first"
            ),
            Error::Cache(e) => write!(f, "cache: {e}"),
            Error::Session(e) => write!(f, "session: {e}"),
            Error::Backend(e) => write!(f, "backend: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// A running instance. Dropping it does **not** stop anything -- stopping is explicit, so
/// a host that loses track of the handle does not silently kill the music.
pub struct Handle {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Handle {
    /// Whether the worker has already ended on its own, which means it failed: the
    /// normal way out is [`Handle::stop`].
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_some_and(|t| t.is_finished())
    }

    /// Stops playback and waits for the worker to wind down.
    pub fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Starts librespot on its own thread and returns once it is running.
///
/// Returns early with [`Error::NoCredentials`] rather than starting a doomed worker, so a
/// host can tell "not signed in yet" apart from "running".
pub fn start(config: Config) -> Result<Handle, Error> {
    init();

    let cache = Cache::new(
        Some(config.cache_dir.as_path()),
        Some(config.cache_dir.as_path()),
        None,
        None,
    )
    .map_err(|e| Error::Cache(e.to_string()))?;

    let credentials = cache.credentials().ok_or(Error::NoCredentials)?;

    // Fail before spawning if the backend is missing, so the caller gets a real error
    // instead of a thread that dies on its own a moment later.
    let backend = audio_backend::find(Some("http".to_string()))
        .ok_or_else(|| Error::Backend("built without the http backend".to_string()))?;

    let (stop_tx, stop_rx) = oneshot::channel();
    let thread = thread::Builder::new()
        .name("librespot-embed".to_string())
        .spawn(move || run(config, cache, credentials, backend, stop_rx))
        .map_err(|e| Error::Session(e.to_string()))?;

    Ok(Handle {
        stop: Some(stop_tx),
        thread: Some(thread),
    })
}

fn run(
    config: Config,
    cache: Cache,
    credentials: librespot_core::authentication::Credentials,
    backend: audio_backend::SinkBuilder,
    stop_rx: oneshot::Receiver<()>,
) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            error!("could not start the runtime: {e}");
            return;
        }
    };

    runtime.block_on(async move {
        let session = Session::new(session_config(), Some(cache));

        let player_config = PlayerConfig {
            crossfade: std::time::Duration::from_secs(config.crossfade_secs),
            crossfade_albums: config.crossfade_albums,
            ..Default::default()
        };

        let mixer = match SoftMixer::open(MixerConfig::default()) {
            Ok(m) => Arc::new(m),
            Err(e) => {
                error!("could not open the mixer: {e}");
                return;
            }
        };
        // Casting to a group hands the volume to that group, so nothing is attenuated
        // here. Doing both multiplies two attenuations -- Spotify's slider at 45% is
        // already -33 dB on librespot's 60 dB log curve, and the group's own level lands
        // on top of it -- and every dB taken in software is resolution the speakers never
        // get. Spotify's slider stops doing anything while casting, by design: the volume
        // lives where the speakers are.
        let soft_volume: Box<dyn librespot_playback::mixer::VolumeGetter + Send> =
            if config.cast_to.is_some() {
                info!("casting: leaving the volume to the group");
                Box::new(librespot_playback::mixer::NoOpVolume)
            } else {
                mixer.get_soft_volume()
            };

        // The http backend takes its bind address through the `device` argument, the same
        // way every other backend names its output.
        let device = Some(config.bind.clone());
        let player = Player::new(player_config, session.clone(), soft_volume, move || {
            (backend)(device, AudioFormat::S16)
        });

        #[cfg(feature = "cast")]
        if let Some(target) = config.cast_to.clone() {
            librespot_playback::cast::spawn(target, port_of(&config.bind));
        }

        let connect_config = ConnectConfig {
            name: config.device_name.clone(),
            ..Default::default()
        };

        let (spirc, spirc_task) =
            match Spirc::new(connect_config, session.clone(), credentials, player, mixer).await {
                Ok(pair) => pair,
                Err(e) => {
                    error!("could not initialize spirc: {e}");
                    return;
                }
            };

        info!("librespot-embed running as <{}>", config.device_name);

        tokio::select! {
            _ = spirc_task => info!("spirc task ended"),
            _ = stop_rx => {
                info!("stopping on request");
                let _ = spirc.shutdown();
            }
        }

        session.shutdown();
    });
}

/// Port out of a bind address, falling back to the backend's own default.
///
/// Split from the right: an IPv6 literal such as `[::]:8321` has colons of its own, and
/// splitting from the left would read the port as part of the address.
fn port_of(bind: &str) -> u16 {
    bind.rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(8321)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_port_out_of_a_bind_address() {
        assert_eq!(port_of("0.0.0.0:8321"), 8321);
        assert_eq!(port_of("192.168.1.89:9000"), 9000);
    }

    #[test]
    fn an_ipv6_literal_does_not_confuse_the_port() {
        // Splitting from the left would take "" or ":" here and silently fall back.
        assert_eq!(port_of("[::]:8321"), 8321);
        assert_eq!(port_of("[::1]:9000"), 9000);
    }

    #[test]
    fn a_bind_address_with_no_port_falls_back() {
        assert_eq!(port_of("0.0.0.0"), 8321);
        assert_eq!(port_of(""), 8321);
    }

    #[test]
    fn starting_without_credentials_says_so_instead_of_spawning() {
        let dir = std::env::temp_dir().join("librespot-embed-no-creds-test");
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::remove_file(dir.join("credentials.json"));

        match start(Config::new("test", &dir)) {
            Err(Error::NoCredentials) => {}
            Err(other) => panic!("expected NoCredentials, got {other}"),
            Ok(_) => panic!("started with no credentials; the host cannot tell it is broken"),
        }
    }
}
