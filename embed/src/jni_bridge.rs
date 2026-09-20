//! The only part of this crate that knows Android exists.
//!
//! Everything it does is translate: JVM strings in, a Rust [`Handle`] parked in a static,
//! a boolean out. Keeping it this thin is deliberate -- logic here can only be exercised
//! over `adb`, while logic in the parent module runs in `cargo test`.
//!
//! Expected Kotlin side:
//!
//! ```text
//! object Librespot {
//!     init { System.loadLibrary("librespot_embed") }
//!     external fun nativeStart(name: String, bind: String, castTo: String?,
//!                              crossfadeSecs: Int, crossfadeAlbums: Boolean,
//!                              cacheDir: String): Boolean
//!     external fun nativeStop()
//! }
//! ```

use std::sync::Mutex;

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint};
use log::{error, info};

use crate::{Config, Handle};

/// The running instance. A static because the JVM side is an object with no place to keep
/// a Rust pointer, and because starting twice must not leave an orphan running with the
/// port already bound.
static RUNNING: Mutex<Option<Handle>> = Mutex::new(None);

fn take_string(env: &mut JNIEnv, s: &JString) -> Option<String> {
    env.get_string(s).ok().map(|s| s.into())
}

/// Routes Rust's `log` to logcat and makes panics visible.
///
/// Without the panic hook a panic inside a worker thread unwinds into nothing and the app
/// simply goes quiet, which is indistinguishable from "it did not start".
fn init_logging() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        android_logger::init_once(
            android_logger::Config::default()
                .with_max_level(log::LevelFilter::Info)
                .with_tag("librespot"),
        );
        std::panic::set_hook(Box::new(|info| {
            error!("panic: {info}");
        }));
    });
}

/// Starts librespot. Returns false if it could not start, including when nobody has
/// signed in yet -- the caller is expected to send the user through OAuth in that case.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_librespot_embed_Librespot_nativeStart(
    mut env: JNIEnv,
    _class: JClass,
    name: JString,
    bind: JString,
    cast_to: JString,
    crossfade_secs: jint,
    crossfade_albums: jboolean,
    cache_dir: JString,
) -> jboolean {
    init_logging();

    let Ok(mut running) = RUNNING.lock() else {
        error!("the instance lock is poisoned; refusing to start");
        return JNI_FALSE;
    };
    // A worker that ended by itself failed; leaving it parked here would make every
    // later start a silent no-op that reports success.
    if running.as_ref().is_some_and(Handle::is_finished) {
        info!("the previous instance had already died; starting a new one");
        *running = None;
    }
    if running.is_some() {
        info!("already running");
        return JNI_TRUE;
    }

    let (Some(name), Some(bind), Some(cache_dir)) = (
        take_string(&mut env, &name),
        take_string(&mut env, &bind),
        take_string(&mut env, &cache_dir),
    ) else {
        error!("could not read the arguments from the JVM");
        return JNI_FALSE;
    };

    let mut config = Config::new(name, cache_dir);
    config.bind = bind;
    config.crossfade_secs = crossfade_secs.max(0) as u64;
    config.crossfade_albums = crossfade_albums == JNI_TRUE;
    // A null Java string arrives as a valid-but-null JString, so an unreadable value here
    // means "no target", not an error.
    config.cast_to = take_string(&mut env, &cast_to).filter(|s| !s.trim().is_empty());

    match crate::start(config) {
        Ok(handle) => {
            *running = Some(handle);
            JNI_TRUE
        }
        Err(e) => {
            error!("could not start: {e}");
            JNI_FALSE
        }
    }
}

/// Stops librespot if it is running. Safe to call when it is not.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_librespot_embed_Librespot_nativeStop(_env: JNIEnv, _class: JClass) {
    let Ok(mut running) = RUNNING.lock() else {
        error!("the instance lock is poisoned; cannot stop");
        return;
    };
    if let Some(handle) = running.take() {
        handle.stop();
        info!("stopped");
    }
}

/// Starts a device sign-in and returns "CODE|URL" to display, or an empty string if it
/// could not even be started.
///
/// Credentials are minted on the device on purpose. Copying them from a desktop looks
/// like it works -- the session authenticates -- and then Spirc is rejected, because the
/// stored blob is tied to the client id of the machine that created it.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_librespot_embed_Librespot_nativeAuthBegin<'a>(
    mut env: JNIEnv<'a>,
    _class: JClass,
    cache_dir: JString,
) -> JString<'a> {
    init_logging();

    // Built first and reused: handing back an empty string is the failure signal, and
    // building it later would need `env` while it is already borrowed.
    let out = match take_string(&mut env, &cache_dir) {
        None => {
            error!("could not read the cache dir from the JVM");
            String::new()
        }
        Some(cache_dir) => {
            match crate::begin_device_auth(Config::new("Crossfade Bridge", cache_dir)) {
                Ok(auth) => {
                    info!("pair at {} with code {}", auth.url, auth.code);
                    format!("{}|{}", auth.code, auth.url)
                }
                Err(e) => {
                    error!("could not begin device auth: {e}");
                    String::new()
                }
            }
        }
    };

    env.new_string(out).unwrap_or_else(|_| JString::default())
}

/// 0 idle, 1 waiting for the user, 2 done, 3 failed.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_librespot_embed_Librespot_nativeAuthStatus(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    crate::auth_status() as jint
}

/// Tells librespot the Android API level, which it cannot work out for itself.
///
/// `sysinfo` reports the release ("14") and Spotify expects the API level ("34"). With
/// the release in the user agent the access point still authenticates the session, and
/// then Spirc's first login5 call is denied with a bare BAD_REQUEST -- a failure that
/// reads like bad credentials and is not one. Called from `Librespot`'s initialiser so
/// it cannot be forgotten at one of the entry points.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_librespot_embed_Librespot_nativeSetOsVersion(
    mut env: JNIEnv,
    _class: JClass,
    version: JString,
) {
    init_logging();
    match take_string(&mut env, &version) {
        Some(version) => {
            info!("reporting API level {version}");
            librespot_core::config::set_os_version(version);
        }
        None => error!("could not read the API level from the JVM"),
    }
}

/// Every Cast device and group on the network, one name per line.
///
/// Blocks for the length of an mDNS browse, so the caller must not be on the UI thread.
/// An empty string means nothing answered, which is not an error: a group that is off
/// simply is not there.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_librespot_embed_Librespot_nativeDiscover<'a>(
    env: JNIEnv<'a>,
    _class: JClass,
) -> JString<'a> {
    init_logging();
    let names = librespot_playback::cast::device_names().join("
");
    info!("discovery found {} device(s)", names.lines().count());
    env.new_string(names).unwrap_or_else(|_| JString::default())
}
