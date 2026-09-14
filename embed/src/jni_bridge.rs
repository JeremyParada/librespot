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
//!                              crossfadeSecs: Int, cacheDir: String): Boolean
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
    cache_dir: JString,
) -> jboolean {
    init_logging();

    let Ok(mut running) = RUNNING.lock() else {
        error!("the instance lock is poisoned; refusing to start");
        return JNI_FALSE;
    };
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
