//! Drives the embed API exactly as the Android service will, but on a desktop where a
//! failure costs seconds instead of an adb session.
fn main() {
    env_logger::init();
    let cache = std::env::args()
        .nth(1)
        .expect("usage: run <cache_dir> [cast_to]");
    let mut config = librespot_embed::Config::new("Crossfade Bridge", cache);
    config.crossfade_secs = 8;
    config.cast_to = std::env::args().nth(2);

    let handle = librespot_embed::start(config).expect("start failed");
    println!("running; Ctrl+C to stop");
    std::thread::sleep(std::time::Duration::from_secs(3600));
    handle.stop();
}
