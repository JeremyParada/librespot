//! Points a Google Cast device or speaker group at the stream served by the `http` backend.
//!
//! Without this, something else has to tell the group to start playing, which is fine on a
//! desktop and useless on an appliance. With `--cast-to "Living room"` the binary finds the
//! target itself on startup and re-attaches if the session goes away.
//!
//! Homes commonly have several groups, so the target is always named explicitly. There is
//! deliberately no "just pick the first group" path: on a two-group network that would be a
//! coin flip, and the failure is music starting in the wrong room.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

use crate::audio_backend::http::{idle_for, listeners};
use log::{info, warn};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use rust_cast::channels::media::{Media, StreamType};
use rust_cast::channels::receiver::CastDeviceApp;
use rust_cast::{CastDevice, ChannelMessage};

/// The platform receiver. `rust_cast` keeps its own copy private.
const RECEIVER_ID: &str = "receiver-0";

const SERVICE: &str = "_googlecast._tcp.local.";
const DISCOVERY_WINDOW: Duration = Duration::from_secs(5);
const RETRY_DELAY: Duration = Duration::from_secs(15);

/// Let the group go after this long with nothing playing.
///
/// Holding a Cast session keeps the speakers busy for everyone else in the house and
/// pushes 1.4 Mbps of silence across the network around the clock. A short pause must
/// still hold the session -- letting the stream dry up is what ends it -- so this is
/// deliberately much longer than any pause a listener would take.
const IDLE_RELEASE: Duration = Duration::from_secs(10 * 60);

/// Below this, the player is considered to be producing audio right now.
const ACTIVE_WITHIN: Duration = Duration::from_secs(5);

const POLL: Duration = Duration::from_secs(2);

/// How long audio may play with nobody pulling the stream before the session counts as
/// dead. Long enough to cover a receiver that has been told to play and has not opened
/// the connection yet.
const DEAD_SESSION: Duration = Duration::from_secs(20);

/// Blocks until the player is producing audio, so nothing is cast before there is
/// anything to listen to.
fn wait_for_audio(state: &AtomicU8) -> bool {
    // `None` means nothing has ever played, which is a reason to keep waiting.
    while !idle_for().is_some_and(|d| d <= ACTIVE_WITHIN) {
        if winding(state).is_some() {
            return false;
        }
        thread::sleep(POLL);
    }
    true
}

/// What discovery found: the address to talk to, plus the name a human would recognise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    pub name: String,
    pub host: String,
    pub port: u16,
}

/// Chooses the requested device out of everything on the network.
///
/// Matching is case-insensitive and ignores surrounding whitespace, because these names are
/// typed by hand into a config and copied from the Google Home app, which is happy to keep
/// trailing spaces. Anything looser would risk picking the wrong room.
pub fn pick<'a>(found: &'a [Device], target: &str) -> Option<&'a Device> {
    let target = target.trim();
    found
        .iter()
        .find(|d| d.name.trim().eq_ignore_ascii_case(target))
}

/// Every Cast device and group on the network, by the name a person would recognise.
///
/// Sorted, because it is shown in a list: discovery order is arrival order, which
/// reshuffles the list under the user between one browse and the next.
pub fn device_names() -> Vec<String> {
    let mut names: Vec<String> = discover()
        .unwrap_or_else(|e| {
            warn!("cast: could not browse for devices: {e}");
            Vec::new()
        })
        .into_iter()
        .map(|d| d.name)
        .collect();
    names.sort();
    names
}

/// Browses mDNS for the discovery window and returns every Cast endpoint seen, keyed by
/// name so a device announced on several interfaces is only listed once.
fn discover() -> Result<Vec<Device>, Box<dyn std::error::Error>> {
    let daemon = ServiceDaemon::new()?;
    let rx = daemon.browse(SERVICE)?;
    let mut found: BTreeMap<String, Device> = BTreeMap::new();

    let deadline = std::time::Instant::now() + DISCOVERY_WINDOW;
    while let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) {
        match rx.recv_timeout(remaining) {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                // The mDNS instance name is a UUID; the human-readable name lives in the
                // `fn` TXT record, and that is what the Google Home app shows.
                let name = info
                    .get_property_val_str("fn")
                    .unwrap_or_else(|| info.get_fullname())
                    .to_string();
                if let Some(addr) = info.get_addresses().iter().next() {
                    found.entry(name.clone()).or_insert(Device {
                        name,
                        host: addr.to_string(),
                        port: info.get_port(),
                    });
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    let _ = daemon.shutdown();
    Ok(found.into_values().collect())
}

/// The address the device should call back on.
///
/// Asking the OS which local address it would use to reach *this* device is the only
/// reliable answer on a machine with several interfaces -- a dev box with WSL, Hyper-V and
/// a VPN will otherwise happily advertise an address the speaker cannot route to. No
/// packet is actually sent; connecting a UDP socket just fixes the route.
fn local_ip_towards(host: &str, port: u16) -> Option<IpAddr> {
    // `host` is already an address, so parse it as one. Formatting it back into
    // "host:port" and parsing that drops every IPv6 device on the floor, because a
    // SocketAddr wants the address in brackets there.
    let ip: IpAddr = match host.parse() {
        Ok(ip) => ip,
        Err(e) => {
            warn!("cast: <{host}> is not an address: {e}");
            return None;
        }
    };

    // The socket has to be of the same family as the device, or connecting it fails and
    // the route question never gets asked.
    let bind: SocketAddr = if ip.is_ipv4() {
        ([0, 0, 0, 0], 0).into()
    } else {
        ([0u16; 8], 0).into()
    };
    let sock = match UdpSocket::bind(bind) {
        Ok(sock) => sock,
        Err(e) => {
            warn!("cast: could not open a socket towards {ip}: {e}");
            return None;
        }
    };
    if let Err(e) = sock.connect(SocketAddr::new(ip, port)) {
        warn!("cast: no route to {ip}:{port}: {e}");
        return None;
    }
    sock.local_addr().ok().map(|a| a.ip())
}

/// Whether a session with audio playing but nobody listening has gone on long enough to
/// call it dead.
///
/// Kept apart from the network loop so it can be exercised without a Chromecast.
fn session_is_dead(
    playing: bool,
    listeners: usize,
    silent_since: &mut Option<Instant>,
    now: Instant,
) -> bool {
    if !playing || listeners > 0 {
        *silent_since = None;
        return false;
    }
    now.duration_since(*silent_since.get_or_insert(now)) >= DEAD_SESSION
}

/// Casts once and stays attached until the connection drops, answering heartbeats.
fn attach(
    device: &Device,
    stream_port: u16,
    state: &AtomicU8,
) -> Result<(), Box<dyn std::error::Error>> {
    let ip = local_ip_towards(&device.host, device.port)
        .ok_or("could not work out which local address this device can reach")?;
    let url = format!("http://{ip}:{stream_port}/");

    // Cast devices present a self-signed certificate, so host verification cannot apply.
    let cast = CastDevice::connect_without_host_verification(device.host.clone(), device.port)?;
    cast.connection.connect(RECEIVER_ID)?;
    cast.heartbeat.ping()?;

    let app = cast
        .receiver
        .launch_app(&CastDeviceApp::DefaultMediaReceiver)?;
    cast.connection.connect(app.transport_id.as_str())?;
    cast.media.load(
        app.transport_id.as_str(),
        app.session_id.as_str(),
        &Media {
            content_id: url.clone(),
            // Live, or the receiver treats it as a file, tries to seek something with no
            // length, and fails.
            stream_type: StreamType::Live,
            content_type: "audio/wav".to_string(),
            metadata: None,
            duration: None,
        },
    )?;

    info!("Casting {url} to <{}>", device.name);

    let mut silent_since = None;
    loop {
        match cast.receive() {
            Ok(ChannelMessage::Heartbeat(_)) => {
                cast.heartbeat.pong()?;

                // The only place this loop is free to look: `receive` blocks, and the
                // device pings every few seconds.
                match winding(state) {
                    Some(Winding::Release) => {
                        info!("Released <{}>", device.name);
                        let _ = cast.receiver.stop_app(app.session_id.as_str());
                        return Ok(());
                    }
                    Some(Winding::Superseded) => {
                        info!("Another caster took over <{}>", device.name);
                        return Ok(());
                    }
                    _ => {}
                }

                // A receiver can stop playing while this connection stays up and keeps
                // answering heartbeats: on a Chromecast the system kills its own
                // receiver under memory pressure, which leaves a session that looks
                // healthy from here and plays nothing. Audio going out with nobody
                // reading it is the only sign of it, so it is what we watch.
                if session_is_dead(
                    idle_for().is_some_and(|d| d <= ACTIVE_WITHIN),
                    listeners(),
                    &mut silent_since,
                    Instant::now(),
                ) {
                    warn!(
                        "<{}> stopped pulling the stream; casting again",
                        device.name
                    );
                    let _ = cast.receiver.stop_app(app.session_id.as_str());
                    return Ok(());
                }

                // The device pings every few seconds, which is a good enough clock to
                // notice a long idle without polling anything ourselves.
                if idle_for().is_some_and(|d| d >= IDLE_RELEASE) {
                    info!(
                        "Nothing played for {} min, releasing <{}>",
                        IDLE_RELEASE.as_secs() / 60,
                        device.name
                    );
                    let _ = cast.receiver.stop_app(app.session_id.as_str());
                    return Ok(());
                }
            }
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
    }
}

fn run(target: String, stream_port: u16, state: Arc<AtomicU8>) {
    loop {
        if winding(&state).is_some() {
            return;
        }
        // Nothing is cast until there is audio, so a box that sits unused never takes
        // the speakers away from anyone.
        if !wait_for_audio(&state) {
            return;
        }
        match discover() {
            Ok(found) => match pick(&found, &target) {
                Some(device) => match attach(device, stream_port, &state) {
                    // A clean return means it was released on purpose; loop straight
                    // back to waiting for the next thing to play.
                    Ok(()) => continue,
                    Err(e) => warn!("Cast session to <{target}> ended: {e}"),
                },
                None => {
                    // Naming what *is* there turns "it does not work" into one glance,
                    // which matters most on a headless box with several groups.
                    let names: Vec<&str> = found.iter().map(|d| d.name.as_str()).collect();
                    warn!(
                        "No Cast device named <{target}>. Found: {}",
                        if names.is_empty() {
                            "nothing".to_string()
                        } else {
                            names.join(", ")
                        }
                    );
                }
            },
            Err(e) => warn!("Cast discovery failed: {e}"),
        }
        if !sleep_unless_winding(&state, RETRY_DELAY) {
            return;
        }
    }
}

/// Starts casting in the background. Never blocks startup: if the group is off or the
/// network is not up yet, it keeps retrying while the stream stays served.
/// Why a caster is winding down, which decides whether it lets go of the group.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Winding {
    /// Still going.
    No = 0,
    /// The host stopped playback: let the group go, so the speakers are free.
    Release = 1,
    /// Another caster took over. Leave the receiver alone -- stopping the app here
    /// would kill the session the new caster has just set up on the same group.
    Superseded = 2,
}

/// The caster that is allowed to drive the group.
///
/// A static because a caster cannot be handed back to the host: `spawn` is called from
/// deep inside startup, and an embedder that starts and stops repeatedly would otherwise
/// leave one thread per start, all fighting over the same speakers.
static CURRENT: Mutex<Option<Arc<AtomicU8>>> = Mutex::new(None);

/// Starts casting, and stops whatever was casting before.
pub fn spawn(target: String, stream_port: u16) {
    let state = Arc::new(AtomicU8::new(Winding::No as u8));

    if let Ok(mut current) = CURRENT.lock() {
        if let Some(previous) = current.replace(state.clone()) {
            previous.store(Winding::Superseded as u8, Ordering::Relaxed);
        }
    }

    thread::spawn(move || run(target, stream_port, state));
}

/// Lets the group go. Safe to call when nothing is casting.
pub fn release() {
    if let Ok(mut current) = CURRENT.lock() {
        if let Some(state) = current.take() {
            state.store(Winding::Release as u8, Ordering::Relaxed);
        }
    }
}

fn winding(state: &AtomicU8) -> Option<Winding> {
    match state.load(Ordering::Relaxed) {
        1 => Some(Winding::Release),
        2 => Some(Winding::Superseded),
        _ => None,
    }
}

/// Sleeps in small steps so a caster that has been told to stop does not keep the group
/// for the rest of a long wait.
fn sleep_unless_winding(state: &AtomicU8, total: Duration) -> bool {
    let deadline = Instant::now() + total;
    while Instant::now() < deadline {
        if winding(state).is_some() {
            return false;
        }
        thread::sleep(POLL.min(deadline - Instant::now()));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_nobody_listens_to_is_dead_only_after_the_grace_period() {
        let t0 = Instant::now();
        let mut since = None;

        // Just cast: playing, and the receiver has not opened the stream yet. Not dead.
        assert!(!session_is_dead(true, 0, &mut since, t0));
        assert!(!session_is_dead(true, 0, &mut since, t0 + DEAD_SESSION / 2));
        // Still nobody once the grace period is up: dead.
        assert!(session_is_dead(true, 0, &mut since, t0 + DEAD_SESSION));

        // A listener turning up clears it, and the wait starts over.
        assert!(!session_is_dead(true, 1, &mut since, t0 + DEAD_SESSION));
        assert_eq!(since, None);
        assert!(!session_is_dead(true, 0, &mut since, t0 + DEAD_SESSION));

        // Paused with nobody listening is normal, not a dead session: pausing must not
        // tear a cast down, since holding the group between tracks is the point.
        let mut since = None;
        assert!(!session_is_dead(false, 0, &mut since, t0));
        assert!(!session_is_dead(false, 0, &mut since, t0 + DEAD_SESSION * 10));
    }

    fn dev(name: &str) -> Device {
        Device {
            name: name.to_string(),
            host: "192.168.1.91".to_string(),
            port: 32032,
        }
    }

    #[test]
    fn picks_the_named_group_out_of_several() {
        let found = vec![dev("Kitchen"), dev("Grupo de la casa"), dev("Bedroom")];
        assert_eq!(pick(&found, "Grupo de la casa"), Some(&found[1]));
        assert_eq!(pick(&found, "Kitchen"), Some(&found[0]));
    }

    #[test]
    fn matching_survives_case_and_stray_whitespace() {
        let found = vec![dev("Grupo de la Casa ")];
        assert_eq!(pick(&found, "grupo de la casa"), Some(&found[0]));
        assert_eq!(pick(&found, "  Grupo de la Casa  "), Some(&found[0]));
    }

    #[test]
    fn an_unknown_name_never_falls_back_to_another_group() {
        // The whole point: guessing here would start music in the wrong room.
        let found = vec![dev("Kitchen"), dev("Bedroom")];
        assert_eq!(pick(&found, "Living room"), None);
        assert_eq!(pick(&[], "Kitchen"), None);
    }

    #[test]
    fn a_partial_name_is_not_a_match() {
        let found = vec![dev("Grupo de la casa")];
        assert_eq!(pick(&found, "Grupo"), None);
    }
}
