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
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::thread;
use std::time::Duration;

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
    let addr: SocketAddr = format!("{host}:{port}").parse().ok()?;
    let sock = UdpSocket::bind(("0.0.0.0", 0)).ok()?;
    sock.connect(addr).ok()?;
    sock.local_addr().ok().map(|a| a.ip())
}

/// Casts once and stays attached until the connection drops, answering heartbeats.
fn attach(device: &Device, stream_port: u16) -> Result<(), Box<dyn std::error::Error>> {
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

    loop {
        match cast.receive() {
            Ok(ChannelMessage::Heartbeat(_)) => cast.heartbeat.pong()?,
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
    }
}

fn run(target: String, stream_port: u16) {
    loop {
        match discover() {
            Ok(found) => match pick(&found, &target) {
                Some(device) => {
                    if let Err(e) = attach(device, stream_port) {
                        warn!("Cast session to <{target}> ended: {e}");
                    }
                }
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
        thread::sleep(RETRY_DELAY);
    }
}

/// Starts casting in the background. Never blocks startup: if the group is off or the
/// network is not up yet, it keeps retrying while the stream stays served.
pub fn spawn(target: String, stream_port: u16) {
    thread::spawn(move || run(target, stream_port));
}

#[cfg(test)]
mod tests {
    use super::*;

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
