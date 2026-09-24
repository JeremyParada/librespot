//! What is playing right now, for a host that wants to show it.
//!
//! The bridge casts to speaker groups, which have no screen; the television it runs on
//! does. The player already announces every track and every pause, so this only keeps
//! the last of each where a host can ask for it.

use std::sync::Mutex;
use std::time::Instant;

use librespot_metadata::audio::UniqueFields;
use librespot_playback::player::{PlayerEvent, PlayerEventChannel};

struct Track {
    title: String,
    artists: String,
    album: String,
    cover: String,
    duration_ms: u32,
}

struct State {
    track: Option<Track>,
    /// Position at `since`; the host is told where it is *now*.
    position_ms: u32,
    since: Option<Instant>,
    playing: bool,
}

const EMPTY: State = State {
    track: None,
    position_ms: 0,
    since: None,
    playing: false,
};

static NOW: Mutex<State> = Mutex::new(EMPTY);

/// The state, even if a panic poisoned the lock: losing the picture for the rest of the
/// process over one bad event would be a silent failure nobody could trace.
fn now() -> std::sync::MutexGuard<'static, State> {
    NOW.lock().unwrap_or_else(|e| e.into_inner())
}

/// Follows the player until it goes away.
pub async fn follow(mut events: PlayerEventChannel) {
    while let Some(event) = events.recv().await {
        apply(event);
    }
    clear();
}

fn apply(event: PlayerEvent) {
    let mut now = now();
    match event {
        PlayerEvent::TrackChanged { audio_item } => {
            let (artists, album) = match &audio_item.unique_fields {
                UniqueFields::Track { artists, album, .. } => (
                    artists
                        .iter()
                        .map(|a| a.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    album.clone(),
                ),
                UniqueFields::Local { artists, album, .. } => (
                    artists.clone().unwrap_or_default(),
                    album.clone().unwrap_or_default(),
                ),
                UniqueFields::Episode { show_name, .. } => (show_name.clone(), String::new()),
            };
            // The largest: it fills most of a television.
            let cover = audio_item
                .covers
                .iter()
                .max_by_key(|c| c.width)
                .map(|c| c.url.clone())
                .unwrap_or_default();
            // A new track starts at its beginning. Without this, during a crossfade the
            // new title showed with the old track's position until the next event.
            now.position_ms = 0;
            now.since = Some(Instant::now());
            now.track = Some(Track {
                title: audio_item.name.clone(),
                artists,
                album,
                cover,
                duration_ms: audio_item.duration_ms,
            });
        }
        PlayerEvent::Playing { position_ms, .. } => {
            now.position_ms = position_ms;
            now.since = Some(Instant::now());
            now.playing = true;
        }
        PlayerEvent::Paused { position_ms, .. } => {
            now.position_ms = position_ms;
            now.since = Some(Instant::now());
            now.playing = false;
        }
        PlayerEvent::PositionCorrection { position_ms, .. }
        | PlayerEvent::Seeked { position_ms, .. } => {
            now.position_ms = position_ms;
            now.since = Some(Instant::now());
        }
        PlayerEvent::Stopped { .. } => *now = EMPTY,
        _ => {}
    }
}

/// Forgets the track, so a stopped bridge does not keep showing the last one.
pub fn clear() {
    *now() = EMPTY;
}

/// Seven lines -- title, artists, album, cover URL, duration ms, position ms, playing
/// (1/0) -- or `None` when nothing is loaded. Lines rather than JSON: the host is a
/// dependency-free APK, and no field can hold a newline once cleaned.
pub fn snapshot() -> Option<String> {
    let now = now();
    let track = now.track.as_ref()?;
    let elapsed = match (now.playing, now.since) {
        (true, Some(t)) => u32::try_from(t.elapsed().as_millis()).unwrap_or(u32::MAX),
        _ => 0,
    };
    let position = now
        .position_ms
        .saturating_add(elapsed)
        .min(track.duration_ms);
    let clean = |s: &str| s.replace(['\n', '\r'], " ");
    Some(format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}",
        clean(&track.title),
        clean(&track.artists),
        clean(&track.album),
        clean(&track.cover),
        track.duration_ms,
        position,
        u8::from(now.playing),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_loaded_means_nothing_to_show() {
        clear();
        assert!(snapshot().is_none());
    }
}
