//! Codec version schedule: which Phoenix event codec (and decoded schema) version is
//! responsible for decoding a slot. A program upgrade changes the codec version mid-range; the
//! version a row carries (`decoder_version`) and the content hash a revision is keyed by both
//! follow the slot, so a range after an upgrade re-decodes under its new version while earlier
//! rows keep the version that produced them.
//!
//! Today exactly one codec is linked (`phoenix-rise-events` 0.6.12), so the schedule stamps that
//! version on every slot. The audit fixture in `tests/data/codec-audit.json` places two existing
//! captures on either side of an *invented* boundary and stamps the pre side `0.6.11`; that
//! version is only a label, the same linked 0.6.12 codec decodes both sides. Nothing here claims
//! a real historical codec boundary.

use super::schema::VERSION;

/// One boundary: from `slot` (inclusive) the codec version is `version`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Upgrade {
    /// First slot the version covers.
    pub slot: i64,
    /// Codec (and schema) version, e.g. `phoenix-rise-events-0.6.12/schema-1`.
    pub version: &'static str,
}

/// The production schedule: the pinned codec everywhere. No historical boundary is configured
/// because `phoenix-rise-events` 0.6.12 is backward compatible with every event the Phoenix
/// program has emitted so far. When a future program upgrade adds event variants the decoder
/// does not know, the new codec version is added here with its first slot; rows after that slot
/// are then re-decoded (their content hash changes) and re-stamped, while earlier rows keep the
/// version that produced them.
///
/// The audit fixture exercises that mechanism with an *example* boundary whose pre-upgrade
/// `0.6.11` stamp is synthetic: the same linked 0.6.12 codec decodes both sides of the fixture,
/// so the per-slot version is only a stamp there, not a different decoder.
pub const SCHEDULE: &[Upgrade] = &[Upgrade {
    slot: 0,
    version: VERSION,
}];

/// The codec version that decodes `slot` under the production [`SCHEDULE`].
#[must_use]
pub fn codec_version(slot: i64) -> &'static str {
    codec_version_for(SCHEDULE, slot)
}

/// The newest version in `schedule` whose boundary is at or below `slot`.
///
/// An empty schedule, or a schedule whose first boundary is above `slot`, falls back to
/// [`VERSION`] so a slot before any boundary is never silently unversioned.
#[must_use]
pub fn codec_version_for(schedule: &[Upgrade], slot: i64) -> &'static str {
    schedule
        .iter()
        .filter(|upgrade| upgrade.slot <= slot)
        .max_by_key(|upgrade| upgrade.slot)
        .map_or(VERSION, |upgrade| upgrade.version)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD: &str = "phoenix-rise-events-0.6.11/schema-1";

    #[test]
    fn the_production_schedule_stamps_the_pinned_codec_everywhere() {
        assert_eq!(codec_version(0), VERSION);
        assert_eq!(codec_version(123_456), VERSION);
        assert_eq!(codec_version(i64::MAX), VERSION);
        assert_eq!(codec_version(-1), VERSION, "before slot 0 still resolves");
    }

    #[test]
    fn a_schedule_picks_the_newest_boundary_at_or_below_the_slot() {
        let schedule = [
            Upgrade {
                slot: 0,
                version: OLD,
            },
            Upgrade {
                slot: 428_000_000,
                version: VERSION,
            },
        ];
        assert_eq!(codec_version_for(&schedule, 0), OLD);
        assert_eq!(codec_version_for(&schedule, 427_999_999), OLD);
        assert_eq!(codec_version_for(&schedule, 428_000_000), VERSION);
        assert_eq!(codec_version_for(&schedule, i64::MAX), VERSION);
    }

    #[test]
    fn an_empty_schedule_or_a_boundary_above_the_slot_falls_back_to_the_pinned_codec() {
        assert_eq!(codec_version_for(&[], 1), VERSION);
        assert_eq!(
            codec_version_for(
                &[Upgrade {
                    slot: 100,
                    version: OLD
                }],
                99
            ),
            VERSION
        );
    }
}
