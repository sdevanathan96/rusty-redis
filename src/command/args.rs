//! Argument parsers shared across command groups.

use std::time::Duration;

use bytes::Bytes;

use super::{Blocking, CommandError};
use crate::db::End;
use crate::int::strict_i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExpiryUnit {
    Seconds,
    Millis,
}

/// Redis rejects an `EX` whose milliseconds would overflow an i64. `PX`'s
/// limit depends on the wall clock, so `Db::set` checks it.
const MAX_EXPIRE_SECS: i64 = i64::MAX / 1000;

/// The argument after the expiry keyword at `i`.
pub(super) fn expiry_arg(
    rest: &[Bytes],
    i: usize,
    cmd_name: &Bytes,
    unit: ExpiryUnit,
) -> Result<Duration, CommandError> {
    let raw = rest.get(i + 1).ok_or(CommandError::Syntax)?;
    let n = parse_i64(raw)?;
    let too_big = unit == ExpiryUnit::Seconds && n > MAX_EXPIRE_SECS;
    if n <= 0 || too_big {
        return Err(CommandError::InvalidExpiry(cmd_name.clone()));
    }
    Ok(match unit {
        ExpiryUnit::Seconds => Duration::from_secs(n as u64),
        ExpiryUnit::Millis => Duration::from_millis(n as u64),
    })
}

/// A stream id component. Like Redis's strtoull, skips leading whitespace.
pub(super) fn lenient_u64(text: &str) -> Result<u64, CommandError> {
    text.trim_start_matches(|c: char| c.is_ascii_whitespace())
        .parse()
        .map_err(|_| CommandError::InvalidStreamId)
}

pub(super) fn parse_i64(raw: &[u8]) -> Result<i64, CommandError> {
    strict_i64(raw).ok_or(CommandError::NotAnInteger)
}

pub(super) fn parse_end(raw: &[u8]) -> Result<End, CommandError> {
    match raw.to_ascii_uppercase().as_slice() {
        b"LEFT" => Ok(End::Left),
        b"RIGHT" => Ok(End::Right),
        _ => Err(CommandError::Syntax),
    }
}

/// A BLPOP style timeout in seconds, where 0 means forever (`None`). The
/// order of the checks decides Redis's message: `-inf` is "negative", not
/// "out of range".
pub(super) fn parse_timeout(raw: &[u8]) -> Result<Option<Duration>, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::TimeoutNotAFloat)?;
    let seconds: f64 = text.parse().map_err(|_| CommandError::TimeoutNotAFloat)?;
    if seconds.is_nan() {
        return Err(CommandError::TimeoutNotAFloat);
    }
    if seconds < 0.0 {
        return Err(CommandError::TimeoutNegative);
    }
    if seconds == 0.0 {
        return Ok(None);
    }
    // try_ because from_secs_f64 panics on infinity and huge values.
    Duration::try_from_secs_f64(seconds)
        .map(Some)
        .map_err(|_| CommandError::TimeoutOutOfRange)
}

/// An XREAD `BLOCK` in milliseconds, where 0 means forever.
pub(super) fn parse_block(raw: &[u8]) -> Result<Blocking, CommandError> {
    const MAX_BLOCK_MS: i64 = i64::MAX - (1 << 42);
    let millis = strict_i64(raw).ok_or(CommandError::TimeoutNotAnInteger)?;
    if millis < 0 {
        return Err(CommandError::TimeoutNegative);
    }
    if millis > MAX_BLOCK_MS {
        return Err(CommandError::TimeoutOutOfRange);
    }
    Ok(match millis {
        0 => Blocking::Forever,
        ms => Blocking::For(Duration::from_millis(ms as u64)),
    })
}
