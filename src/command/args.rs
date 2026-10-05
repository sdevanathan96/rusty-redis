//! Argument parsers shared across command groups: integers, list ends,
//! expiries and the two kinds of blocking timeout.

use std::time::Duration;

use bytes::Bytes;

use super::{Blocking, CommandError};
use crate::db::End;
use crate::int::strict_i64;

/// Which unit an expiry keyword is expressed in. `EX` is seconds, `PX` is
/// milliseconds. The unit matters for more than the multiplication: Redis
/// applies an upper bound to `EX` that it does not apply to `PX`, because it
/// converts seconds to milliseconds and refuses to overflow doing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExpiryUnit {
    Seconds,
    Millis,
}

/// The largest `EX` argument Redis accepts, from its own check
/// `milliseconds > LLONG_MAX / 1000`.
///
/// VERIFY on 6380 before trusting this:
///   SET k v EX 9223372036854775807   (expected: invalid expire time)
///
/// PX gets no bound here. Its limit is i64::MAX minus the wall clock, which
/// only `Db::set` can see.
const MAX_EXPIRE_SECS: i64 = i64::MAX / 1000;

/// Reads the argument following an expiry keyword at index `i` and converts it
/// to a `Duration`.
///
///   missing argument    -> Syntax
///   not a number        -> NotAnInteger
///   zero or negative    -> InvalidExpiry
///   too large for EX    -> InvalidExpiry
pub(super) fn expiry_arg(
    rest: &[Bytes],
    i: usize,
    cmd_name: &Bytes,
    unit: ExpiryUnit,
) -> Result<Duration, CommandError> {
    let raw = rest.get(i + 1).ok_or(CommandError::Syntax)?;
    let n = parse_i64(raw)?;
    if n <= 0 {
        return Err(CommandError::InvalidExpiry(cmd_name.clone()));
    }
    match unit {
        ExpiryUnit::Seconds => {
            if n > MAX_EXPIRE_SECS {
                return Err(CommandError::InvalidExpiry(cmd_name.clone()));
            }
            Ok(Duration::from_secs(n as u64))
        }
        // No bound here on purpose: the real limit depends on the wall clock,
        // so `Db::set` enforces it.
        ExpiryUnit::Millis => Ok(Duration::from_millis(n as u64)),
    }
}

/// Redis parses stream id components with string2ull, which falls back to
/// strtoull and therefore skips leading whitespace, while accepting a leading
/// plus and leading zeros. Rust's parse accepts the plus and the zeros but not
/// the whitespace, so trim and delegate.
///
/// Trailing whitespace stays rejected on both sides: string2ull requires the
/// end pointer to reach the terminator, and Rust's parse rejects it outright.
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

/// A BLPOP style timeout: a float number of seconds, where zero means block
/// forever. That is why the success type is `Option<Duration>` and not
/// `Duration`.
///
/// Three separate rejections with three different Redis messages, and the order
/// of the checks is what decides which message a value gets. `-inf` is the
/// interesting one: Redis parses it, multiplies by 1000, finds it is not above
/// LLONG_MAX, casts it, and only then sees a negative number, so it reports
/// "timeout is negative" rather than out of range. Checking negative before
/// range here reproduces that.
///
/// VERIFY the whole table on 6380: nan, inf, -inf, 1e300, -1, -0, abc.
pub(super) fn parse_timeout(raw: &[u8]) -> Result<Option<Duration>, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::TimeoutNotAFloat)?;
    let seconds: f64 = text.parse().map_err(|_| CommandError::TimeoutNotAFloat)?;

    // NaN first, because it compares false against every bound below. Left to
    // fall through it would reach the constructor, and the panicking
    // constructor treats NaN as a panic, not an error.
    if seconds.is_nan() {
        return Err(CommandError::TimeoutNotAFloat);
    }
    if seconds < 0.0 {
        return Err(CommandError::TimeoutNegative);
    }
    if seconds == 0.0 {
        return Ok(None); // block forever
    }

    // try_from_secs_f64, not from_secs_f64. The latter panics on infinity and
    // on anything too large for a Duration, which means `BLPOP k 1e300` would
    // take down the connection task instead of producing an error reply.
    Duration::try_from_secs_f64(seconds)
        .map(Some)
        .map_err(|_| CommandError::TimeoutOutOfRange)
}

/// An XREAD style `BLOCK` argument: whole milliseconds, where zero means block
/// forever.
pub(super) fn parse_block(raw: &[u8]) -> Result<Blocking, CommandError> {
    let millis = strict_i64(raw).ok_or(CommandError::TimeoutNotAnInteger)?;
    const MAX_BLOCK_MS: i64 = i64::MAX - (1 << 42);
    if millis < 0 {
        return Err(CommandError::TimeoutNegative);
    }
    if millis > MAX_BLOCK_MS {
        return Err(CommandError::TimeoutOutOfRange);
    }
    Ok(if millis == 0 {
        Blocking::Forever
    } else {
        Blocking::For(Duration::from_millis(millis as u64))
    })
}
