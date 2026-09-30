//! Integer parsing shared by commands and startup flags.

/// Parse an integer command argument or startup flag the way Redis does.
///
/// Redis uses string2ll here, which is stricter than Rust's FromStr in three
/// ways that a client can trip over: no leading `+`, no leading zeros unless
/// the whole argument is exactly `0`, and no surrounding whitespace. `-0` is
/// rejected too, because after the sign the first digit has to be 1 through 9.
///
/// This is only for integer *arguments*: EX, PX, COUNT, BLOCK, LPOP's count,
/// LRANGE's indexes, and numeric startup flags such as `--port`. Stream IDs go
/// through a different function in Redis (string2ull, which falls back to
/// strtoull and is therefore lenient), so do not reuse this for them without
/// checking the oracle first.
///
/// The scenarios in test.sh under "strict integer parsing" are the arbiter for
/// every rule above.
pub fn strict_i64(raw: &[u8]) -> Option<i64> {
    // The one case where a leading zero is legal, and the only way to write
    // zero at all.
    if raw == b"0" {
        return Some(0);
    }

    let (negative, digits) = match raw.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, raw),
    };

    // Requiring 1 through 9 here does most of the work at once: it rejects the
    // empty string, a second sign, `+`, a leading zero, `-0`, leading
    // whitespace, and anything non numeric.
    match digits.first() {
        Some(b'1'..=b'9') => {}
        _ => return None,
    }

    let mut magnitude: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None; // catches trailing junk and embedded whitespace
        }
        magnitude = magnitude
            .checked_mul(10)?
            .checked_add((b - b'0') as u64)?;
    }

    if negative {
        // The range is not symmetric: i64::MIN has magnitude 2^63, one more
        // than i64::MAX. wrapping_neg on the u64 followed by a bit preserving
        // cast lands exactly on i64::MIN for that one value.
        if magnitude > (i64::MAX as u64) + 1 {
            return None;
        }
        Some(magnitude.wrapping_neg() as i64)
    } else {
        if magnitude > i64::MAX as u64 {
            return None;
        }
        Some(magnitude as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::strict_i64;

    /// One row per rule. When the harness disagrees with a row, change the row
    /// and then the parser, in that order, so the rule is always written down
    /// somewhere before it is implemented.
    #[test]
    fn accepts_what_redis_accepts() {
        let good: &[(&[u8], i64)] = &[
            (b"0", 0),
            (b"1", 1),
            (b"-1", -1),
            (b"10", 10),
            (b"-10", -10),
            (b"9223372036854775807", i64::MAX),
            (b"-9223372036854775808", i64::MIN),
        ];
        for (raw, want) in good {
            assert_eq!(strict_i64(raw), Some(*want), "input {:?}", String::from_utf8_lossy(raw));
        }
    }

    #[test]
    fn rejects_what_redis_rejects() {
        let bad: &[&[u8]] = &[
            b"",                        // empty
            b"+1",                      // leading plus
            b"01",                      // leading zero
            b"00",                      // leading zeros, value zero
            b"-0",                      // sign then zero: first digit must be 1..=9
            b"-01",
            b" 1",                      // leading space
            b"1 ",                      // trailing space
            b"1\n",
            b"1.0",                     // not an integer
            b"1e3",
            b"abc",
            b"-",                       // sign with no digits
            b"--1",
            b"9223372036854775808",     // i64::MAX + 1
            b"-9223372036854775809",    // i64::MIN - 1
            b"99999999999999999999999", // overflows u64 during accumulation
        ];
        for raw in bad {
            assert_eq!(strict_i64(raw), None, "input {:?}", String::from_utf8_lossy(raw));
        }
    }

    #[test]
    fn round_trips_every_boundary_of_the_accumulator() {
        // The checked_mul then checked_add pair is the part most likely to be
        // wrong by one, so walk the values either side of the limits.
        for n in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            assert_eq!(strict_i64(n.to_string().as_bytes()), Some(n), "{n}");
        }
    }
}
