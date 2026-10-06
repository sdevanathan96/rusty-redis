//! Integer parsing shared by commands and startup flags.

/// An integer argument parsed as Redis's string2ll does: stricter than
/// `str::parse`, with no `+`, no leading zeros, no whitespace and no `-0`.
/// Not for stream ids, which Redis parses leniently.
pub fn strict_i64(raw: &[u8]) -> Option<i64> {
    if raw == b"0" {
        return Some(0);
    }
    let (negative, digits) = match raw.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, raw),
    };
    // A first digit of 1 to 9 rules out empty input, a second sign, `+`,
    // leading zeros, `-0` and leading whitespace.
    if !matches!(digits.first(), Some(b'1'..=b'9')) {
        return None;
    }
    let mut magnitude: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        magnitude = magnitude.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    if negative {
        0i64.checked_sub_unsigned(magnitude)
    } else {
        i64::try_from(magnitude).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::strict_i64;

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
            assert_eq!(
                strict_i64(raw),
                Some(*want),
                "input {:?}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[test]
    fn rejects_what_redis_rejects() {
        let bad: &[&[u8]] = &[
            b"",   // empty
            b"+1", // leading plus
            b"01", // leading zero
            b"00", // leading zeros, value zero
            b"-0", // sign then zero: first digit must be 1..=9
            b"-01",
            b" 1", // leading space
            b"1 ", // trailing space
            b"1\n",
            b"1.0", // not an integer
            b"1e3",
            b"abc",
            b"-", // sign with no digits
            b"--1",
            b"9223372036854775808",     // i64::MAX + 1
            b"-9223372036854775809",    // i64::MIN - 1
            b"99999999999999999999999", // overflows u64 during accumulation
        ];
        for raw in bad {
            assert_eq!(
                strict_i64(raw),
                None,
                "input {:?}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[test]
    fn round_trips_every_boundary_of_the_accumulator() {
        // Either side of both limits.
        for n in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            assert_eq!(strict_i64(n.to_string().as_bytes()), Some(n), "{n}");
        }
    }
}
