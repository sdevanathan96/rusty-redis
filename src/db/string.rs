//! Strings: the `Str` encoding, and SET, GET and INCR.

use std::time::Duration;

use bytes::Bytes;

use super::{Data, Db, Entry, WrongType};
use crate::int::strict_i64;

/// Why INCR failed. A type of its own, so every other accessor goes on
/// returning plain `WrongType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrError {
    WrongType,
    NotAnInteger,
    Overflow,
}

/// The expiry deadline does not fit in Redis's i64 of milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidExpireTime;

/// A string's value, stored the way Redis encodes it. `Integer` holds only what
/// `strict_i64` accepts, which is the canonical form, so turning it back into
/// bytes gives exactly what the client wrote: `010` stays `Raw`.
#[derive(Debug, Clone)]
pub(super) enum Str {
    Raw(Bytes),
    Integer(i64),
}

impl Str {
    fn from_bytes(value: Bytes) -> Self {
        match strict_i64(&value) {
            Some(n) => Str::Integer(n),
            None => Str::Raw(value),
        }
    }

    /// Owned, because an `Integer` has no bytes to lend. Cheap for `Raw`, a
    /// reference count bump; an `Integer` is formatted.
    fn to_bytes(&self) -> Bytes {
        match self {
            Str::Raw(bytes) => bytes.clone(),
            Str::Integer(n) => Bytes::from(n.to_string()),
        }
    }
}

impl Db {
    /// ttl is relative. The absolute deadline is computed here, once, because
    /// this is the only layer that owns a clock. Fails, writing nothing, if
    /// that deadline overflows.
    pub fn set(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), InvalidExpireTime> {
        let now = self.clock.now();
        let now_ms = self.clock.now_ms();
        // Two separate hazards here.
        //
        // First, `now + d` panics: Instant's Add impl is
        // `checked_add(..).expect(..)`, and this runs inside the keyspace
        // task, where a panic takes down every connection in the process.
        //
        // Second, Redis computes the deadline as `mstime() + ms` in a signed
        // 64 bit integer and replies "invalid expire time" when the sum
        // overflows. Duration and Instant have far more headroom than an i64
        // of milliseconds, so that boundary has to be reproduced by hand.
        //
        // Redis detects the overflow with `if (ms <= 0)` after the addition,
        // which relies on signed wraparound, undefined behaviour in C. Some
        // builds (Homebrew on macOS, at least) compile the check out: they
        // reply OK and store a deadline in the past, so the next read finds
        // the key gone. The Linux build keeps the check, and that is what this
        // matches.
        //
        // The boundary depends on the current wall clock, exactly as it
        // does in Redis, so it cannot be pinned to a constant.
        let expires_at = match ttl {
            Some(d) if now_ms as i128 + d.as_millis() as i128 > i64::MAX as i128 => {
                return Err(InvalidExpireTime);
            }
            Some(d) => Some(now.checked_add(d).unwrap_or(now)),
            None => None,
        };
        self.map.insert(
            key,
            Entry {
                data: Data::String(Str::from_bytes(value)),
                expires_at,
            },
        );
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, WrongType> {
        self.reap(key);
        match self.data(key) {
            None => Ok(None),
            Some(d) => Ok(Some(d.as_str()?.to_bytes())),
        }
    }

    /// INCR. A missing key starts from 0, so it becomes 1 with no expiry. An
    /// existing value changes in place, which keeps its TTL, as Redis does.
    pub fn incr(&mut self, key: Bytes) -> Result<i64, IncrError> {
        self.reap(&key);
        match self.data_mut(&key) {
            None => {
                self.map.insert(
                    key,
                    Entry {
                        data: Data::String(Str::Integer(1)),
                        expires_at: None,
                    },
                );
                Ok(1)
            }
            Some(d) => {
                let value = d.as_integer_mut()?;
                *value = value.checked_add(1).ok_or(IncrError::Overflow)?;
                Ok(*value)
            }
        }
    }
}
