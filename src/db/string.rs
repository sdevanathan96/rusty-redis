//! Strings: the `Str` encoding, and SET, GET and INCR.

use std::time::Duration;

use bytes::Bytes;

use super::{Data, Db, Entry, WrongType};
use crate::int::strict_i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrError {
    WrongType,
    NotAnInteger,
    Overflow,
}

/// The expiry deadline does not fit in Redis's i64 of milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidExpireTime;

/// A string, encoded as Redis does. Only canonical integers become `Integer`,
/// so `010` stays `Raw` and reads back exactly as written.
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

    fn to_bytes(&self) -> Bytes {
        match self {
            Str::Raw(bytes) => bytes.clone(),
            Str::Integer(n) => Bytes::from(n.to_string()),
        }
    }
}

impl Db {
    /// Fails, writing nothing, if the deadline overflows Redis's i64 of
    /// milliseconds. Linux Redis rejects that; Homebrew's build compiles the
    /// check out.
    pub fn set(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), InvalidExpireTime> {
        let now = self.clock.now();
        let now_ms = self.clock.now_ms();
        let expires_at = match ttl {
            Some(d) if now_ms as i128 + d.as_millis() as i128 > i64::MAX as i128 => {
                return Err(InvalidExpireTime);
            }
            Some(d) => Some(now.checked_add(d).unwrap_or(now)), // `+` would panic
            None => None,
        };
        self.map.insert(
            key.clone(),
            Entry {
                data: Data::String(Str::from_bytes(value)),
                expires_at,
            },
        );
        self.mark_modified(key);
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, WrongType> {
        self.reap(key);
        match self.data(key) {
            None => Ok(None),
            Some(d) => Ok(Some(d.as_str()?.to_bytes())),
        }
    }

    /// A missing key starts at 0. An existing one changes in place, keeping its
    /// TTL.
    pub fn incr(&mut self, key: Bytes) -> Result<i64, IncrError> {
        self.reap(&key);
        match self.data_mut(&key) {
            None => {
                self.map.insert(
                    key.clone(),
                    Entry {
                        data: Data::String(Str::Integer(1)),
                        expires_at: None,
                    },
                );
                self.mark_modified(key);
                Ok(1)
            }
            Some(d) => {
                let value = d.as_integer_mut()?;
                *value = value.checked_add(1).ok_or(IncrError::Overflow)?;
                let n = *value;
                self.mark_modified(key);
                Ok(n)
            }
        }
    }
}
