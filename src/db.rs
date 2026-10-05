mod stream;

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::db::stream::Stream;
pub use crate::db::stream::{
    EntryId, IdSpec, Mode, ReadFrom, RefPolicy, StreamEntry, Trim, TrimBy, XaddError,
};
use crate::int::strict_i64;

#[cfg(test)]
use std::sync::Mutex;

pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_millis() as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrongType;

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

struct Entry {
    data: Data,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        match self.expires_at {
            Some(deadline) => now >= deadline,
            None => false,
        }
    }
}

/// A string's value, stored the way Redis encodes it. `Integer` holds only what
/// `strict_i64` accepts, which is the canonical form, so turning it back into
/// bytes gives exactly what the client wrote: `010` stays `Raw`.
#[derive(Debug, Clone)]
enum Str {
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

// private: the storage
#[derive(Debug, Clone)]
enum Data {
    String(Str),
    List(VecDeque<Bytes>),
    Stream(Stream),
}

impl Data {
    // One line each, and they live next to the enum rather than being spelled
    // out inside six nearly identical Db methods. A new variant adds one of
    // these, not two Db methods.
    fn as_str(&self) -> Result<&Str, WrongType> {
        match self {
            Data::String(s) => Ok(s),
            _ => Err(WrongType),
        }
    }
    fn as_integer_mut(&mut self) -> Result<&mut i64, IncrError> {
        match self {
            Data::String(Str::Integer(n)) => Ok(n),
            Data::String(Str::Raw(_)) => Err(IncrError::NotAnInteger),
            _ => Err(IncrError::WrongType),
        }
    }
    fn as_list(&self) -> Result<&VecDeque<Bytes>, WrongType> {
        match self {
            Data::List(l) => Ok(l),
            _ => Err(WrongType),
        }
    }
    fn as_stream(&self) -> Result<&Stream, WrongType> {
        match self {
            Data::Stream(s) => Ok(s),
            _ => Err(WrongType),
        }
    }
    fn as_stream_mut(&mut self) -> Result<&mut Stream, WrongType> {
        match self {
            Data::Stream(s) => Ok(s),
            _ => Err(WrongType),
        }
    }
}

// public: the tag only
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    List,
    Stream,
}

impl DataType {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            DataType::String => b"string",
            DataType::List => b"list",
            DataType::Stream => b"stream",
        }
    }
}

/// Which end of a list an operation acts on. Shared by push, pop, and LMOVE,
/// which takes two of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum End {
    Left,
    Right,
}

impl Data {
    fn kind(&self) -> DataType {
        match self {
            Data::String(_) => DataType::String,
            Data::List(_) => DataType::List,
            Data::Stream(_) => DataType::Stream,
        }
    }
}

pub struct Db {
    map: HashMap<Bytes, Entry>,
    clock: Arc<dyn Clock>,
}

impl Db {
    /// Drop the key if its deadline has passed.
    ///
    /// Split out from reading on purpose. Reaping needs `&mut self`, so while
    /// the two were fused every accessor took `&mut self` and could not lend
    /// anything past the call. That is the only reason `xrange` cloned every
    /// entry it returned.
    fn reap(&mut self, key: &[u8]) {
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
    }

    /// The stored value, if the key is live. Reap first; every public method
    /// below does so on its first line.
    fn data(&self, key: &[u8]) -> Option<&Data> {
        self.map.get(key).map(|e| &e.data)
    }

    fn data_mut(&mut self, key: &[u8]) -> Option<&mut Data> {
        self.map.get_mut(key).map(|e| &mut e.data)
    }

    pub fn type_of(&mut self, key: &[u8]) -> Option<DataType> {
        self.reap(key);
        self.map.get(key).map(|e| e.data.kind())
    }

    pub fn new() -> Self {
        Db::with_clock(Arc::new(SystemClock))
    }

    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Db {
            map: HashMap::new(),
            clock,
        }
    }

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

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let now = self.clock.now();
        // An expired entry counts as absent, so removing it returns false.
        match self.map.remove(key) {
            Some(entry) => !entry.is_expired(now),
            None => false,
        }
    }

    /// Counts entries still in the map, including ones past their deadline that
    /// have not been lazily reaped. Debug and test use only.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn exists(&mut self, key: &[u8]) -> bool {
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
        self.map.contains_key(key)
    }

    pub fn llen(&mut self, key: &[u8]) -> Result<usize, WrongType> {
        self.reap(key);
        match self.data(key) {
            None => Ok(0),
            Some(d) => Ok(d.as_list()?.len()),
        }
    }

    pub fn lrange(&mut self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>, WrongType> {
        self.reap(key);
        self.data(key).map_or(Ok(Vec::new()), |d| {
            let l = d.as_list()?;
            Ok(match resolve_range(l.len(), start, stop) {
                Some((from, to)) => l.range(from..to).cloned().collect(),
                None => Vec::new(),
            })
        })
    }

    pub fn push(&mut self, key: Bytes, values: Vec<Bytes>, end: End) -> Result<usize, WrongType> {
        self.reap(&key);
        push_to(&mut self.map, key, values, end)
    }

    pub fn pop(
        &mut self,
        key: &[u8],
        count: Option<usize>,
        end: End,
    ) -> Result<Option<Vec<Bytes>>, WrongType> {
        self.reap(key);
        pop_from(&mut self.map, key, count.unwrap_or(1), end)
    }

    pub fn lmove(
        &mut self,
        src: &[u8],
        dst: Bytes,
        from: End,
        to: End,
    ) -> Result<Option<Bytes>, WrongType> {
        self.reap(src);
        if self
            .data(src)
            .map(Data::as_list)
            .transpose()?
            .is_none_or(|l| l.is_empty())
        {
            return Ok(None);
        }
        self.reap(&dst);
        let _ = self.data(&dst).map(Data::as_list).transpose()?;

        let popped = match pop_from(&mut self.map, src, 1, from)? {
            None => return Ok(None),
            Some(v) => match v.into_iter().next() {
                None => return Ok(None),
                Some(value) => value,
            },
        };

        push_to(&mut self.map, dst, vec![popped.clone()], to)?;
        Ok(Some(popped))
    }

    /// Appends, then trims, in that order, so `MAXLEN 0` removes the entry just
    /// added. `None` means NOMKSTREAM found no key, and nothing was created. A
    /// failed append returns before anything is trimmed.
    pub fn xadd(
        &mut self,
        key: Bytes,
        spec: IdSpec,
        fields: Vec<(Bytes, Bytes)>,
        trim: Option<Trim>,
        nomkstream: bool,
    ) -> Result<Option<EntryId>, XaddError> {
        self.reap(&key);
        let now_ms = self.clock.now_ms();

        // Before or_insert_with, which would create the key.
        if nomkstream && !self.map.contains_key(&key) {
            return Ok(None);
        }

        let entry = self.map.entry(key).or_insert_with(|| Entry {
            data: Data::Stream(Stream::default()),
            expires_at: None,
        });

        let stream = match &mut entry.data {
            Data::Stream(s) => s,
            _ => return Err(XaddError::WrongType),
        };

        let entry_id = match spec {
            IdSpec::Explicit(id) => stream.append(id, fields),
            IdSpec::AutoSeq(ms) => stream.append_auto_seq(ms, fields),
            IdSpec::Auto => stream.append_auto(now_ms, fields),
        }?;
        if let Some(t) = &trim {
            stream.trim(t);
        }
        Ok(Some(entry_id))
    }

    pub fn xlen(&mut self, key: &[u8]) -> Result<usize, WrongType> {
        self.reap(key);
        self.data(key).map_or(Ok(0), |s| Ok(s.as_stream()?.len()))
    }

    pub fn xrange(
        &mut self,
        key: &[u8],
        start: EntryId,
        end: EntryId,
        count: Option<usize>,
    ) -> Result<Option<&[StreamEntry]>, WrongType> {
        self.reap(key);
        match self.data(key) {
            None => Ok(None),
            Some(d) => Ok(Some(d.as_stream()?.range(start, end, count))),
        }
    }

    pub fn stream_last_id(&mut self, key: &[u8]) -> Result<EntryId, WrongType> {
        self.reap(key);
        self.data(key).map_or(Ok(EntryId { ms: 0, seq: 0 }), |s| {
            let stream = s.as_stream()?;
            Ok(stream.last_id())
        })
    }

    pub fn xlast(&mut self, key: &[u8]) -> Result<Option<&[StreamEntry]>, WrongType> {
        self.reap(key);
        self.data(key)
            .map(|d| Ok(d.as_stream()?.last_slice()))
            .transpose()
    }

    pub fn xrange_after(
        &mut self,
        key: &[u8],
        after: EntryId,
        count: Option<usize>,
    ) -> Result<Option<&[StreamEntry]>, WrongType> {
        self.reap(key);
        match self.data(key) {
            None => Ok(None),
            Some(d) => Ok(Some(d.as_stream()?.range_after(after, count))),
        }
    }

    pub fn xdel(&mut self, key: &[u8], ids: &[EntryId]) -> Result<usize, WrongType> {
        self.reap(key);
        let stream = match self.data_mut(key) {
            None => return Ok(0),
            Some(d) => d.as_stream_mut()?,
        };
        let mut n = 0usize;
        for &id in ids {
            n += stream.delete(id);
        }
        Ok(n)
    }

    /// A missing key is 0 and stays missing. An emptied stream keeps its key
    /// and its last id, as in Redis.
    pub fn xtrim(&mut self, key: &[u8], trim: &Trim) -> Result<usize, WrongType> {
        self.reap(key);
        let stream = match self.data_mut(key) {
            None => return Ok(0),
            Some(d) => d.as_stream_mut()?,
        };
        Ok(stream.trim(trim))
    }
}

/// Removes the entry at `key` if its deadline has passed. Every accessor calls
/// this first, so a write path never appends to a stale value.
fn reap_if_expired(map: &mut HashMap<Bytes, Entry>, key: &[u8], now: Instant) {
    let expired = map.get(key).is_some_and(|e| e.is_expired(now));
    if expired {
        map.remove(key);
    }
}

/// Resolves LRANGE style indexes to a half open range into a list of `len`.
/// Returns None when the range is empty.
fn resolve_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let len = len as i64;

    // negative counts from the end
    let mut start = if start < 0 { len + start } else { start };
    let mut stop = if stop < 0 { len + stop } else { stop };

    // clamp, do not wrap
    if start < 0 {
        start = 0;
    }
    if stop >= len {
        stop = len - 1;
    }

    if start > stop || start >= len || len == 0 {
        return None;
    }
    Some((start as usize, stop as usize + 1))
}

fn pop_from(
    map: &mut HashMap<Bytes, Entry>,
    key: &[u8],
    count: usize,
    end: End,
) -> Result<Option<Vec<Bytes>>, WrongType> {
    let (popped, now_empty) = match map.get_mut(key) {
        None => return Ok(None),
        Some(entry) => match &mut entry.data {
            Data::List(l) => {
                let n = count.min(l.len());
                let out: Vec<_> = match end {
                    End::Left => l.drain(..n).collect(),
                    End::Right => {
                        let start = l.len() - n;
                        let mut tail: Vec<_> = l.drain(start..).collect();
                        tail.reverse();
                        tail
                    }
                };
                (out, l.is_empty())
            }
            _ => return Err(WrongType),
        },
    };

    if now_empty {
        map.remove(key);
    }
    Ok(Some(popped))
}

fn push_to(
    map: &mut HashMap<Bytes, Entry>,
    key: Bytes,
    values: Vec<Bytes>,
    end: End,
) -> Result<usize, WrongType> {
    let entry = map.entry(key).or_insert_with(|| Entry {
        data: Data::List(VecDeque::new()),
        expires_at: None,
    });

    match &mut entry.data {
        Data::List(l) => {
            match end {
                End::Right => l.extend(values),
                // LPUSH inserts one at a time at the head, so arguments end up reversed
                End::Left => {
                    for v in values {
                        l.push_front(v);
                    }
                }
            }
            Ok(l.len())
        }
        _ => Err(WrongType),
    }
}

impl Default for Db {
    fn default() -> Self {
        Db::new()
    }
}

/// One offset, read by both clocks, so a test can never move monotonic time
/// while stream ids stay frozen, or the reverse. The wall clock starts at a
/// plausible time rather than zero, because behavior that depends on how close
/// `mstime()` is to `i64::MAX`, such as the PX overflow in `Db::set`, cannot
/// happen at epoch zero.
#[cfg(test)]
pub struct TestClock {
    start: Instant,
    base_ms: u64,
    offset: Mutex<Duration>,
}

#[cfg(test)]
impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        TestClock {
            start: Instant::now(),
            // A plausible wall clock rather than zero.
            base_ms: 1_700_000_000_000,
            offset: Mutex::new(Duration::ZERO),
        }
    }

    pub fn advance(&self, d: Duration) {
        *self.offset.lock().unwrap() += d;
    }

    /// Kept for callers that think in milliseconds. It advances both readings,
    /// because there is only one now.
    pub fn advance_ms(&self, ms: u64) {
        self.advance(Duration::from_millis(ms));
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.start + *self.offset.lock().unwrap()
    }

    fn now_ms(&self) -> u64 {
        self.base_ms + self.offset.lock().unwrap().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{Clock, Db, End, IncrError, InvalidExpireTime, TestClock};
    use std::sync::Arc;
    use std::time::Duration;

    fn fixture() -> (Arc<TestClock>, Db) {
        let clock = Arc::new(TestClock::new());
        let db = Db::with_clock(clock.clone());
        (clock, db)
    }

    #[test]
    fn test_clock_advances() {
        let clock = TestClock::new();
        let t1 = clock.now();
        clock.advance(Duration::from_secs(1));
        assert!(clock.now() > t1);
    }

    #[test]
    fn set_then_get() {
        let (_clock, mut db) = fixture();
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None)
            .unwrap();
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));
        assert_eq!(db.get(b"missing"), Ok(None));
    }

    #[test]
    fn key_expires_exactly_at_deadline() {
        let (clock, mut db) = fixture();
        db.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(100)),
        )
        .unwrap();

        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));

        clock.advance(Duration::from_millis(99));
        assert_eq!(
            db.get(b"k"),
            Ok(Some(Bytes::from_static(b"v"))),
            "not yet expired"
        );

        clock.advance(Duration::from_millis(1));
        assert_eq!(
            db.get(b"k"),
            Ok(None),
            "expired at the deadline, not after it"
        );
    }

    #[test]
    fn no_expiry_never_expires() {
        let (clock, mut db) = fixture();
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None)
            .unwrap();
        clock.advance(Duration::from_secs(86_400 * 365));
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));
    }

    #[test]
    fn get_reaps_the_expired_entry() {
        let (clock, mut db) = fixture();
        db.set(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(100)),
        )
        .unwrap();
        db.set(
            Bytes::from_static(b"b"),
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(50)),
        )
        .unwrap();
        assert_eq!(db.len(), 2);

        clock.advance(Duration::from_millis(99));
        assert_eq!(db.get(b"b"), Ok(None));
        assert_eq!(db.len(), 1, "get must remove, not just hide");

        clock.advance(Duration::from_millis(1));
        assert_eq!(db.get(b"a"), Ok(None));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn delete_reports_false_for_expired_key() {
        let (clock, mut db) = fixture();
        db.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(10)),
        )
        .unwrap();
        clock.advance(Duration::from_millis(10));
        assert!(!db.delete(b"k"), "expired key counts as absent");
        assert_eq!(db.len(), 0, "but it is still removed");
    }

    #[test]
    fn overwriting_clears_the_old_ttl() {
        let (clock, mut db) = fixture();
        db.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v1"),
            Some(Duration::from_millis(10)),
        )
        .unwrap();
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v2"), None)
            .unwrap();
        clock.advance(Duration::from_secs(1));
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v2"))));
    }

    #[test]
    fn px_that_overflows_the_redis_deadline_is_rejected() {
        // Redis replies "invalid expire time" because mstime() + i64::MAX
        // overflows, and it bails out before touching the key.
        let clock = Arc::new(TestClock::new());
        let mut db = Db::with_clock(clock.clone());
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"old"), None)
            .unwrap();

        let result = db.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(i64::MAX as u64)),
        );

        assert_eq!(result, Err(InvalidExpireTime));
        assert_eq!(
            db.get(b"k"),
            Ok(Some(Bytes::from_static(b"old"))),
            "existing value untouched"
        );
    }

    #[test]
    fn px_just_under_the_boundary_still_lives() {
        // The boundary is i64::MAX minus the current wall clock, so an
        // argument a little below it must survive. This is the assertion that
        // fails if TestClock ever goes back to a zero wall clock.
        let clock = Arc::new(TestClock::new());
        let mut db = Db::with_clock(clock.clone());
        let just_under = i64::MAX as u64 - clock.now_ms() - 1;

        db.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(just_under)),
        )
        .unwrap();

        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));
    }

    fn set(db: &mut Db, key: &'static [u8], value: &'static [u8]) {
        db.set(Bytes::from_static(key), Bytes::from_static(value), None)
            .unwrap();
    }

    #[test]
    fn get_returns_a_number_exactly_as_it_was_set() {
        let (_clock, mut db) = fixture();
        for value in [
            &b"5"[..],
            b"-7",
            b"0",
            b"9223372036854775807",
            b"010",
            b"+5",
            b"-0",
            b"hello",
        ] {
            db.set(
                Bytes::from_static(b"k"),
                Bytes::copy_from_slice(value),
                None,
            )
            .unwrap();
            assert_eq!(
                db.get(b"k"),
                Ok(Some(Bytes::copy_from_slice(value))),
                "{value:?}"
            );
        }
    }

    #[test]
    fn incr_on_a_missing_key_gives_one() {
        let (_clock, mut db) = fixture();
        assert_eq!(db.incr(Bytes::from_static(b"k")), Ok(1));
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"1"))));
    }

    #[test]
    fn incr_adds_to_a_number_written_by_set() {
        let (_clock, mut db) = fixture();
        set(&mut db, b"k", b"5");
        assert_eq!(db.incr(Bytes::from_static(b"k")), Ok(6));
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"6"))));
    }

    #[test]
    fn incr_on_a_non_canonical_number_is_not_an_integer() {
        let (_clock, mut db) = fixture();
        for value in [&b"hello"[..], b"010", b"+5", b""] {
            db.set(
                Bytes::from_static(b"k"),
                Bytes::copy_from_slice(value),
                None,
            )
            .unwrap();
            assert_eq!(
                db.incr(Bytes::from_static(b"k")),
                Err(IncrError::NotAnInteger),
                "{value:?}"
            );
        }
    }

    #[test]
    fn incr_on_a_list_is_wrongtype() {
        let (_clock, mut db) = fixture();
        db.push(
            Bytes::from_static(b"l"),
            vec![Bytes::from_static(b"a")],
            End::Right,
        )
        .unwrap();
        assert_eq!(db.incr(Bytes::from_static(b"l")), Err(IncrError::WrongType));
    }

    #[test]
    fn incr_at_the_maximum_fails_and_leaves_the_value() {
        let (_clock, mut db) = fixture();
        set(&mut db, b"k", b"9223372036854775807");
        assert_eq!(db.incr(Bytes::from_static(b"k")), Err(IncrError::Overflow));
        assert_eq!(
            db.get(b"k"),
            Ok(Some(Bytes::from_static(b"9223372036854775807")))
        );
    }

    #[test]
    fn incr_keeps_the_ttl() {
        let (clock, mut db) = fixture();
        db.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"5"),
            Some(Duration::from_millis(100)),
        )
        .unwrap();
        assert_eq!(db.incr(Bytes::from_static(b"k")), Ok(6));
        clock.advance(Duration::from_millis(100));
        assert_eq!(
            db.get(b"k"),
            Ok(None),
            "the deadline from SET still applies"
        );
    }
}
