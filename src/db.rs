mod stream;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::u64;
use bytes::Bytes;

use crate::db::stream::{Stream};
pub use crate::db::stream::{EntryId, XaddError, IdSpec, ReadFrom, StreamEntry};

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

// private: the storage
#[derive(Debug, Clone)]
enum Data {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(Stream),
}

impl Data {
    // One line each, and they live next to the enum rather than being spelled
    // out inside six nearly identical Db methods. A new variant adds one of
    // these, not two Db methods.
    fn as_string(&self) -> Result<&Bytes, WrongType> {
        match self { Data::String(s) => Ok(s), _ => Err(WrongType) }
    }
    fn as_string_mut(&mut self) -> Result<&mut Bytes, WrongType> {
        match self { Data::String(s) => Ok(s), _ => Err(WrongType) }
    }
    fn as_list(&self) -> Result<&VecDeque<Bytes>, WrongType> {
        match self { Data::List(l) => Ok(l), _ => Err(WrongType) }
    }
    fn as_list_mut(&mut self) -> Result<&mut VecDeque<Bytes>, WrongType> {
        match self { Data::List(l) => Ok(l), _ => Err(WrongType) }
    }
    fn as_stream(&self) -> Result<&Stream, WrongType> {
        match self { Data::Stream(s) => Ok(s), _ => Err(WrongType) }
    }
    fn as_stream_mut(&mut self) -> Result<&mut Stream, WrongType> {
        match self { Data::Stream(s) => Ok(s), _ => Err(WrongType) }
    }
}

// public: the tag only
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType { String, List, Stream }

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
    /// this is the only layer that owns a clock.
    pub fn set(&mut self, key: Bytes, value: Bytes, ttl: Option<Duration>) {
        let now = self.clock.now();
        let now_ms = self.clock.now_ms();
        let entry = Entry {
            data: Data::String(value),
            // Two separate hazards here.
            //
            // First, `now + d` panics: Instant's Add impl is
            // `checked_add(..).expect(..)`, and this runs inside the keyspace
            // task, where a panic takes down every connection in the process.
            //
            // Second, and this is what the harness caught: Redis computes the
            // deadline as `mstime() + ms` in a signed 64 bit integer with no
            // overflow check, so `SET k v PX 9223372036854775807` wraps to a
            // moment in the past. The command replies OK, the key is written,
            // and the very next read finds it already expired. Duration and
            // Instant have far more headroom than an i64 of milliseconds, so
            // without reproducing that boundary the key would instead live for
            // 292 million years. Folding an unrepresentable deadline to "never
            // expires" was the wrong direction; it folds to "already expired".
            //
            // The boundary depends on the current wall clock, exactly as it
            // does in Redis, so it cannot be pinned to a constant.
            expires_at: ttl.map(|d| {
                if now_ms as i128 + d.as_millis() as i128 > i64::MAX as i128 {
                    now // is_expired is `now >= deadline`, so this is already dead
                } else {
                    now.checked_add(d).unwrap_or(now)
                }
            }),
        };
        self.map.insert(key, entry);
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, WrongType>{
        self.reap(key);
        match self.data(key) {
            None => Ok(None),
            Some(d) => Ok(Some(d.as_string()?.clone())),
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

    pub fn pop(&mut self, key: &[u8], count: Option<usize>, end: End) -> Result<Option<Vec<Bytes>>, WrongType>{
        self.reap(key);
        pop_from(&mut self.map, key, count.unwrap_or(1), end)
    }


    pub fn lmove(&mut self, src: &[u8], dst: Bytes, from: End, to: End)
    -> Result<Option<Bytes>, WrongType>
    {
        self.reap(src);
        // self.reap(src);
        // let src_ready = match self.data(src) {
        //     None => false,
        //     Some(d) => !d.as_list()?.is_empty(),
        // };
        // if !src_ready {
        //     return Ok(None);
        // }
        if self.data(src).map(Data::as_list).transpose()?.map_or(true, |l| l.is_empty()) {
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


    pub fn xadd(&mut self, key: Bytes, spec: IdSpec, fields: Vec<(Bytes, Bytes)>) -> Result<EntryId, XaddError> {
        // reap_if_expired
        // get or create Data::Stream
        // resolve IdSpec::Auto using self.clock
        // delegate to Stream::append or append_auto_seq
        self.reap(&key);
        let now_ms = self.clock.now_ms();

        let entry = self.map.entry(key).or_insert_with(|| Entry {
            data: Data::Stream(Stream::default()),
            expires_at: None,
        });

        let stream = match &mut entry.data {
            Data::Stream(s) => s,
            _ => return Err(XaddError::WrongType),
        };

        match spec {
            IdSpec::Explicit(id) => stream.append(id, fields),
            IdSpec::AutoSeq(ms) => stream.append_auto_seq(ms, fields),
            IdSpec::Auto => stream.append_auto(now_ms, fields),
        }
    }

    pub fn xlen(&mut self, key: &[u8]) -> Result<usize, WrongType> {
        self.reap(key);
        self.data(key).map_or(Ok(0), |s| Ok(s.as_stream()?.len()))
    }

    pub fn xrange(&mut self, key: &[u8], start: EntryId, end: EntryId, count: Option<usize>)
    -> Result<Option<&[StreamEntry]>, WrongType> {
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
        self.data(key).map(|d| Ok(d.as_stream()?.last_slice())).transpose()
    }

    pub fn xrange_after(&mut self, key: &[u8], after: EntryId, count: Option<usize>)
    -> Result<Option<&[StreamEntry]>, WrongType>
    {
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
            n+=stream.delete(id);
        }
        Ok(n)
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
    let mut stop  = if stop  < 0 { len + stop  } else { stop  };

    // clamp, do not wrap
    if start < 0 { start = 0; }
    if stop >= len { stop = len - 1; }

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

/// One offset, read by both clocks.
///
/// The previous version kept `now` and `base_ms` as independent counters, so a
/// test could advance monotonic time while stream ids stayed frozen, or the
/// reverse. It also started the wall clock at zero, which hides every behavior
/// that depends on how close `mstime()` is to `i64::MAX`, including the PX
/// overflow in `Db::set`: at epoch zero no i64 argument can push the sum past
/// the limit, so the case is untestable.
#[cfg(test)]
pub struct TestClock {
    start: Instant,
    base_ms: u64,
    offset: Mutex<Duration>,
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

use super::{Clock, Db, TestClock};
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
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None);
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));
        assert_eq!(db.get(b"missing"), Ok(None));
    }

    #[test]
    fn key_expires_exactly_at_deadline() {
        let (clock, mut db) = fixture();
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), Some(Duration::from_millis(100)));

        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));

        clock.advance(Duration::from_millis(99));
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))), "not yet expired");

        clock.advance(Duration::from_millis(1));
        assert_eq!(db.get(b"k"), Ok(None), "expired at the deadline, not after it");
    }

    #[test]
    fn no_expiry_never_expires() {
        let (clock, mut db) = fixture();
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None);
        clock.advance(Duration::from_secs(86_400 * 365));
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));
    }

    #[test]
    fn get_reaps_the_expired_entry() {
        let (clock, mut db) = fixture();
        db.set(Bytes::from_static(b"a"), Bytes::from_static(b"v"), Some(Duration::from_millis(100)));
        db.set(Bytes::from_static(b"b"), Bytes::from_static(b"v"), Some(Duration::from_millis(50)));
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
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), Some(Duration::from_millis(10)));
        clock.advance(Duration::from_millis(10));
        assert!(!db.delete(b"k"), "expired key counts as absent");
        assert_eq!(db.len(), 0, "but it is still removed");
    }

    #[test]
    fn overwriting_clears_the_old_ttl() {
        let (clock, mut db) = fixture();
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v1"), Some(Duration::from_millis(10)));
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v2"), None);
        clock.advance(Duration::from_secs(1));
        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v2"))));
    }


    #[test]
    fn px_that_overflows_the_redis_deadline_expires_immediately() {
        // Redis replies OK and writes the key, then the next read finds it
        // gone, because mstime() + i64::MAX wraps into the past.
        let clock = Arc::new(TestClock::new());
        let mut db = Db::with_clock(clock.clone());

        db.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(i64::MAX as u64)),
        );

        assert_eq!(db.get(b"k"), Ok(None), "should already be expired");
        assert!(!db.exists(b"k"), "and reaped on the way out");
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
        );

        assert_eq!(db.get(b"k"), Ok(Some(Bytes::from_static(b"v"))));
    }
}