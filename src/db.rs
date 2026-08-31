
mod stream;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::db::stream::{Stream};
pub use crate::db::stream::{EntryId, XaddError, IdSpec};

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
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Stream(Stream),
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
    map: HashMap<Vec<u8>, Entry>,
    clock: Arc<dyn Clock>,
}

impl Db {

    pub fn type_of(&mut self, key: &[u8]) -> Option<DataType> {
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
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
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let now = self.clock.now();
        let entry = Entry {
            data: Data::String(value),
            expires_at: ttl.map(|d| now + d),
        };
        self.map.insert(key, entry);
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongType>{
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);

        match self.map.get(key) {
            Some(entry) => match &entry.data {
                Data::String(v) => Ok(Some(v.clone())),
                _ => Err(WrongType),
            },
            None => Ok(None),
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
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
        match self.map.get(key) {
            Some(entry) => match &entry.data {
                Data::List(l) => Ok(l.len()),
                _ => Err(WrongType),
            },
            None => Ok(0),
        }
    }

    pub fn lrange(&mut self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, WrongType> {
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
        match self.map.get(key) {
            Some(entry) => match &entry.data {
                Data::List(l) => {
                    let len = l.len();
                    if let Some((from, to)) = resolve_range(len, start, stop) {
                        Ok(l.range(from..to).cloned().collect())
                    } else {
                        Ok(Vec::new())
                    }
                },
                _ => Err(WrongType),
            },
            None => Ok(Vec::new()),
        }
    }


    pub fn push(&mut self, key: &[u8], values: Vec<Vec<u8>>, end: End) -> Result<usize, WrongType> {
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
        push_to(&mut self.map, key, values, end)
    }

    pub fn pop(&mut self, key: &[u8], count: Option<usize>, end: End) -> Result<Option<Vec<Vec<u8>>>, WrongType>{
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
        pop_from(&mut self.map, key, count.unwrap_or(1), end)
    }


    pub fn lmove(&mut self, src: &[u8], dst: &[u8], from: End, to: End) -> Result<Option<Vec<u8>>, WrongType> {
        let now = self.clock.now();
        reap_if_expired(&mut self.map, src, now);
        reap_if_expired(&mut self.map, dst, now);

        match self.map.get(src) {
            None => return Ok(None),
            Some(entry) => match &entry.data {
                Data::List(l) => {
                    match l.is_empty() {
                        true => return Ok(None),
                        false => {}
                    }
                }
                _ => return Err(WrongType),
            }
        }
        match self.map.get(dst) {
            None => {},
            Some(entry) => match &entry.data {
                Data::List(_) => {}
                _ => return Err(WrongType),
            }
        }

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


    pub fn xadd(&mut self, key: &[u8], spec: IdSpec, fields: Vec<(Vec<u8>, Vec<u8>)>) -> Result<EntryId, XaddError> {
        // reap_if_expired
        // get or create Data::Stream
        // resolve IdSpec::Auto using self.clock
        // delegate to Stream::append or append_auto_seq
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
        let now_ms = self.clock.now_ms();

        let entry = self.map.entry(key.to_vec()).or_insert_with(|| Entry {
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
}

/// Removes the entry at `key` if its deadline has passed. Every accessor calls
/// this first, so a write path never appends to a stale value.
fn reap_if_expired(map: &mut HashMap<Vec<u8>, Entry>, key: &[u8], now: Instant) {
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
    map: &mut HashMap<Vec<u8>, Entry>,
    key: &[u8],
    count: usize,
    end: End,
) -> Result<Option<Vec<Vec<u8>>>, WrongType> {
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
    map: &mut HashMap<Vec<u8>, Entry>,
    key: &[u8],
    values: Vec<Vec<u8>>,
    end: End,
) -> Result<usize, WrongType> {
    let entry = map.entry(key.to_vec()).or_insert_with(|| Entry {
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

#[cfg(test)]
pub struct TestClock {
    now: Mutex<Instant>,
    base_ms: Mutex<u64>,
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        TestClock { now: Mutex::new(Instant::now()), base_ms: Mutex::new(0) }
    }
    pub fn advance(&self, d: Duration) {
        *self.now.lock().unwrap() += d;
    }
    
    pub fn advance_ms(&self, d: u64) {
        *self.base_ms.lock().unwrap() += d;
    }

}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
    fn now_ms(&self) -> u64 { *self.base_ms.lock().unwrap() }
}

#[cfg(test)]
mod tests {
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
        db.set(b"k".to_vec(), b"v".to_vec(), None);
        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())));
        assert_eq!(db.get(b"missing"), Ok(None));
    }

    #[test]
    fn key_expires_exactly_at_deadline() {
        let (clock, mut db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), Some(Duration::from_millis(100)));

        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())));

        clock.advance(Duration::from_millis(99));
        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())), "not yet expired");

        clock.advance(Duration::from_millis(1));
        assert_eq!(db.get(b"k"), Ok(None), "expired at the deadline, not after it");
    }

    #[test]
    fn no_expiry_never_expires() {
        let (clock, mut db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), None);
        clock.advance(Duration::from_secs(86_400 * 365));
        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())));
    }

    #[test]
    fn get_reaps_the_expired_entry() {
        let (clock, mut db) = fixture();
        db.set(b"a".to_vec(), b"v".to_vec(), Some(Duration::from_millis(100)));
        db.set(b"b".to_vec(), b"v".to_vec(), Some(Duration::from_millis(50)));
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
        db.set(b"k".to_vec(), b"v".to_vec(), Some(Duration::from_millis(10)));
        clock.advance(Duration::from_millis(10));
        assert!(!db.delete(b"k"), "expired key counts as absent");
        assert_eq!(db.len(), 0, "but it is still removed");
    }

    #[test]
    fn overwriting_clears_the_old_ttl() {
        let (clock, mut db) = fixture();
        db.set(b"k".to_vec(), b"v1".to_vec(), Some(Duration::from_millis(10)));
        db.set(b"k".to_vec(), b"v2".to_vec(), None);
        clock.advance(Duration::from_secs(1));
        assert_eq!(db.get(b"k"), Ok(Some(b"v2".to_vec())));
    }
}