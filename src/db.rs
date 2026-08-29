use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
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
}

// public: the tag only
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType { String, List }

impl DataType {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            DataType::String => b"string",
            DataType::List => b"list",
        }
    }
}

impl Data {
    fn kind(&self) -> DataType {
        match self {
            Data::String(_) => DataType::String,
            Data::List(_) => DataType::List,
        }
    }
}

pub struct Db {
    map: Mutex<HashMap<Vec<u8>, Entry>>,
    clock: Arc<dyn Clock>,
}

impl Db {

    pub fn type_of(&self, key: &[u8]) -> Option<DataType> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);
        guard.get(key).map(|e| e.data.kind())
    }

    pub fn new() -> Self {
        Db::with_clock(Arc::new(SystemClock))
    }

    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Db {
            map: Mutex::new(HashMap::new()),
            clock,
        }
    }

    /// ttl is relative. The absolute deadline is computed here, once, because
    /// this is the only layer that owns a clock.
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let now = self.clock.now();
        let entry = Entry {
            data: Data::String(value),
            expires_at: ttl.map(|d| now + d),
        };
        self.map.lock().unwrap().insert(key, entry);
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongType>{
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);

        match guard.get(key) {
            Some(entry) => match &entry.data {
                Data::String(v) => Ok(Some(v.clone())),
                _ => Err(WrongType),
            },
            None => Ok(None),
        }
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        // An expired entry counts as absent, so removing it returns false.
        match guard.remove(key) {
            Some(entry) => !entry.is_expired(now),
            None => false,
        }
    }

    /// Counts entries still in the map, including ones past their deadline that
    /// have not been lazily reaped. Debug and test use only.
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn rpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> Result<usize, WrongType> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);

        let entry = guard.entry(key.to_vec()).or_insert_with(|| Entry {
            data: Data::List(VecDeque::new()),
            expires_at: None,
        });

        match &mut entry.data {
            Data::List(list) => {
                list.extend(values);
                Ok(list.len())
            }
            _ => Err(WrongType),
        }
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);
        guard.contains_key(key)
    }

    pub fn llen(&self, key: &[u8]) -> Result<usize, WrongType> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);
        match guard.get(key) {
            Some(entry) => match &entry.data {
                Data::List(l) => Ok(l.len()),
                _ => Err(WrongType),
            },
            None => Ok(0),
        }
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, WrongType> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);
        match guard.get(key) {
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

    pub fn lpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> Result<usize, WrongType> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);

        let entry = guard.entry(key.to_vec()).or_insert_with(|| Entry {
            data: Data::List(VecDeque::new()),
            expires_at: None,
        });

        match &mut entry.data {
            Data::List(list) => {
                for v in values {
                    list.push_front(v);
                }
                Ok(list.len())
            }
            _ => Err(WrongType),
        }
    }

    pub fn lpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>, WrongType> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);
        let (popped, now_empty) = match guard.get_mut(key) {
            None => return Ok(None),
            Some(entry) => match &mut entry.data {
                Data::List(l) => {
                    let n = count.unwrap_or(1).min(l.len());
                    let out: Vec<_> = l.drain(..n).collect();
                    (out, l.is_empty())
                }
                _ => return Err(WrongType),
            },
        };
        if now_empty {
            guard.remove(key);
        }
        Ok(Some(popped))
    }

    pub fn rpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>, WrongType> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();
        reap_if_expired(&mut guard, key, now);
        let (popped, now_empty) = match guard.get_mut(key) {
            None => return Ok(None),
            Some(entry) => match &mut entry.data {
                Data::List(l) => {
                    let n = count.unwrap_or(1).min(l.len());
                    let start = l.len() - n;
                    let mut out: Vec<_> = l.drain(start..).collect();
                    out.reverse();
                    (out, l.is_empty())
                }
                _ => return Err(WrongType),
            },
        };
        if now_empty {
            guard.remove(key);
        }
        Ok(Some(popped))
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

impl Default for Db {
    fn default() -> Self {
        Db::new()
    }
}

#[cfg(test)]
pub struct TestClock {
    now: Mutex<Instant>,
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Self {
        TestClock { now: Mutex::new(Instant::now()) }
    }
    pub fn advance(&self, d: Duration) {
        *self.now.lock().unwrap() += d;
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
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
        let (_clock, db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), None);
        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())));
        assert_eq!(db.get(b"missing"), Ok(None));
    }

    #[test]
    fn key_expires_exactly_at_deadline() {
        let (clock, db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), Some(Duration::from_millis(100)));

        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())));

        clock.advance(Duration::from_millis(99));
        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())), "not yet expired");

        clock.advance(Duration::from_millis(1));
        assert_eq!(db.get(b"k"), Ok(None), "expired at the deadline, not after it");
    }

    #[test]
    fn no_expiry_never_expires() {
        let (clock, db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), None);
        clock.advance(Duration::from_secs(86_400 * 365));
        assert_eq!(db.get(b"k"), Ok(Some(b"v".to_vec())));
    }

    #[test]
    fn get_reaps_the_expired_entry() {
        let (clock, db) = fixture();
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
        let (clock, db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), Some(Duration::from_millis(10)));
        clock.advance(Duration::from_millis(10));
        assert!(!db.delete(b"k"), "expired key counts as absent");
        assert_eq!(db.len(), 0, "but it is still removed");
    }

    #[test]
    fn overwriting_clears_the_old_ttl() {
        let (clock, db) = fixture();
        db.set(b"k".to_vec(), b"v1".to_vec(), Some(Duration::from_millis(10)));
        db.set(b"k".to_vec(), b"v2".to_vec(), None);
        clock.advance(Duration::from_secs(1));
        assert_eq!(db.get(b"k"), Ok(Some(b"v2".to_vec())));
    }
}