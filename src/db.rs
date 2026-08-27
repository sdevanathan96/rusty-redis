use std::collections::HashMap;
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

struct Entry {
    value: Vec<u8>,
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

pub struct Db {
    map: Mutex<HashMap<Vec<u8>, Entry>>,
    clock: Arc<dyn Clock>,
}

impl Db {
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
            value,
            expires_at: ttl.map(|d| now + d),
        };
        self.map.lock().unwrap().insert(key, entry);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let now = self.clock.now();
        let mut guard = self.map.lock().unwrap();

        let expired = match guard.get(key) {
            None => return None,
            Some(entry) => entry.is_expired(now),
        };

        if expired {
            guard.remove(key);
            return None;
        }
        Some(guard.get(key).expect("present under the lock").value.clone())
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
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(db.get(b"missing"), None);
    }

    #[test]
    fn key_expires_exactly_at_deadline() {
        let (clock, db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), Some(Duration::from_millis(100)));

        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));

        clock.advance(Duration::from_millis(99));
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()), "not yet expired");

        clock.advance(Duration::from_millis(1));
        assert_eq!(db.get(b"k"), None, "expired at the deadline, not after it");
    }

    #[test]
    fn no_expiry_never_expires() {
        let (clock, db) = fixture();
        db.set(b"k".to_vec(), b"v".to_vec(), None);
        clock.advance(Duration::from_secs(86_400 * 365));
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn get_reaps_the_expired_entry() {
        let (clock, db) = fixture();
        db.set(b"a".to_vec(), b"v".to_vec(), Some(Duration::from_millis(100)));
        db.set(b"b".to_vec(), b"v".to_vec(), Some(Duration::from_millis(50)));
        assert_eq!(db.len(), 2);

        clock.advance(Duration::from_millis(99));
        assert_eq!(db.get(b"b"), None);
        assert_eq!(db.len(), 1, "get must remove, not just hide");

        clock.advance(Duration::from_millis(1));
        assert_eq!(db.get(b"a"), None);
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
        assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));
    }
}