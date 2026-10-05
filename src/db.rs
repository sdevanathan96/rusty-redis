//! The keyspace: every key's value and expiry, owned by the keyspace task.
//! Each data type keeps its `Db` methods in its own file; this one holds what
//! they all share.

mod clock;
mod list;
mod stream;
mod string;

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
pub use clock::TestClock;
pub use clock::{Clock, SystemClock};
pub use list::End;
use stream::Stream;
pub use stream::{
    EntryId, IdSpec, Mode, ReadFrom, RefPolicy, StreamEntry, Trim, TrimBy, XaddError,
};
use string::Str;
pub use string::{IncrError, InvalidExpireTime};

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
}

/// Removes the entry at `key` if its deadline has passed. Every accessor calls
/// this first, so a write path never appends to a stale value.
fn reap_if_expired(map: &mut HashMap<Bytes, Entry>, key: &[u8], now: Instant) {
    let expired = map.get(key).is_some_and(|e| e.is_expired(now));
    if expired {
        map.remove(key);
    }
}

impl Default for Db {
    fn default() -> Self {
        Db::new()
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
