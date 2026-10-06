//! Every key's value and expiry, owned by the keyspace task. Each data type's
//! `Db` methods live in its own file.

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

#[derive(Debug, Clone)]
enum Data {
    String(Str),
    List(VecDeque<Bytes>),
    Stream(Stream),
}

impl Data {
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
    /// Keys changed since the last `take_modified`, for WATCH. Lazy expiry
    /// never adds to it: `reap_if_expired` cannot reach it.
    modified: Vec<Bytes>,
}

impl Db {
    /// Separate from reading, so reads can take `&self` and lend what they
    /// return.
    fn reap(&mut self, key: &[u8]) {
        let now = self.clock.now();
        reap_if_expired(&mut self.map, key, now);
    }

    /// Call only after a real change: a write that fails or does nothing must
    /// not abort a WATCH.
    fn mark_modified(&mut self, key: Bytes) {
        self.modified.push(key);
    }

    pub fn take_modified(&mut self) -> Vec<Bytes> {
        std::mem::take(&mut self.modified)
    }

    /// When a live key expires: `None` if it is absent or has no TTL.
    pub fn deadline(&mut self, key: &[u8]) -> Option<Instant> {
        self.reap(key);
        self.map.get(key).and_then(|e| e.expires_at)
    }

    /// The same rule as `Entry::is_expired`.
    pub fn has_passed(&self, deadline: Instant) -> bool {
        self.clock.now() >= deadline
    }

    /// Callers reap first.
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
            modified: Vec::new(),
        }
    }

    pub fn delete(&mut self, key: &Bytes) -> bool {
        let now = self.clock.now();
        // An expired entry counts as absent.
        let removed = self.map.remove(key).is_some_and(|e| !e.is_expired(now));
        if removed {
            self.mark_modified(key.clone());
        }
        removed
    }

    /// Includes expired entries not yet reaped. For tests.
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

/// Every accessor calls this first, so no write lands on an expired value.
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
        assert!(
            !db.delete(&Bytes::from_static(b"k")),
            "expired key counts as absent"
        );
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
        // mstime() + i64::MAX overflows, and the key is left alone.
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
        // Just under i64::MAX minus the wall clock must still work.
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

/// A write reports its key for WATCH only if it changed something.
#[cfg(test)]
mod modified_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;

    use super::{Db, End, EntryId, IdSpec, Mode, RefPolicy, TestClock, Trim, TrimBy};

    fn fixture() -> (Arc<TestClock>, Db) {
        let clock = Arc::new(TestClock::new());
        let db = Db::with_clock(clock.clone());
        (clock, db)
    }

    fn k(key: &'static [u8]) -> Bytes {
        Bytes::from_static(key)
    }

    fn xadd(db: &mut Db, key: &'static [u8], ms: u64, nomkstream: bool) -> bool {
        let id = IdSpec::Explicit(EntryId { ms, seq: 0 });
        db.xadd(k(key), id, vec![(k(b"f"), k(b"v"))], None, nomkstream)
            .is_ok_and(|added| added.is_some())
    }

    fn max_len(n: usize) -> Trim {
        Trim {
            by: TrimBy::MaxLen(n),
            mode: Mode::Exact,
            refs: RefPolicy::KeepRef,
        }
    }

    #[test]
    fn writes_that_change_something_report_their_key() {
        let (_clock, mut db) = fixture();
        db.set(k(b"s"), k(b"v"), None).unwrap();
        assert_eq!(db.take_modified(), vec![k(b"s")], "set");

        db.incr(k(b"n")).unwrap();
        db.incr(k(b"n")).unwrap();
        assert_eq!(
            db.take_modified(),
            vec![k(b"n"), k(b"n")],
            "incr, new then existing"
        );

        db.push(k(b"l"), vec![k(b"a"), k(b"b")], End::Right)
            .unwrap();
        db.pop(&k(b"l"), None, End::Left).unwrap();
        assert_eq!(db.take_modified(), vec![k(b"l"), k(b"l")], "push then pop");

        assert!(xadd(&mut db, b"x", 1, false));
        assert!(xadd(&mut db, b"x", 2, false));
        db.xdel(&k(b"x"), &[EntryId { ms: 1, seq: 0 }]).unwrap();
        db.xtrim(&k(b"x"), &max_len(0)).unwrap();
        assert_eq!(
            db.take_modified(),
            vec![k(b"x"); 4],
            "xadd twice, xdel, xtrim"
        );

        assert!(db.delete(&k(b"s")));
        assert_eq!(db.take_modified(), vec![k(b"s")], "delete");
    }

    #[test]
    fn writes_that_change_nothing_report_nothing() {
        let (_clock, mut db) = fixture();
        db.set(k(b"s"), k(b"text"), None).unwrap();
        db.push(k(b"l"), vec![k(b"a")], End::Right).unwrap();
        assert!(xadd(&mut db, b"x", 5, false));
        db.take_modified();

        assert!(!db.delete(&k(b"missing")));
        assert_eq!(db.pop(&k(b"missing"), None, End::Left), Ok(None));
        assert_eq!(db.pop(&k(b"l"), Some(0), End::Left), Ok(Some(vec![])));
        assert_eq!(
            db.lmove(&k(b"missing"), k(b"l"), End::Left, End::Right),
            Ok(None)
        );
        assert!(
            db.push(k(b"s"), vec![k(b"a")], End::Left).is_err(),
            "WRONGTYPE"
        );
        assert!(db.incr(k(b"s")).is_err(), "not an integer");
        let overflowing = Some(Duration::from_millis(i64::MAX as u64));
        assert!(db.set(k(b"s"), k(b"v"), overflowing).is_err());
        assert!(!xadd(&mut db, b"missing", 1, true), "NOMKSTREAM");
        assert!(!xadd(&mut db, b"x", 1, false), "id too small");
        assert_eq!(db.xdel(&k(b"x"), &[EntryId { ms: 9, seq: 0 }]), Ok(0));
        assert_eq!(db.xtrim(&k(b"x"), &max_len(5)), Ok(0));

        assert_eq!(db.take_modified(), Vec::<Bytes>::new());
    }

    #[test]
    fn lmove_reports_both_keys() {
        let (_clock, mut db) = fixture();
        db.push(k(b"src"), vec![k(b"a")], End::Right).unwrap();
        db.take_modified();
        db.lmove(&k(b"src"), k(b"dst"), End::Left, End::Right)
            .unwrap();
        assert_eq!(db.take_modified(), vec![k(b"dst"), k(b"src")]);
    }

    #[test]
    fn lazy_expiry_reports_nothing() {
        let (clock, mut db) = fixture();
        db.set(k(b"k"), k(b"v"), Some(Duration::from_millis(10)))
            .unwrap();
        db.take_modified();
        clock.advance(Duration::from_millis(10));
        assert_eq!(db.get(b"k"), Ok(None), "reaped here");
        assert!(!db.delete(&k(b"k")));
        assert_eq!(db.take_modified(), Vec::<Bytes>::new());
    }

    #[test]
    fn take_modified_empties_the_list() {
        let (_clock, mut db) = fixture();
        db.set(k(b"k"), k(b"v"), None).unwrap();
        assert_eq!(db.take_modified(), vec![k(b"k")]);
        assert_eq!(db.take_modified(), Vec::<Bytes>::new());
    }
}
