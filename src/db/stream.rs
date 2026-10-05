use bytes::Bytes;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId {
    pub ms: u64,
    pub seq: u64,
}
impl EntryId {
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(format!("{}-{}", self.ms, self.seq))
    }
}
impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}
/// What a trim keeps: the newest `MaxLen` entries, or those at or above `MinId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrimBy {
    MaxLen(usize),
    MinId(EntryId),
}

/// A parsed MAXLEN or MINID clause, shared by XADD and XTRIM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Trim {
    pub by: TrimBy,
    pub mode: Mode,
    #[allow(dead_code)] // consumer groups will read this; not built yet
    pub refs: RefPolicy,
}

/// `Exact` for `=` or no marker, `Approx` for `~`. A LIMIT exists only with
/// `~`, so the type cannot hold the combination Redis rejects. `None` is no
/// limit, which is also what `LIMIT 0` means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    Exact,
    Approx { limit: Option<usize> },
}

/// What trimming does to consumer group references. Parsed and kept for when
/// groups exist; with none, all three behave the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefPolicy {
    KeepRef,
    DelRef,
    Acked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: EntryId,
    pub fields: Vec<(Bytes, Bytes)>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadFrom {
    Id(EntryId),
    Latest,
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XaddError {
    WrongType,
    IdIsZero,
    IdTooSmall,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stream {
    entries: Vec<StreamEntry>,
    last_id: EntryId,
}

impl Stream {
    // explicit id: validate and append
    pub fn append(
        &mut self,
        id: EntryId,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<EntryId, XaddError> {
        if id.ms == 0 && id.seq == 0 {
            return Err(XaddError::IdIsZero);
        }
        if id <= self.last_id {
            return Err(XaddError::IdTooSmall);
        }
        self.last_id = id;
        self.entries.push(StreamEntry { id, fields });
        Ok(id)
    }

    // auto sequence: derive the seq from last_id, then append
    pub fn append_auto_seq(
        &mut self,
        ms: u64,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<EntryId, XaddError> {
        if ms < self.last_id.ms {
            return Err(XaddError::IdTooSmall);
        }

        let id = if ms == self.last_id.ms {
            EntryId {
                ms,
                seq: self
                    .last_id
                    .seq
                    .checked_add(1)
                    .ok_or(XaddError::IdTooSmall)?,
            }
        } else {
            EntryId { ms, seq: 0 }
        };

        self.last_id = id;
        self.entries.push(StreamEntry { id, fields });
        Ok(id)
    }

    /// `*`: use the clock's millisecond, unless the last entry is already ahead
    /// of the clock, in which case reuse that millisecond so IDs never go
    /// backwards.
    pub fn append_auto(
        &mut self,
        clock_ms: u64,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<EntryId, XaddError> {
        let ms = clock_ms.max(self.last_id.ms);
        self.append_auto_seq(ms, fields)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn last_slice(&self) -> &[StreamEntry] {
        match self.entries.len() {
            0 => &[],
            n => &self.entries[n - 1..],
        }
    }

    pub fn range(&self, start: EntryId, end: EntryId, count: Option<usize>) -> &[StreamEntry] {
        let lo = self.entries.partition_point(|e| e.id < start); // inclusive start
        let hi = self.entries.partition_point(|e| e.id <= end); // inclusive end
        let slice = &self.entries[lo..hi];
        match count {
            Some(n) => &slice[..n.min(slice.len())],
            None => slice,
        }
    }

    pub(super) fn range_after(&self, after: EntryId, count: Option<usize>) -> &[StreamEntry] {
        let lo = self.entries.partition_point(|e| e.id <= after); // exclusive: skip `after` itself
        let slice = &self.entries[lo..];
        match count {
            Some(n) => &slice[..n.min(slice.len())],
            None => slice,
        }
    }

    pub(super) fn last_id(&self) -> EntryId {
        self.last_id
    }

    pub(super) fn delete(&mut self, id: EntryId) -> usize {
        if let Ok(i) = self.entries.binary_search_by_key(&id, |e| e.id) {
            self.entries.remove(i);
            return 1;
        }
        0
    }
    /// Removes entries from the front and returns how many. `last_id` is left
    /// alone, so an id at or below it stays rejected after a full trim.
    pub(super) fn trim(&mut self, trim: &Trim) -> usize {
        let count = match trim.by {
            TrimBy::MaxLen(max_len) => self.entries.len().saturating_sub(max_len),
            TrimBy::MinId(min_id) => self.entries.partition_point(|e| e.id < min_id),
        };
        // `~` trims exactly here, which keeps Redis's only promise that at
        // least the threshold remains. Its LIMIT caps the removals.
        let count = match trim.mode {
            Mode::Exact | Mode::Approx { limit: None } => count,
            Mode::Approx { limit: Some(limit) } => count.min(limit),
        };
        self.entries.drain(..count);
        count
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdSpec {
    Explicit(EntryId),
    AutoSeq(u64), // ms given, seq to be generated
    Auto,         // both generated
}

#[cfg(test)]
mod trim_tests {
    use super::*;

    /// Entries 1-0 through 5-0.
    fn five() -> Stream {
        let mut s = Stream::default();
        for ms in 1..=5 {
            s.append(EntryId { ms, seq: 0 }, vec![]).unwrap();
        }
        s
    }

    fn trim(by: TrimBy, mode: Mode) -> Trim {
        Trim {
            by,
            mode,
            refs: RefPolicy::KeepRef,
        }
    }

    fn id(ms: u64, seq: u64) -> EntryId {
        EntryId { ms, seq }
    }

    /// The milliseconds of every entry left, oldest first.
    fn left(s: &Stream) -> Vec<u64> {
        s.range(id(0, 0), id(u64::MAX, u64::MAX), None)
            .iter()
            .map(|e| e.id.ms)
            .collect()
    }

    #[test]
    fn maxlen_keeps_the_newest() {
        let mut s = five();
        assert_eq!(s.trim(&trim(TrimBy::MaxLen(2), Mode::Exact)), 3);
        assert_eq!(left(&s), [4, 5]);
    }

    #[test]
    fn maxlen_above_the_length_removes_nothing() {
        let mut s = five();
        assert_eq!(
            s.trim(&trim(TrimBy::MaxLen(10), Mode::Exact)),
            0,
            "saturating, not an underflow"
        );
        assert_eq!(s.len(), 5);
    }

    #[test]
    fn maxlen_zero_empties_the_stream() {
        let mut s = five();
        assert_eq!(s.trim(&trim(TrimBy::MaxLen(0), Mode::Exact)), 5);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn minid_keeps_the_id_itself() {
        // Redis's MINID 3-0 on 1-0..5-0 removes 2. A `<=` would remove 3.
        let mut s = five();
        assert_eq!(s.trim(&trim(TrimBy::MinId(id(3, 0)), Mode::Exact)), 2);
        assert_eq!(left(&s), [3, 4, 5]);
    }

    #[test]
    fn minid_between_two_entries() {
        let mut s = five();
        assert_eq!(s.trim(&trim(TrimBy::MinId(id(3, 5)), Mode::Exact)), 3);
        assert_eq!(left(&s), [4, 5]);
    }

    #[test]
    fn approx_trims_exactly_here() {
        // Redis would remove nothing from a stream this short.
        let mut s = five();
        assert_eq!(
            s.trim(&trim(TrimBy::MaxLen(2), Mode::Approx { limit: None })),
            3
        );
        assert_eq!(left(&s), [4, 5]);
    }

    #[test]
    fn limit_caps_the_removals() {
        let mut s = five();
        assert_eq!(
            s.trim(&trim(TrimBy::MaxLen(0), Mode::Approx { limit: Some(2) })),
            2
        );
        assert_eq!(left(&s), [3, 4, 5], "the oldest two go first");
    }

    #[test]
    fn a_full_trim_keeps_the_last_id() {
        let mut s = five();
        s.trim(&trim(TrimBy::MaxLen(0), Mode::Exact));
        assert!(matches!(
            s.append(id(1, 0), vec![]),
            Err(XaddError::IdTooSmall)
        ));
        assert!(
            matches!(s.append_auto_seq(5, vec![]), Ok(EntryId { ms: 5, seq: 1 })),
            "auto sequence continues from 5-0, not from an empty stream"
        );
        assert!(s.append(id(6, 0), vec![]).is_ok());
    }
}
