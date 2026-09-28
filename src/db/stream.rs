use bytes::Bytes;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId { pub ms: u64, pub seq: u64 }
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
// pub type StreamEntry = (EntryId, Vec<(Bytes, Bytes)>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: EntryId,
    pub fields: Vec<(Bytes, Bytes)>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadFrom {
    Id(EntryId),
    Latest,
    Last
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
    pub fn append(&mut self, id: EntryId, fields: Vec<(Bytes, Bytes)>)
    -> Result<EntryId, XaddError>
    {
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
    pub fn append_auto_seq(&mut self, ms: u64, fields: Vec<(Bytes, Bytes)>)
        -> Result<EntryId, XaddError>
    {
        if ms < self.last_id.ms {
            return Err(XaddError::IdTooSmall);
        }

        let id = if ms == self.last_id.ms {
            EntryId { ms, seq: self.last_id.seq.checked_add(1).ok_or(XaddError::IdTooSmall)? }
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
        let lo = self.entries.partition_point(|e| e.id < start);   // inclusive start
        let hi = self.entries.partition_point(|e| e.id <= end);    // inclusive end
        let slice = &self.entries[lo..hi];
        match count {
            Some(n) => &slice[..n.min(slice.len())],
            None => slice,
        }
    }

    pub(super) fn range_after(&self, after: EntryId, count: Option<usize>)
    -> &[StreamEntry] {
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

    pub(super) fn last_entry(&self) -> Option<&StreamEntry> {
        match self.len() {
            0 => None,
            _ => self.entries.last(),
        }
    }

    pub(super) fn delete(&mut self, id: EntryId) -> usize {
        if let Ok(i) = self.entries.binary_search_by_key(&id, |e| e.id) {
            self.entries.remove(i);
            return 1;
        }
        0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdSpec {
    Explicit(EntryId),
    AutoSeq(u64),      // ms given, seq to be generated
    Auto,              // both generated
}
