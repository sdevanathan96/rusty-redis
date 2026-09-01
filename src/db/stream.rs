#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId { pub ms: u64, pub seq: u64 }
impl EntryId {
    pub fn to_bytes(&self) -> Vec<u8> {
        format!("{}-{}", self.ms, self.seq).into_bytes()
    }
}
impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XaddError {
    WrongType,
    IdIsZero,
    IdTooSmall,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stream {
    entries: Vec<(EntryId, Vec<(Vec<u8>, Vec<u8>)>)>,
    last_id: EntryId,
}

impl Stream {
    // explicit id: validate and append
    pub fn append(&mut self, id: EntryId, fields: Vec<(Vec<u8>, Vec<u8>)>)
    -> Result<EntryId, XaddError>
    {
        if id.ms == 0 && id.seq == 0 {
            return Err(XaddError::IdIsZero);
        }
        if id <= self.last_id {
            return Err(XaddError::IdTooSmall);
        }
        self.last_id = id;
        self.entries.push((id, fields));
        Ok(id)
    }

    // auto sequence: derive the seq from last_id, then append
    pub fn append_auto_seq(&mut self, ms: u64, fields: Vec<(Vec<u8>, Vec<u8>)>)
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
        self.entries.push((id, fields));
        Ok(id)
    }

    /// `*`: use the clock's millisecond, unless the last entry is already ahead
    /// of the clock, in which case reuse that millisecond so IDs never go
    /// backwards.
    pub fn append_auto(
        &mut self,
        clock_ms: u64,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<EntryId, XaddError> {
        let ms = clock_ms.max(self.last_id.ms);
        self.append_auto_seq(ms, fields)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn range(&self, start: EntryId, end: EntryId, count: Option<usize>) -> &[(EntryId, Vec<(Vec<u8>, Vec<u8>)>)] {
        let lo = match self.entries.binary_search_by_key(&start, |(id, _)| *id) {
            Ok(i) => i, // exact match
            Err(i) => i, // not present, i is the insertion point, which is the lower bound
        };
        let hi = match self.entries.binary_search_by_key(&end, |(id, _)| *id) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        let slice = &self.entries[lo..hi];
        match count {
            Some(n) => &slice[..n.min(slice.len())],
            None => slice,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdSpec {
    Explicit(EntryId),
    AutoSeq(u64),      // ms given, seq to be generated
    Auto,              // both generated
}
