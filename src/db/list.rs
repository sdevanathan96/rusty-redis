//! Lists: push, pop, LRANGE, LMOVE, and the helpers they share.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use super::{Data, Db, Entry, WrongType};

/// Which end of a list an operation acts on. Shared by push, pop, and LMOVE,
/// which takes two of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum End {
    Left,
    Right,
}

impl Db {
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
