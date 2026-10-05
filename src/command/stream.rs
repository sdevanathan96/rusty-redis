use bytes::Bytes;

use crate::{
    command::{Blocking, Command, CommandError, Outcome, lenient_u64, parse_block, parse_i64},
    db::{Db, EntryId, IdSpec, Mode, ReadFrom, RefPolicy, StreamEntry, Trim, TrimBy},
    resp::Value,
};

/// The trim options XADD and XTRIM share, collected one token at a time.
#[derive(Default)]
struct TrimOptions {
    by: Option<TrimBy>,
    approx: bool,
    limit: Option<usize>,
    refs: Option<RefPolicy>,
}

impl TrimOptions {
    /// If `args[i]` starts a trim option, consume it and return how many
    /// tokens it used. `Ok(None)` means `args[i]` is not a trim option.
    fn take(&mut self, args: &[Bytes], i: usize) -> Result<Option<usize>, CommandError> {
        let more = args.len() - i - 1; // tokens after this one
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"MAXLEN" | b"MINID" if more > 0 => {
                if self.by.is_some() {
                    return Err(CommandError::MaxlenWithMinid);
                }
                // `=` or `~` counts only if another token follows it.
                let mut used = 1;
                if more >= 2 && (args[i + 1] == "~" || args[i + 1] == "=") {
                    self.approx = args[i + 1] == "~";
                    used += 1;
                }
                let value = &args[i + used];
                self.by = Some(if upper == b"MAXLEN" {
                    let n = parse_i64(value)?;
                    if n < 0 {
                        return Err(CommandError::MaxlenNegative);
                    }
                    TrimBy::MaxLen(n as usize)
                } else {
                    TrimBy::MinId(parse_bound(value, 0)?)
                });
                Ok(Some(used + 1))
            }
            b"LIMIT" if more > 0 => {
                let n = parse_i64(&args[i + 1])?;
                if n < 0 {
                    return Err(CommandError::LimitNegative);
                }
                self.limit = Some(n as usize);
                Ok(Some(2))
            }
            // Consumer group options. Only one may appear, anywhere among the
            // trim options.
            b"KEEPREF" | b"DELREF" | b"ACKED" => {
                if self.refs.is_some() {
                    return Err(CommandError::Syntax);
                }
                self.refs = Some(match upper.as_slice() {
                    b"KEEPREF" => RefPolicy::KeepRef,
                    b"DELREF" => RefPolicy::DelRef,
                    _ => RefPolicy::Acked,
                });
                Ok(Some(1))
            }
            _ => Ok(None),
        }
    }

    /// Check the combination once every option has been read. `None` means
    /// no MAXLEN or MINID was given.
    fn finish(self) -> Result<Option<Trim>, CommandError> {
        let Some(by) = self.by else {
            // No MAXLEN or MINID. A LIMIT on its own has nothing to limit.
            if self.limit.is_some() {
                return Err(CommandError::LimitWithoutStrategy);
            }
            return Ok(None);
        };
        let mode = if self.approx {
            Mode::Approx {
                limit: self.limit.filter(|&n| n != 0),
            } // LIMIT 0 means no limit
        } else if self.limit.is_some() {
            return Err(CommandError::LimitWithoutApprox);
        } else {
            Mode::Exact
        };
        Ok(Some(Trim {
            by,
            mode,
            refs: self.refs.unwrap_or(RefPolicy::KeepRef),
        }))
    }
}

fn parse_xadd_id(raw: &[u8]) -> Result<IdSpec, CommandError> {
    if raw == b"*" {
        return Ok(IdSpec::Auto);
    }
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::InvalidStreamId)?;

    match text.split_once('-') {
        None => Ok(IdSpec::Explicit(EntryId {
            ms: lenient_u64(text)?,
            seq: 0,
        })),
        Some((ms, "*")) => Ok(IdSpec::AutoSeq(lenient_u64(ms)?)),
        Some((ms, seq)) => Ok(IdSpec::Explicit(EntryId {
            ms: lenient_u64(ms)?,
            seq: lenient_u64(seq)?,
        })),
    }
}

/// `default_seq` is what a bare millisecond gets: 0 for a start bound,
/// u64::MAX for an end bound, so `XRANGE k 5 5` spans all of millisecond 5.
fn parse_bound(raw: &[u8], default_seq: u64) -> Result<EntryId, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::InvalidStreamId)?;
    match text.split_once('-') {
        None => Ok(EntryId {
            ms: lenient_u64(text)?,
            seq: default_seq,
        }),
        Some((ms, seq)) => Ok(EntryId {
            ms: lenient_u64(ms)?,
            seq: lenient_u64(seq)?,
        }),
    }
}

fn parse_range_start(raw: &[u8]) -> Result<EntryId, CommandError> {
    if raw == b"-" {
        return Ok(EntryId { ms: 0, seq: 0 });
    }
    parse_bound(raw, 0)
}

fn parse_range_end(raw: &[u8]) -> Result<EntryId, CommandError> {
    if raw == b"+" {
        return Ok(EntryId {
            ms: u64::MAX,
            seq: u64::MAX,
        });
    }
    parse_bound(raw, u64::MAX)
}

fn parse_xrange_count(raw: &[u8]) -> Result<i64, CommandError> {
    parse_i64(raw)
}

fn parse_read_from(raw: &[u8]) -> Result<ReadFrom, CommandError> {
    match raw {
        b"$" => Ok(ReadFrom::Latest),
        b"+" => Ok(ReadFrom::Last),
        _ => Ok(ReadFrom::Id(parse_bound(raw, 0)?)),
    } // reuse XRANGE's helper
}

fn arg_after(args: &[Bytes], i: usize) -> Result<&Bytes, CommandError> {
    args.get(i + 1).ok_or(CommandError::Syntax)
}
/// Field/value pairs from a flat argument list. At least one pair, and the
/// count must be even.
fn parse_fields(args: &[Bytes], name: &Bytes) -> Result<Vec<(Bytes, Bytes)>, CommandError> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(CommandError::WrongArity(name.clone()));
    }
    Ok(args
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect())
}

fn xentries_to_value(entries: &[StreamEntry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|e| {
                let mut flat = Vec::with_capacity(e.fields.len() * 2);
                for (f, v) in &e.fields {
                    flat.push(Value::BulkString(f.clone()));
                    flat.push(Value::BulkString(v.clone()));
                }
                Value::Array(vec![Value::BulkString(e.id.to_bytes()), Value::Array(flat)])
            })
            .collect(),
    )
}

fn build_xread_reply(out: Vec<(Bytes, Value)>) -> Value {
    Value::Array(
        out.into_iter()
            .map(|(key, entries)| Value::Array(vec![Value::BulkString(key), entries]))
            .collect(),
    )
}
pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Bytes],
    name: &Bytes,
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"XADD" => xadd_command(rest, name),
        b"XLEN" => xlen_command(rest, name),
        b"XRANGE" => xrange_command(rest, name),
        b"XREAD" => xread_command(rest, name),
        b"XDEL" => xdel_command(rest, name),
        b"XTRIM" => xtrim_command(rest, name),
        _ => return None, // not a Stream command
    })
}

fn xadd_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    if rest.len() < 4 {
        return Err(CommandError::WrongArity(name.clone()));
    }
    let mut opts = TrimOptions::default();
    let mut nomkstream = false;
    let mut i = 1;
    loop {
        let Some(token) = rest.get(i) else {
            return Err(CommandError::WrongArity(name.clone()));
        };
        if token.eq_ignore_ascii_case(b"NOMKSTREAM") {
            nomkstream = true;
            i += 1;
            continue;
        }
        match opts.take(rest, i)? {
            Some(used) => i += used,
            None => break, // rest[i] is the ID
        }
    }
    let trim = opts.finish()?; // before the ID, so trim errors win, as in Redis
    let id = parse_xadd_id(&rest[i])?;
    let fields = parse_fields(&rest[i + 1..], name)?;
    if let IdSpec::Explicit(EntryId { ms: 0, seq: 0 }) = id {
        return Err(CommandError::XaddIdZero);
    }
    Ok(Command::XAdd {
        key: rest[0].clone(),
        id,
        fields,
        trim,
        nomkstream,
    })
}

fn xtrim_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    if rest.len() < 3 {
        // key, MAXLEN|MINID, threshold at the very least
        return Err(CommandError::WrongArity(name.clone()));
    }
    let mut opts = TrimOptions::default();
    let mut i = 1;
    while i < rest.len() {
        match opts.take(rest, i)? {
            Some(used) => i += used,
            None => return Err(CommandError::Syntax), // a word that isn't an option
        }
    }
    let trim = opts.finish()?.ok_or(CommandError::Syntax)?; // XTRIM must have a strategy
    Ok(Command::XTrim {
        key: rest[0].clone(),
        trim,
    })
}

fn xlen_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [key] => Ok(Command::XLen { key: key.clone() }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn xrange_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    let (key, start, end, tail) = match rest {
        [key, start, end, tail @ ..] => (key, start, end, tail),
        _ => return Err(CommandError::WrongArity(name.clone())),
    };
    let start = parse_range_start(start)?;
    let stop = parse_range_end(end)?;
    let mut count = None;
    let mut i = 0;
    while i < tail.len() {
        match tail[i].to_ascii_uppercase().as_slice() {
            b"COUNT" => {
                count = Some(parse_xrange_count(arg_after(tail, i)?)?);
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
    }
    Ok(Command::XRange {
        key: key.clone(),
        start,
        stop,
        count,
    })
}

fn xread_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    // STREAMS is the last option, so everything before it is flags and
    // everything after is N keys followed by N ids.
    if rest.len() < 3 {
        return Err(CommandError::WrongArity(name.clone()));
    }
    let mut count = None;
    let mut block = Blocking::No;
    let mut i = 0;
    let streams_at = loop {
        if i >= rest.len() {
            // ran out of arguments without ever seeing STREAMS
            return Err(CommandError::WrongArity(name.clone()));
        }
        match rest[i].to_ascii_uppercase().as_slice() {
            b"STREAMS" => break i,
            b"COUNT" => {
                count = Some(parse_i64(arg_after(rest, i)?)?);
                i += 2;
            }
            b"BLOCK" => {
                block = parse_block(arg_after(rest, i)?)?;
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
    };

    let tail = &rest[streams_at + 1..];

    if tail.is_empty() || !tail.len().is_multiple_of(2) {
        return Err(CommandError::UnbalancedXread);
    }
    let n = tail.len() / 2;
    let streams = tail[..n]
        .iter()
        .zip(&tail[n..])
        .map(|(k, id)| Ok((k.clone(), parse_read_from(id)?)))
        .collect::<Result<Vec<_>, CommandError>>()?;

    Ok(Command::XRead {
        count,
        timeout: block,
        streams,
    })
}

fn xdel_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        // Redis arity for XDEL is -3: name, key, and at least one id. Without
        // the guard, `[key, ids @ ..]` also matches a bare `XDEL k` and replies :0.
        [key, ids @ ..] if !ids.is_empty() => Ok(Command::XDel {
            key: key.clone(),
            ids: ids
                .iter()
                .map(|id| parse_bound(id, 0))
                .collect::<Result<_, CommandError>>()?,
        }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

pub(super) fn xadd(
    key: Bytes,
    id: IdSpec,
    fields: Vec<(Bytes, Bytes)>,
    trim: Option<Trim>,
    nomkstream: bool,
    db: &mut Db,
) -> Result<Outcome, CommandError> {
    Ok(Outcome::Reply(
        match db.xadd(key, id, fields, trim, nomkstream)? {
            Some(id) => Value::BulkString(id.to_bytes()),
            None => Value::NullBulkString, // NOMKSTREAM on a missing key: $-1, as Redis sends
        },
    ))
}

pub(super) fn xtrim(key: &[u8], trim: &Trim, db: &mut Db) -> Result<Outcome, CommandError> {
    Ok(Outcome::Reply(Value::Integer(db.xtrim(key, trim)? as i64)))
}

pub(super) fn xlen(key: &[u8], db: &mut Db) -> Result<Outcome, CommandError> {
    Ok(Outcome::Reply(Value::Integer(db.xlen(key)? as i64)))
}

pub(super) fn xrange(
    key: &[u8],
    start: EntryId,
    end: EntryId,
    count: Option<i64>,
    db: &mut Db,
) -> Result<Outcome, CommandError> {
    let limit = count.map(|n| if n <= 0 { 0 } else { n as usize });
    match db.xrange(key, start, end, limit)? {
        None => Ok(Outcome::Reply(Value::Array(vec![]))), // key missing
        Some(_) if count.is_some_and(|n| n <= 0) => Ok(Outcome::Reply(Value::NullArray)),
        Some(entries) => Ok(Outcome::Reply(xentries_to_value(entries))),
    }
}

pub(super) fn xread(
    count: Option<i64>,
    timeout: Blocking,
    streams: Vec<(Bytes, ReadFrom)>,
    db: &mut Db,
) -> Result<Outcome, CommandError> {
    let limit = match count {
        Some(n) if n > 0 => Some(n as usize),
        _ => None, // absent, zero, or negative: no limit
    };

    let mut out: Vec<(Bytes, Value)> = Vec::new();
    for (key, from) in &streams {
        let entries = match *from {
            ReadFrom::Last => db.xlast(key)?,
            ReadFrom::Id(id) => db.xrange_after(key, id, limit)?,
            ReadFrom::Latest => None, // `$` means added after this call, so nothing yet
        };
        match entries {
            Some(e) if !e.is_empty() => out.push((key.clone(), xentries_to_value(e))),
            _ => {}
        }
    }

    if !out.is_empty() {
        return Ok(Outcome::Reply(build_xread_reply(out)));
    }
    if matches!(timeout, Blocking::No) {
        return Ok(Outcome::Reply(Value::NullArray));
    }

    let resolved: Vec<(Bytes, ReadFrom)> = streams
        .into_iter()
        .map(|(k, from)| {
            let from = match from {
                ReadFrom::Latest => ReadFrom::Id(db.stream_last_id(&k)?),
                other => other,
            };
            Ok((k, from))
        })
        .collect::<Result<_, CommandError>>()?;

    let keys = resolved.iter().map(|(k, _)| k.clone()).collect();

    Ok(Outcome::Block {
        keys,
        retry: Command::XRead {
            count,
            timeout,
            streams: resolved,
        },
    })
}

pub(super) fn xdel(key: &[u8], ids: &[EntryId], db: &mut Db) -> Result<Outcome, CommandError> {
    Ok(Outcome::Reply(Value::Integer(db.xdel(key, ids)? as i64)))
}
