use crate::{command::{Blocking, Command, CommandError, Outcome, parse_block, parse_i64}, db::{Db, EntryId, IdSpec, ReadFrom, StreamEntry}, resp::Value};

fn parse_xadd_id(raw: &[u8]) -> Result<IdSpec, CommandError> {
    if raw == b"*" {
        return Ok(IdSpec::Auto);
    }

    let text = std::str::from_utf8(raw).map_err(|_| CommandError::InvalidStreamId)?;

    match text.split_once('-') {
        None => {
            // bare millisecond: sequence is zero
            let ms = text.parse().map_err(|_| CommandError::InvalidStreamId)?;
            Ok(IdSpec::Explicit(EntryId { ms, seq: 0 }))
        }
        Some((ms, "*")) => {
            let ms = ms.parse().map_err(|_| CommandError::InvalidStreamId)?;
            Ok(IdSpec::AutoSeq(ms))
        }
        Some((ms, seq)) => Ok(IdSpec::Explicit(EntryId {
            ms: ms.parse().map_err(|_| CommandError::InvalidStreamId)?,
            seq: seq.parse().map_err(|_| CommandError::InvalidStreamId)?,
        })),
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
        return Ok(EntryId { ms: u64::MAX, seq: u64::MAX });
    }
    parse_bound(raw, u64::MAX)
}

/// `default_seq` is what a bare millisecond gets: 0 for a start bound,
/// u64::MAX for an end bound, so `XRANGE k 5 5` spans all of millisecond 5.
fn parse_bound(raw: &[u8], default_seq: u64) -> Result<EntryId, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::InvalidStreamId)?;
    match text.split_once('-') {
        None => Ok(EntryId {
            ms: text.parse().map_err(|_| CommandError::InvalidStreamId)?,
            seq: default_seq,
        }),
        Some((ms, seq)) => Ok(EntryId {
            ms: ms.parse().map_err(|_| CommandError::InvalidStreamId)?,
            seq: seq.parse().map_err(|_| CommandError::InvalidStreamId)?,
        }),
    }
}

fn parse_xrange_count(raw: &[u8]) -> Result<i64, CommandError> {
    parse_i64(raw)
}

fn parse_read_from(raw: &[u8]) -> Result<ReadFrom, CommandError> {
    match raw {
        b"$" => Ok(ReadFrom::Latest),
        b"+" => Ok(ReadFrom::Last),
        _ => Ok(ReadFrom::Id(parse_bound(raw, 0)?)) 
    }    // reuse XRANGE's helper
}

fn arg_after(args: &[Vec<u8>], i: usize) -> Result<&Vec<u8>, CommandError> {
    args.get(i + 1).ok_or(CommandError::Syntax)
}
/// Field/value pairs from a flat argument list. At least one pair, and the
/// count must be even.
fn parse_fields(
    args: &[Vec<u8>],
    name: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CommandError> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(CommandError::WrongArity(name.to_vec()));
    }
    Ok(args
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect())
}

fn parse_xresp(entries: Vec<(EntryId, Vec<(Vec<u8>, Vec<u8>)>)>) -> Value {
    Value::Array(entries
    .into_iter()
    .map(|(id, fields)| {
        let mut flat = Vec::with_capacity(fields.len() * 2);
        for (f, v) in fields {
            flat.push(Value::BulkString(f));
            flat.push(Value::BulkString(v));
        }
        Value::Array(vec![
            Value::BulkString(id.to_bytes()),
            Value::Array(flat),
        ])
    })
    .collect())
}

fn build_xread_reply(out: Vec<(Vec<u8>, Vec<(EntryId, Vec<(Vec<u8>, Vec<u8>)>)>)>) -> Value {
    Value::Array(
        out.into_iter()
            .map(|(key, entries)| {
                Value::Array(vec![
                    Value::BulkString(key),
                    parse_xresp(entries),
                ])
            })
            .collect(),
    )
}

pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Vec<u8>],
    name: &[u8],
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"XADD" => xadd_command(rest, name),
        b"XLEN" => xlen_command(rest, name),
        b"XRANGE" => xrange_command(rest, name),
        b"XREAD" => xread_command(rest),
        _ => return None,          // not a list command
    })
}


fn xadd_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [key, id, fields @ ..] => Ok(Command::XAdd {
            key: key.clone(),
            id: parse_xadd_id(id)?,
            fields: parse_fields(fields, name)?,
        }),
        _ => return Err(CommandError::WrongArity(name.to_vec())),
    }
}

fn xlen_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [key] => Ok(Command::XLen { key: key.clone() }),
        _ => return Err(CommandError::WrongArity(name.to_vec())),
    }
}

fn xrange_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {

    let (key, start, end, tail) = match rest {
        [key, start, end, tail @ ..] => (key, start, end, tail),
        _ => return Err(CommandError::WrongArity(name.to_vec())),
    };

    let mut count = None;
    let mut i = 0;
    while i < tail.len() {
        match tail[i].to_ascii_uppercase().as_slice() {
            b"COUNT" => {
                let raw = tail.get(i + 1).ok_or(CommandError::Syntax)?;
                count = Some(parse_xrange_count(raw)?);
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
    }
    Ok(Command::XRange { 
        key: key.clone(),
        start: parse_range_start(start)?,
        stop: parse_range_end(end)?,
        count: count 
    })
}

fn xread_command(rest: &[Vec<u8>]) -> Result<Command, CommandError> {

    // STREAMS is the last option, so everything before it is flags and
    // everything after is N keys followed by N ids.
    let pos = rest
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))
        .ok_or(CommandError::Syntax)?;
    let (opts, tail) = (&rest[..pos], &rest[pos + 1..]);

    let mut count = None;
    let mut block = Blocking::No;
    let mut i = 0;
    while i < opts.len() {
        match opts[i].to_ascii_uppercase().as_slice() {
            b"COUNT" => {
                count = Some(parse_i64(arg_after(opts, i)?)?);
                i += 2;
            }
            b"BLOCK" => {
                block = parse_block(arg_after(opts, i)?)?;
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
    }

    if tail.is_empty() || tail.len() % 2 != 0 {
        return Err(CommandError::UnbalancedXread);
    }
    let n = tail.len() / 2;
    let streams = tail[..n]
        .iter()
        .zip(&tail[n..])
        .map(|(k, id)| Ok((k.clone(), parse_read_from(id)?)))
        .collect::<Result<Vec<_>, CommandError>>()?;

    Ok(Command::XRead { count, timeout: block, streams })
}

pub(super) fn xadd(key: &Vec<u8>, id: IdSpec, fields: Vec<(Vec<u8>, Vec<u8>)>, db: &mut Db) -> Result<Outcome, CommandError> {
    Ok(Outcome::Reply(Value::BulkString(db.xadd(key, id, fields)?.to_bytes())))
}

pub(super) fn xlen(key: &Vec<u8>, db: &mut Db) -> Result<Outcome, CommandError> {
    Ok(Outcome::Reply(Value::Integer(db.xlen(&key)? as i64)))
}

pub(super) fn xrange(key: &Vec<u8>, start: EntryId, end: EntryId, count: Option<i64>, db: &mut Db) -> Result<Outcome, CommandError> {
    let limit = count.map(|n| if n <= 0 { 0 } else { n as usize });
    match db.xrange(&key, start, end, limit)? {
        None => Ok(Outcome::Reply(Value::Array(vec![]))),                    // key missing
        Some(_) if count.is_some_and(|n| n <= 0) => Ok(Outcome::Reply(Value::NullArray)),
        Some(entries) => Ok(Outcome::Reply(parse_xresp(entries))),
    }
}

pub(super) fn xread(
    count: Option<i64>,
    timeout: super::Blocking,
    streams: Vec<(Vec<u8>, ReadFrom)>,
    db: &mut Db,
) -> Result<Outcome, CommandError> {
    let limit = count.map(|n| if n <= 0 { 0 } else { n as usize });

    let mut out: Vec<(Vec<u8>, Vec<StreamEntry>)> = Vec::new();
    for (key, from) in &streams {
        let entries = match *from {
            ReadFrom::Last => db.xlast(key)?,
            ReadFrom::Id(id) => db.xrange_after(key, id, limit)?,
            ReadFrom::Latest => {
                let last = db.stream_last_id(key)?;
                db.xrange_after(key, last, limit)?
            }
        };
        match entries {
            Some(e) if !e.is_empty() => out.push((key.clone(), e)),
            _ => {}
        }
    }

    if !out.is_empty() {
        return Ok(Outcome::Reply(build_xread_reply(out)));
    }
    if matches!(timeout, Blocking::No) {
        return Ok(Outcome::Reply(Value::NullArray));
    }

    let resolved: Vec<(Vec<u8>, ReadFrom)> = streams
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
        retry: Command::XRead { count: count, timeout: timeout, streams: resolved },
    })
}