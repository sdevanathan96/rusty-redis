use crate::{command::{parse_i64, Command, CommandError, Outcome}, db::{Db, EntryId, IdSpec}, resp::Value};

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

pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Vec<u8>],
    name: &[u8],
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"XADD" => xadd_command(rest, name),
        b"XLEN" => xlen_command(rest, name),
        b"XRANGE" => xrange_command(rest, name),
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
        Some(entries) => Ok(Outcome::Reply(Value::Array(
            entries
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
            .collect(),
        ))),
    }
}