use bytes::Bytes;

use super::args::{parse_end, parse_i64, parse_timeout};
use super::{Command, CommandError, Outcome, Timeout};
use crate::db::{Db, End};
use crate::resp::Value;

pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Bytes],
    name: &Bytes,
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"RPUSH" => push_command(rest, name, End::Right),
        b"LPUSH" => push_command(rest, name, End::Left),
        b"LPOP" => pop_command(rest, name, End::Left),
        b"RPOP" => pop_command(rest, name, End::Right),
        b"BLPOP" => bpop_command(rest, name, End::Left),
        b"BRPOP" => bpop_command(rest, name, End::Right),
        b"LLEN" => llen_command(rest, name),
        b"LRANGE" => lrange_command(rest, name),
        b"LMOVE" => lmove_command(rest, name),
        b"BLMOVE" => blmove_command(rest, name),
        _ => return None, // not a list command
    })
}

fn push_command(rest: &[Bytes], name: &Bytes, from: End) -> Result<Command, CommandError> {
    match rest {
        [key, first, more @ ..] => Ok(Command::Push {
            key: key.clone(),
            values: std::iter::once(first).chain(more).cloned().collect(),
            from,
        }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn pop_command(rest: &[Bytes], name: &Bytes, from: End) -> Result<Command, CommandError> {
    let (key, raw_count) = match rest {
        [key] => (key, None),
        [key, c] => (key, Some(c)),
        _ => return Err(CommandError::WrongArity(name.clone())),
    };

    // Not a number and negative share one message.
    let count = match raw_count {
        None => None,
        Some(raw) => Some(
            parse_i64(raw)
                .ok()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or(CommandError::OutOfRange)?,
        ),
    };

    Ok(Command::Pop {
        key: key.clone(),
        count,
        from,
    })
}

fn bpop_command(rest: &[Bytes], name: &Bytes, from: End) -> Result<Command, CommandError> {
    match rest {
        [keys @ .., timeout] if !keys.is_empty() => Ok(Command::BPop {
            keys: keys.to_vec(),
            from,
            timeout: parse_timeout(timeout)?,
        }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn llen_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [key] => Ok(Command::LLen { key: key.clone() }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn lrange_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [key, start, stop] => Ok(Command::LRange {
            key: key.clone(),
            start: parse_i64(start)?,
            stop: parse_i64(stop)?,
        }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn lmove_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [src, dst, from, to] => Ok(Command::LMove {
            src: src.clone(),
            dst: dst.clone(),
            from: parse_end(from)?,
            to: parse_end(to)?,
        }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn blmove_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [src, dst, from, to, timeout] => Ok(Command::BLMove {
            src: src.clone(),
            dst: dst.clone(),
            from: parse_end(from)?,
            to: parse_end(to)?,
            timeout: parse_timeout(timeout)?,
        }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

pub(super) fn push(
    key: Bytes,
    values: Vec<Bytes>,
    from: End,
    db: &mut Db,
) -> Result<Value, CommandError> {
    Ok(Value::Integer(db.push(key, values, from)? as i64))
}
pub(super) fn pop(
    key: &Bytes,
    count: Option<usize>,
    from: End,
    db: &mut Db,
) -> Result<Value, CommandError> {
    let popped = db.pop(key, count, from)?;
    Ok(match (count, popped) {
        (None, Some(v)) => v
            .into_iter()
            .next()
            .map_or(Value::NullBulkString, Value::BulkString),
        (None, None) => Value::NullBulkString,
        (Some(_), Some(v)) => Value::Array(v.into_iter().map(Value::BulkString).collect()),
        (Some(_), None) => Value::NullArray,
    })
}

pub(super) fn bpop(
    keys: Vec<Bytes>,
    from: End,
    timeout: Timeout,
    db: &mut Db,
) -> Result<Outcome, CommandError> {
    for key in &keys {
        if let Some(v) = db.pop(key, None, from)?
            && let Some(elem) = v.into_iter().next()
        {
            return Ok(Outcome::Reply(Value::Array(vec![
                Value::BulkString(key.clone()),
                Value::BulkString(elem),
            ])));
        }
    }

    Ok(Outcome::Block {
        keys: keys.clone(),
        retry: Command::BPop {
            keys,
            from,
            timeout,
        },
    })
}

pub(super) fn llen(key: &Bytes, db: &mut Db) -> Result<Value, CommandError> {
    Ok(Value::Integer(db.llen(key)? as i64))
}
pub(super) fn lrange(
    key: &Bytes,
    start: i64,
    stop: i64,
    db: &mut Db,
) -> Result<Value, CommandError> {
    Ok(Value::Array(
        db.lrange(key, start, stop)?
            .into_iter()
            .map(Value::BulkString)
            .collect(),
    ))
}
pub(super) fn lmove(
    src: &Bytes,
    dst: Bytes,
    from: End,
    to: End,
    db: &mut Db,
) -> Result<Value, CommandError> {
    Ok(match db.lmove(src, dst, from, to)? {
        Some(v) => Value::BulkString(v),
        None => Value::NullBulkString,
    })
}

pub(super) fn blmove(
    src: Bytes,
    dst: Bytes,
    from: End,
    to: End,
    timeout: Timeout,
    db: &mut Db,
) -> Result<Outcome, CommandError> {
    match db.lmove(&src, dst.clone(), from, to)? {
        Some(v) => Ok(Outcome::Reply(Value::BulkString(v))),
        None => Ok(Outcome::Block {
            keys: vec![src.clone()],
            retry: Command::BLMove {
                src,
                dst,
                from,
                to,
                timeout,
            },
        }),
    }
}

#[cfg(test)]
mod list_tests {
    use super::*;
    use crate::test_support::{b, cmd, cmd_ok};

    #[test]
    fn pop_count_errors_use_out_of_range() {
        assert!(matches!(
            cmd(&["LPOP", "k", "abc"]),
            Err(CommandError::OutOfRange)
        ));
        assert!(matches!(
            cmd(&["LPOP", "k", "-1"]),
            Err(CommandError::OutOfRange)
        ));
        // arity is checked before the count is parsed
        assert!(matches!(
            cmd(&["LPOP", "k", "abc", "x"]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn bare_pop_and_pop_one_are_different_commands() {
        // `LPOP k` replies with a bulk string, `LPOP k 1` with an array.
        match cmd_ok(&["LPOP", "k"]) {
            Command::Pop { count, .. } => assert_eq!(count, None),
            other => panic!("{other:?}"),
        }
        match cmd_ok(&["LPOP", "k", "1"]) {
            Command::Pop { count, .. } => assert_eq!(count, Some(1)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn lmove_directions_are_case_insensitive() {
        assert_eq!(
            cmd_ok(&["lmove", "a", "b", "left", "right"]),
            Command::LMove {
                src: b("a"),
                dst: b("b"),
                from: End::Left,
                to: End::Right,
            }
        );
        assert!(matches!(
            cmd(&["LMOVE", "a", "b", "SIDEWAYS", "RIGHT"]),
            Err(CommandError::Syntax)
        ));
    }
}
