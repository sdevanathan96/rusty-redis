use std::time::Duration;

use bytes::Bytes;

use super::args::{ExpiryUnit, expiry_arg};
use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Value;

pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Bytes],
    name: &Bytes,
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"GET" => get_command(rest, name),
        b"SET" => set_command(rest, name),
        b"INCR" => incr_command(rest, name),
        _ => return None, // not a string command
    })
}

fn get_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [key] => Ok(Command::Get { key: key.clone() }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn set_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    let (key, value) = match rest {
        [k, v, ..] => (k.clone(), v.clone()),
        _ => return Err(CommandError::WrongArity(name.clone())),
    };

    let mut expiry: Option<Duration> = None;
    let mut i = 2;
    while i < rest.len() {
        match rest[i].to_ascii_uppercase().as_slice() {
            b"PX" => {
                if expiry.is_some() {
                    return Err(CommandError::Syntax); // conflicting expiry options
                }
                expiry = Some(expiry_arg(rest, i, name, ExpiryUnit::Millis)?);
                i += 2;
            }
            b"EX" => {
                if expiry.is_some() {
                    return Err(CommandError::Syntax);
                }
                expiry = Some(expiry_arg(rest, i, name, ExpiryUnit::Seconds)?);
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
    }

    Ok(Command::Set { key, value, expiry })
}

fn incr_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [key] => Ok(Command::Incr { key: key.clone() }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

pub(super) fn get(key: &Bytes, db: &mut Db) -> Result<Value, CommandError> {
    Ok(match db.get(key)? {
        Some(v) => Value::BulkString(v),
        None => Value::NullBulkString,
    })
}

pub(super) fn set(
    key: Bytes,
    value: Bytes,
    expiry: Option<Duration>,
    db: &mut Db,
) -> Result<Value, CommandError> {
    db.set(key, value, expiry)
        .map_err(|_| CommandError::InvalidExpiry(Bytes::from_static(b"set")))?;
    Ok(Value::ok())
}

pub(super) fn incr(key: &Bytes, db: &mut Db) -> Result<Value, CommandError> {
    Ok(Value::Integer(db.incr(key.clone())?))
}

#[cfg(test)]
mod string_tests {
    use super::*;
    use crate::test_support::{b, cmd, cmd_ok};

    #[test]
    fn option_keyword_is_case_insensitive() {
        assert_eq!(
            cmd_ok(&["set", "k", "v", "px", "100"]),
            Command::Set {
                key: b("k"),
                value: b("v"),
                expiry: Some(Duration::from_millis(100)),
            }
        );
    }

    #[test]
    fn ex_is_seconds_px_is_millis() {
        let ex = cmd_ok(&["SET", "k", "v", "EX", "1"]);
        let px = cmd_ok(&["SET", "k", "v", "PX", "1"]);
        match (ex, px) {
            (
                Command::Set {
                    expiry: Some(a), ..
                },
                Command::Set {
                    expiry: Some(b), ..
                },
            ) => {
                assert_eq!(a, Duration::from_secs(1));
                assert_eq!(b, Duration::from_millis(1));
            }
            other => panic!("expected two Set commands with expiry, got {other:?}"),
        }
    }

    #[test]
    fn set_without_expiry_has_none() {
        assert_eq!(
            cmd_ok(&["SET", "k", "v"]),
            Command::Set {
                key: b("k"),
                value: b("v"),
                expiry: None,
            }
        );
    }

    #[test]
    fn expiry_errors_are_distinct() {
        assert!(matches!(
            cmd(&["SET", "k", "v", "PX"]),
            Err(CommandError::Syntax)
        ));
        assert!(matches!(
            cmd(&["SET", "k", "v", "PX", "abc"]),
            Err(CommandError::NotAnInteger)
        ));
        assert!(matches!(
            cmd(&["SET", "k", "v", "PX", "0"]),
            Err(CommandError::InvalidExpiry(_))
        ));
        assert!(matches!(
            cmd(&["SET", "k", "v", "ZZ", "5"]),
            Err(CommandError::Syntax)
        ));
    }

    #[test]
    fn conflicting_expiry_options_are_rejected() {
        assert!(matches!(
            cmd(&["SET", "k", "v", "PX", "100", "EX", "5"]),
            Err(CommandError::Syntax)
        ));
    }
}
