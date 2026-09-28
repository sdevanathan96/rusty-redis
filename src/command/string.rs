use std::time::Duration;

use bytes::Bytes;

use super::{expiry_arg, Command, CommandError, ExpiryUnit};
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
        _ => return None,          // not a list command
    })
}

fn get_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError>{
    match rest {
        [key] => Ok(Command::Get { key: key.clone() }),
        _ => return Err(CommandError::WrongArity(name.clone())),
    }
}
fn set_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError>{
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

pub(super) fn get(key: &Bytes, db: &mut Db) -> Result<Value, CommandError> {
    Ok(match db.get(key)? {
        Some(v) => Value::BulkString(v),
        None => Value::NullBulkString,
    })
}

pub(super) fn set(key: Bytes, value: Bytes, expiry: Option<Duration>, db: &mut Db) -> Result<Value, CommandError> {
    db.set(key, value, expiry);
    Ok(Value::SimpleString(Bytes::from_static(b"OK")))
}

#[cfg(test)]
mod string_tests {
    use super::*;
    use super::super::test_support::{cmd, cmd_ok};

        #[test]
    fn option_keyword_is_case_insensitive() {
        assert_eq!(
            cmd_ok(b"*5\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\npx\r\n$3\r\n100\r\n"),
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                expiry: Some(Duration::from_millis(100)),
            }
        );
    }

    /// The bug an `nc` test cannot see: EX 1 and PX 1 both look alive a
    /// millisecond later.
    #[test]
    fn ex_is_seconds_px_is_millis() {
        let ex = cmd_ok(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n1\r\n");
        let px = cmd_ok(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$1\r\n1\r\n");
        match (ex, px) {
            (Command::Set { expiry: Some(a), .. }, Command::Set { expiry: Some(b), .. }) => {
                assert_eq!(a, Duration::from_secs(1));
                assert_eq!(b, Duration::from_millis(1));
            }
            other => panic!("expected two Set commands with expiry, got {other:?}"),
        }
    }

    #[test]
    fn set_without_expiry_has_none() {
        assert_eq!(
            cmd_ok(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"),
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                expiry: None,
            }
        );
    }

    #[test]
    fn expiry_errors_are_distinct() {
        assert!(matches!(
            cmd(b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n"),
            Err(CommandError::Syntax)
        ));
        assert!(matches!(
            cmd(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$3\r\nabc\r\n"),
            Err(CommandError::NotAnInteger)
        ));
        assert!(matches!(
            cmd(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$1\r\n0\r\n"),
            Err(CommandError::InvalidExpiry(_))
        ));
        assert!(matches!(
            cmd(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nZZ\r\n$1\r\n5\r\n"),
            Err(CommandError::Syntax)
        ));
    }

    #[test]
    fn conflicting_expiry_options_are_rejected() {
        assert!(matches!(
            cmd(b"*7\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$3\r\n100\r\n$2\r\nEX\r\n$1\r\n5\r\n"),
            Err(CommandError::Syntax)
        ));
    }
}