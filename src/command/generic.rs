use bytes::Bytes;

use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Value;

pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Bytes],
    name: &Bytes,
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"PING" => ping_command(rest, name),
        b"ECHO" => echo_command(rest, name),
        b"TYPE" => type_of_command(rest, name),
        b"DEL" => del_command(rest, name),
        b"EXISTS" => exists_command(rest, name),
        b"MULTI" => no_args(rest, name, Command::Multi),
        // A rejected EXEC is EXECABORT in Redis, not the plain arity error.
        b"EXEC" => {
            no_args(rest, name, Command::Exec).map_err(|e| CommandError::ExecRejected(Box::new(e)))
        }
        b"DISCARD" => no_args(rest, name, Command::Discard),
        _ => return None,
    })
}

fn ping_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [] => Ok(Command::Ping(None)),
        [msg] => Ok(Command::Ping(Some(msg.clone()))),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn echo_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [msg] => Ok(Command::Echo(msg.clone())),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn type_of_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [key] => Ok(Command::Type { key: key.clone() }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

fn del_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [] => Err(CommandError::WrongArity(name.clone())),
        keys => Ok(Command::Del {
            keys: keys.to_vec(),
        }),
    }
}

fn exists_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError> {
    match rest {
        [] => Err(CommandError::WrongArity(name.clone())),
        keys => Ok(Command::Exists {
            keys: keys.to_vec(),
        }),
    }
}

fn no_args(rest: &[Bytes], name: &Bytes, cmd: Command) -> Result<Command, CommandError> {
    if rest.is_empty() {
        Ok(cmd)
    } else {
        Err(CommandError::WrongArity(name.clone()))
    }
}

pub(super) fn ping(msg: Option<Bytes>) -> Result<Value, CommandError> {
    match msg {
        None => Ok(Value::SimpleString(Bytes::from_static(b"PONG"))),
        Some(v) => Ok(Value::BulkString(v)),
    }
}

pub(super) fn echo(msg: Bytes) -> Result<Value, CommandError> {
    Ok(Value::BulkString(msg))
}

pub(super) fn type_of(key: &Bytes, db: &mut Db) -> Result<Value, CommandError> {
    Ok(Value::SimpleString(Bytes::from(match db.type_of(key) {
        Some(t) => t.as_bytes().to_vec(),
        None => b"none".to_vec(),
    })))
}

pub(super) fn del(keys: Vec<Bytes>, db: &mut Db) -> Result<Value, CommandError> {
    let mut n = 0i64;
    for k in &keys {
        if db.delete(k) {
            n += 1;
        }
    }
    Ok(Value::Integer(n))
}

pub(super) fn exists(keys: Vec<Bytes>, db: &mut Db) -> Result<Value, CommandError> {
    let mut n = 0i64;
    for k in &keys {
        if db.exists(k) {
            n += 1;
        }
    }
    Ok(Value::Integer(n))
}

#[cfg(test)]
mod generic_tests {
    use super::super::test_support::cmd_ok;
    use super::*;

    #[test]
    fn ping_and_echo_reply_with_different_types() {
        // bare PING is a simple string, PING <msg> is a bulk string
        assert_eq!(
            ping(None).unwrap(),
            Value::SimpleString(Bytes::from_static(b"PONG"))
        );
        assert_eq!(
            ping(Some(Bytes::from_static(b"his"))).unwrap(),
            Value::BulkString(Bytes::from_static(b"his"))
        );
        assert_eq!(
            echo(Bytes::from_static(b"hi")).unwrap(),
            Value::BulkString(Bytes::from_static(b"hi"))
        );
    }

    #[test]
    fn command_ping_echo_works() {
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nPING\r\n"), Command::Ping(None));
        assert_eq!(
            cmd_ok(b"*2\r\n$4\r\nping\r\n$2\r\nhi\r\n"),
            Command::Ping(Some(Bytes::from_static(b"hi")))
        );
        assert_eq!(
            cmd_ok(b"*2\r\n$4\r\nECHO\r\n$4\r\necho\r\n"),
            Command::Echo(Bytes::from_static(b"echo"))
        );
    }
}
