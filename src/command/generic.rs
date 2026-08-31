use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Value;

pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Vec<u8>],
    name: &[u8],
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"PING" => ping_command(rest, name),
        b"ECHO" => echo_command(rest, name),
        b"TYPE"  => type_of_command(rest, name),
        b"DEL"  => del_command(rest, name),
        b"EXISTS"  => exists_command(rest, name),
        _ => return None,
    })
}

fn ping_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [] => Ok(Command::Ping(None)),
        [msg] => Ok(Command::Ping(Some(msg.clone()))),
        _ => return Err(CommandError::WrongArity(name.to_vec())),
    }
}

fn echo_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [msg] => Ok(Command::Echo(msg.clone())),
        _ => return Err(CommandError::WrongArity(name.to_vec())),
    }
}

fn type_of_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [key] => Ok(Command::Type { key: key.clone() }),
        _ => return Err(CommandError::WrongArity(name.to_vec())),
    }
}

fn del_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [] => return Err(CommandError::WrongArity(name.to_vec())),
        keys => Ok(Command::Del { keys: keys.to_vec() }),
    }
}

fn exists_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [] => return Err(CommandError::WrongArity(name.to_vec())),
        keys => Ok(Command::Exists { keys: keys.to_vec() }),
    }
}

pub(super) fn ping(msg: Option<Vec<u8>>) -> Result<Value, CommandError> {
    match msg {
        None => Ok(Value::SimpleString(b"PONG".to_vec())),
        Some(v) => Ok(Value::BulkString(v)),
    }
}

pub(super) fn echo(msg: Vec<u8>) -> Result<Value, CommandError> {
    Ok(Value::BulkString(msg))
}

pub(super) fn type_of(key: &Vec<u8>, db: &mut Db) -> Result<Value, CommandError> {
    Ok(Value::SimpleString(match db.type_of(&key) {
        Some(t) => t.as_bytes().to_vec(),
        None => b"none".to_vec(),
    }))
}

pub(super) fn del(keys: &Vec<Vec<u8>>, db: &mut Db) -> Result<Value, CommandError> {
    let n = keys.iter().filter(|k| db.delete(k)).count();
    Ok(Value::Integer(n as i64))
}

pub(super) fn exists(keys: &Vec<Vec<u8>>, db: &mut Db) -> Result<Value, CommandError> {
    let n = keys.iter().filter(|k| db.exists(k)).count();
    Ok(Value::Integer(n as i64))
}


#[cfg(test)]
mod generic_tests {
    use super::*;
    use super::super::test_support::{cmd_ok};

    #[test]
    fn ping_and_echo_reply_with_different_types() {
        // bare PING is a simple string, PING <msg> is a bulk string
        assert_eq!(
            ping(None).unwrap(),
            Value::SimpleString(b"PONG".to_vec())
        );
        assert_eq!(
            ping(Some(b"his".to_vec())).unwrap(),
            Value::BulkString(b"his".to_vec())
        );
        assert_eq!(
            echo(b"hi".to_vec()).unwrap(),
            Value::BulkString(b"hi".to_vec())
        );
    }

    #[test]
    fn command_ping_echo_works() {
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nPING\r\n"), Command::Ping(None));
        assert_eq!(cmd_ok(b"*2\r\n$4\r\nping\r\n$2\r\nhi\r\n"), Command::Ping(Some(b"hi".to_vec())));
        assert_eq!(cmd_ok(b"*2\r\n$4\r\nECHO\r\n$4\r\necho\r\n"), Command::Echo(b"echo".to_vec()));
    }
}