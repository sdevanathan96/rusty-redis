use crate::resp::{Value, RespError, parse};
use crate::db::Db;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Command {
    Ping(Option<Vec<u8>>),   // PING and PING <msg> are both legal
    Echo(Vec<u8>),
    Get{_key: Vec<u8>},
    Set{_key: Vec<u8>, _value: Vec<u8>},
    Unknown(Vec<u8>),        // keep the name so the error can quote it
}
#[derive(Debug)]
pub enum CommandError {
    UnknownCommand(Vec<u8>),
    NotBulkString,
    WrongNumberOfArguments(usize, usize), // expected, actual
    NotAnArray,
}

impl CommandError {
    pub fn to_resp(&self) -> Vec<u8> {
        match self {
            CommandError::UnknownCommand(name) => format!("ERR unknown command '{}'", String::from_utf8_lossy(name)).into_bytes(),
            CommandError::NotBulkString => b"ERR argument is not a bulk string".to_vec(),
            CommandError::WrongNumberOfArguments(expected, actual) => format!("ERR wrong number of arguments (expected {}, got {})", expected, actual).into_bytes(),
            CommandError::NotAnArray => b"ERR command is not an array".to_vec(),
        }
    }
}

pub fn to_command(v: Value) -> Result<Command, CommandError> {
    let items = match v {
        Value::Array(items) => items,
        _ => return Err(CommandError::NotAnArray),
    };
    let mut args = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::BulkString(bytes) => args.push(bytes),
            _ => return Err(CommandError::NotBulkString),
        }
    }
    let (name, rest) = args.split_first().ok_or(CommandError::WrongNumberOfArguments(1, 0))?;
    match name.to_ascii_uppercase().as_slice() {
        b"PING" => match rest.len() {
            0 => Ok(Command::Ping(None)),
            1 => Ok(Command::Ping(Some(rest[0].clone()))),
            _ => Err(CommandError::WrongNumberOfArguments(1, rest.len())),
        },
        b"ECHO" => match rest.len() {
            1 => Ok(Command::Echo(rest[0].clone())),
            _ => Err(CommandError::WrongNumberOfArguments(1, rest.len())),
        },
        b"GET" => match rest.len() {
            1 => Ok(Command::Get { _key: rest[0].clone() }),
            _ => Err(CommandError::WrongNumberOfArguments(1, rest.len())),
        },
        b"SET" => match rest.len() {
            2 => Ok(Command::Set { _key: rest[0].clone(), _value: rest[1].clone() }),
            _ => Err(CommandError::WrongNumberOfArguments(2, rest.len())),
        },
        _ => Ok(Command::Unknown(name.clone())),
    }


}

pub fn execute(cmd: Command, db: &Db) -> Value {
    match cmd {
        Command::Ping(None) => Value::SimpleString(b"PONG".to_vec()),
        Command::Ping(Some(msg)) => Value::BulkString(msg),
        Command::Echo(msg) => Value::BulkString(msg),
        Command::Unknown(name) => Value::Error(
            format!("-ERR unknown command '{}'", String::from_utf8_lossy(&name)).into_bytes(),
        ),
        Command::Set { _key, _value } => {
            db.set(_key, _value);
            Value::SimpleString(b"OK".to_vec())
        },
        Command::Get { _key } => match db.get(&_key) {
            Some(v) => Value::BulkString(v),
            None => Value::NullBulkString,
        },
    }
}

#[cfg(test)]

mod command_tests {
    use super::*;

    #[test]
    fn test_error_messages_have_no_sigil() {
        for msg in [
            RespError::BadTerminator.to_resp(),
            CommandError::UnknownCommand(b"FOO".to_vec()).to_resp(),
        ] {
            assert_ne!(msg.first(), Some(&b'-'), "to_resp must not include the sigil");
            assert!(!msg.ends_with(b"\r\n"), "to_resp must not include CRLF");
        }
    }

    #[test]
    fn test_command_to_resp() {
        let cmd = Command::Ping(Some(b"hello".to_vec()));
        let reply = execute(cmd, &Db::new());
        assert_eq!(reply, Value::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn set_then_get() {
        let db = Db::new();
        db.set(b"k".to_vec(), b"v".to_vec());
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(db.get(b"missing"), None);
    }

    #[test]
    fn get_missing_key_is_null_bulk_string() {
        let db = Db::new();
        let cmd = to_command(parse(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").unwrap().unwrap().1).unwrap();
        assert_eq!(execute(cmd, &db), Value::NullBulkString);
    }

    #[test]
    fn concurrent_writes_do_not_lose_updates() {
        let db = Arc::new(Db::new());
        let mut handles = Vec::new();
        for t in 0..8 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                for i in 0..1000 {
                    db.set(format!("k{t}-{i}").into_bytes(), b"v".to_vec());
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(db.get(b"k7-999"), Some(b"v".to_vec()));
    }

}
