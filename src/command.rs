use crate::resp::{Value, RespError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Command {
    Ping(Option<Vec<u8>>),   // PING and PING <msg> are both legal
    Echo(Vec<u8>),
    Unknown(Vec<u8>),        // keep the name so the error can quote it
}

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
        _ => Ok(Command::Unknown(name.clone())),
    }


}

pub fn execute(cmd: Command) -> Value {
    match cmd {
        Command::Ping(None) => Value::SimpleString(b"PONG".to_vec()),
        Command::Ping(Some(msg)) => Value::BulkString(msg),
        Command::Echo(msg) => Value::BulkString(msg),
        Command::Unknown(name) => Value::Error(
            format!("-ERR unknown command '{}'", String::from_utf8_lossy(&name)).into_bytes(),
        ),
    }
}