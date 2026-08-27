use crate::db::Db;
use crate::resp::Value;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Command {
    Ping(Option<Vec<u8>>),
    Echo(Vec<u8>),
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Option<Duration>,
    },
    Unknown(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    NotAnArray,
    NotBulkString,
    EmptyCommand,
    WrongArity(Vec<u8>),
    NotAnInteger,
    InvalidExpiry(Vec<u8>),
    Syntax,
    UnknownCommand(Vec<u8>),
}

impl CommandError {
    pub fn to_resp(&self) -> Vec<u8> {
        match self {
            CommandError::NotAnArray => b"ERR Protocol error: expected an array of bulk strings".to_vec(),
            CommandError::NotBulkString => b"ERR Protocol error: expected a bulk string".to_vec(),
            CommandError::EmptyCommand => b"ERR empty command".to_vec(),
            CommandError::WrongArity(name) => format!(
                "ERR wrong number of arguments for '{}' command",
                lower(name)
            )
            .into_bytes(),
            CommandError::NotAnInteger => b"ERR value is not an integer or out of range".to_vec(),
            CommandError::InvalidExpiry(name) => {
                format!("ERR invalid expire time in '{}' command", lower(name)).into_bytes()
            }
            CommandError::Syntax => b"ERR syntax error".to_vec(),
            CommandError::UnknownCommand(name) => {
                format!("ERR unknown command '{}'", quote(name)).into_bytes()
            },
        }
    }
}

fn lower(name: &[u8]) -> String {
    String::from_utf8_lossy(name).to_lowercase()
}

pub fn to_command(v: Value) -> Result<Command, CommandError> {
    let items = match v {
        Value::Array(items) => items,
        _ => return Err(CommandError::NotAnArray),
    };

    let mut args: Vec<Vec<u8>> = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::BulkString(bytes) => args.push(bytes),
            _ => return Err(CommandError::NotBulkString),
        }
    }

    let (name, rest) = args.split_first().ok_or(CommandError::EmptyCommand)?;

    match name.to_ascii_uppercase().as_slice() {
        b"PING" => match rest {
            [] => Ok(Command::Ping(None)),
            [msg] => Ok(Command::Ping(Some(msg.clone()))),
            _ => Err(CommandError::WrongArity(name.clone())),
        },

        b"ECHO" => match rest {
            [msg] => Ok(Command::Echo(msg.clone())),
            _ => Err(CommandError::WrongArity(name.clone())),
        },

        b"GET" => match rest {
            [key] => Ok(Command::Get { key: key.clone() }),
            _ => Err(CommandError::WrongArity(name.clone())),
        },

        b"SET" => {
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
                            return Err(CommandError::Syntax);
                        }
                        expiry = Some(Duration::from_millis(expiry_arg(rest, i, name)? as u64));
                        i += 2;
                    }
                    b"EX" => {
                        if expiry.is_some() {
                            return Err(CommandError::Syntax);
                        }
                        expiry = Some(Duration::from_secs(expiry_arg(rest, i, name)? as u64));
                        i += 2;
                    }
                    _ => return Err(CommandError::Syntax),
                }
            }

            Ok(Command::Set { key, value, expiry })
        }

        _ => Ok(Command::Unknown(name.clone())),
    }
}

/// Reads the argument following the option keyword at index `i`.
fn expiry_arg(rest: &[Vec<u8>], i: usize, cmd_name: &[u8]) -> Result<i64, CommandError> {
    let raw = rest.get(i + 1).ok_or(CommandError::Syntax)?;
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::NotAnInteger)?;
    let n: i64 = text.parse().map_err(|_| CommandError::NotAnInteger)?;
    if n <= 0 {
        return Err(CommandError::InvalidExpiry(cmd_name.to_vec()));
    }
    Ok(n)
}

pub fn execute(cmd: Command, db: &Db) -> Value {
    match cmd {
        Command::Ping(None) => Value::SimpleString(b"PONG".to_vec()),
        Command::Ping(Some(msg)) => Value::BulkString(msg),

        Command::Echo(msg) => Value::BulkString(msg),

        Command::Set { key, value, expiry } => {
            db.set(key, value, expiry);
            Value::SimpleString(b"OK".to_vec())
        }

        Command::Get { key } => match db.get(&key) {
            Some(v) => Value::BulkString(v),
            None => Value::NullBulkString,
        },
        Command::Unknown(name) => {
            Value::Error(CommandError::UnknownCommand(name).to_resp())
        },
    }
}


/// Renders client supplied bytes for an error message. Escapes so the message
/// reads clearly, truncates so a huge argument cannot fill the reply.
fn quote(bytes: &[u8]) -> String {
    const MAX: usize = 64;
    let mut s = String::new();
    for &b in bytes.iter().take(MAX) {
        match b {
            b'\\' => s.push_str("\\\\"),
            b'\'' => s.push_str("\\'"),
            b'\r' => s.push_str("\\r"),
            b'\n' => s.push_str("\\n"),
            0x20..=0x7e => s.push(b as char),
            other => s.push_str(&format!("\\x{other:02x}")),
        }
    }
    if bytes.len() > MAX {
        s.push_str("...");
    }
    s
}

#[cfg(test)]
mod command_tests {
    use super::*;
    use crate::db::TestClock;
    use crate::resp::{encode, parse};
    use std::sync::Arc;

    fn cmd(bytes: &[u8]) -> Result<Command, CommandError> {
        to_command(parse(bytes).unwrap().unwrap().1)
    }

    fn db() -> Db {
        Db::with_clock(Arc::new(TestClock::new()))
    }

    #[test]
    fn command_name_is_case_insensitive() {
        assert_eq!(cmd(b"*1\r\n$4\r\nPING\r\n").unwrap(), Command::Ping(None));
        assert_eq!(cmd(b"*1\r\n$4\r\nping\r\n").unwrap(), Command::Ping(None));
        assert_eq!(cmd(b"*1\r\n$4\r\nPiNg\r\n").unwrap(), Command::Ping(None));
    }

    #[test]
    fn option_keyword_is_case_insensitive() {
        let c = cmd(b"*5\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\npx\r\n$3\r\n100\r\n").unwrap();
        assert_eq!(
            c,
            Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expiry: Some(Duration::from_millis(100)),
            }
        );
    }

    /// The bug an `nc` test cannot see: EX 1 and PX 1 both look alive a
    /// millisecond later.
    #[test]
    fn ex_is_seconds_px_is_millis() {
        let ex = cmd(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n1\r\n").unwrap();
        let px = cmd(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$1\r\n1\r\n").unwrap();
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
        let c = cmd(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").unwrap();
        assert_eq!(
            c,
            Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
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

    #[test]
    fn request_must_be_an_array_of_bulk_strings() {
        assert!(matches!(cmd(b":5\r\n"), Err(CommandError::NotAnArray)));
        assert!(matches!(cmd(b"*1\r\n:5\r\n"), Err(CommandError::NotBulkString)));
        assert!(matches!(cmd(b"*0\r\n"), Err(CommandError::EmptyCommand)));
    }

    #[test]
    fn arity_errors() {
        assert!(matches!(
            cmd(b"*1\r\n$4\r\nECHO\r\n"),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            cmd(b"*3\r\n$4\r\nECHO\r\n$1\r\na\r\n$1\r\nb\r\n"),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            cmd(b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n"),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn ping_and_echo_reply_with_different_types() {
        let d = db();
        // bare PING is a simple string, PING <msg> is a bulk string
        assert_eq!(
            execute(Command::Ping(None), &d),
            Value::SimpleString(b"PONG".to_vec())
        );
        assert_eq!(
            execute(Command::Ping(Some(b"hi".to_vec())), &d),
            Value::BulkString(b"hi".to_vec())
        );
        assert_eq!(
            execute(Command::Echo(b"hi".to_vec()), &d),
            Value::BulkString(b"hi".to_vec())
        );
    }

    #[test]
    fn get_missing_key_is_null_bulk_string() {
        let d = db();
        let c = cmd(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").unwrap();
        assert_eq!(execute(c, &d), Value::NullBulkString);
    }

    #[test]
    fn set_then_get_round_trip_through_the_command_layer() {
        let d = db();
        execute(cmd(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").unwrap(), &d);
        assert_eq!(
            execute(cmd(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").unwrap(), &d),
            Value::BulkString(b"v".to_vec())
        );
    }

    /// Checks the encoded bytes, not `to_resp`, because the double-sigil bug
    /// lived in an inline `format!` that a `to_resp` test could not reach.
    #[test]
    fn error_replies_have_exactly_one_sigil() {
        let d = db();
        let reply = execute(Command::Unknown(b"FOO".to_vec()), &d);
        let mut out = Vec::new();
        encode(&reply, &mut out);
        assert_eq!(out[0], b'-', "error replies start with one dash");
        assert_ne!(out[1], b'-', "sigil added twice");
        assert!(out.ends_with(b"\r\n"));
    }

    #[test]
    fn error_messages_carry_no_framing() {
        for e in [
            CommandError::Syntax,
            CommandError::NotAnInteger,
            CommandError::WrongArity(b"SET".to_vec()),
            CommandError::InvalidExpiry(b"SET".to_vec()),
            CommandError::UnknownCommand(b"FOO".to_vec()),
        ] {
            let msg = e.to_resp();
            assert_ne!(msg.first(), Some(&b'-'), "{e:?} must not include the sigil");
            assert!(!msg.ends_with(b"\r\n"), "{e:?} must not include CRLF");
        }
    }
}