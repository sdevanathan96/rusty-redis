//! Parsing a request into a `Command`, and running it. Each command group has
//! its own file.

mod args;
mod error;
mod generic;
mod list;
mod stream;
mod string;

use std::time::Duration;

use bytes::Bytes;

use crate::db::{Db, End, EntryId, IdSpec, ReadFrom, Trim};
use crate::resp::Value;
pub use error::CommandError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Outcome {
    Reply(Value),
    Block { keys: Vec<Bytes>, retry: Command },
}

/// How long a blocking command waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timeout {
    Forever,
    After(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get {
        key: Bytes,
    },
    Set {
        key: Bytes,
        value: Bytes,
        expiry: Option<Duration>,
    },
    Type {
        key: Bytes,
    },
    Del {
        keys: Vec<Bytes>,
    },
    Exists {
        keys: Vec<Bytes>,
    },
    Push {
        key: Bytes,
        values: Vec<Bytes>,
        from: End,
    },
    Pop {
        key: Bytes,
        count: Option<usize>,
        from: End,
    },
    LLen {
        key: Bytes,
    },
    LRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    LMove {
        src: Bytes,
        dst: Bytes,
        from: End,
        to: End,
    },
    BPop {
        keys: Vec<Bytes>,
        timeout: Timeout,
        from: End,
    },
    BLMove {
        src: Bytes,
        dst: Bytes,
        from: End,
        to: End,
        timeout: Timeout,
    },
    XAdd {
        key: Bytes,
        id: IdSpec,
        fields: Vec<(Bytes, Bytes)>,
        trim: Option<Trim>,
        nomkstream: bool,
    },
    XLen {
        key: Bytes,
    },
    XRange {
        key: Bytes,
        start: EntryId,
        stop: EntryId,
        count: Option<i64>,
    },
    XRead {
        count: Option<i64>,
        /// `None` without BLOCK.
        block: Option<Timeout>,
        streams: Vec<(Bytes, ReadFrom)>,
    },
    XDel {
        key: Bytes,
        ids: Vec<EntryId>,
    },
    XTrim {
        key: Bytes,
        trim: Trim,
    },
    Incr {
        key: Bytes,
    },
    Multi,
    Exec,
    Discard,
    Unknown {
        name: Bytes,
        args: Vec<Bytes>,
    },
    Watch {
        keys: Vec<Bytes>,
    },
    Unwatch,
}

pub struct BlockSpec {
    pub timeout: Timeout,
    pub on_timeout: Value,
}

pub struct Meta {
    pub feeds: Vec<Bytes>,
    pub blocks: Option<BlockSpec>,
}

impl Command {
    pub fn meta(&self) -> Meta {
        match self {
            Command::Push { key, .. } => Meta {
                feeds: vec![key.clone()],
                blocks: None,
            },
            Command::BPop { timeout, .. } => Meta {
                feeds: vec![],
                blocks: Some(BlockSpec {
                    timeout: *timeout,
                    on_timeout: Value::NullArray,
                }),
            },
            Command::BLMove { dst, timeout, .. } => Meta {
                feeds: vec![dst.clone()],
                blocks: Some(BlockSpec {
                    timeout: *timeout,
                    on_timeout: Value::NullArray,
                }),
            },
            Command::XAdd { key, .. } => Meta {
                feeds: vec![key.clone()],
                blocks: None,
            },
            Command::XRead { block, .. } => Meta {
                feeds: vec![],
                blocks: block.map(|timeout| BlockSpec {
                    timeout,
                    on_timeout: Value::NullArray,
                }),
            },
            Command::Ping(_)
            | Command::Echo(_)
            | Command::Get { .. }
            | Command::Set { .. }
            | Command::Type { .. }
            | Command::Del { .. }
            | Command::Exists { .. }
            | Command::Pop { .. }
            | Command::LLen { .. }
            | Command::LRange { .. }
            | Command::LMove { .. }
            | Command::XLen { .. }
            | Command::XRange { .. }
            | Command::XDel { .. }
            | Command::XTrim { .. }
            | Command::Incr { .. }
            | Command::Unknown { .. }
            | Command::Exec
            | Command::Discard
            | Command::Watch { .. }
            | Command::Unwatch
            | Command::Multi => Meta {
                feeds: vec![],
                blocks: None,
            },
        }
    }
}

/// `Ok(None)` is an empty array, which gets no reply.
///
/// Must not read the keyspace or a clock: inside MULTI a command is parsed when
/// queued but runs at EXEC. So `EX` stays relative, and `XADD *` and `XREAD $`
/// are resolved by `execute`.
pub fn to_command(v: Value) -> Result<Option<Command>, CommandError> {
    let items = match v {
        Value::Array(items) => items,
        _ => return Err(CommandError::NotAnArray),
    };

    let mut args: Vec<Bytes> = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::BulkString(bytes) => args.push(bytes),
            _ => return Err(CommandError::NotBulkString),
        }
    }

    // `*0\r\n` is a valid frame. Redis consumes it and says nothing.
    let (name, rest) = match args.split_first() {
        Some(v) => v,
        None => return Ok(None),
    };

    let upper = name.to_ascii_uppercase();
    let parsed = generic::try_parse(&upper, rest, name)
        .or_else(|| string::try_parse(&upper, rest, name))
        .or_else(|| list::try_parse(&upper, rest, name))
        .or_else(|| stream::try_parse(&upper, rest, name));

    let cmd = match parsed {
        Some(result) => result?,
        None => Command::Unknown {
            name: name.clone(),
            args: rest.to_vec(),
        },
    };
    Ok(Some(cmd))
}

pub fn execute(cmd: Command, db: &mut Db) -> Result<Outcome, CommandError> {
    match cmd {
        Command::Ping(msg) => Ok(Outcome::Reply(generic::ping(msg)?)),
        Command::Echo(msg) => Ok(Outcome::Reply(generic::echo(msg)?)),
        Command::Type { key } => Ok(Outcome::Reply(generic::type_of(&key, db)?)),
        Command::Del { keys } => Ok(Outcome::Reply(generic::del(keys, db)?)),
        Command::Exists { keys } => Ok(Outcome::Reply(generic::exists(keys, db)?)),
        Command::Get { key } => Ok(Outcome::Reply(string::get(&key, db)?)),
        Command::Set { key, value, expiry } => {
            Ok(Outcome::Reply(string::set(key, value, expiry, db)?))
        }
        Command::Push { key, values, from } => {
            Ok(Outcome::Reply(list::push(key, values, from, db)?))
        }
        Command::Pop { key, count, from } => Ok(Outcome::Reply(list::pop(&key, count, from, db)?)),
        Command::LLen { key } => Ok(Outcome::Reply(list::llen(&key, db)?)),
        Command::LRange { key, start, stop } => {
            Ok(Outcome::Reply(list::lrange(&key, start, stop, db)?))
        }
        Command::LMove { src, dst, from, to } => {
            Ok(Outcome::Reply(list::lmove(&src, dst, from, to, db)?))
        }
        Command::BPop {
            keys,
            timeout,
            from,
        } => list::bpop(keys, from, timeout, db),
        Command::BLMove {
            src,
            dst,
            from,
            to,
            timeout,
        } => list::blmove(src, dst, from, to, timeout, db),
        Command::XAdd {
            key,
            id,
            fields,
            trim,
            nomkstream,
        } => stream::xadd(key, id, fields, trim, nomkstream, db),
        Command::XLen { key } => stream::xlen(&key, db),
        Command::XRange {
            key,
            start,
            stop,
            count,
        } => stream::xrange(&key, start, stop, count, db),
        Command::XRead {
            count,
            block,
            streams,
        } => stream::xread(count, block, streams, db),
        Command::XDel { key, ids } => stream::xdel(&key, &ids, db),
        Command::XTrim { key, trim } => stream::xtrim(&key, &trim, db),
        Command::Incr { key } => Ok(Outcome::Reply(string::incr(&key, db)?)),
        Command::Unknown { name, args } => Err(CommandError::UnknownCommand { name, args }),
        // The connection handles these. Not unreachable!(): a panic here would
        // stop the server.
        Command::Multi | Command::Exec | Command::Discard | Command::Watch { .. } => {
            Err(CommandError::HandledByConnection)
        }
        // Only inside EXEC, which has already dropped the watches.
        Command::Unwatch => Ok(Outcome::Reply(Value::ok())),
    }
}

#[cfg(test)]
mod command_tests {
    use super::*;
    use crate::resp::{encode, parse};
    use crate::test_support::{b, cmd, cmd_ok, db, parse_raw};

    #[test]
    fn command_name_is_case_insensitive() {
        assert_eq!(cmd_ok(&["PING"]), Command::Ping(None));
        assert_eq!(cmd_ok(&["ping"]), Command::Ping(None));
        assert_eq!(cmd_ok(&["PiNg"]), Command::Ping(None));
    }

    #[test]
    fn request_must_be_an_array_of_bulk_strings() {
        assert!(matches!(
            parse_raw(b":5\r\n"),
            Err(CommandError::NotAnArray)
        ));
        assert!(matches!(
            parse_raw(b"*1\r\n:5\r\n"),
            Err(CommandError::NotBulkString)
        ));
    }

    /// `*0\r\n` is a valid frame that Redis consumes without replying.
    #[test]
    fn empty_array_produces_no_command_and_no_error() {
        assert_eq!(cmd(&[]), Ok(None));
    }

    #[test]
    fn arity_errors() {
        for parts in [
            &["ECHO"][..],
            &["ECHO", "a", "b"],
            &["SET", "k"],
            &["RPUSH"],
            &["RPUSH", "k"],
            &["LMOVE", "a", "b", "LEFT"],
        ] {
            assert!(
                matches!(cmd(parts), Err(CommandError::WrongArity(_))),
                "expected WrongArity for {parts:?}"
            );
        }
    }

    #[test]
    fn get_missing_key_is_null_bulk_string() {
        let mut d = db();
        let c = cmd_ok(&["GET", "k"]);
        assert_eq!(
            execute(c, &mut d).unwrap(),
            Outcome::Reply(Value::NullBulkString)
        );
    }

    #[test]
    fn set_then_get_round_trip_through_the_command_layer() {
        let mut d = db();
        execute(cmd_ok(&["SET", "k", "v"]), &mut d).unwrap();
        assert_eq!(
            execute(cmd_ok(&["GET", "k"]), &mut d).unwrap(),
            Outcome::Reply(Value::BulkString(b("v")))
        );
    }

    #[test]
    fn unknown_command_carries_its_arguments() {
        let mut d = db();
        let c = cmd_ok(&["FOO", "bar"]);
        let msg = execute(c, &mut d).unwrap_err().to_resp();
        let text = String::from_utf8_lossy(&msg);
        assert!(text.starts_with("ERR unknown command 'FOO'"), "{text}");
        assert!(text.contains("bar"), "{text}");
    }

    /// Checks the encoded bytes, where a doubled `-` would show.
    #[test]
    fn error_replies_have_exactly_one_sigil() {
        let mut d = db();
        let err = execute(
            Command::Unknown {
                name: b("FOO"),
                args: vec![],
            },
            &mut d,
        )
        .unwrap_err();

        let mut out = Vec::new();
        encode(&Value::Error(err.to_resp()), &mut out);
        assert_eq!(out[0], b'-', "error replies start with one dash");
        assert_ne!(out[1], b'-', "sigil added twice");
        assert!(out.ends_with(b"\r\n"));
    }

    /// A command name containing CRLF must still produce exactly one frame.
    #[test]
    fn unknown_command_name_cannot_inject_a_reply() {
        let mut d = db();
        let err = execute(
            Command::Unknown {
                name: Bytes::from_static(b"FOO\r\n+INJECTED"),
                args: vec![],
            },
            &mut d,
        )
        .unwrap_err();

        let mut out = Vec::new();
        encode(&Value::Error(err.to_resp()), &mut out);
        let (consumed, _) = parse(&out).unwrap().unwrap();
        assert_eq!(
            consumed,
            out.len(),
            "one command must produce one reply frame"
        );
    }

    #[test]
    fn error_messages_carry_no_framing() {
        for e in [
            CommandError::Syntax,
            CommandError::NotAnInteger,
            CommandError::OutOfRange,
            CommandError::WrongType,
            CommandError::WrongArity(b("SET")),
            CommandError::InvalidExpiry(b("SET")),
            CommandError::UnknownCommand {
                name: b("FOO"),
                args: vec![b("a")],
            },
        ] {
            let msg = e.to_resp();
            assert_ne!(msg.first(), Some(&b'-'), "{e:?} must not include the sigil");
            assert!(!msg.ends_with(b"\r\n"), "{e:?} must not include CRLF");
        }
    }
}
