//! Turning a request into a `Command`, and running it against the keyspace.
//! Each command group parses and runs its own commands in its own file; this
//! one holds the types they share and the two entry points.

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Blocking {
    No,
    Forever,
    For(Duration),
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
        timeout: Option<Duration>,
        from: End,
    },
    BLMove {
        src: Bytes,
        dst: Bytes,
        from: End,
        to: End,
        timeout: Option<Duration>,
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
        timeout: Blocking,
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
    pub timeout: Option<Duration>, // None means forever
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
            Command::XRead { timeout, .. } => Meta {
                feeds: vec![],
                blocks: match timeout {
                    Blocking::No => None,
                    Blocking::Forever => Some(BlockSpec {
                        timeout: None,
                        on_timeout: Value::NullArray,
                    }),
                    Blocking::For(d) => Some(BlockSpec {
                        timeout: Some(*d),
                        on_timeout: Value::NullArray,
                    }),
                },
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

/// Parses one client request.
///
///   Ok(Some(cmd)) - run it and reply
///   Ok(None)      - a valid frame that produces no reply at all (empty array)
///   Err(e)        - reply with the error
///
/// Must stay pure: it reads only the request, never the keyspace or a clock.
/// Inside MULTI a command is parsed when it is queued but runs at EXEC, so
/// anything resolved here would reflect the moment of queueing. That is why
/// `EX` stays a relative `Duration` and `XADD *` and `XREAD $` are resolved by
/// `execute`. Redis parses at EXEC instead; purity makes the two equivalent.
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
            timeout,
            streams,
        } => stream::xread(count, timeout, streams, db),
        Command::XDel { key, ids } => stream::xdel(&key, &ids, db),
        Command::XTrim { key, trim } => stream::xtrim(&key, &trim, db),
        Command::Incr { key } => Ok(Outcome::Reply(string::incr(&key, db)?)),
        Command::Unknown { name, args } => Err(CommandError::UnknownCommand { name, args }),
        // Never reached: the connection intercepts these before anything goes
        // to the keyspace task. An error rather than unreachable!(), because a
        // panic here would stop the whole server.
        Command::Multi | Command::Exec | Command::Discard | Command::Watch { .. } => {
            Err(CommandError::HandledByConnection)
        }
        // Reached only inside EXEC, which has already dropped the watches, so
        // there is nothing left to do.
        Command::Unwatch => Ok(Outcome::Reply(Value::SimpleString(Bytes::from_static(
            b"OK",
        )))),
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use crate::db::{Db, TestClock};
    use crate::resp::parse;
    use std::sync::Arc;

    pub(crate) fn cmd(bytes: &[u8]) -> Result<Option<Command>, CommandError> {
        let (n, frame) = parse(bytes).unwrap().unwrap();
        let owned = Bytes::copy_from_slice(&bytes[..n]);
        to_command(frame.into_value(&owned))
    }
    pub(crate) fn cmd_ok(bytes: &[u8]) -> Command {
        cmd(bytes).unwrap().expect("expected a command")
    }
    pub(crate) fn db() -> Db {
        Db::with_clock(Arc::new(TestClock::new()))
    }
}

#[cfg(test)]
mod command_tests {
    use super::*;
    use crate::command::test_support::{cmd, cmd_ok, db};
    use crate::resp::{encode, parse};

    #[test]
    fn command_name_is_case_insensitive() {
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nPING\r\n"), Command::Ping(None));
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nping\r\n"), Command::Ping(None));
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nPiNg\r\n"), Command::Ping(None));
    }

    #[test]
    fn request_must_be_an_array_of_bulk_strings() {
        assert!(matches!(cmd(b":5\r\n"), Err(CommandError::NotAnArray)));
        assert!(matches!(
            cmd(b"*1\r\n:5\r\n"),
            Err(CommandError::NotBulkString)
        ));
    }

    /// `*0\r\n` is a valid frame that Redis consumes without replying.
    #[test]
    fn empty_array_produces_no_command_and_no_error() {
        assert_eq!(cmd(b"*0\r\n"), Ok(None));
    }

    #[test]
    fn arity_errors() {
        for frame in [
            &b"*1\r\n$4\r\nECHO\r\n"[..],
            &b"*3\r\n$4\r\nECHO\r\n$1\r\na\r\n$1\r\nb\r\n"[..],
            &b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n"[..],
            &b"*1\r\n$5\r\nRPUSH\r\n"[..],
            &b"*2\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n"[..],
            &b"*4\r\n$5\r\nLMOVE\r\n$1\r\na\r\n$1\r\nb\r\n$4\r\nLEFT\r\n"[..],
        ] {
            assert!(
                matches!(cmd(frame), Err(CommandError::WrongArity(_))),
                "expected WrongArity for {frame:?}"
            );
        }
    }

    #[test]
    fn ping_and_echo_reply_with_different_types() {
        let mut d = db();
        // bare PING is a simple string, PING <msg> is a bulk string
        assert_eq!(
            execute(Command::Ping(None), &mut d).unwrap(),
            Outcome::Reply(Value::SimpleString(Bytes::from_static(b"PONG")))
        );
        assert_eq!(
            execute(Command::Ping(Some(Bytes::from_static(b"hi"))), &mut d).unwrap(),
            Outcome::Reply(Value::BulkString(Bytes::from_static(b"hi")))
        );
        assert_eq!(
            execute(Command::Echo(Bytes::from_static(b"hi")), &mut d).unwrap(),
            Outcome::Reply(Value::BulkString(Bytes::from_static(b"hi")))
        );
    }

    #[test]
    fn get_missing_key_is_null_bulk_string() {
        let mut d = db();
        let c = cmd_ok(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        assert_eq!(
            execute(c, &mut d).unwrap(),
            Outcome::Reply(Value::NullBulkString)
        );
    }

    #[test]
    fn set_then_get_round_trip_through_the_command_layer() {
        let mut d = db();
        execute(cmd_ok(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"), &mut d).unwrap();
        assert_eq!(
            execute(cmd_ok(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"), &mut d).unwrap(),
            Outcome::Reply(Value::BulkString(Bytes::from_static(b"v")))
        );
    }

    #[test]
    fn unknown_command_carries_its_arguments() {
        let mut d = db();
        let c = cmd_ok(b"*2\r\n$3\r\nFOO\r\n$3\r\nbar\r\n");
        let msg = execute(c, &mut d).unwrap_err().to_resp();
        let text = String::from_utf8_lossy(&msg);
        assert!(text.starts_with("ERR unknown command 'FOO'"), "{text}");
        assert!(text.contains("bar"), "{text}");
    }

    /// Checks the encoded bytes, not `to_resp`, because the double-sigil bug
    /// lived in an inline `format!` that a `to_resp` test could not reach.
    #[test]
    fn error_replies_have_exactly_one_sigil() {
        let mut d = db();
        let err = execute(
            Command::Unknown {
                name: Bytes::from_static(b"FOO"),
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
            CommandError::WrongArity(Bytes::from_static(b"SET")),
            CommandError::InvalidExpiry(Bytes::from_static(b"SET")),
            CommandError::UnknownCommand {
                name: Bytes::from_static(b"FOO"),
                args: vec![Bytes::from_static(b"a")],
            },
        ] {
            let msg = e.to_resp();
            assert_ne!(msg.first(), Some(&b'-'), "{e:?} must not include the sigil");
            assert!(!msg.ends_with(b"\r\n"), "{e:?} must not include CRLF");
        }
    }
}
