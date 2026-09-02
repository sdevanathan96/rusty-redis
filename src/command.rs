use crate::db::{Db, End, EntryId, IdSpec, ReadFrom, WrongType, XaddError};
use crate::resp::Value;
use std::time::Duration;
mod stream;
mod generic;
mod list;
mod string;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Outcome {
    Reply(Value),
    Block { keys: Vec<Vec<u8>>, retry: Command },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Blocking { No, Forever, Until(Duration) }

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Command {
    Ping(Option<Vec<u8>>),
    Echo(Vec<u8>),
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8>, expiry: Option<Duration> },
    Type { key: Vec<u8> },
    Del { keys: Vec<Vec<u8>> },
    Exists { keys: Vec<Vec<u8>> },
    Push { key: Vec<u8>, values: Vec<Vec<u8>>, from: End },
    Pop { key: Vec<u8>, count: Option<usize>, from: End },
    LLen { key: Vec<u8> },
    LRange { key: Vec<u8>, start: i64, stop: i64 },
    LMove { src: Vec<u8>, dst: Vec<u8>, from: End, to: End },
    BPop { keys: Vec<Vec<u8>>, timeout: Option<Duration>, from: End },
    BLMove { src: Vec<u8>, dst: Vec<u8>, from: End, to: End , timeout: Option<Duration> },
    XAdd { key: Vec<u8>, id: IdSpec, fields: Vec<(Vec<u8>, Vec<u8>)> },
    XLen { key: Vec<u8> },
    XRange {key: Vec<u8>, start: EntryId, stop: EntryId, count: Option<i64> },
    XRead {count: Option<i64>, timeout: Blocking, streams: Vec<(Vec<u8>, ReadFrom)>},
    Unknown { name: Vec<u8>, args: Vec<Vec<u8>> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    NotAnArray,
    NotBulkString,
    WrongArity(Vec<u8>),
    NotAnInteger,
    InvalidExpiry(Vec<u8>),
    OutOfRange,
    Syntax,
    WrongType,
    UnknownCommand { name: Vec<u8>, args: Vec<Vec<u8>> },
    TimeoutError,
    MTimeoutError,
    NegativeTimeout,
    InvalidStreamId,
    XaddIdZero,
    XaddIdTooSmall,
    UnbalancedXread
}

impl From<WrongType> for CommandError {
    fn from(_: WrongType) -> Self {
        CommandError::WrongType
    }
}

impl From<XaddError> for CommandError {
    fn from(e: XaddError) -> Self {
        match e {
            XaddError::WrongType => CommandError::WrongType,
            XaddError::IdIsZero => CommandError::XaddIdZero,
            XaddError::IdTooSmall => CommandError::XaddIdTooSmall,
        }
    }
}

pub struct BlockSpec {
    pub timeout: Option<Duration>,   // None means forever
    pub on_timeout: Value,
}

impl Command {
    /// Lists this command adds elements to. A parked waiter on one of these
    /// keys may become satisfiable after this command runs.
    /// ADD AN ARM when you add a command that pushes to a list.
    pub fn feeds(&self) -> Vec<Vec<u8>> {
        match self {
            Command::Push { key, .. } => vec![key.clone()],
            Command::LMove { dst, .. } => vec![dst.clone()],
            Command::BLMove { dst, .. } => vec![dst.clone()],
            Command::XAdd { key, .. } => vec![key.clone()],
            _ => vec![],
        }
    }

    /// Whether this command can park, and what to send if it times out.
    /// ADD AN ARM when you add a blocking command.
    pub fn blocking(&self) -> Option<BlockSpec> {
        match self {
            Command::BPop { timeout, .. } => Some(BlockSpec {
                timeout: *timeout,
                on_timeout: Value::NullArray,
            }),
            Command::BLMove { timeout, .. } => Some(BlockSpec {
                timeout: *timeout,
                on_timeout: Value::NullArray,
            }),
            Command::XRead { timeout, .. } => match timeout {
                Blocking::No => None,
                Blocking::Forever => Some(BlockSpec { timeout: None, on_timeout: Value::NullArray }),
                Blocking::Until(d) => Some(BlockSpec { timeout: Some(*d), on_timeout: Value::NullArray }),
            },
            _ => None,
        }
    }
}

impl CommandError {
    /// The message body only. No leading '-' and no trailing CRLF: those belong
    /// to `resp::encode` when it writes a `Value::Error`.
    pub fn to_resp(&self) -> Vec<u8> {
        match self {
            CommandError::NotAnArray => {
                b"ERR Protocol error: expected an array of bulk strings".to_vec()
            }
            CommandError::NotBulkString => b"ERR Protocol error: expected a bulk string".to_vec(),
            CommandError::WrongArity(name) => {
                format!("ERR wrong number of arguments for '{}' command", lower(name)).into_bytes()
            }
            CommandError::NotAnInteger => b"ERR value is not an integer or out of range".to_vec(),
            CommandError::InvalidExpiry(name) => {
                format!("ERR invalid expire time in '{}' command", lower(name)).into_bytes()
            }
            CommandError::OutOfRange => b"ERR value is out of range, must be positive".to_vec(),
            CommandError::Syntax => b"ERR syntax error".to_vec(),
            CommandError::WrongType => {
                b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec()
            }
            CommandError::UnknownCommand { name, args } => {
                if args.is_empty() {
                    format!("ERR unknown command '{}'", quote(name)).into_bytes()
                } else {
                    format!(
                        "ERR unknown command '{}', with args beginning with: {}",
                        quote(name),
                        quote_args(args)
                    )
                    .into_bytes()
                }
            }
            CommandError::NegativeTimeout => { b"ERR timeout is negative".to_vec() }
            CommandError::TimeoutError => { b"ERR timeout is not a float or out of range".to_vec() }
            CommandError::MTimeoutError => { b"ERR timeout is not an integer or out of range".to_vec() }
            CommandError::InvalidStreamId =>
                b"ERR Invalid stream ID specified as stream command argument".to_vec(),
            CommandError::XaddIdZero =>
                b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
            CommandError::XaddIdTooSmall =>
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item".to_vec(),
            CommandError::UnbalancedXread => 
                b"ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.".to_vec()
        }
    }
}

/// Redis lowercases the command name in arity and expiry errors even when the
/// client sent it uppercase, but quotes it verbatim in "unknown command".
fn lower(name: &[u8]) -> String {
    String::from_utf8_lossy(name).to_lowercase()
}

/// Parses one client request.
///
///   Ok(Some(cmd)) - run it and reply
///   Ok(None)      - a valid frame that produces no reply at all (empty array)
///   Err(e)        - reply with the error
pub fn to_command(v: Value) -> Result<Option<Command>, CommandError> {
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
        None => Command::Unknown { name: name.clone(), args: rest.to_vec() },
    };
    Ok(Some(cmd))
}

/// Reads the argument following an expiry keyword at index `i`.
///
///   missing argument -> Syntax
///   not a number     -> NotAnInteger
///   zero or negative -> InvalidExpiry
fn expiry_arg(rest: &[Vec<u8>], i: usize, cmd_name: &[u8]) -> Result<i64, CommandError> {
    let raw = rest.get(i + 1).ok_or(CommandError::Syntax)?;
    let n = parse_i64(raw)?;
    if n <= 0 {
        return Err(CommandError::InvalidExpiry(cmd_name.to_vec()));
    }
    Ok(n)
}

fn parse_i64(raw: &[u8]) -> Result<i64, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::NotAnInteger)?;
    text.parse().map_err(|_| CommandError::NotAnInteger)
}

// fn parse_u64(raw: &[u8]) -> Result<u64, CommandError> {
//     let text = std::str::from_utf8(raw).map_err(|_| CommandError::InvalidStreamId)?;
//     text.parse().map_err(|_| CommandError::InvalidStreamId)
// }

fn parse_end(raw: &[u8]) -> Result<End, CommandError> {
    match raw.to_ascii_uppercase().as_slice() {
        b"LEFT" => Ok(End::Left),
        b"RIGHT" => Ok(End::Right),
        _ => Err(CommandError::Syntax),
    }
}

fn parse_timeout(raw: &[u8]) -> Result<Option<Duration>, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::TimeoutError)?;
    let seconds: f64 = text.parse().map_err(|_| CommandError::TimeoutError)?;
    if seconds < 0.0 || !seconds.is_finite() {
        return Err(CommandError::NegativeTimeout);
    }
    if seconds == 0.0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_secs_f64(seconds)))
}

fn parse_block(raw: &[u8]) -> Result<Blocking, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::MTimeoutError)?;
    let millis: i64 = text.parse().map_err(|_| CommandError::MTimeoutError)?;
    if millis < 0 {
        return Err(CommandError::NegativeTimeout);
    }
    Ok(if millis == 0 {
        Blocking::Forever
    } else {
        Blocking::Until(Duration::from_millis(millis as u64))
    })
}

pub fn execute(cmd: Command, db: &mut Db) -> Result<Outcome, CommandError>{
    match cmd {
        Command::Ping(msg) => Ok(Outcome::Reply(generic::ping(msg)?)), 
        Command::Echo(msg) => Ok(Outcome::Reply(generic::echo(msg)?)),
        Command::Type { key } => Ok(Outcome::Reply(generic::type_of(&key, db)?)),
        Command::Del { keys } => Ok(Outcome::Reply(generic::del(&keys, db)?)),
        Command::Exists { keys } => Ok(Outcome::Reply(generic::exists(&keys, db)?)),
        Command::Get { key } => Ok(Outcome::Reply(string::get(&key, db)?)),
        Command::Set { key, value, expiry } => Ok(Outcome::Reply(string::set(key, value, expiry, db)?)),
        Command::Push { key, values, from } => Ok(Outcome::Reply(list::push(&key, values, from, db)?)),
        Command::Pop { key, count, from } => Ok(Outcome::Reply(list::pop(&key, count, from, db)?)),
        Command::LLen { key } => Ok(Outcome::Reply(list::llen(&key, db)?)),
        Command::LRange { key, start, stop } => Ok(Outcome::Reply(list::lrange(&key, start, stop, db)?)),
        Command::LMove { src, dst, from, to } => Ok(Outcome::Reply(list::lmove(&src, &dst, from, to, db)?)),
        Command::BPop { keys, timeout, from } => list::bpop(keys, from, timeout, db),
        Command::BLMove { src, dst, from, to, timeout } => list::blmove(src, dst, from, to, timeout, db),
        Command::XAdd { key, id, fields } => stream::xadd(&key, id, fields, db),
        Command::XLen { key } => stream::xlen(&key, db),
        Command::XRange { key, start, stop, count } => stream::xrange(&key, start, stop, count, db),
        Command::XRead { count, timeout, streams } => stream::xread(count, timeout, streams, db),
        Command::Unknown { name, args } => Err(CommandError::UnknownCommand { name, args }),
    }
}

// pub fn blocking_timeout(cmd: &Command) -> Blocking {
//     match cmd {
//         Command::BPop { timeout: Some(d), .. } => Blocking::Until(*d),
//         Command::BPop { timeout: None, .. } => Blocking::Forever,
//         _ => Blocking::No,
//     }
// }

/// Renders client supplied bytes for an error message. Escapes so the message
/// reads clearly, truncates so a huge argument cannot fill the reply.
///
/// `resp::encode` already guarantees a single frame by substituting CR and LF,
/// but escaping here keeps the message readable instead of mangled to spaces,
/// and escaping the quote stops a client closing it and appending its own text.
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

fn quote_args(args: &[Vec<u8>]) -> String {
    const MAX_ARGS: usize = 8;
    let mut s = String::new();
    for a in args.iter().take(MAX_ARGS) {
        s.push('\'');
        s.push_str(&quote(a));
        s.push_str("' ");
    }
    s
}


#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use crate::db::{Db, TestClock};
    use crate::resp::parse;
    use std::sync::Arc;

    pub(crate) fn cmd(bytes: &[u8]) -> Result<Option<Command>, CommandError> {
        to_command(parse(bytes).unwrap().unwrap().1)
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
    use crate::resp::{encode, parse};
    use crate::command::test_support::{cmd, cmd_ok, db};

    #[test]
    fn command_name_is_case_insensitive() {
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nPING\r\n"), Command::Ping(None));
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nping\r\n"), Command::Ping(None));
        assert_eq!(cmd_ok(b"*1\r\n$4\r\nPiNg\r\n"), Command::Ping(None));
    }

    #[test]
    fn request_must_be_an_array_of_bulk_strings() {
        assert!(matches!(cmd(b":5\r\n"), Err(CommandError::NotAnArray)));
        assert!(matches!(cmd(b"*1\r\n:5\r\n"), Err(CommandError::NotBulkString)));
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
            Outcome::Reply(Value::SimpleString(b"PONG".to_vec()))
        );
        assert_eq!(
            execute(Command::Ping(Some(b"hi".to_vec())), &mut d).unwrap(),
            Outcome::Reply(Value::BulkString(b"hi".to_vec()))
        );
        assert_eq!(
            execute(Command::Echo(b"hi".to_vec()), &mut d).unwrap(),
            Outcome::Reply(Value::BulkString(b"hi".to_vec()))
        );
    }

    #[test]
    fn get_missing_key_is_null_bulk_string() {
        let mut d = db();
        let c = cmd_ok(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        assert_eq!(execute(c, &mut d).unwrap(), Outcome::Reply(Value::NullBulkString));
    }

    #[test]
    fn set_then_get_round_trip_through_the_command_layer() {
        let mut d = db();
        execute(cmd_ok(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"), &mut d).unwrap();
        assert_eq!(
            execute(cmd_ok(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"), &mut d).unwrap(),
            Outcome::Reply(Value::BulkString(b"v".to_vec()))
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
            Command::Unknown { name: b"FOO".to_vec(), args: vec![] },
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
                name: b"FOO\r\n+INJECTED".to_vec(),
                args: vec![],
            },
            &mut d,
        )
        .unwrap_err();

        let mut out = Vec::new();
        encode(&Value::Error(err.to_resp()), &mut out);
        let (consumed, _) = parse(&out).unwrap().unwrap();
        assert_eq!(consumed, out.len(), "one command must produce one reply frame");
    }

    #[test]
    fn error_messages_carry_no_framing() {
        for e in [
            CommandError::Syntax,
            CommandError::NotAnInteger,
            CommandError::OutOfRange,
            CommandError::WrongType,
            CommandError::WrongArity(b"SET".to_vec()),
            CommandError::InvalidExpiry(b"SET".to_vec()),
            CommandError::UnknownCommand {
                name: b"FOO".to_vec(),
                args: vec![b"a".to_vec()],
            },
        ] {
            let msg = e.to_resp();
            assert_ne!(msg.first(), Some(&b'-'), "{e:?} must not include the sigil");
            assert!(!msg.ends_with(b"\r\n"), "{e:?} must not include CRLF");
        }
    }
}