use bytes::Bytes;

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
    Block { keys: Vec<Bytes>, retry: Command },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Blocking { No, Forever, Until(Duration) }

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes, expiry: Option<Duration> },
    Type { key: Bytes },
    Del { keys: Vec<Bytes> },
    Exists { keys: Vec<Bytes> },
    Push { key: Bytes, values: Vec<Bytes>, from: End },
    Pop { key: Bytes, count: Option<usize>, from: End },
    LLen { key: Bytes },
    LRange { key: Bytes, start: i64, stop: i64 },
    LMove { src: Bytes, dst: Bytes, from: End, to: End },
    BPop { keys: Vec<Bytes>, timeout: Option<Duration>, from: End },
    BLMove { src: Bytes, dst: Bytes, from: End, to: End , timeout: Option<Duration> },
    XAdd { key: Bytes, id: IdSpec, fields: Vec<(Bytes, Bytes)> },
    XLen { key: Bytes },
    XRange {key: Bytes, start: EntryId, stop: EntryId, count: Option<i64> },
    XRead {count: Option<i64>, timeout: Blocking, streams: Vec<(Bytes, ReadFrom)>},
    XDel { key: Bytes, ids: Vec<EntryId>},
    Unknown { name: Bytes, args: Vec<Bytes> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    NotAnArray,
    NotBulkString,
    WrongArity(Bytes),
    NotAnInteger,
    InvalidExpiry(Bytes),
    OutOfRange,
    Syntax,
    WrongType,
    UnknownCommand { name: Bytes, args: Vec<Bytes> },
    TimeoutError,
    MTimeoutError,
    NegativeTimeout,
    TimeoutOutOfRange,
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

pub struct Meta {
        pub feeds: Vec<Bytes>,
        pub blocks: Option<BlockSpec>,
    }

impl Command {
    /// Lists this command adds elements to. A parked waiter on one of these
    /// keys may become satisfiable after this command runs.
    /// ADD AN ARM when you add a command that pushes to a list.
    // pub fn feeds(&self) -> Vec<Bytes> {
    //     match self {
    //         Command::Push { key, .. } => vec![key.clone()],
    //         Command::LMove { dst, .. } => vec![dst.clone()],
    //         Command::BLMove { dst, .. } => vec![dst.clone()],
    //         Command::XAdd { key, .. } => vec![key.clone()],
    //         Command::Ping(_)
    //         | Command::Echo(_)
    //         | Command::Get { .. }
    //         | Command::Set { .. }
    //         | Command::Type { .. }
    //         | Command::Del { .. }
    //         | Command::Exists { .. }
    //         | Command::Pop { .. }
    //         | Command::LLen { .. }
    //         | Command::LRange { .. }
    //         | Command::BPop { .. }
    //         | Command::XLen { .. }
    //         | Command::XRange { .. }
    //         | Command::XRead { .. }
    //         | Command::XDel { .. }
    //         | Command::Unknown { .. } => vec![],
    //     }
    // }

    // /// Whether this command can park, and what to send if it times out.
    // /// ADD AN ARM when you add a blocking command.
    // pub fn blocking(&self) -> Option<BlockSpec> {
    //     match self {
    //         Command::BPop { timeout, .. } => Some(BlockSpec {
    //             timeout: *timeout,
    //             on_timeout: Value::NullArray,
    //         }),
    //         Command::BLMove { timeout, .. } => Some(BlockSpec {
    //             timeout: *timeout,
    //             on_timeout: Value::NullArray,
    //         }),
    //         Command::XRead { timeout, .. } => match timeout {
    //             Blocking::No => None,
    //             Blocking::Forever => Some(BlockSpec { timeout: None, on_timeout: Value::NullArray }),
    //             Blocking::Until(d) => Some(BlockSpec { timeout: Some(*d), on_timeout: Value::NullArray }),
    //         },
    //         Command::Ping(_)
    //         | Command::Echo(_)
    //         | Command::Get { .. }
    //         | Command::Set { .. }
    //         | Command::Type { .. }
    //         | Command::Del { .. }
    //         | Command::Exists { .. }
    //         | Command::Push { .. }
    //         | Command::Pop { .. }
    //         | Command::LLen { .. }
    //         | Command::LRange { .. }
    //         | Command::LMove { .. }
    //         | Command::XAdd { .. }
    //         | Command::XLen { .. }
    //         | Command::XRange { .. }
    //         | Command::XDel { .. }
    //         | Command::Unknown { .. } => None,
    //     }
    // }

    pub fn meta(&self) -> Meta {
        match self {
            Command::Push { key, .. } => Meta { feeds: vec![key.clone()], blocks: None },
            Command::BPop { timeout, .. } => Meta {
                feeds: vec![],
                blocks: Some(BlockSpec { timeout: *timeout, on_timeout: Value::NullArray }),
            },
            Command::BLMove { dst, timeout, .. } => Meta {
                feeds: vec![dst.clone()],
                blocks: Some(BlockSpec { timeout: *timeout, on_timeout: Value::NullArray }),
            },
            Command::XAdd { key, .. } => Meta { feeds: vec![key.clone()], blocks: None },
            Command::XRead { timeout, .. } => Meta {
                feeds: vec![],
                blocks: match timeout {
                    Blocking::No => None,
                    Blocking::Forever => Some(BlockSpec { timeout: None, on_timeout: Value::NullArray }),
                    Blocking::Until(d) => Some(BlockSpec { timeout: Some(*d), on_timeout: Value::NullArray }),
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
            | Command::Unknown { .. } => Meta { feeds: vec![], blocks: None },
        }
    }

}

impl CommandError {
    /// The message body only. No leading '-' and no trailing CRLF: those belong
    /// to `resp::encode` when it writes a `Value::Error`.
    pub fn to_resp(&self) -> Bytes {
        match self {
            CommandError::NotAnArray => {
                Bytes::from_static(b"ERR Protocol error: expected an array of bulk strings")
            }
            CommandError::NotBulkString => Bytes::from_static(b"ERR Protocol error: expected a bulk string"),
            CommandError::WrongArity(name) => {
                Bytes::from(format!("ERR wrong number of arguments for '{}' command", lower(name)).into_bytes())
            }
            CommandError::NotAnInteger => Bytes::from_static(b"ERR value is not an integer or out of range"),
            CommandError::InvalidExpiry(name) => {
                Bytes::from(format!("ERR invalid expire time in '{}' command", lower(name)).into_bytes())
            }
            CommandError::OutOfRange => Bytes::from_static(b"ERR value is out of range, must be positive"),
            CommandError::Syntax => Bytes::from_static(b"ERR syntax error"),
            CommandError::WrongType => {
                Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            CommandError::UnknownCommand { name, args } => {
                if args.is_empty() {
                    Bytes::from(format!("ERR unknown command '{}'", quote(name)).into_bytes())
                } else {
                    Bytes::from(format!(
                        "ERR unknown command '{}', with args beginning with: {}",
                        quote(name),
                        quote_args(args)
                    ).into_bytes())
                }
            }
            CommandError::NegativeTimeout => { Bytes::from_static(b"ERR timeout is negative") }
            // VERIFY this string on 6380 with `BLPOP k 1e300`. It is a
            // third distinct message, not a reuse of TimeoutError.
            CommandError::TimeoutOutOfRange => { Bytes::from_static(b"ERR timeout is out of range") }
            CommandError::TimeoutError => { Bytes::from_static(b"ERR timeout is not a float or out of range") }
            CommandError::MTimeoutError => { Bytes::from_static(b"ERR timeout is not an integer or out of range") }
            CommandError::InvalidStreamId =>
                Bytes::from_static(b"ERR Invalid stream ID specified as stream command argument"),
            CommandError::XaddIdZero =>
                Bytes::from_static(b"ERR The ID specified in XADD must be greater than 0-0"),
            CommandError::XaddIdTooSmall =>
                Bytes::from_static(b"ERR The ID specified in XADD is equal or smaller than the target stream top item"),
            CommandError::UnbalancedXread => 
                Bytes::from_static(b"ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.")
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
        None => Command::Unknown { name: name.clone(), args: rest.to_vec() },
    };
    Ok(Some(cmd))
}

/// Which unit an expiry keyword is expressed in. `EX` is seconds, `PX` is
/// milliseconds. The unit matters for more than the multiplication: Redis
/// applies an upper bound to `EX` that it does not apply to `PX`, because it
/// converts seconds to milliseconds and refuses to overflow doing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryUnit {
    Seconds,
    Millis,
}

/// The largest `EX` argument Redis accepts, from its own check
/// `milliseconds > LLONG_MAX / 1000`.
///
/// VERIFY on 6380 before trusting this:
///   SET k v EX 9223372036854775807   (expected: invalid expire time)
///
/// PX gets no bound here. Its limit is i64::MAX minus the wall clock, which
/// only `Db::set` can see.
const MAX_EXPIRE_SECS: i64 = i64::MAX / 1000;

/// Reads the argument following an expiry keyword at index `i` and converts it
/// to a `Duration`.
///
///   missing argument    -> Syntax
///   not a number        -> NotAnInteger
///   zero or negative    -> InvalidExpiry
///   too large for EX    -> InvalidExpiry
fn expiry_arg(
    rest: &[Bytes],
    i: usize,
    cmd_name: &Bytes,
    unit: ExpiryUnit,
) -> Result<Duration, CommandError> {
    let raw = rest.get(i + 1).ok_or(CommandError::Syntax)?;
    let n = parse_i64(raw)?;
    if n <= 0 {
        return Err(CommandError::InvalidExpiry(cmd_name.clone()));
    }
    match unit {
        ExpiryUnit::Seconds => {
            if n > MAX_EXPIRE_SECS {
                return Err(CommandError::InvalidExpiry(cmd_name.clone()));
            }
            Ok(Duration::from_secs(n as u64))
        }
        // No bound here on purpose: the real limit depends on the wall clock,
        // so `Db::set` enforces it.
        ExpiryUnit::Millis => Ok(Duration::from_millis(n as u64)),
    }
}

/// Redis parses stream id components with string2ull, which falls back to
/// strtoull and therefore skips leading whitespace, while accepting a leading
/// plus and leading zeros. Rust's parse accepts the plus and the zeros but not
/// the whitespace, so trim and delegate.
///
/// Trailing whitespace stays rejected on both sides: string2ull requires the
/// end pointer to reach the terminator, and Rust's parse rejects it outright.
fn lenient_u64(text: &str) -> Result<u64, CommandError> {
    text.trim_start_matches(|c: char| c.is_ascii_whitespace())
        .parse()
        .map_err(|_| CommandError::InvalidStreamId)
}

/// Parse an integer command argument the way Redis does.
///
/// Redis uses string2ll here, which is stricter than Rust's FromStr in three
/// ways that a client can trip over: no leading `+`, no leading zeros unless
/// the whole argument is exactly `0`, and no surrounding whitespace. `-0` is
/// rejected too, because after the sign the first digit has to be 1 through 9.
///
/// This is only for integer *arguments*: EX, PX, COUNT, BLOCK, LPOP's count,
/// LRANGE's indexes. Stream IDs go through a different function in Redis
/// (string2ull, which falls back to strtoull and is therefore lenient), so do
/// not reuse this for them without checking the oracle first.
///
/// The scenarios in test.sh under "strict integer parsing" are the arbiter for
/// every rule above.
fn strict_i64(raw: &[u8]) -> Option<i64> {
    // The one case where a leading zero is legal, and the only way to write
    // zero at all.
    if raw == b"0" {
        return Some(0);
    }

    let (negative, digits) = match raw.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, raw),
    };

    // Requiring 1 through 9 here does most of the work at once: it rejects the
    // empty string, a second sign, `+`, a leading zero, `-0`, leading
    // whitespace, and anything non numeric.
    match digits.first() {
        Some(b'1'..=b'9') => {}
        _ => return None,
    }

    let mut magnitude: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None; // catches trailing junk and embedded whitespace
        }
        magnitude = magnitude
            .checked_mul(10)?
            .checked_add((b - b'0') as u64)?;
    }

    if negative {
        // The range is not symmetric: i64::MIN has magnitude 2^63, one more
        // than i64::MAX. wrapping_neg on the u64 followed by a bit preserving
        // cast lands exactly on i64::MIN for that one value.
        if magnitude > (i64::MAX as u64) + 1 {
            return None;
        }
        Some(magnitude.wrapping_neg() as i64)
    } else {
        if magnitude > i64::MAX as u64 {
            return None;
        }
        Some(magnitude as i64)
    }
}

fn parse_i64(raw: &[u8]) -> Result<i64, CommandError> {
    strict_i64(raw).ok_or(CommandError::NotAnInteger)
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

/// A BLPOP style timeout: a float number of seconds, where zero means block
/// forever. That is why the success type is `Option<Duration>` and not
/// `Duration`.
///
/// Three separate rejections with three different Redis messages, and the order
/// of the checks is what decides which message a value gets. `-inf` is the
/// interesting one: Redis parses it, multiplies by 1000, finds it is not above
/// LLONG_MAX, casts it, and only then sees a negative number, so it reports
/// "timeout is negative" rather than out of range. Checking negative before
/// range here reproduces that.
///
/// VERIFY the whole table on 6380: nan, inf, -inf, 1e300, -1, -0, abc.
fn parse_timeout(raw: &[u8]) -> Result<Option<Duration>, CommandError> {
    let text = std::str::from_utf8(raw).map_err(|_| CommandError::TimeoutError)?;
    let seconds: f64 = text.parse().map_err(|_| CommandError::TimeoutError)?;

    // NaN first, because it compares false against every bound below. Left to
    // fall through it would reach the constructor, and the panicking
    // constructor treats NaN as a panic, not an error.
    if seconds.is_nan() {
        return Err(CommandError::TimeoutError);
    }
    if seconds < 0.0 {
        return Err(CommandError::NegativeTimeout);
    }
    if seconds == 0.0 {
        return Ok(None); // block forever
    }

    // try_from_secs_f64, not from_secs_f64. The latter panics on infinity and
    // on anything too large for a Duration, which means `BLPOP k 1e300` would
    // take down the connection task instead of producing an error reply.
    Duration::try_from_secs_f64(seconds)
        .map(Some)
        .map_err(|_| CommandError::TimeoutOutOfRange)
}

fn parse_block(raw: &[u8]) -> Result<Blocking, CommandError> {
    let millis = strict_i64(raw).ok_or(CommandError::MTimeoutError)?;
    const MAX_BLOCK_MS: i64 = i64::MAX - (1 << 42);
    if millis < 0 {
        return Err(CommandError::NegativeTimeout);
    }
    if millis > MAX_BLOCK_MS {
        return Err(CommandError::TimeoutOutOfRange);
    }
    Ok(if millis == 0 {
        Blocking::Forever
    } else {
        Blocking::Until(Duration::from_millis(millis as u64))
    })
}

pub fn execute(cmd: Command, db: &mut Db) -> Result<Outcome, CommandError> {
    match cmd {
        Command::Ping(msg) => Ok(Outcome::Reply(generic::ping(msg)?)), 
        Command::Echo(msg) => Ok(Outcome::Reply(generic::echo(msg)?)),
        Command::Type { key } => Ok(Outcome::Reply(generic::type_of(&key, db)?)),
        Command::Del { keys } => Ok(Outcome::Reply(generic::del(keys, db)?)),
        Command::Exists { keys } => Ok(Outcome::Reply(generic::exists(keys, db)?)),
        Command::Get { key } => Ok(Outcome::Reply(string::get(&key, db)?)),
        Command::Set { key, value, expiry } => Ok(Outcome::Reply(string::set(key, value, expiry, db)?)),
        Command::Push { key, values, from } => Ok(Outcome::Reply(list::push(key, values, from, db)?)),
        Command::Pop { key, count, from } => Ok(Outcome::Reply(list::pop(&key, count, from, db)?)),
        Command::LLen { key } => Ok(Outcome::Reply(list::llen(&key, db)?)),
        Command::LRange { key, start, stop } => Ok(Outcome::Reply(list::lrange(&key, start, stop, db)?)),
        Command::LMove { src, dst, from, to } => Ok(Outcome::Reply(list::lmove(&src, dst, from, to, db)?)),
        Command::BPop { keys, timeout, from } => list::bpop(keys, from, timeout, db),
        Command::BLMove { src, dst, from, to, timeout } => list::blmove(src, dst, from, to, timeout, db),
        Command::XAdd { key, id, fields } => stream::xadd(key, id, fields, db),
        Command::XLen { key } => stream::xlen(&key, db),
        Command::XRange { key, start, stop, count } => stream::xrange(&key, start, stop, count, db),
        Command::XRead { count, timeout, streams } => stream::xread(count, timeout, streams, db),
        Command::XDel { key, ids} => stream::xdel(&key, &ids, db),
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

fn quote_args(args: &[Bytes]) -> String {
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
        assert_eq!(execute(c, &mut d).unwrap(), Outcome::Reply(Value::NullBulkString));
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
            Command::Unknown { name: Bytes::from_static(b"FOO"), args: vec![] },
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
        assert_eq!(consumed, out.len(), "one command must produce one reply frame");
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
#[cfg(test)]
mod strict_integer_tests {
    use super::strict_i64;

    /// One row per rule. When the harness disagrees with a row, change the row
    /// and then the parser, in that order, so the rule is always written down
    /// somewhere before it is implemented.
    #[test]
    fn accepts_what_redis_accepts() {
        let good: &[(&[u8], i64)] = &[
            (b"0", 0),
            (b"1", 1),
            (b"-1", -1),
            (b"10", 10),
            (b"-10", -10),
            (b"9223372036854775807", i64::MAX),
            (b"-9223372036854775808", i64::MIN),
        ];
        for (raw, want) in good {
            assert_eq!(strict_i64(raw), Some(*want), "input {:?}", String::from_utf8_lossy(raw));
        }
    }

    #[test]
    fn rejects_what_redis_rejects() {
        let bad: &[&[u8]] = &[
            b"",                        // empty
            b"+1",                      // leading plus
            b"01",                      // leading zero
            b"00",                      // leading zeros, value zero
            b"-0",                      // sign then zero: first digit must be 1..=9
            b"-01",
            b" 1",                      // leading space
            b"1 ",                      // trailing space
            b"1\n",
            b"1.0",                     // not an integer
            b"1e3",
            b"abc",
            b"-",                       // sign with no digits
            b"--1",
            b"9223372036854775808",     // i64::MAX + 1
            b"-9223372036854775809",    // i64::MIN - 1
            b"99999999999999999999999", // overflows u64 during accumulation
        ];
        for raw in bad {
            assert_eq!(strict_i64(raw), None, "input {:?}", String::from_utf8_lossy(raw));
        }
    }

    #[test]
    fn round_trips_every_boundary_of_the_accumulator() {
        // The checked_mul then checked_add pair is the part most likely to be
        // wrong by one, so walk the values either side of the limits.
        for n in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            assert_eq!(strict_i64(n.to_string().as_bytes()), Some(n), "{n}");
        }
    }
}