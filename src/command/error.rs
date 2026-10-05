//! Every error a command can answer with, and the exact text Redis sends for
//! each.

use bytes::Bytes;

use crate::db::{IncrError, WrongType, XaddError};

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
    UnknownCommand {
        name: Bytes,
        args: Vec<Bytes>,
    },
    /// MULTI, EXEC or DISCARD reached `execute`. The connection handles them,
    /// so this means a bug, but it still replies with a well formed error.
    HandledByConnection,
    TimeoutNotAFloat,
    TimeoutNotAnInteger,
    TimeoutNegative,
    TimeoutOutOfRange,
    InvalidStreamId,
    XaddIdZero,
    XaddIdTooSmall,
    UnbalancedXread,
    IncrOverflow,
    LimitWithoutApprox,
    MaxlenNegative,
    MaxlenWithMinid,
    LimitNegative,
    LimitWithoutStrategy,
    NestedMulti,
    ExecAbortPreviousErrors,
    ExecWithoutMulti,
    DiscardWithoutMulti,
    /// EXEC itself was rejected before running, for now only for a wrong
    /// argument count. Redis answers every such rejection with EXECABORT,
    /// inside MULTI or not, and discards any open transaction.
    ExecAbortRejected(Box<CommandError>),
}

impl From<WrongType> for CommandError {
    fn from(_: WrongType) -> Self {
        CommandError::WrongType
    }
}

impl From<IncrError> for CommandError {
    fn from(e: IncrError) -> Self {
        match e {
            IncrError::NotAnInteger => CommandError::NotAnInteger,
            IncrError::Overflow => CommandError::IncrOverflow,
            IncrError::WrongType => CommandError::WrongType,
        }
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

impl CommandError {
    /// The message body only. No leading '-' and no trailing CRLF: those belong
    /// to `resp::encode` when it writes a `Value::Error`.
    pub fn to_resp(&self) -> Bytes {
        match self {
            CommandError::NotAnArray => {
                Bytes::from_static(b"ERR Protocol error: expected an array of bulk strings")
            }
            CommandError::NotBulkString => {
                Bytes::from_static(b"ERR Protocol error: expected a bulk string")
            }
            CommandError::WrongArity(name) => Bytes::from(format!(
                "ERR wrong number of arguments for '{}' command",
                lower(name)
            )),
            CommandError::NotAnInteger => {
                Bytes::from_static(b"ERR value is not an integer or out of range")
            }
            CommandError::InvalidExpiry(name) => Bytes::from(format!(
                "ERR invalid expire time in '{}' command",
                lower(name)
            )),
            CommandError::OutOfRange => {
                Bytes::from_static(b"ERR value is out of range, must be positive")
            }
            CommandError::Syntax => Bytes::from_static(b"ERR syntax error"),
            CommandError::WrongType => Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
            CommandError::UnknownCommand { name, args } => {
                if args.is_empty() {
                    Bytes::from(format!("ERR unknown command '{}'", quote(name)))
                } else {
                    Bytes::from(format!(
                        "ERR unknown command '{}', with args beginning with: {}",
                        quote(name),
                        quote_args(args)
                    ))
                }
            }
            CommandError::HandledByConnection => {
                Bytes::from_static(b"ERR MULTI, EXEC and DISCARD are handled by the connection")
            }
            CommandError::TimeoutNotAFloat => {
                Bytes::from_static(b"ERR timeout is not a float or out of range")
            }
            CommandError::TimeoutNotAnInteger => {
                Bytes::from_static(b"ERR timeout is not an integer or out of range")
            }
            CommandError::TimeoutNegative => Bytes::from_static(b"ERR timeout is negative"),
            CommandError::TimeoutOutOfRange => Bytes::from_static(b"ERR timeout is out of range"),
            CommandError::InvalidStreamId => {
                Bytes::from_static(b"ERR Invalid stream ID specified as stream command argument")
            }
            CommandError::XaddIdZero => {
                Bytes::from_static(b"ERR The ID specified in XADD must be greater than 0-0")
            }
            CommandError::XaddIdTooSmall => Bytes::from_static(
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ),
            CommandError::UnbalancedXread => Bytes::from_static(
                b"ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.",
            ),
            CommandError::IncrOverflow => {
                Bytes::from_static(b"ERR increment or decrement would overflow")
            }
            CommandError::LimitWithoutApprox => Bytes::from_static(
                b"ERR syntax error, LIMIT cannot be used without the special ~ option",
            ),
            CommandError::MaxlenNegative => {
                Bytes::from_static(b"ERR The MAXLEN argument must be >= 0.")
            }
            CommandError::MaxlenWithMinid => Bytes::from_static(
                b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
            ),
            CommandError::LimitNegative => Bytes::from_static(b"ERR The LIMIT argument must be >= 0."),
            CommandError::LimitWithoutStrategy => Bytes::from_static(
                b"ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
            ),
            CommandError::NestedMulti => Bytes::from_static(b"ERR MULTI calls can not be nested"),
            CommandError::ExecAbortPreviousErrors => Bytes::from_static(
                b"EXECABORT Transaction discarded because of previous errors.",
            ),
            CommandError::ExecWithoutMulti => Bytes::from_static(b"ERR EXEC without MULTI"),
            CommandError::DiscardWithoutMulti => Bytes::from_static(b"ERR DISCARD without MULTI"),
            CommandError::ExecAbortRejected(inner) => {
                // Redis gives the reason without its ERR prefix.
                let reason = inner.to_resp();
                let reason = reason.strip_prefix(b"ERR ").unwrap_or(&reason);
                let mut out = b"EXECABORT Transaction discarded because of: ".to_vec();
                out.extend_from_slice(reason);
                Bytes::from(out)
            }
        }
    }
}

/// Redis lowercases the command name in arity and expiry errors even when the
/// client sent it uppercase, but quotes it verbatim in "unknown command".
fn lower(name: &[u8]) -> String {
    String::from_utf8_lossy(name).to_lowercase()
}

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
