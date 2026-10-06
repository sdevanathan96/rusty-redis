//! RESP2: a two pass parser and the encoder.

use bytes::Bytes;

/// One RESP value, generic over how payloads are stored: a `Frame` holds
/// offsets into the read buffer, a `Value` owns slices of it. One enum keeps
/// the two in step, while the two types keep a `Frame` out of `encode`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Node<S> {
    SimpleString(S),
    Error(S),
    Integer(i64),
    BulkString(S),
    Array(Vec<Node<S>>),
    NullBulkString,
    NullArray,
}

pub type Frame = Node<Span>;
pub type Value = Node<Bytes>;

impl<S> Node<S> {
    /// Rebuilds the tree with every payload converted. `f` is borrowed so each
    /// recursive call can reuse it.
    pub fn map<T>(self, f: &impl Fn(S) -> T) -> Node<T> {
        match self {
            Node::SimpleString(s) => Node::SimpleString(f(s)),
            Node::Error(s) => Node::Error(f(s)),
            Node::BulkString(s) => Node::BulkString(f(s)),
            Node::Integer(i) => Node::Integer(i),
            Node::Array(items) => Node::Array(items.into_iter().map(|n| n.map(f)).collect()),
            Node::NullBulkString => Node::NullBulkString,
            Node::NullArray => Node::NullArray,
        }
    }
}

impl Node<Span> {
    /// Resolves every span against the buffer the frame was parsed from. The
    /// spans count from the start of that buffer.
    pub fn into_value(self, buf: &Bytes) -> Value {
        self.map(&|s| s.as_bytes(buf))
    }
}

impl Value {
    pub fn ok() -> Value {
        Value::SimpleString(Bytes::from_static(b"OK"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    fn as_slice<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
        &buf[self.start..self.end]
    }

    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.start..self.end)
    }
}

pub type RedisResult = Result<Option<(usize, Frame)>, RespError>;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum RespError {
    UnknownType(u8),
    BadInteger,
    BadLength(i64),
    BadTerminator,
    TooDeep,
    TooLongLine,
}

impl RespError {
    pub fn to_resp(&self) -> Bytes {
        match self {
            RespError::UnknownType(b) => Bytes::from(format!(
                "ERR Protocol error: unexpected type byte 0x{b:02x}"
            )),
            RespError::BadInteger => Bytes::from_static(b"ERR Protocol error: invalid integer"),
            RespError::BadLength(n) => {
                Bytes::from(format!("ERR Protocol error: invalid length {n}"))
            }
            RespError::BadTerminator => Bytes::from_static(b"ERR Protocol error: expected CRLF"),
            RespError::TooDeep => Bytes::from_static(b"ERR Protocol error: nested arrays too deep"),
            // Redis says this or "too big mbulk count string", depending on
            // where the line was; both close the connection.
            RespError::TooLongLine => {
                Bytes::from_static(b"ERR Protocol error: too big inline request")
            }
        }
    }
}

const MAX_DEPTH: usize = 32;

/// `Ok(Some((n, frame)))` for a frame in the first `n` bytes, `Ok(None)` if
/// incomplete, `Err` if malformed, which closes the connection.
pub fn parse(input: &[u8]) -> RedisResult {
    parse_for(input, 0, 0)
}

fn parse_for(input: &[u8], pos: usize, depth: usize) -> RedisResult {
    if pos >= input.len() {
        return Ok(None);
    }
    if depth > MAX_DEPTH {
        return Err(RespError::TooDeep);
    }
    match input[pos] {
        b'+' => simple_string(input, pos + 1),
        b'-' => error(input, pos + 1),
        b':' => integer(input, pos + 1),
        b'$' => bulk_string(input, pos + 1),
        b'*' => array(input, pos + 1, depth),
        other => Err(RespError::UnknownType(other)),
    }
}

/// Redis's PROTO_INLINE_MAX_SIZE. Only headers are lines, never payloads, and
/// without a cap a line with no CRLF would grow `inbuf` forever.
const MAX_LINE_LEN: usize = 64 * 1024;

/// The CRLF terminated line at `pos`, and the index just past the CRLF.
fn line(input: &[u8], pos: usize) -> Result<Option<(Span, usize)>, RespError> {
    let Some(rest) = input.get(pos..) else {
        return Ok(None);
    };
    let cr = match rest.iter().position(|&b| b == b'\r') {
        Some(i) => i,
        None if rest.len() > MAX_LINE_LEN => return Err(RespError::TooLongLine),
        None => return Ok(None),
    };
    if cr > MAX_LINE_LEN {
        return Err(RespError::TooLongLine);
    }
    if pos + cr + 1 >= input.len() {
        return Ok(None); // CR arrived, LF has not
    }
    if rest[cr + 1] != b'\n' {
        return Err(RespError::BadTerminator);
    }
    Ok(Some((
        Span {
            start: pos,
            end: pos + cr,
        },
        pos + cr + 2,
    )))
}

fn bulk_string(input: &[u8], pos: usize) -> RedisResult {
    const MAX_BULK_LEN: i64 = 512 * 1024 * 1024; // Redis's proto-max-bulk-len

    let Some((len_bytes, data_start)) = line(input, pos)? else {
        return Ok(None);
    };
    let len: i64 = parse_int(input, len_bytes)?;
    if len == -1 {
        return Ok(Some((data_start, Frame::NullBulkString)));
    }
    if !(0..=MAX_BULK_LEN).contains(&len) {
        return Err(RespError::BadLength(len));
    }
    // The payload is taken by length, never scanned.
    let end = data_start + len as usize;
    if input.len() < end + 2 {
        return Ok(None);
    }
    if &input[end..end + 2] != b"\r\n" {
        return Err(RespError::BadTerminator);
    }

    Ok(Some((
        end + 2,
        Frame::BulkString(Span {
            start: data_start,
            end,
        }),
    )))
}

fn parse_int(input: &[u8], span: Span) -> Result<i64, RespError> {
    let text = std::str::from_utf8(span.as_slice(input)).map_err(|_| RespError::BadInteger)?;
    text.parse().map_err(|_| RespError::BadInteger)
}

fn simple_string(input: &[u8], pos: usize) -> RedisResult {
    let Some((span, next)) = line(input, pos)? else {
        return Ok(None);
    };
    Ok(Some((next, Frame::SimpleString(span))))
}

fn error(input: &[u8], pos: usize) -> RedisResult {
    let Some((span, next)) = line(input, pos)? else {
        return Ok(None);
    };
    Ok(Some((next, Frame::Error(span))))
}

fn integer(input: &[u8], pos: usize) -> RedisResult {
    let Some((span, next)) = line(input, pos)? else {
        return Ok(None);
    };
    Ok(Some((next, Frame::Integer(parse_int(input, span)?))))
}

fn array(input: &[u8], pos: usize, depth: usize) -> RedisResult {
    const MAX_ARRAY_LEN: i64 = 1024 * 1024;
    // `*1000000\r\n` alone must not allocate a million frames, on every retry
    // of an incomplete parse.
    const PREALLOC_CAP: usize = 1024;

    let Some((len_bytes, data_start)) = line(input, pos)? else {
        return Ok(None);
    };
    let len: i64 = parse_int(input, len_bytes)?;
    if len == -1 {
        return Ok(Some((data_start, Frame::NullArray)));
    }
    if !(0..=MAX_ARRAY_LEN).contains(&len) {
        return Err(RespError::BadLength(len));
    }

    let mut curr_pos = data_start;
    let mut values = Vec::with_capacity((len as usize).min(PREALLOC_CAP));
    for _ in 0..len {
        match parse_for(input, curr_pos, depth + 1)? {
            Some((new_pos, value)) => {
                curr_pos = new_pos;
                values.push(value);
            }
            None => return Ok(None),
        }
    }

    Ok(Some((curr_pos, Frame::Array(values))))
}

pub fn encode<B: bytes::BufMut>(value: &Value, out: &mut B) {
    match value {
        Value::Integer(i) => {
            out.put_u8(b':');
            out.put_slice(i.to_string().as_bytes());
            out.put_slice(b"\r\n");
        }
        Value::Array(items) => {
            out.put_u8(b'*');
            out.put_slice(items.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            for item in items {
                encode(item, out);
            }
        }
        Value::BulkString(bytes) => {
            out.put_u8(b'$');
            out.put_slice(bytes.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            out.put_slice(bytes);
            out.put_slice(b"\r\n");
        }
        Value::SimpleString(bytes) => {
            out.put_u8(b'+');
            put_line_payload(out, bytes);
            out.put_slice(b"\r\n");
        }
        Value::Error(bytes) => {
            out.put_u8(b'-');
            put_line_payload(out, bytes);
            out.put_slice(b"\r\n");
        }
        Value::NullBulkString => {
            out.put_slice(b"$-1\r\n");
        }
        Value::NullArray => {
            out.put_slice(b"*-1\r\n");
        }
    }
}

/// A simple string or error must stay one line, so CR and LF become spaces,
/// as in Redis.
fn put_line_payload<B: bytes::BufMut>(out: &mut B, bytes: &[u8]) {
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\r' || b == b'\n' {
            out.put_slice(&bytes[start..i]);
            out.put_u8(b' ');
            start = i + 1;
        }
    }
    out.put_slice(&bytes[start..]);
}

#[cfg(test)]
mod resp_parser_tests {
    use bytes::Bytes;

    use crate::resp::{Frame, RespError, Span, Value, encode, parse};

    fn parse_value(input: &[u8]) -> Result<Option<(usize, Value)>, RespError> {
        Ok(parse(input)?.map(|(n, f)| {
            let buf = Bytes::copy_from_slice(&input[..n]);
            (n, f.into_value(&buf))
        }))
    }

    #[test]
    fn test_parse_simple_string() {
        let input = b"+OK\r\n";
        let result = parse(input).unwrap();
        assert_eq!(
            result,
            Some((5, Frame::SimpleString(Span { start: 1, end: 3 })))
        );
    }

    #[test]
    fn test_parse_error() {
        let input = b"-Error message\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((16, Frame::Error(Span { start: 1, end: 14 }))));
    }

    #[test]
    fn test_parse_integer() {
        let input = b":1000\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((7, Frame::Integer(1000))));
    }

    #[test]
    fn test_parse_bulk_string() {
        let input = b"$6\r\nfoobar\r\n";
        let result = parse(input).unwrap();
        assert_eq!(
            result,
            Some((12, Frame::BulkString(Span { start: 4, end: 10 })))
        );
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let input = b"$-1\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((5, Frame::NullBulkString)));
        let input2 = b"$0\r\n\r\n";
        let result2 = parse(input2).unwrap();
        assert_eq!(
            result2,
            Some((6, Frame::BulkString(Span { start: 4, end: 4 })))
        );
    }

    #[test]
    fn test_parse_array() {
        let input = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let result = parse(input).unwrap();
        assert_eq!(
            result,
            Some((
                22,
                Frame::Array(vec![
                    Frame::BulkString(Span { start: 8, end: 11 }),
                    Frame::BulkString(Span { start: 17, end: 20 })
                ])
            ))
        );
        let result2 = parse(b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n").unwrap();
        assert_eq!(
            result2,
            Some((
                27,
                Frame::Array(vec![
                    Frame::BulkString(Span { start: 8, end: 11 }),
                    Frame::NullBulkString,
                    Frame::BulkString(Span { start: 22, end: 25 })
                ])
            ))
        );
    }

    #[test]
    fn test_parse_null_array() {
        let input = b"*-1\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((5, Frame::NullArray)));
    }

    #[test]
    fn test_parse_empty_array() {
        let input = b"*0\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((4, Frame::Array(vec![]))));
    }

    #[test]
    fn test_multiple() {
        assert_eq!(
            parse(b"$5\r\nhello\r\n"),
            Ok(Some((11, Frame::BulkString(Span { start: 4, end: 9 }))))
        );

        assert_eq!(parse(b"*1\r\n"), Ok(None), "partial array");

        // CRLF inside a payload is data.
        assert_eq!(
            parse(b"$5\r\na\r\nbc\r\n"),
            Ok(Some((11, Frame::BulkString(Span { start: 4, end: 9 }))))
        );

        // partial array with one element delivered
        assert_eq!(parse(b"*3\r\n$1\r\na\r\n"), Ok(None));
    }

    #[test]
    fn test_parse_invalid() {
        let input = b"$3\r\nfoo"; // missing CRLF after payload
        let result = parse(input);
        assert_eq!(result, Ok(None)); // Incomplete, should read more bytes
        let input2 = b"$3\r\nfoo\r"; // missing LF after CR
        let result2 = parse(input2);
        assert_eq!(result2, Ok(None)); // Malformed
        let input3 = b"$3\r\nfoo\r\r"; // valid bulk string followed by null bulk string
        let result3 = parse(input3);
        assert_eq!(result3, Err(RespError::BadTerminator)); // Malformed
    }

    #[test]
    fn test_nested_arrays() {
        let input = b"*2\r\n*1\r\n:1\r\n*2\r\n+a\r\n-b\r\n";
        assert_eq!(
            parse_value(input),
            Ok(Some((
                input.len(),
                Value::Array(vec![
                    Value::Array(vec![Value::Integer(1)]),
                    Value::Array(vec![
                        Value::SimpleString(Bytes::from_static(b"a")),
                        Value::Error(Bytes::from_static(b"b")),
                    ]),
                ])
            )))
        );
    }

    #[test]
    fn test_parse_unknown() {
        let input = b"@";
        let result = parse(input);
        assert_eq!(result, Err(RespError::UnknownType(b'@')));
        let input2 = b"";
        let result2 = parse(input2);
        assert_eq!(result2, Ok(None));
    }

    #[test]
    fn test_absurd_array_length_is_rejected() {
        let input = b"*77777777777771\r\n";
        assert_eq!(parse(input), Err(RespError::BadLength(77777777777771)));
    }

    #[test]
    fn encode_parse_round_trip() {
        for v in round_trip_values() {
            let mut out = Vec::new();
            encode(&v, &mut out);
            assert_eq!(
                parse(&out).map(|result| result.map(|(consumed, _)| consumed)),
                Ok(Some(out.len())),
                "failed for {v:?}"
            );
        }
    }

    #[test]
    fn crlf_in_a_line_payload_cannot_split_the_reply() {
        let evil = Value::Error(Bytes::from_static(b"ERR unknown command 'FOO\r\n+INJECTED"));
        let mut out = Vec::new();
        encode(&evil, &mut out);

        let (consumed, parsed) = parse(&out).unwrap().unwrap();
        assert_eq!(
            consumed,
            out.len(),
            "one value must encode to exactly one frame"
        );

        let owned = Bytes::copy_from_slice(&out);
        match parsed.into_value(&owned) {
            Value::Error(msg) => {
                assert!(!msg.contains(&b'\r'));
                assert!(!msg.contains(&b'\n'));
            }
            other => panic!("expected an error, got {other:?}"),
        }
    }
    #[test]
    fn deep_nesting_is_rejected() {
        let bomb = b"*1\r\n".repeat(100_000);
        assert!(parse(&bomb).is_err());
    }

    fn assert_streams(bytes: &[u8]) {
        for n in 0..bytes.len() {
            assert_eq!(
                parse(&bytes[..n]),
                Ok(None),
                "prefix of length {n} should be incomplete"
            );
        }
        assert_eq!(
            parse(bytes).map(|result| result.map(|(consumed, _)| consumed)),
            Ok(Some(bytes.len()))
        );
    }

    fn round_trip_values() -> Vec<Value> {
        let values = vec![
            Value::SimpleString(Bytes::from_static(b"OK")),
            Value::SimpleString(Bytes::copy_from_slice(&[])),
            Value::Error(Bytes::from_static(b"ERR something went wrong")), // was missing
            Value::Integer(0),
            Value::Integer(-42),
            Value::Integer(i64::MAX),
            Value::Integer(i64::MIN),
            Value::BulkString(Bytes::copy_from_slice(&[])),
            Value::BulkString(Bytes::from_static(b"a\r\nb")), // binary safe, must survive
            Value::BulkString(Bytes::copy_from_slice(&[0, 255, 13, 10])), // arbitrary bytes
            Value::NullBulkString,
            Value::NullArray,
            Value::Array(vec![]),
            Value::Array(vec![Value::Integer(1), Value::NullBulkString]),
            Value::Array(vec![
                Value::Array(vec![Value::SimpleString(Bytes::from_static(b"a"))]),
                Value::Error(Bytes::from_static(b"ERR nested")),
                Value::BulkString(Bytes::from_static(b"x\r\ny")),
            ]),
        ];
        values
    }

    #[test]
    fn every_frame_streams_byte_by_byte() {
        for v in round_trip_values() {
            let mut out = Vec::new();
            encode(&v, &mut out);
            assert_streams(&out);
        }
    }
}
