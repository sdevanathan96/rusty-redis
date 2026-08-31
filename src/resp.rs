// src/resp.rs
use std::convert::From;
// use std::io;
// use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    SimpleString(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<Value>),
    NullBulkString,
    NullArray,
}

#[derive(Debug)]
pub enum RespError {
    UnknownType(u8),
    BadInteger,
    IOError(std::io::Error),
    BadLength(i64),
    BadTerminator,
    TooDeep,
}

impl From<std::io::Error> for RespError {
    fn from(e: std::io::Error) -> RespError {
        RespError::IOError(e)
    }
}

impl RespError {
    pub fn to_resp(&self) -> Vec<u8> {
        match self {
            RespError::UnknownType(b) =>
                format!("ERR Protocol error: unexpected type byte 0x{b:02x}").into_bytes(),
            RespError::BadInteger =>
                b"ERR Protocol error: invalid integer".to_vec(),
            RespError::BadLength(n) =>
                format!("ERR Protocol error: invalid length {n}").into_bytes(),
            RespError::BadTerminator =>
                b"ERR Protocol error: expected CRLF".to_vec(),
            RespError::IOError(e) =>
                format!("ERR IO error: {}", e).into_bytes(),
            RespError::TooDeep =>
                b"ERR Protocol error: nested arrays too deep".to_vec(),
        }
    }
}

impl PartialEq for RespError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::UnknownType(left), Self::UnknownType(right)) => left == right,
            (Self::BadInteger, Self::BadInteger) => true,
            (Self::IOError(left), Self::IOError(right)) => {
                left.kind() == right.kind() && left.to_string() == right.to_string()
            }
            (Self::BadLength(left), Self::BadLength(right)) => left == right,
            (Self::BadTerminator, Self::BadTerminator) => true,
            (Self::TooDeep, Self::TooDeep) => true,
            _ => false,
        }
    }
}

#[derive(Default)]
pub struct RespParser;

const MAX_DEPTH: usize = 32;

/// Returns:
///   Ok(Some((n, value))) - parsed a value occupying the first n bytes
///   Ok(None)             - incomplete, caller should read more bytes
///   Err(e)               - malformed, caller should close the connection
pub fn parse(input: &[u8]) -> Result<Option<(usize, Value)>, RespError> {
    parse_for(input, 0, 0)
}


fn parse_for(input: &[u8], pos: usize, depth: usize) -> Result<Option<(usize, Value)>, RespError>{
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
        // everything below is stage 6
        // b'_' => null(input, pos + 1),
        // b'#' => boolean(input, pos + 1),
        // b',' => double(input, pos + 1),
        // b'%' => map(input, pos + 1),
        // b'~' => set(input, pos + 1),
        // b'>' => push(input, pos + 1),
        other => Err(RespError::UnknownType(other)),
    }
}

/// Find the next CRLF terminated line starting at `pos`.
/// Returns the line contents and the index just past the CRLF.
fn line(input: &[u8], pos: usize) -> Result<Option<(&[u8], usize)>, RespError> {
    let rest = match input.get(pos..) {
        Some(r) => r,
        None => return Ok(None),          // cursor past the end, need more bytes
    };
    let cr = match rest.iter().position(|&b| b == b'\r') {
        Some(i) => i,
        None => return Ok(None),          // no CR yet
    };
    if pos + cr + 1 >= input.len() {
        return Ok(None);                  // CR arrived, LF has not
    }
    if rest[cr + 1] != b'\n' {
        return Err(RespError::BadTerminator);
    }
    Ok(Some((&rest[..cr], pos + cr + 2)))
}

fn bulk_string(input: &[u8], pos: usize) -> Result<Option<(usize, Value)>, RespError> {
    let (len_bytes, data_start) = match line(input, pos)? {
        Some(v) => v,
        None => return Ok(None),
    };

    let len: i64 = parse_int(len_bytes)?;

    if len == -1 {
        return Ok(Some((data_start, Value::NullBulkString)));
    }
    if len < 0 {
        return Err(RespError::BadLength(len));
    }

    // payload: taken blind, never scanned
    const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;   // proto-max-bulk-len

    if len > MAX_BULK_LEN {
        return Err(RespError::BadLength(len));
    }
    let end = data_start + len as usize;
    if input.len() < end + 2 {
        return Ok(None);              // payload or trailing CRLF still in flight
    }
    if &input[end..end + 2] != b"\r\n" {
        return Err(RespError::BadTerminator);
    }

    Ok(Some((end + 2, Value::BulkString(input[data_start..end].to_vec()))))
}

fn parse_int(bytes: &[u8]) -> Result<i64, RespError> {
    let text = std::str::from_utf8(bytes).map_err(|_| RespError::BadInteger)?;
    text.parse().map_err(|_| RespError::BadInteger)
}

fn simple_string(input: &[u8], pos: usize) -> Result<Option<(usize, Value)>, RespError> {
    let (line_bytes, next_pos) = match line(input, pos)? {
        Some(v) => v,
        None => return Ok(None),
    };
    Ok(Some((next_pos, Value::SimpleString(line_bytes.to_vec()))))
}

fn error(input: &[u8], pos: usize) -> Result<Option<(usize, Value)>, RespError> {
    let (line_bytes, next_pos) = match line(input, pos)? {
        Some(v) => v,
        None => return Ok(None),
    };
    Ok(Some((next_pos, Value::Error(line_bytes.to_vec()))))
}

fn integer(input: &[u8], pos: usize) -> Result<Option<(usize, Value)>, RespError> {
    let (line_bytes, next_pos) = match line(input, pos)? {
        Some(v) => v,
        None => return Ok(None),
    };
    let integer_val: i64 = parse_int(line_bytes)?;
    Ok(Some((next_pos, Value::Integer(integer_val))))
}

fn array(input: &[u8], pos: usize, depth: usize) -> Result<Option<(usize, Value)>, RespError> {
    let (len_bytes, data_start) = match line(input, pos)? {
        Some(v) => v,
        None => return Ok(None),
    };

    let len: i64 = parse_int(len_bytes)?;

    if len == -1 {
        return Ok(Some((data_start, Value::NullArray)));
    }
    if len < 0 {
        return Err(RespError::BadLength(len));
    }

    let mut curr_pos = data_start;
    const MAX_ARRAY_LEN: i64 = 1024 * 1024;

    if len > MAX_ARRAY_LEN {
        return Err(RespError::BadLength(len));
    }
    let mut values = Vec::with_capacity(len as usize);
    for _ in 0..len {
        match parse_for(input, curr_pos, depth + 1)? {
            Some((new_pos, value)) => {
                curr_pos = new_pos;
                values.push(value);
            }
            None => return Ok(None),
        }
    }

    Ok(Some((curr_pos, Value::Array(values))))
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
                encode(item, out);      // recursion passes the same B through
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
    use crate::resp::{encode, parse, RespError, Value};

    #[test]
    fn test_parse_simple_string() {
        let input = b"+OK\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((5, Value::SimpleString(b"OK".to_vec()))));
    }

    #[test]
    fn test_parse_error() {
        let input = b"-Error message\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((16, Value::Error(b"Error message".to_vec()))));
    }

    #[test]
    fn test_parse_integer() {
        let input = b":1000\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((7, Value::Integer(1000))));    
    }

    #[test]
    fn test_parse_bulk_string() {
        let input = b"$6\r\nfoobar\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((12, Value::BulkString(b"foobar".to_vec()))));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let input = b"$-1\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((5, Value::NullBulkString)));
        let input2 = b"$0\r\n\r\n";
        let result2 = parse(input2).unwrap();
        assert_eq!(result2, Some((6, Value::BulkString(b"".to_vec()))));
    }

    #[test]
    fn test_parse_array() {
        let input = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((22, Value::Array(vec![
            Value::BulkString(b"foo".to_vec()),
            Value::BulkString(b"bar".to_vec())
        ]))));  
        let result2 = parse(b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n").unwrap();
        assert_eq!(result2, Some((27, Value::Array(vec![
            Value::BulkString(b"foo".to_vec()),
            Value::NullBulkString,
            Value::BulkString(b"bar".to_vec())
        ]))));
    }

    #[test]
    fn test_parse_null_array() {
        let input = b"*-1\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((5, Value::NullArray)));
    }

    #[test]
    fn test_parse_empty_array() {
        let input = b"*0\r\n";
        let result = parse(input).unwrap();
        assert_eq!(result, Some((4, Value::Array(vec![]))));
    }

    #[test]
    fn test_multiple(){
        // catches bug 2
        assert_eq!(parse(b"$5\r\nhello\r\n"),
                Ok(Some((11, Value::BulkString(b"hello".to_vec())))));

        // catches bug 1 and 3, and is the partial array case you asked about
        assert_eq!(parse(b"*1\r\n"), Ok(None));

        // the CRLF inside a payload case
        assert_eq!(parse(b"$5\r\na\r\nbc\r\n"),
                Ok(Some((11, Value::BulkString(b"a\r\nbc".to_vec())))));

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
        assert_eq!(parse(input), Ok(Some((input.len(), Value::Array(vec![
            Value::Array(vec![Value::Integer(1)]),
            Value::Array(vec![Value::SimpleString(b"a".to_vec()),
                            Value::Error(b"b".to_vec())]),
        ])))));
    }

    #[test]
    fn test_parse_unknown(){
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
            assert_eq!(parse(&out), Ok(Some((out.len(), v.clone()))), "failed for {v:?}");
        }
    }

    #[test]
    fn crlf_in_a_line_payload_cannot_split_the_reply() {
        let evil = Value::Error(b"ERR unknown command 'FOO\r\n+INJECTED".to_vec());
        let mut out = Vec::new();
        encode(&evil, &mut out);

        let (consumed, parsed) = parse(&out).unwrap().unwrap();
        assert_eq!(consumed, out.len(), "must encode to exactly one frame");
        match parsed {
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

    fn assert_streams(bytes: &[u8], expected: Value) {
        for n in 0..bytes.len() {
            assert_eq!(parse(&bytes[..n]), Ok(None), "prefix of length {n} should be incomplete");
        }
        assert_eq!(parse(bytes), Ok(Some((bytes.len(), expected))));
    }

    fn round_trip_values() -> Vec<Value> {
        let values = vec![
            Value::SimpleString(b"OK".to_vec()),
            Value::SimpleString(vec![]),
            Value::Error(b"ERR something went wrong".to_vec()),   // was missing
            Value::Integer(0),
            Value::Integer(-42),
            Value::Integer(i64::MAX),
            Value::Integer(i64::MIN),
            Value::BulkString(vec![]),
            Value::BulkString(b"a\r\nb".to_vec()),                // binary safe, must survive
            Value::BulkString(vec![0, 255, 13, 10]),              // arbitrary bytes
            Value::NullBulkString,
            Value::NullArray,                                     // was missing
            Value::Array(vec![]),
            Value::Array(vec![Value::Integer(1), Value::NullBulkString]),
            Value::Array(vec![                                    // nesting was missing
                Value::Array(vec![Value::SimpleString(b"a".to_vec())]),
                Value::Error(b"ERR nested".to_vec()),
                Value::BulkString(b"x\r\ny".to_vec()),
            ]),
        ];
        values
    }

    #[test]
    fn every_frame_streams_byte_by_byte() {
        for v in round_trip_values() {
            let mut out = Vec::new();
            encode(&v, &mut out);
            assert_streams(&out, v);
        }
    }

}