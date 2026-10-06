//! Helpers shared by the unit tests.

use std::sync::Arc;

use bytes::Bytes;

use crate::command::{Command, CommandError, to_command};
use crate::db::{Db, TestClock};
use crate::resp::{Value, parse};

pub fn b(s: &'static str) -> Bytes {
    Bytes::from_static(s.as_bytes())
}

pub fn bulk(s: &'static str) -> Value {
    Value::BulkString(b(s))
}

pub fn db() -> Db {
    db_and_clock().1
}

/// A database on a clock the test moves by hand.
pub fn db_and_clock() -> (Arc<TestClock>, Db) {
    let clock = Arc::new(TestClock::new());
    let db = Db::with_clock(clock.clone());
    (clock, db)
}

/// One request, from the raw bytes a client sent.
pub fn parse_raw(bytes: &[u8]) -> Result<Option<Command>, CommandError> {
    let (n, frame) = parse(bytes).unwrap().unwrap();
    let owned = Bytes::copy_from_slice(&bytes[..n]);
    to_command(frame.into_value(&owned))
}

/// One request, from its arguments.
pub fn cmd(parts: &[&str]) -> Result<Option<Command>, CommandError> {
    let mut raw = format!("*{}\r\n", parts.len());
    for p in parts {
        raw += &format!("${}\r\n{}\r\n", p.len(), p);
    }
    parse_raw(raw.as_bytes())
}

pub fn cmd_ok(parts: &[&str]) -> Command {
    cmd(parts).unwrap().expect("a command")
}
