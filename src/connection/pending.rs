//! What the connection has parsed but not yet acted on, in arrival order.

use std::collections::VecDeque;

use bytes::BytesMut;

use crate::command::{Command, CommandError, to_command};
use crate::resp::{self, Value};

/// One thing the connection must do, in the order the client sent it.
pub(super) enum Parsed {
    Run(Command),
    Error(CommandError),
    ProtocolError(Value),
}

/// Items parsed but not yet acted on, and how many input bytes they hold.
/// The count is what is pending right now, not a running total, so the query
/// buffer limit can check `inbuf.len() + pending.bytes()`.
pub(super) struct Pending {
    items: VecDeque<(Parsed, usize)>,
    bytes: usize,
}

impl Pending {
    pub(super) fn new() -> Self {
        Pending {
            items: VecDeque::new(),
            bytes: 0,
        }
    }

    /// `size` is the item's length on the wire: `consumed` from `resp::parse`.
    pub(super) fn push(&mut self, item: Parsed, size: usize) {
        self.bytes += size;
        self.items.push_back((item, size));
    }

    /// The item and its size, so a command moving on into a MULTI queue
    /// keeps its bytes counted.
    pub(super) fn pop(&mut self) -> Option<(Parsed, usize)> {
        let (item, size) = self.items.pop_front()?;
        self.bytes -= size;
        Some((item, size))
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Parses every complete frame in `inbuf` into `pending`. Runs nothing. Stops
/// at a protocol error, since nothing after it can be framed.
pub(super) fn parse_into(inbuf: &mut BytesMut, pending: &mut Pending) {
    loop {
        match resp::parse(inbuf) {
            Ok(Some((consumed, frame))) => {
                let owned = inbuf.split_to(consumed).freeze();
                let value = frame.into_value(&owned);
                match to_command(value) {
                    Ok(Some(cmd)) => pending.push(Parsed::Run(cmd), consumed),
                    Ok(None) => {}
                    Err(e) => pending.push(Parsed::Error(e), consumed),
                }
            }
            Ok(None) => return,
            Err(e) => {
                pending.push(Parsed::ProtocolError(Value::Error(e.to_resp())), 0);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_track_what_is_pending_now() {
        let mut p = Pending::new();
        p.push(Parsed::Error(CommandError::Syntax), 10);
        p.push(Parsed::Error(CommandError::Syntax), 5);
        assert_eq!(p.bytes(), 15);
        assert!(p.pop().is_some());
        assert_eq!(p.bytes(), 5, "popping gives the bytes back");
        assert!(p.pop().is_some());
        assert!(p.pop().is_none());
        assert_eq!(p.bytes(), 0);
    }
}
