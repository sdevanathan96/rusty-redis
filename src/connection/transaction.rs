//! An open MULTI: the commands held for EXEC, and the rule for which errors
//! fail the transaction as they are queued.

use bytes::Bytes;

use super::pending::Parsed;
use crate::command::{Command, CommandError};
use crate::resp::Value;

/// An open MULTI. `None` on the connection means no transaction is open.
#[derive(Default)]
pub(super) struct Transaction {
    /// Parse errors other than a wrong argument count are held as `Err` and
    /// answered by EXEC in their slot, as Redis does.
    pub(super) commands: Vec<Result<Command, CommandError>>,
    /// A queue-time error happened; EXEC answers EXECABORT.
    pub(super) failed: bool,
    /// Wire bytes of everything in `commands`, counted toward MAX_QUERY_BUF.
    /// No parked wait needs it: nothing parks while a transaction is open.
    pub(super) bytes: usize,
}

/// Queues one item inside MULTI and returns the immediate reply. Only an
/// unknown command and a wrong argument count fail at queue time, which marks
/// the transaction; every other error waits for EXEC. `size` is the item's
/// length on the wire, counted only if the item is actually queued.
pub(super) fn queue_in(transaction: &mut Transaction, item: Parsed, size: usize) -> Value {
    let queued_reply = Value::SimpleString(Bytes::from_static(b"QUEUED"));
    match item {
        Parsed::Run(Command::Unknown { name, args }) => {
            transaction.failed = true;
            Value::Error(CommandError::UnknownCommand { name, args }.to_resp())
        }
        Parsed::Error(e @ CommandError::WrongArity(_)) => {
            transaction.failed = true;
            Value::Error(e.to_resp())
        }
        Parsed::Run(cmd) => {
            transaction.commands.push(Ok(cmd));
            transaction.bytes += size;
            queued_reply
        }
        Parsed::Error(e) => {
            transaction.commands.push(Err(e));
            transaction.bytes += size;
            queued_reply
        }
        // Never reached: the drain loop handles protocol errors before
        // queue_in, since broken framing closes the connection either way.
        Parsed::ProtocolError(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::to_command;

    fn ping() -> Command {
        let request = Value::Array(vec![Value::BulkString(Bytes::from_static(b"PING"))]);
        to_command(request).unwrap().unwrap()
    }

    fn queued_reply() -> Value {
        Value::SimpleString(Bytes::from_static(b"QUEUED"))
    }

    #[test]
    fn a_queued_command_counts_its_bytes() {
        let mut t = Transaction::default();
        assert_eq!(queue_in(&mut t, Parsed::Run(ping()), 14), queued_reply());
        assert_eq!((t.commands.len(), t.bytes, t.failed), (1, 14, false));
    }

    #[test]
    fn an_error_waiting_for_exec_counts_too() {
        // SET k v ZZ: queued, and answered by EXEC in its slot.
        let mut t = Transaction::default();
        let reply = queue_in(&mut t, Parsed::Error(CommandError::Syntax), 20);
        assert_eq!(reply, queued_reply());
        assert_eq!((t.commands.len(), t.bytes, t.failed), (1, 20, false));
    }

    #[test]
    fn queue_time_failures_mark_the_transaction_and_hold_nothing() {
        let mut t = Transaction::default();
        let arity = CommandError::WrongArity(Bytes::from_static(b"get"));
        let reply = queue_in(&mut t, Parsed::Error(arity.clone()), 9);
        assert_eq!(reply, Value::Error(arity.to_resp()));
        let unknown = Command::Unknown {
            name: Bytes::from_static(b"NOSUCH"),
            args: vec![],
        };
        assert!(matches!(
            queue_in(&mut t, Parsed::Run(unknown), 16),
            Value::Error(_)
        ));
        assert_eq!((t.commands.len(), t.bytes, t.failed), (0, 0, true));
    }
}
