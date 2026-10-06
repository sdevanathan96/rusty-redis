//! MULTI, EXEC, DISCARD and WATCH for one connection.

use bytes::Bytes;

use super::Conn;
use super::pending::Parsed;
use crate::command::{Command, CommandError};
use crate::keyspace::Request;
use crate::resp::Value;

#[derive(Default)]
pub(super) struct Transaction {
    /// Most parse errors are queued too, and EXEC replies them in their slot.
    pub(super) commands: Vec<Result<Command, CommandError>>,
    /// EXEC will reply EXECABORT.
    pub(super) failed: bool,
    /// Wire bytes of `commands`, for MAX_QUERY_BUF.
    pub(super) bytes: usize,
}

/// Queues one item and returns its reply. Only an unknown command or a wrong
/// argument count fails the transaction now; other errors wait for EXEC.
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
        // Never reached: protocol errors close the connection first.
        Parsed::ProtocolError(e) => e,
    }
}

/// Methods that talk to the keyspace task return `None` if it is gone.
impl Conn {
    pub(super) fn multi(&mut self) -> Value {
        if self.transaction.is_some() {
            return Value::Error(CommandError::NestedMulti.to_resp());
        }
        self.transaction = Some(Transaction::default());
        Value::ok()
    }

    pub(super) async fn exec(&mut self) -> Option<Value> {
        let Some(t) = self.transaction.take() else {
            return Some(Value::Error(CommandError::ExecWithoutMulti.to_resp()));
        };
        if t.failed {
            self.unwatch().await;
            let abort = CommandError::ExecAbortPreviousErrors;
            return Some(Value::Error(abort.to_resp()));
        }
        let request = Request::Exec {
            cmds: t.commands,
            reply: self.reply_tx.clone(),
            conn: self.id,
        };
        self.tx.send(request).await.ok()?;
        self.watching = false; // Exec drops them
        self.reply_rx.recv().await
    }

    pub(super) async fn discard(&mut self) -> Value {
        if self.transaction.take().is_none() {
            // Without MULTI the watches stay.
            return Value::Error(CommandError::DiscardWithoutMulti.to_resp());
        }
        self.unwatch().await;
        Value::ok()
    }

    /// EXEC with arguments ends the transaction and the watches, inside MULTI
    /// or not.
    pub(super) async fn reject_exec(&mut self, e: CommandError) -> Value {
        self.transaction = None;
        self.unwatch().await;
        Value::Error(e.to_resp())
    }

    pub(super) async fn watch(&mut self, keys: Vec<Bytes>) -> Option<Value> {
        if self.transaction.is_some() {
            // Refused without failing the transaction.
            return Some(Value::Error(CommandError::WatchInsideMulti.to_resp()));
        }
        let request = Request::Watch {
            conn: self.id,
            keys,
        };
        self.tx.send(request).await.ok()?;
        self.watching = true;
        Some(Value::ok())
    }

    /// A failed send needs no handling: the watches went with the keyspace
    /// task.
    pub(super) async fn unwatch(&mut self) {
        if self.watching {
            self.watching = false;
            let _ = self.tx.send(Request::Unwatch { conn: self.id }).await;
        }
    }

    /// Only called inside MULTI.
    pub(super) fn queue(&mut self, item: Parsed, size: usize) -> Value {
        let transaction = self.transaction.as_mut().expect("inside MULTI");
        queue_in(transaction, item, size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{b, cmd_ok};

    fn ping() -> Command {
        cmd_ok(&["PING"])
    }

    fn queued_reply() -> Value {
        Value::SimpleString(b("QUEUED"))
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
        let arity = CommandError::WrongArity(b("get"));
        let reply = queue_in(&mut t, Parsed::Error(arity.clone()), 9);
        assert_eq!(reply, Value::Error(arity.to_resp()));
        let unknown = Command::Unknown {
            name: b("NOSUCH"),
            args: vec![],
        };
        assert!(matches!(
            queue_in(&mut t, Parsed::Run(unknown), 16),
            Value::Error(_)
        ));
        assert_eq!((t.commands.len(), t.bytes, t.failed), (0, 0, true));
    }
}
