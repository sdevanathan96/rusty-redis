use std::collections::VecDeque;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::command::execute;
use crate::command::{Command, CommandError, Outcome};
use crate::db::Db;
use crate::resp::Value;

pub type ReplyTx = mpsc::UnboundedSender<Value>;

pub enum Request {
    Run {
        cmd: Command,
        reply: ReplyTx,
        id: u64,
    },
    Unpark {
        id: u64,
        on_timeout: Value,
    },
    Gone {
        id: u64,
    },
    Exec {
        cmds: Vec<Result<Command, CommandError>>,
        reply: ReplyTx,
    },
}

struct Waiter {
    id: u64,
    reply: ReplyTx,
    keys: Vec<Bytes>,
    retry: Command,
}

pub async fn keyspace_task(mut db: Db, mut rx: mpsc::Receiver<Request>) {
    let mut waiters: VecDeque<Waiter> = VecDeque::new();
    while let Some(req) = rx.recv().await {
        handle(req, &mut db, &mut waiters);
    }
}

fn serve_waiters(db: &mut Db, waiters: &mut VecDeque<Waiter>, key: &Bytes) -> Vec<Bytes> {
    let mut fed = Vec::new();
    let mut i = 0;

    while i < waiters.len() {
        if !waiters[i].keys.iter().any(|k| &k[..] == key) {
            i += 1;
            continue;
        }
        if waiters[i].reply.is_closed() {
            waiters.remove(i); // client gave up; do not shift i
            continue;
        }

        match execute(waiters[i].retry.clone(), db) {
            Ok(Outcome::Reply(v)) => {
                fed.extend(waiters[i].retry.meta().feeds);
                if let Some(w) = waiters.remove(i) {
                    let _ = w.reply.send(v);
                }
            }
            Ok(Outcome::Block { .. }) => {
                i += 1;
            }
            Err(e) => {
                if let Some(w) = waiters.remove(i) {
                    let _ = w.reply.send(Value::Error(e.to_resp()));
                }
            }
        }
    }
    fed
}

fn handle(req: Request, db: &mut Db, waiters: &mut VecDeque<Waiter>) {
    match req {
        Request::Run { cmd, reply, id } => {
            let touched = cmd.meta().feeds;
            match execute(cmd, db) {
                Ok(Outcome::Reply(v)) => {
                    let _ = reply.send(v);
                }
                Ok(Outcome::Block { keys, retry }) => {
                    waiters.push_back(Waiter {
                        id,
                        reply,
                        keys,
                        retry,
                    });
                }
                Err(e) => {
                    let _ = reply.send(Value::Error(e.to_resp()));
                }
            }
            // No round cap needed: every serve removes a waiter and none
            // are added mid cascade, so this ends. A cap could only
            // abandon parked clients.
            wake(db, waiters, touched);
        }
        Request::Unpark { id, on_timeout } => {
            if let Some(i) = waiters.iter().position(|w| w.id == id)
                && let Some(w) = waiters.remove(i)
            {
                let _ = w.reply.send(on_timeout);
            }
        }
        Request::Gone { id } => {
            if let Some(i) = waiters.iter().position(|w| w.id == id) {
                waiters.remove(i);
            }
        }
        Request::Exec { cmds, reply } => {
            let mut replies = Vec::with_capacity(cmds.len());
            let mut touched = Vec::new();
            for item in cmds {
                // by value: each command is moved out
                let value = match item {
                    Err(e) => Value::Error(e.to_resp()),
                    Ok(cmd) => {
                        let meta = cmd.meta(); // before execute, which takes cmd
                        touched.extend(meta.feeds);
                        match execute(cmd, db) {
                            Ok(Outcome::Reply(v)) => v,
                            // Nothing parks inside EXEC: answer as if it had timed out.
                            Ok(Outcome::Block { .. }) => {
                                meta.blocks.map_or(Value::NullArray, |b| b.on_timeout)
                            }
                            Err(e) => Value::Error(e.to_resp()),
                        }
                    }
                };
                replies.push(value);
            }
            let _ = reply.send(Value::Array(replies));
            wake(db, waiters, touched); // only now, so nothing is served mid-transaction
        }
    }
}

/// Serves every waiter the written keys can now satisfy, including the chain
/// of wakeups a BLMOVE can start.
fn wake(db: &mut Db, waiters: &mut VecDeque<Waiter>, touched: Vec<Bytes>) {
    let mut pending: VecDeque<Bytes> = touched.into();
    while let Some(key) = pending.pop_front() {
        pending.extend(serve_waiters(db, waiters, &key));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::command::test_support::cmd_ok;
    use crate::db::TestClock;

    fn blpop_forever(key: &str) -> Command {
        let raw = format!(
            "*3\r\n$5\r\nBLPOP\r\n${}\r\n{}\r\n$1\r\n0\r\n",
            key.len(),
            key
        );
        cmd_ok(raw.as_bytes())
    }

    #[test]
    fn gone_removes_the_waiter_at_once() {
        let mut db = Db::with_clock(Arc::new(TestClock::new()));
        let mut waiters = VecDeque::new();
        let (reply, _rx) = mpsc::unbounded_channel();

        handle(
            Request::Run {
                cmd: blpop_forever("k"),
                reply,
                id: 7,
            },
            &mut db,
            &mut waiters,
        );
        assert_eq!(waiters.len(), 1, "BLPOP on an empty list parks");

        handle(Request::Gone { id: 7 }, &mut db, &mut waiters);
        assert!(waiters.is_empty(), "without waiting for a write to k");
    }

    #[test]
    fn gone_for_an_unknown_id_changes_nothing() {
        let mut db = Db::with_clock(Arc::new(TestClock::new()));
        let mut waiters = VecDeque::new();
        let (reply, _rx) = mpsc::unbounded_channel();

        handle(
            Request::Run {
                cmd: blpop_forever("k"),
                reply,
                id: 7,
            },
            &mut db,
            &mut waiters,
        );
        handle(Request::Gone { id: 8 }, &mut db, &mut waiters);
        assert_eq!(waiters.len(), 1, "a stale or duplicate Gone is harmless");
    }

    /// A command from its arguments, through the real parser.
    fn cmd(parts: &[&str]) -> Command {
        let mut raw = format!("*{}\r\n", parts.len());
        for p in parts {
            raw += &format!("${}\r\n{}\r\n", p.len(), p);
        }
        cmd_ok(raw.as_bytes())
    }

    fn bulk(s: &'static str) -> Value {
        Value::BulkString(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn exec_answers_a_blocking_command_with_its_timeout_reply() {
        let mut db = Db::with_clock(Arc::new(TestClock::new()));
        let mut waiters = VecDeque::new();
        let (reply, mut rx) = mpsc::unbounded_channel();

        handle(
            Request::Exec {
                cmds: vec![Ok(blpop_forever("k"))],
                reply,
            },
            &mut db,
            &mut waiters,
        );

        assert_eq!(rx.try_recv().unwrap(), Value::Array(vec![Value::NullArray]));
        assert!(waiters.is_empty(), "nothing parks inside EXEC");
    }

    #[test]
    fn exec_puts_each_error_in_its_slot_and_runs_the_rest() {
        let mut db = Db::with_clock(Arc::new(TestClock::new()));
        let mut waiters = VecDeque::new();
        let (reply, mut rx) = mpsc::unbounded_channel();
        let cmds = vec![
            Err(CommandError::Syntax),
            Ok(cmd(&["SET", "k", "1"])),
            Ok(cmd(&["GET", "k"])),
        ];

        handle(Request::Exec { cmds, reply }, &mut db, &mut waiters);

        let ok = Value::SimpleString(Bytes::from_static(b"OK"));
        let syntax = Value::Error(CommandError::Syntax.to_resp());
        assert_eq!(
            rx.try_recv().unwrap(),
            Value::Array(vec![syntax, ok, bulk("1")])
        );
    }

    #[test]
    fn exec_wakes_waiters_only_after_the_whole_transaction() {
        let mut db = Db::with_clock(Arc::new(TestClock::new()));
        let mut waiters = VecDeque::new();
        let (waiter_reply, mut waiter_rx) = mpsc::unbounded_channel();
        handle(
            Request::Run {
                cmd: blpop_forever("k"),
                reply: waiter_reply,
                id: 1,
            },
            &mut db,
            &mut waiters,
        );
        assert_eq!(waiters.len(), 1);

        let (reply, mut rx) = mpsc::unbounded_channel();
        let cmds = vec![Ok(cmd(&["RPUSH", "k", "x", "y"])), Ok(cmd(&["LLEN", "k"]))];
        handle(Request::Exec { cmds, reply }, &mut db, &mut waiters);

        // LLEN inside the transaction still sees both: nobody was served yet.
        assert_eq!(
            rx.try_recv().unwrap(),
            Value::Array(vec![Value::Integer(2), Value::Integer(2)])
        );
        assert_eq!(
            waiter_rx.try_recv().unwrap(),
            Value::Array(vec![bulk("k"), bulk("x")])
        );
        assert!(waiters.is_empty());
    }
}
