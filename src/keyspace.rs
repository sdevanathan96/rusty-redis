mod watchers;

use std::collections::VecDeque;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::command::execute;
use crate::command::{Command, CommandError, Outcome};
use crate::db::Db;
use crate::resp::Value;
use watchers::Watches;

pub type ReplyTx = mpsc::UnboundedSender<Value>;

/// What a connection asks of the keyspace task. `conn` is the connection's id.
pub enum Request {
    Run {
        cmd: Command,
        reply: ReplyTx,
        conn: u64,
    },
    /// The parked command timed out, unless a write served it first.
    Unpark {
        conn: u64,
        on_timeout: Value,
    },
    /// The connection left while parked.
    Gone {
        conn: u64,
    },
    Exec {
        cmds: Vec<Result<Command, CommandError>>,
        reply: ReplyTx,
        conn: u64,
    },
    /// No reply: the channel is FIFO, so the client's next request comes after
    /// this one anyway. The same goes for `Unwatch`.
    Watch {
        conn: u64,
        keys: Vec<Bytes>,
    },
    Unwatch {
        conn: u64,
    },
}

struct Waiter {
    conn: u64,
    reply: ReplyTx,
    keys: Vec<Bytes>,
    retry: Command,
}

pub async fn keyspace_task(mut db: Db, mut rx: mpsc::Receiver<Request>) {
    let mut waiters: VecDeque<Waiter> = VecDeque::new();
    let mut watches = Watches::default();
    while let Some(req) = rx.recv().await {
        process(req, &mut db, &mut waiters, &mut watches);
    }
}

/// One request, then the keys it changed, including through waiters it
/// served, reported to WATCH.
fn process(req: Request, db: &mut Db, waiters: &mut VecDeque<Waiter>, watches: &mut Watches) {
    handle(req, db, waiters, watches);
    // Always taken: nothing else empties it.
    let modified = db.take_modified();
    if !watches.is_empty() {
        for key in &modified {
            watches.touch(key);
        }
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

fn handle(req: Request, db: &mut Db, waiters: &mut VecDeque<Waiter>, watches: &mut Watches) {
    match req {
        Request::Run { cmd, reply, conn } => {
            let fed = cmd.meta().feeds;
            match execute(cmd, db) {
                Ok(Outcome::Reply(v)) => {
                    let _ = reply.send(v);
                }
                Ok(Outcome::Block { keys, retry }) => {
                    waiters.push_back(Waiter {
                        conn,
                        reply,
                        keys,
                        retry,
                    });
                }
                Err(e) => {
                    let _ = reply.send(Value::Error(e.to_resp()));
                }
            }
            wake(db, waiters, fed);
        }
        Request::Unpark { conn, on_timeout } => {
            if let Some(i) = waiters.iter().position(|w| w.conn == conn)
                && let Some(w) = waiters.remove(i)
            {
                let _ = w.reply.send(on_timeout);
            }
        }
        Request::Gone { conn } => {
            if let Some(i) = waiters.iter().position(|w| w.conn == conn) {
                waiters.remove(i);
            }
        }
        Request::Exec { cmds, reply, conn } => {
            let aborted = watches.aborts(conn, db);
            watches.unwatch(conn);
            if aborted {
                let _ = reply.send(Value::NullArray);
                return;
            }
            let mut replies = Vec::with_capacity(cmds.len());
            let mut fed = Vec::new();
            for item in cmds {
                let value = match item {
                    Err(e) => Value::Error(e.to_resp()),
                    Ok(cmd) => {
                        let meta = cmd.meta(); // before execute, which takes cmd
                        fed.extend(meta.feeds);
                        match execute(cmd, db) {
                            Ok(Outcome::Reply(v)) => v,
                            // Nothing parks inside EXEC: reply as if it timed out.
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
            wake(db, waiters, fed); // only now, so nothing is served mid-transaction
        }
        Request::Watch { conn, keys } => {
            for key in keys {
                let deadline = db.deadline(&key);
                watches.watch(conn, key, deadline);
            }
        }
        Request::Unwatch { conn } => {
            watches.unwatch(conn);
        }
    }
}

/// Serves every waiter the written keys can satisfy, following the chain a
/// BLMOVE can start. It ends: every serve removes a waiter.
fn wake(db: &mut Db, waiters: &mut VecDeque<Waiter>, fed: Vec<Bytes>) {
    let mut pending: VecDeque<Bytes> = fed.into();
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

    /// The task's state, fed through `process` as the task's loop does.
    struct Keyspace {
        clock: Arc<TestClock>,
        db: Db,
        waiters: VecDeque<Waiter>,
        watches: Watches,
    }

    impl Keyspace {
        fn new() -> Self {
            let clock = Arc::new(TestClock::new());
            Keyspace {
                db: Db::with_clock(clock.clone()),
                clock,
                waiters: VecDeque::new(),
                watches: Watches::default(),
            }
        }

        fn send(&mut self, req: Request) {
            process(req, &mut self.db, &mut self.waiters, &mut self.watches);
        }

        fn run(&mut self, conn: u64, parts: &[&str]) -> Value {
            let (reply, mut rx) = mpsc::unbounded_channel();
            self.send(Request::Run {
                cmd: cmd(parts),
                reply,
                conn,
            });
            rx.try_recv()
                .expect("a command that does not park replies at once")
        }

        fn watch(&mut self, conn: u64, keys: &[&'static str]) {
            let keys = keys.iter().map(|k| Bytes::from_static(k.as_bytes()));
            self.send(Request::Watch {
                conn,
                keys: keys.collect(),
            });
        }

        fn exec(&mut self, conn: u64, cmds: &[&[&str]]) -> Value {
            let (reply, mut rx) = mpsc::unbounded_channel();
            self.send(Request::Exec {
                cmds: cmds.iter().map(|parts| Ok(cmd(parts))).collect(),
                reply,
                conn,
            });
            rx.try_recv().expect("EXEC always replies at once")
        }
    }

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
        let mut ks = Keyspace::new();
        let (reply, _rx) = mpsc::unbounded_channel();

        ks.send(Request::Run {
            cmd: blpop_forever("k"),
            reply,
            conn: 7,
        });
        assert_eq!(ks.waiters.len(), 1, "BLPOP on an empty list parks");

        ks.send(Request::Gone { conn: 7 });
        assert!(ks.waiters.is_empty(), "without waiting for a write to k");
    }

    #[test]
    fn gone_for_an_unknown_id_changes_nothing() {
        let mut ks = Keyspace::new();
        let (reply, _rx) = mpsc::unbounded_channel();

        ks.send(Request::Run {
            cmd: blpop_forever("k"),
            reply,
            conn: 7,
        });
        ks.send(Request::Gone { conn: 8 });
        assert_eq!(ks.waiters.len(), 1, "a stale or duplicate Gone is harmless");
    }

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
        let mut ks = Keyspace::new();
        let (reply, mut rx) = mpsc::unbounded_channel();

        ks.send(Request::Exec {
            cmds: vec![Ok(blpop_forever("k"))],
            reply,
            conn: 1,
        });

        assert_eq!(rx.try_recv().unwrap(), Value::Array(vec![Value::NullArray]));
        assert!(ks.waiters.is_empty(), "nothing parks inside EXEC");
    }

    #[test]
    fn exec_puts_each_error_in_its_slot_and_runs_the_rest() {
        let mut ks = Keyspace::new();
        let (reply, mut rx) = mpsc::unbounded_channel();
        let cmds = vec![
            Err(CommandError::Syntax),
            Ok(cmd(&["SET", "k", "1"])),
            Ok(cmd(&["GET", "k"])),
        ];

        ks.send(Request::Exec {
            cmds,
            reply,
            conn: 1,
        });

        let ok = Value::ok();
        let syntax = Value::Error(CommandError::Syntax.to_resp());
        assert_eq!(
            rx.try_recv().unwrap(),
            Value::Array(vec![syntax, ok, bulk("1")])
        );
    }

    #[test]
    fn exec_wakes_waiters_only_after_the_whole_transaction() {
        let mut ks = Keyspace::new();
        let (waiter_reply, mut waiter_rx) = mpsc::unbounded_channel();
        ks.send(Request::Run {
            cmd: blpop_forever("k"),
            reply: waiter_reply,
            conn: 1,
        });
        assert_eq!(ks.waiters.len(), 1);

        let (reply, mut rx) = mpsc::unbounded_channel();
        let cmds = vec![Ok(cmd(&["RPUSH", "k", "x", "y"])), Ok(cmd(&["LLEN", "k"]))];
        ks.send(Request::Exec {
            cmds,
            reply,
            conn: 2,
        });

        // LLEN still sees both: nobody was served yet.
        assert_eq!(
            rx.try_recv().unwrap(),
            Value::Array(vec![Value::Integer(2), Value::Integer(2)])
        );
        assert_eq!(
            waiter_rx.try_recv().unwrap(),
            Value::Array(vec![bulk("k"), bulk("x")])
        );
        assert!(ks.waiters.is_empty());
    }

    // WATCH

    fn pong() -> Value {
        Value::SimpleString(Bytes::from_static(b"PONG"))
    }

    fn ran(replies: Vec<Value>) -> Value {
        Value::Array(replies)
    }

    #[test]
    fn a_write_by_another_connection_aborts_exec() {
        let mut ks = Keyspace::new();
        ks.watch(1, &["k"]);
        ks.run(2, &["SET", "k", "theirs"]);
        assert_eq!(ks.exec(1, &[&["SET", "k", "mine"]]), Value::NullArray);
        assert_eq!(
            ks.run(2, &["GET", "k"]),
            bulk("theirs"),
            "nothing in it ran"
        );
    }

    #[test]
    fn exec_runs_when_nothing_wrote_the_watched_key() {
        let mut ks = Keyspace::new();
        ks.watch(1, &["k"]);
        ks.run(2, &["GET", "k"]);
        ks.run(2, &["SET", "other", "v"]);
        assert_eq!(ks.exec(1, &[&["PING"]]), ran(vec![pong()]));
    }

    #[test]
    fn a_write_that_changes_nothing_does_not_abort() {
        let mut ks = Keyspace::new();
        ks.run(2, &["SET", "s", "text"]);
        ks.watch(1, &["k", "s"]);
        ks.run(2, &["DEL", "k"]);
        ks.run(2, &["LPOP", "k"]);
        ks.run(2, &["LPUSH", "s", "x"]); // WRONGTYPE
        ks.run(2, &["INCR", "s"]); // not an integer
        assert_eq!(ks.exec(1, &[&["PING"]]), ran(vec![pong()]));
    }

    #[test]
    fn a_key_that_expires_after_watch_aborts_exec() {
        let mut ks = Keyspace::new();
        ks.run(2, &["SET", "k", "v", "PX", "100"]);
        ks.watch(1, &["k"]);
        ks.clock.advance_ms(100);
        assert_eq!(ks.exec(1, &[&["PING"]]), Value::NullArray);
    }

    #[test]
    fn a_key_with_a_ttl_still_live_at_exec_does_not_abort() {
        let mut ks = Keyspace::new();
        ks.run(2, &["SET", "k", "v", "PX", "100"]);
        ks.watch(1, &["k"]);
        ks.clock.advance_ms(99);
        assert_eq!(ks.exec(1, &[&["PING"]]), ran(vec![pong()]));
    }

    #[test]
    fn a_key_already_expired_at_watch_counts_as_absent() {
        let mut ks = Keyspace::new();
        ks.run(2, &["SET", "k", "v", "PX", "100"]);
        ks.clock.advance_ms(100);
        ks.watch(1, &["k"]);
        ks.run(2, &["GET", "k"]); // reaps it: not a change
        assert_eq!(ks.exec(1, &[&["PING"]]), ran(vec![pong()]));
    }

    #[test]
    fn a_waiter_served_by_a_write_counts_as_a_write() {
        // Nobody writes dst directly; the served BLMOVE does.
        let mut ks = Keyspace::new();
        let (reply, _rx) = mpsc::unbounded_channel();
        ks.send(Request::Run {
            cmd: cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"]),
            reply,
            conn: 3,
        });
        ks.watch(1, &["dst"]);
        ks.run(2, &["RPUSH", "src", "x"]);
        assert!(ks.waiters.is_empty(), "the push served the waiter");
        assert_eq!(ks.exec(1, &[&["PING"]]), Value::NullArray);
    }

    #[test]
    fn unwatch_forgets_earlier_writes() {
        let mut ks = Keyspace::new();
        ks.watch(1, &["k"]);
        ks.run(2, &["SET", "k", "v"]);
        ks.send(Request::Unwatch { conn: 1 });
        assert_eq!(ks.exec(1, &[&["PING"]]), ran(vec![pong()]));
    }

    #[test]
    fn exec_ends_the_watch_even_when_it_aborts() {
        let mut ks = Keyspace::new();
        ks.watch(1, &["k"]);
        ks.run(2, &["SET", "k", "1"]);
        assert_eq!(ks.exec(1, &[&["PING"]]), Value::NullArray);
        ks.run(2, &["SET", "k", "2"]);
        assert_eq!(
            ks.exec(1, &[&["PING"]]),
            ran(vec![pong()]),
            "no longer watched"
        );
    }

    #[test]
    fn one_write_aborts_every_watcher_of_the_key() {
        let mut ks = Keyspace::new();
        ks.watch(1, &["k"]);
        ks.watch(2, &["k"]);
        ks.run(3, &["SET", "k", "v"]);
        assert_eq!(ks.exec(1, &[&["PING"]]), Value::NullArray);
        assert_eq!(ks.exec(2, &[&["PING"]]), Value::NullArray);
    }

    #[test]
    fn ending_every_watch_leaves_the_table_empty() {
        let mut ks = Keyspace::new();
        ks.watch(1, &["a", "b"]);
        ks.watch(1, &["a"]); // twice is harmless
        ks.watch(2, &["a"]);
        ks.send(Request::Unwatch { conn: 1 });
        assert!(!ks.watches.is_empty(), "conn 2 still watches a");
        ks.exec(2, &[&["PING"]]);
        assert!(ks.watches.is_empty(), "no key or connection left behind");
    }
}
