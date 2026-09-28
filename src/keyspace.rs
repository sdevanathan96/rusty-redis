use std::collections::VecDeque;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, Outcome};
use crate::resp::Value;
use crate::command::execute;
use crate::db::Db;

pub enum Request {
    Run {
        cmd: Command,
        reply: oneshot::Sender<Value>,
        id: u64,
    },
    Unpark { id: u64 },
}

struct Waiter {
    id: u64,
    reply: oneshot::Sender<Value>,
    keys: Vec<Bytes>,
    retry: Command,
}

pub async fn keyspace_task(mut db: Db, mut rx: mpsc::Receiver<Request>) {
    let mut waiters: VecDeque<Waiter> = VecDeque::new();
    while let Some(req) = rx.recv().await {
        match req {
            Request::Run { cmd, reply, id } => {
                let touched = cmd.meta().feeds;
                match execute(cmd, &mut db) {
                    Ok(Outcome::Reply(v)) => { let _ = reply.send(v); }
                    Ok(Outcome::Block { keys, retry }) => {
                        waiters.push_back(Waiter { id, reply, keys, retry });
                    }
                    Err(e) => { let _ = reply.send(Value::Error(e.to_resp())); }
                }
                // The cascade terminates on its own. Every serve removes one
                // waiter, no waiter is added while a cascade is running, and a
                // key is only queued by a serve, so the number of pops is
                // bounded by one plus the total keys fed. A round counter could
                // therefore never prevent a live lock, only silently abandon
                // parked clients once a fan out got large enough.
                let mut pending: VecDeque<Bytes> = touched.into();
                while let Some(key) = pending.pop_front() {
                    pending.extend(serve_waiters(&mut db, &mut waiters, &key));
                }
            }
            Request::Unpark { id } => {
                waiters.retain(|w| w.id != id);
            }
        }
    }
}

fn serve_waiters(
    db: &mut Db,
    waiters: &mut VecDeque<Waiter>,
    key: &Bytes,
) -> Vec<Bytes> {
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
                let w = waiters.remove(i).unwrap();
                let _ = w.reply.send(v);
            }
            Ok(Outcome::Block { .. }) => {i+=1;},
            Err(e) => {
                let w = waiters.remove(i).unwrap();
                let _ = w.reply.send(Value::Error(e.to_resp()));
            }
        }
    }
    fed
}