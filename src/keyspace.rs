use std::collections::VecDeque;

use tokio::sync::{mpsc, oneshot};

use crate::command::{touched_keys, Command, Outcome};
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
    keys: Vec<Vec<u8>>,
    retry: Command,
}

pub async fn keyspace_task(mut db: Db, mut rx: mpsc::Receiver<Request>) {
    let mut waiters: VecDeque<Waiter> = VecDeque::new();
    while let Some(req) = rx.recv().await {
        match req {
            Request::Run { cmd, reply, id } => {
                let touched = touched_keys(&cmd);
                match execute(cmd, &mut db) {
                    Ok(Outcome::Reply(v)) => { let _ = reply.send(v); }
                    Ok(Outcome::Block { keys, retry }) => {
                        waiters.push_back(Waiter { id, reply, keys, retry });
                    }
                    Err(e) => { let _ = reply.send(Value::Error(e.to_resp())); }
                }
                let mut pending: VecDeque<Vec<u8>> = touched.into();
                let mut rounds = 0;
                while let Some(key) = pending.pop_front() {
                    rounds += 1;
                    if rounds > 1000 { break; }
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
    key: &[u8],
) -> Vec<Vec<u8>> {
    let mut fed = Vec::new();
    let mut i = 0;

    while i < waiters.len() {
        if !waiters[i].keys.iter().any(|k| k == key) {
            i += 1;
            continue;
        }
        if waiters[i].reply.is_closed() {
            waiters.remove(i);          // client gave up; do not shift i
            continue;
        }

        match execute(waiters[i].retry.clone(), db) {
            Ok(Outcome::Reply(v)) => {
                fed.extend(touched_keys(&waiters[i].retry));
                let w = waiters.remove(i).unwrap();
                let _ = w.reply.send(v);
            }
            Ok(Outcome::Block { .. }) => break,   // nothing left on this key
            Err(e) => {
                let w = waiters.remove(i).unwrap();
                let _ = w.reply.send(Value::Error(e.to_resp()));
            }
        }
    }
    fed
}