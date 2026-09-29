use std::collections::VecDeque;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, Outcome};
use crate::resp::Value;
use crate::command::execute;
use crate::db::Db;

pub type ReplyTx = mpsc::UnboundedSender<Value>;

pub enum Request {
    Run { cmd: Command, reply: ReplyTx, id: u64 },
    Unpark { id: u64, on_timeout: Value },
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
                let mut pending: VecDeque<Bytes> = touched.into();
                while let Some(key) = pending.pop_front() {
                    pending.extend(serve_waiters(&mut db, &mut waiters, &key));
                }
            }
            Request::Unpark { id, on_timeout } => {
                if let Some(i) = waiters.iter().position(|w| w.id == id) {
                    let w = waiters.remove(i).unwrap();
                    let _ = w.reply.send(on_timeout);
                }
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