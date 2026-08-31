use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use rusty_redis::resp::{self, Value};
use rusty_redis::command::{BlockSpec, to_command};
use rusty_redis::db::Db;
use rusty_redis::db::SystemClock;
use rusty_redis::keyspace::{Request, keyspace_task};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("listening on 127.0.0.1:6379");

    let db = Db::with_clock(Arc::new(SystemClock));
    let (tx, rx) = mpsc::channel::<Request>(64);
    tokio::spawn(keyspace_task(db, rx));    // db moves in, main never sees it again

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                println!("new connection from {peer}");
                let tx = tx.clone();        // clone the SENDER, outside the async block
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, tx).await {
                        eprintln!("connection {peer} ended: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept failed: {e}"),
        }
    }
}

async fn handle_client(
    mut stream: TcpStream,
    tx: mpsc::Sender<Request>,
) -> std::io::Result<()> {
    let mut inbuf = BytesMut::with_capacity(4096);
    let mut outbuf = BytesMut::with_capacity(4096);

    loop {
        loop {
            match resp::parse(&inbuf) {
                Ok(Some((consumed, value))) => {
                    inbuf.advance(consumed);
                    match to_command(value) {
                        Ok(None) => {} // no reply at all
                        Err(e) => resp::encode(&Value::Error(e.to_resp()), &mut outbuf),
                        Ok(Some(cmd)) => {
                            let id = next_id(); // AtomicU64 fetch_add
                            let deadline = cmd.blocking();

                            let (reply_tx, reply_rx) = oneshot::channel();
                            if tx.send(Request::Run { cmd, reply: reply_tx, id }).await.is_err() {
                                return Ok(()); // keyspace task is gone
                            }

                            let received = match deadline {
                                Some(BlockSpec { timeout: Some(d), on_timeout }) => {
                                    match timeout(d, reply_rx).await {
                                        Ok(inner) => inner,
                                        Err(_) => {
                                            let _ = tx.send(Request::Unpark { id }).await;
                                            resp::encode(&on_timeout, &mut outbuf);
                                            continue; // next frame in the drain loop
                                        }
                                    }
                                }
                                _ => reply_rx.await, // forever, or not blocking
                            };

                            match received {
                                Ok(v) => resp::encode(&v, &mut outbuf),
                                Err(_) => return Ok(()),
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    resp::encode(&Value::Error(e.to_resp()), &mut outbuf);
                    stream.write_all(&outbuf).await?;
                    return Ok(());
                }
            }
        }

        if !outbuf.is_empty() {
            stream.write_all(&outbuf).await?;
            outbuf.clear();
        }

        inbuf.reserve(4096);
        if stream.read_buf(&mut inbuf).await? == 0 {
            return Ok(());
        }
    }
}