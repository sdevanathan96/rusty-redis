use std::sync::Arc;
use std::time::Duration;

use bytes::{BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use rusty_redis::resp::{self, Value};
use rusty_redis::command::{BlockSpec, to_command};
use rusty_redis::db::Db;
use rusty_redis::db::SystemClock;
use rusty_redis::keyspace::{Request, keyspace_task};
use tokio::sync::{mpsc};
use tokio::time::sleep;
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
    let keyspace = tokio::spawn(keyspace_task(db, rx));  // db moves in, main never sees it again

    // The keyspace task owns the only copy of Db, so if it ever returns, every
    // connection is already doomed: tx.send starts failing and each connection
    // task quietly closes its socket while the listener keeps accepting new
    // ones. That silent zombie state is worse than either working or dying, so
    // whichever of these two finishes first ends the process.
    tokio::select! {
        result = keyspace => {
            match result {
                Ok(()) => eprintln!("keyspace task exited: every sender was dropped"),
                Err(e) => eprintln!("keyspace task died: {e}"),
            }
            std::process::exit(1)
        }
        result = accept_loop(listener, tx) => result,
    }
}

async fn accept_loop(listener: TcpListener, tx: mpsc::Sender<Request>) -> std::io::Result<()> {
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

/// Flush the reply buffer once it passes this, instead of only at the end of
/// the drain loop.
const OUTBUF_FLUSH_AT: usize = 64 * 1024;

/// Close the connection once unparsed input passes this. Same value and same
/// rule as Redis's client-query-buffer-limit: checked after every read, parked
/// or not, and the client gets no reply.
const MAX_QUERY_BUF: usize = 1024 * 1024 * 1024;

fn query_buf_limit() -> std::io::Error {
    std::io::Error::other("closing client that reached max query buffer length")
}

async fn handle_client(
    mut stream: TcpStream,
    tx: mpsc::Sender<Request>,
) -> std::io::Result<()> {
    let mut inbuf = BytesMut::with_capacity(4096);
    let mut outbuf = BytesMut::with_capacity(4096);
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<Value>();
    loop {
        loop {
            match resp::parse(&inbuf) {
                Ok(Some((consumed, frame))) => {
                    let owned = inbuf.split_to(consumed).freeze();
                    let value = frame.into_value(&owned);
                    match to_command(value) {
                        Ok(None) => {} // no reply at all
                        Err(e) => resp::encode(&Value::Error(e.to_resp()), &mut outbuf),
                        Ok(Some(cmd)) => {
                            let id = next_id(); // AtomicU64 fetch_add
                            let deadline = cmd.meta().blocks;

                            // Anything already encoded belongs to commands that
                            // came before this one in the same packet, and they
                            // are not waiting on it. Real Redis writes those
                            // replies out and leaves the client blocked, so a
                            // pipelined `PING` then `BLPOP k 0` gets its +PONG
                            // immediately. Flushing here rather than after the
                            // drain loop is what reproduces that.
                            if deadline.is_some() && !outbuf.is_empty() {
                                stream.write_all(&outbuf).await?;
                                outbuf.clear();
                            }

                            if tx.send(Request::Run { cmd, reply: reply_tx.clone(), id }).await.is_err(){
                                return Ok(()); // keyspace task is gone
                            }

                            let received = match deadline {
                                Some(BlockSpec { timeout, on_timeout }) => {
                                    wait_parked(&mut stream, &mut inbuf, &mut reply_rx, &tx, id, timeout, on_timeout)
                                        .await?
                                }
                                None => reply_rx.recv().await,
                            };

                            match received {
                                Some(v) => resp::encode(&v, &mut outbuf),
                                None => return Ok(()), // client left, or every sender dropped
                            }
                        }
                    }

                    // A deep pipeline of large replies would otherwise grow
                    // outbuf without limit, since the only flush is after the
                    // drain loop empties. Redis bounds this with
                    // client-output-buffer-limit; this is the crude version.
                    if outbuf.len() >= OUTBUF_FLUSH_AT {
                        stream.write_all(&outbuf).await?;
                        outbuf.clear();
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
        if inbuf.len() > MAX_QUERY_BUF {
            return Err(query_buf_limit());
        }
    }
}

/// Waits for a parked command's reply while still reading the socket. On EOF
/// it returns None, and the caller returning drops `reply_rx`, which is what
/// makes the keyspace task's `is_closed` check skip this waiter. Bytes that
/// arrive meanwhile stay in `inbuf`, unparsed until the reply is in.
async fn wait_parked(
    stream: &mut TcpStream,
    inbuf: &mut BytesMut,
    reply_rx: &mut mpsc::UnboundedReceiver<Value>,
    tx: &mpsc::Sender<Request>,
    id: u64,
    timeout: Option<Duration>,
    on_timeout: Value,
) -> std::io::Result<Option<Value>> {
    // A pinned sleep inside the select rather than a timeout around it, so the
    // socket is still watched while the keyspace task settles the Unpark.
    let timer = sleep(timeout.unwrap_or_default());
    tokio::pin!(timer);
    let mut on_timeout = timeout.map(|_| on_timeout); // Some while the timer is armed

    loop {
        inbuf.reserve(4096);
        tokio::select! {
            reply = reply_rx.recv() => return Ok(reply),
            read = stream.read_buf(inbuf) => {
                if read? == 0 {
                    return Ok(None);
                }
                if inbuf.len() > MAX_QUERY_BUF {
                    return Err(query_buf_limit());
                }
            }
            () = &mut timer, if on_timeout.is_some() => {
                // Do not write the timeout reply here. A push may already have
                // served this waiter; the keyspace task decides and sends
                // exactly one more message either way.
                let on_timeout = on_timeout.take().expect("guarded by the branch condition");
                if tx.send(Request::Unpark { id, on_timeout }).await.is_err() {
                    return Ok(None);
                }
            }
        }
    }
}