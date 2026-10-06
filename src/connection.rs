//! One client connection: parsing requests, parked waits, MULTI and WATCH,
//! and writing replies. The keyspace task runs the commands.

mod pending;
mod transaction;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::command::{BlockSpec, Command, CommandError};
use crate::keyspace::Request;
use crate::resp::{self, Value};
use pending::{Parsed, Pending, parse_into};
use transaction::{Transaction, queue_in};

/// Flush replies early once this many bytes are waiting.
const OUTBUF_FLUSH_AT: usize = 64 * 1024;

/// Initial buffer size, and the room reserved before each read.
const READ_CHUNK: usize = 4096;

/// Redis's client-query-buffer-limit: close a client holding more input than
/// this, counting unparsed bytes, parsed commands and the MULTI queue.
const MAX_QUERY_BUF: usize = 1024 * 1024 * 1024;

fn query_buf_limit() -> std::io::Error {
    std::io::Error::other("closing client that reached max query buffer length")
}

/// The keyspace task knows a client only by this id. A connection parks one
/// command at a time, so the same id names its waiter.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

struct Conn {
    id: u64,
    stream: TcpStream,
    inbuf: BytesMut,
    outbuf: BytesMut,
    /// Replies from the keyspace task; a clone of `reply_tx` goes with each
    /// request.
    reply_tx: mpsc::UnboundedSender<Value>,
    reply_rx: mpsc::UnboundedReceiver<Value>,
    pending: Pending,
    transaction: Option<Transaction>,
    /// Whether the keyspace task holds watches for this connection.
    watching: bool,
}

impl Conn {
    fn new(stream: TcpStream) -> Self {
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        Conn {
            id: next_id(),
            stream,
            inbuf: BytesMut::with_capacity(READ_CHUNK),
            outbuf: BytesMut::with_capacity(READ_CHUNK),
            reply_tx,
            reply_rx,
            pending: Pending::new(),
            transaction: None,
            watching: false,
        }
    }

    /// Buffers a reply; `flush` sends it.
    fn reply(&mut self, value: &Value) {
        resp::encode(value, &mut self.outbuf);
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        if !self.outbuf.is_empty() {
            self.stream.write_all(&self.outbuf).await?;
            self.outbuf.clear();
        }
        Ok(())
    }

    /// Input this client holds, as MAX_QUERY_BUF counts it.
    fn held(&self) -> usize {
        let in_multi = self.transaction.as_ref().map_or(0, |t| t.bytes);
        self.inbuf.len() + self.pending.bytes() + in_multi
    }

    /// A failed send needs no handling: the watches went with the keyspace
    /// task.
    async fn unwatch(&mut self, tx: &mpsc::Sender<Request>) {
        if self.watching {
            self.watching = false;
            let _ = tx.send(Request::Unwatch { conn: self.id }).await;
        }
    }
}

/// Serves one client until it disconnects, then drops its watches however
/// `serve` returned.
pub async fn handle_client(stream: TcpStream, tx: mpsc::Sender<Request>) -> std::io::Result<()> {
    let mut conn = Conn::new(stream);
    let result = serve(&mut conn, &tx).await;
    conn.unwatch(&tx).await;
    result
}

async fn serve(conn: &mut Conn, tx: &mpsc::Sender<Request>) -> std::io::Result<()> {
    loop {
        parse_into(&mut conn.inbuf, &mut conn.pending);
        while let Some((item, size)) = conn.pending.pop() {
            match item {
                Parsed::ProtocolError(err) => {
                    conn.reply(&err);
                    conn.flush().await?;
                    return Ok(());
                }

                Parsed::Run(Command::Multi) => {
                    let reply = if conn.transaction.is_some() {
                        Value::Error(CommandError::NestedMulti.to_resp())
                    } else {
                        conn.transaction = Some(Transaction::default());
                        Value::ok()
                    };
                    conn.reply(&reply);
                }

                Parsed::Run(Command::Exec) => match conn.transaction.take() {
                    None => conn.reply(&Value::Error(CommandError::ExecWithoutMulti.to_resp())),
                    Some(t) if t.failed => {
                        conn.unwatch(tx).await;
                        let abort = CommandError::ExecAbortPreviousErrors;
                        conn.reply(&Value::Error(abort.to_resp()));
                    }
                    Some(t) => {
                        let request = Request::Exec {
                            cmds: t.commands,
                            reply: conn.reply_tx.clone(),
                            conn: conn.id,
                        };
                        if tx.send(request).await.is_err() {
                            return Ok(()); // keyspace task is gone
                        }
                        conn.watching = false; // Exec drops them
                        match conn.reply_rx.recv().await {
                            Some(v) => conn.reply(&v),
                            None => return Ok(()),
                        }
                    }
                },

                Parsed::Run(Command::Discard) => {
                    let reply = match conn.transaction.take() {
                        Some(_) => {
                            conn.unwatch(tx).await;
                            Value::ok()
                        }
                        // Without MULTI the watches stay.
                        None => Value::Error(CommandError::DiscardWithoutMulti.to_resp()),
                    };
                    conn.reply(&reply);
                }

                // Ends the transaction and the watches, inside MULTI or not.
                Parsed::Error(e @ CommandError::ExecAbortRejected(_)) => {
                    conn.unwatch(tx).await;
                    conn.transaction = None;
                    conn.reply(&Value::Error(e.to_resp()));
                }

                // Inside MULTI, refused without failing the transaction.
                Parsed::Run(Command::Watch { keys }) => {
                    if conn.transaction.is_some() {
                        conn.reply(&Value::Error(CommandError::WatchInsideMulti.to_resp()));
                    } else {
                        let request = Request::Watch {
                            conn: conn.id,
                            keys,
                        };
                        if tx.send(request).await.is_err() {
                            return Ok(()); // keyspace task is gone
                        }
                        conn.watching = true;
                        conn.reply(&Value::ok());
                    }
                }

                // Inside MULTI, everything else is queued.
                item if conn.transaction.is_some() => {
                    let reply = queue_in(conn.transaction.as_mut().unwrap(), item, size);
                    conn.reply(&reply);
                }

                Parsed::Run(Command::Unwatch) => {
                    conn.unwatch(tx).await;
                    conn.reply(&Value::ok());
                }

                Parsed::Run(cmd) => {
                    let deadline = cmd.meta().blocks;
                    // Earlier pipelined replies go out before parking, as in Redis.
                    if deadline.is_some() {
                        conn.flush().await?;
                    }

                    let request = Request::Run {
                        cmd,
                        reply: conn.reply_tx.clone(),
                        conn: conn.id,
                    };
                    if tx.send(request).await.is_err() {
                        return Ok(()); // keyspace task is gone
                    }

                    let received = match deadline {
                        Some(BlockSpec {
                            timeout,
                            on_timeout,
                        }) => wait_parked(conn, tx, timeout, on_timeout).await?,
                        None => conn.reply_rx.recv().await,
                    };

                    match received {
                        Some(v) => conn.reply(&v),
                        None => return Ok(()),
                    }
                }

                Parsed::Error(err) => conn.reply(&Value::Error(err.to_resp())),
            }

            if conn.outbuf.len() >= OUTBUF_FLUSH_AT {
                conn.flush().await?;
            }
        }

        conn.flush().await?;
        conn.inbuf.reserve(READ_CHUNK);
        if conn.stream.read_buf(&mut conn.inbuf).await? == 0 {
            return Ok(());
        }
        if conn.held() > MAX_QUERY_BUF {
            return Err(query_buf_limit());
        }
    }
}

/// Waits for a parked command's reply. Any exit without one removes the
/// waiter, so a `?` cannot leave it behind.
async fn wait_parked(
    conn: &mut Conn,
    tx: &mpsc::Sender<Request>,
    timeout: Option<Duration>,
    on_timeout: Value,
) -> std::io::Result<Option<Value>> {
    let result = wait_for_reply(conn, tx, timeout, on_timeout).await;
    if !matches!(result, Ok(Some(_))) {
        let _ = tx.send(Request::Gone { conn: conn.id }).await;
    }
    result
}

/// Waits on the reply, the socket and the timer together. Input that arrives
/// meanwhile is parsed but not run.
async fn wait_for_reply(
    conn: &mut Conn,
    tx: &mpsc::Sender<Request>,
    timeout: Option<Duration>,
    on_timeout: Value,
) -> std::io::Result<Option<Value>> {
    let timer = sleep(timeout.unwrap_or_default());
    tokio::pin!(timer);
    let mut on_timeout = timeout.map(|_| on_timeout); // None: no timer, or it fired

    loop {
        conn.inbuf.reserve(READ_CHUNK);
        tokio::select! {
            reply = conn.reply_rx.recv() => return Ok(reply),
            read = conn.stream.read_buf(&mut conn.inbuf) => {
                if read? == 0 {
                    return Ok(None);
                }
                if conn.held() > MAX_QUERY_BUF {
                    return Err(query_buf_limit());
                }
                parse_into(&mut conn.inbuf, &mut conn.pending);
            }
            () = &mut timer, if on_timeout.is_some() => {
                // The keyspace task sends the reply: a push may have served
                // the waiter already.
                let on_timeout = on_timeout.take().expect("guarded by the branch condition");
                let unpark = Request::Unpark {
                    conn: conn.id,
                    on_timeout,
                };
                if tx.send(unpark).await.is_err() {
                    return Ok(None);
                }
            }
        }
    }
}
