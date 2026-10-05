//! One client connection: reading and parsing requests, the parked wait for
//! blocking commands, MULTI, and writing replies. The keyspace task runs the
//! commands; everything here is per client.

mod pending;
mod transaction;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::command::{BlockSpec, Command, CommandError};
use crate::keyspace::Request;
use crate::resp::{self, Value};
use pending::{Parsed, Pending, parse_into};
use transaction::{Transaction, queue_in};

/// Flush the reply buffer once it passes this, instead of only at the end of
/// the drain loop.
const OUTBUF_FLUSH_AT: usize = 64 * 1024;

/// Starting size of both buffers, and the room reserved in `inbuf` before each
/// read.
const READ_CHUNK: usize = 4096;

/// Close the connection once the input a client holds passes this: unparsed
/// bytes in `inbuf`, commands parsed into `pending` but not yet run, and
/// commands waiting in an open MULTI. Same value and same rule as Redis's
/// client-query-buffer-limit, which also counts the MULTI queue
/// (`argv_len_sums`): checked after every read, parked or not, and the client
/// gets no reply.
const MAX_QUERY_BUF: usize = 1024 * 1024 * 1024;

fn query_buf_limit() -> std::io::Error {
    std::io::Error::other("closing client that reached max query buffer length")
}

/// One id per connection, for its whole life. The keyspace task knows a
/// client only by this number: its watches, and its parked command if it has
/// one. Reusing it for every parked command is safe because a connection parks
/// at most one at a time, and its `Unpark` or `Gone` reaches the keyspace task
/// before its next `Run`.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Everything one client connection owns. One struct rather than separate
/// locals, so the parked wait borrows it as a whole instead of taking each
/// piece as an argument, and the flat event loop pub/sub needs (4.8) has one
/// place to grow.
struct Conn {
    id: u64,
    stream: TcpStream,
    inbuf: BytesMut,
    outbuf: BytesMut,
    /// This connection's mailbox: every reply from the keyspace task arrives
    /// on `reply_rx`, and a clone of `reply_tx` goes with each request.
    reply_tx: mpsc::UnboundedSender<Value>,
    reply_rx: mpsc::UnboundedReceiver<Value>,
    pending: Pending,
    transaction: Option<Transaction>,
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
        }
    }

    /// Encodes a reply into `outbuf`. Nothing is sent until `flush`.
    fn reply(&mut self, value: &Value) {
        resp::encode(value, &mut self.outbuf);
    }

    /// Writes out and empties `outbuf`, if there is anything in it.
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
}

/// Serves one client until it disconnects. `tx` reaches the keyspace task.
pub async fn handle_client(stream: TcpStream, tx: mpsc::Sender<Request>) -> std::io::Result<()> {
    let mut conn = Conn::new(stream);
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
                        Value::SimpleString(Bytes::from_static(b"OK"))
                    };
                    conn.reply(&reply);
                }

                Parsed::Run(Command::Exec) => match conn.transaction.take() {
                    None => conn.reply(&Value::Error(CommandError::ExecWithoutMulti.to_resp())),
                    Some(t) if t.failed => conn.reply(&Value::Error(
                        CommandError::ExecAbortPreviousErrors.to_resp(),
                    )),
                    Some(t) => {
                        let request = Request::Exec {
                            cmds: t.commands,
                            reply: conn.reply_tx.clone(),
                            conn: conn.id,
                        };
                        if tx.send(request).await.is_err() {
                            return Ok(()); // keyspace task is gone
                        }
                        match conn.reply_rx.recv().await {
                            Some(v) => conn.reply(&v),
                            None => return Ok(()),
                        }
                    }
                },

                Parsed::Run(Command::Discard) => {
                    let reply = match conn.transaction.take() {
                        Some(_) => Value::SimpleString(Bytes::from_static(b"OK")),
                        None => Value::Error(CommandError::DiscardWithoutMulti.to_resp()),
                    };
                    conn.reply(&reply);
                }

                // A rejected EXEC ends any open transaction, as Redis's
                // execCommandAbort does. Above the guard, so it is never queued.
                Parsed::Error(e @ CommandError::ExecAbortRejected(_)) => {
                    conn.transaction = None;
                    conn.reply(&Value::Error(e.to_resp()));
                }

                // Any other item while a transaction is open: queue it. The
                // guard has just checked for Some, so the unwrap cannot fail.
                item if conn.transaction.is_some() => {
                    let reply = queue_in(conn.transaction.as_mut().unwrap(), item, size);
                    conn.reply(&reply);
                }

                Parsed::Run(cmd) => {
                    let deadline = cmd.meta().blocks;

                    // Anything already encoded belongs to commands that
                    // came before this one in the same packet, and they
                    // are not waiting on it. Real Redis writes those
                    // replies out and leaves the client blocked, so a
                    // pipelined `PING` then `BLPOP k 0` gets its +PONG
                    // immediately. Flushing here rather than after the
                    // drain loop is what reproduces that.
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
                        }) => wait_parked(&mut conn, &tx, timeout, on_timeout).await?,
                        None => conn.reply_rx.recv().await,
                    };

                    match received {
                        Some(v) => conn.reply(&v),
                        None => return Ok(()), // client left, or every sender dropped
                    }
                }

                Parsed::Error(err) => conn.reply(&Value::Error(err.to_resp())),
            }

            // A deep pipeline of large replies would otherwise grow outbuf
            // without limit. Redis bounds this with client-output-buffer-limit;
            // this is the crude version.
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

/// Waits for a parked command's reply. On every exit without one (EOF, a read
/// error, the query buffer limit) it tells the keyspace task the waiter is
/// gone, so the waiter is removed at once instead of on the next write to its
/// keys. The `is_closed` check in `serve_waiters` stays as a backstop. The
/// cleanup lives here rather than in each exit so a `?` cannot skip it.
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

/// The wait itself: the reply, the socket, and the timer at once. On EOF it
/// returns None. Commands that arrive meanwhile are parsed into `pending` but
/// not run until the reply is in.
async fn wait_for_reply(
    conn: &mut Conn,
    tx: &mpsc::Sender<Request>,
    timeout: Option<Duration>,
    on_timeout: Value,
) -> std::io::Result<Option<Value>> {
    // A pinned sleep inside the select rather than a timeout around it, so the
    // socket is still watched while the keyspace task settles the Unpark.
    let timer = sleep(timeout.unwrap_or_default());
    tokio::pin!(timer);
    let mut on_timeout = timeout.map(|_| on_timeout); // Some while the timer is armed

    loop {
        conn.inbuf.reserve(READ_CHUNK);
        // The branches borrow different fields of conn, which Rust allows at once.
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
                // Do not write the timeout reply here. A push may already have
                // served this waiter; the keyspace task decides and sends
                // exactly one more message either way.
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
