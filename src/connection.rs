//! One client connection: parsing requests, parked waits, MULTI and WATCH,
//! and writing replies. The keyspace task runs the commands.

mod pending;
mod transaction;

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::command::{BlockSpec, Command, CommandError, Timeout};
use crate::keyspace::Request;
use crate::resp::{self, Value};
use pending::{Parsed, Pending, parse_into};
use transaction::Transaction;

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
    /// Requests to the keyspace task.
    tx: mpsc::Sender<Request>,
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

/// Serves one client until it disconnects, then drops its watches however
/// `serve` returned.
pub async fn handle_client(stream: TcpStream, tx: mpsc::Sender<Request>) -> std::io::Result<()> {
    let mut conn = Conn::new(stream, tx);
    let result = conn.serve().await;
    conn.unwatch().await;
    result
}

impl Conn {
    fn new(stream: TcpStream, tx: mpsc::Sender<Request>) -> Self {
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        Conn {
            id: next_id(),
            tx,
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

    async fn serve(&mut self) -> std::io::Result<()> {
        loop {
            parse_into(&mut self.inbuf, &mut self.pending);
            while let Some((item, size)) = self.pending.pop() {
                let reply = match item {
                    Parsed::ProtocolError(err) => {
                        self.reply(&err);
                        self.flush().await?;
                        return Ok(());
                    }
                    // These act at once, inside MULTI or not.
                    Parsed::Run(Command::Multi) => Some(self.multi()),
                    Parsed::Run(Command::Exec) => self.exec().await,
                    Parsed::Run(Command::Discard) => Some(self.discard().await),
                    Parsed::Run(Command::Watch { keys }) => self.watch(keys).await,
                    Parsed::Error(e @ CommandError::ExecAbortRejected(_)) => {
                        Some(self.reject_exec(e).await)
                    }
                    // Inside MULTI, everything else is queued.
                    item if self.transaction.is_some() => Some(self.queue(item, size)),
                    Parsed::Run(Command::Unwatch) => {
                        self.unwatch().await;
                        Some(Value::ok())
                    }
                    Parsed::Run(cmd) => self.run(cmd).await?,
                    Parsed::Error(err) => Some(Value::Error(err.to_resp())),
                };
                // The keyspace task is gone, or the client left while parked.
                let Some(reply) = reply else {
                    return Ok(());
                };
                self.reply(&reply);
                if self.outbuf.len() >= OUTBUF_FLUSH_AT {
                    self.flush().await?;
                }
            }

            self.flush().await?;
            self.inbuf.reserve(READ_CHUNK);
            if self.stream.read_buf(&mut self.inbuf).await? == 0 {
                return Ok(());
            }
            if self.held() > MAX_QUERY_BUF {
                return Err(query_buf_limit());
            }
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

    async fn run(&mut self, cmd: Command) -> std::io::Result<Option<Value>> {
        let blocks = cmd.meta().blocks;
        // Earlier pipelined replies go out before parking, as in Redis.
        if blocks.is_some() {
            self.flush().await?;
        }
        let request = Request::Run {
            cmd,
            reply: self.reply_tx.clone(),
            conn: self.id,
        };
        if self.tx.send(request).await.is_err() {
            return Ok(None);
        }
        match blocks {
            Some(BlockSpec {
                timeout,
                on_timeout,
            }) => self.wait_parked(timeout, on_timeout).await,
            None => Ok(self.reply_rx.recv().await),
        }
    }

    /// Any exit without a reply removes the waiter, so a `?` cannot leave it
    /// behind.
    async fn wait_parked(
        &mut self,
        timeout: Timeout,
        on_timeout: Value,
    ) -> std::io::Result<Option<Value>> {
        let result = self.wait_for_reply(timeout, on_timeout).await;
        if !matches!(result, Ok(Some(_))) {
            let _ = self.tx.send(Request::Gone { conn: self.id }).await;
        }
        result
    }

    /// Waits on the reply, the socket and the timer together. Input that
    /// arrives meanwhile is parsed but not run.
    async fn wait_for_reply(
        &mut self,
        timeout: Timeout,
        on_timeout: Value,
    ) -> std::io::Result<Option<Value>> {
        let timer = async move {
            match timeout {
                Timeout::After(d) => sleep(d).await,
                Timeout::Forever => std::future::pending().await,
            }
        };
        tokio::pin!(timer);
        let mut on_timeout = Some(on_timeout); // taken when the timer fires

        loop {
            self.inbuf.reserve(READ_CHUNK);
            tokio::select! {
                reply = self.reply_rx.recv() => return Ok(reply),
                read = self.stream.read_buf(&mut self.inbuf) => {
                    if read? == 0 {
                        return Ok(None);
                    }
                    if self.held() > MAX_QUERY_BUF {
                        return Err(query_buf_limit());
                    }
                    parse_into(&mut self.inbuf, &mut self.pending);
                }
                () = &mut timer, if on_timeout.is_some() => {
                    // The keyspace task sends the reply: a push may have
                    // served the waiter already.
                    let on_timeout = on_timeout.take().expect("guarded by the branch condition");
                    let unpark = Request::Unpark {
                        conn: self.id,
                        on_timeout,
                    };
                    if self.tx.send(unpark).await.is_err() {
                        return Ok(None);
                    }
                }
            }
        }
    }
}
