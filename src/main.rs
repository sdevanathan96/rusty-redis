use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use pending::Pending;
use rusty_redis::command::Command;
use rusty_redis::command::{BlockSpec, CommandError, to_command};
use rusty_redis::config;
use rusty_redis::db::Db;
use rusty_redis::db::SystemClock;
use rusty_redis::keyspace::{Request, keyspace_task};
use rusty_redis::resp::{self, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::sleep;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Matched here rather than returned: an Err out of main is printed with
    // Debug, and a bad flag deserves one plain line and exit code 1, as in Redis.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = match config::parse(&args) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(1);
        }
    };
    let port = config.net.port;
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("listening on 127.0.0.1:{port}");

    let db = Db::with_clock(Arc::new(SystemClock));
    let (tx, rx) = mpsc::channel::<Request>(64);
    let keyspace = tokio::spawn(keyspace_task(db, rx)); // db moves in, main never sees it again

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
                let tx = tx.clone(); // clone the SENDER, outside the async block
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

/// Close the connection once the input a client holds passes this: unparsed
/// bytes in `inbuf`, commands parsed into `queued` but not yet run, and
/// commands waiting in an open MULTI. Same value and same rule as Redis's
/// client-query-buffer-limit, which also counts the MULTI queue
/// (`argv_len_sums`): checked after every read, parked or not, and the client
/// gets no reply.
const MAX_QUERY_BUF: usize = 1024 * 1024 * 1024;

fn query_buf_limit() -> std::io::Error {
    std::io::Error::other("closing client that reached max query buffer length")
}

/// Everything one client connection owns. One struct rather than separate
/// locals, so the parked wait borrows it as a whole instead of taking each
/// piece as an argument, and the flat event loop pub/sub needs (4.8) has one
/// place to grow.
struct Conn {
    stream: TcpStream,
    inbuf: BytesMut,
    outbuf: BytesMut,
    /// This connection's mailbox: every reply from the keyspace task arrives
    /// on `reply_rx`, and a clone of `reply_tx` goes with each request.
    reply_tx: mpsc::UnboundedSender<Value>,
    reply_rx: mpsc::UnboundedReceiver<Value>,
    queued: Pending,
    transaction: Option<Transaction>,
}

impl Conn {
    fn new(stream: TcpStream) -> Self {
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        Conn {
            stream,
            inbuf: BytesMut::with_capacity(4096),
            outbuf: BytesMut::with_capacity(4096),
            reply_tx,
            reply_rx,
            queued: Pending::new(),
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
        self.inbuf.len() + self.queued.bytes() + in_multi
    }
}

async fn handle_client(stream: TcpStream, tx: mpsc::Sender<Request>) -> std::io::Result<()> {
    let mut conn = Conn::new(stream);
    loop {
        parse_into(&mut conn.inbuf, &mut conn.queued);
        while let Some((item, size)) = conn.queued.pop() {
            match item {
                Queued::ProtocolError(err) => {
                    conn.reply(&err);
                    conn.flush().await?;
                    return Ok(());
                }

                Queued::Run(Command::Multi) => {
                    let reply = if conn.transaction.is_some() {
                        Value::Error(CommandError::NestedMulti.to_resp())
                    } else {
                        conn.transaction = Some(Transaction::default());
                        Value::SimpleString(Bytes::from_static(b"OK"))
                    };
                    conn.reply(&reply);
                }

                Queued::Run(Command::Exec) => match conn.transaction.take() {
                    None => conn.reply(&Value::Error(CommandError::ExecWithoutMulti.to_resp())),
                    Some(t) if t.failed => {
                        conn.reply(&Value::Error(CommandError::ExecAbort.to_resp()))
                    }
                    Some(t) => {
                        let request = Request::Exec {
                            cmds: t.queued,
                            reply: conn.reply_tx.clone(),
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

                Queued::Run(Command::Discard) => {
                    let reply = match conn.transaction.take() {
                        Some(_) => Value::SimpleString(Bytes::from_static(b"OK")),
                        None => Value::Error(CommandError::DiscardWithoutMulti.to_resp()),
                    };
                    conn.reply(&reply);
                }

                // A rejected EXEC ends any open transaction, as Redis's
                // execCommandAbort does. Above the guard, so it is never queued.
                Queued::Error(e @ CommandError::ExecRejected(_)) => {
                    conn.transaction = None;
                    conn.reply(&Value::Error(e.to_resp()));
                }

                // Any other item while a transaction is open: queue it. The
                // guard has just checked for Some, so the unwrap cannot fail.
                item if conn.transaction.is_some() => {
                    let reply = queue_in(conn.transaction.as_mut().unwrap(), item, size);
                    conn.reply(&reply);
                }

                Queued::Run(cmd) => {
                    let id = next_id(); // AtomicU64 fetch_add
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
                        id,
                    };
                    if tx.send(request).await.is_err() {
                        return Ok(()); // keyspace task is gone
                    }

                    let received = match deadline {
                        Some(BlockSpec {
                            timeout,
                            on_timeout,
                        }) => wait_parked(&mut conn, &tx, id, timeout, on_timeout).await?,
                        None => conn.reply_rx.recv().await,
                    };

                    match received {
                        Some(v) => conn.reply(&v),
                        None => return Ok(()), // client left, or every sender dropped
                    }
                }

                Queued::Error(err) => conn.reply(&Value::Error(err.to_resp())),
            }

            // A deep pipeline of large replies would otherwise grow outbuf
            // without limit. Redis bounds this with client-output-buffer-limit;
            // this is the crude version.
            if conn.outbuf.len() >= OUTBUF_FLUSH_AT {
                conn.flush().await?;
            }
        }

        conn.flush().await?;
        conn.inbuf.reserve(4096);
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
    id: u64,
    timeout: Option<Duration>,
    on_timeout: Value,
) -> std::io::Result<Option<Value>> {
    let result = wait_for_reply(conn, tx, id, timeout, on_timeout).await;
    if !matches!(result, Ok(Some(_))) {
        let _ = tx.send(Request::Gone { id }).await;
    }
    result
}

/// The wait itself: the reply, the socket, and the timer at once. On EOF it
/// returns None. Commands that arrive meanwhile are parsed into `queued` but
/// not run until the reply is in.
async fn wait_for_reply(
    conn: &mut Conn,
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
        conn.inbuf.reserve(4096);
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
                parse_into(&mut conn.inbuf, &mut conn.queued);
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

/// One thing the connection must do, in the order the client sent it.
enum Queued {
    Run(Command),
    Error(CommandError),
    ProtocolError(Value),
}

/// An open MULTI. `None` in `handle_client` means no transaction is open.
#[derive(Default)]
struct Transaction {
    /// Parse errors other than a wrong argument count are queued as `Err` and
    /// answered by EXEC in their slot, as Redis does.
    queued: Vec<Result<Command, CommandError>>,
    failed: bool, // a queue-time error happened; EXEC answers EXECABORT
    /// Wire bytes of everything in `queued`, counted toward MAX_QUERY_BUF.
    /// No parked wait needs it: nothing parks while a transaction is open.
    bytes: usize,
}

/// Parses every complete frame in `inbuf` into `queued`. Runs nothing. Stops
/// at a protocol error, since nothing after it can be framed.
fn parse_into(inbuf: &mut BytesMut, queued: &mut Pending) {
    loop {
        match resp::parse(inbuf) {
            Ok(Some((consumed, frame))) => {
                let owned = inbuf.split_to(consumed).freeze();
                let value = frame.into_value(&owned);
                match to_command(value) {
                    Ok(Some(cmd)) => queued.push(Queued::Run(cmd), consumed),
                    Ok(None) => {}
                    Err(e) => queued.push(Queued::Error(e), consumed),
                }
            }
            Ok(None) => return,
            Err(e) => {
                queued.push(Queued::ProtocolError(Value::Error(e.to_resp())), 0);
                return;
            }
        }
    }
}

/// Queues one item inside MULTI and returns the immediate reply. Only an
/// unknown command and a wrong argument count fail at queue time, which marks
/// the transaction; every other error waits for EXEC. `size` is the item's
/// length on the wire, counted only if the item is actually queued.
fn queue_in(transaction: &mut Transaction, item: Queued, size: usize) -> Value {
    let queued = Value::SimpleString(Bytes::from_static(b"QUEUED"));
    match item {
        Queued::Run(Command::Unknown { name, args }) => {
            transaction.failed = true;
            Value::Error(CommandError::UnknownCommand { name, args }.to_resp())
        }
        Queued::Error(e @ CommandError::WrongArity(_)) => {
            transaction.failed = true;
            Value::Error(e.to_resp())
        }
        Queued::Run(cmd) => {
            transaction.queued.push(Ok(cmd));
            transaction.bytes += size;
            queued
        }
        Queued::Error(e) => {
            transaction.queued.push(Err(e));
            transaction.bytes += size;
            queued
        }
        // Never reached: the drain loop handles protocol errors before
        // queue_in, since broken framing closes the connection either way.
        Queued::ProtocolError(e) => e,
    }
}

mod pending {
    use super::Queued;
    use std::collections::VecDeque;

    /// Commands parsed but not yet run, and how many input bytes they hold.
    /// The count is what is queued right now, not a running total, so the
    /// query buffer limit can check `inbuf.len() + pending.bytes()`.
    pub(super) struct Pending {
        items: VecDeque<(Queued, usize)>,
        bytes: usize,
    }

    impl Pending {
        pub(super) fn new() -> Self {
            Pending {
                items: VecDeque::new(),
                bytes: 0,
            }
        }

        /// `size` is the item's length on the wire: `consumed` from `resp::parse`.
        pub(super) fn push(&mut self, item: Queued, size: usize) {
            self.bytes += size;
            self.items.push_back((item, size));
        }

        /// The item and its size, so a command moving on into a MULTI queue
        /// keeps its bytes counted.
        pub(super) fn pop(&mut self) -> Option<(Queued, usize)> {
            let (item, size) = self.items.pop_front()?;
            self.bytes -= size;
            Some((item, size))
        }

        pub(super) fn bytes(&self) -> usize {
            self.bytes
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rusty_redis::command::CommandError;
        #[test]
        fn bytes_track_what_is_queued_now() {
            let mut p = Pending::new();
            p.push(Queued::Error(CommandError::Syntax), 10);
            p.push(Queued::Error(CommandError::Syntax), 5);
            assert_eq!(p.bytes(), 15);
            assert!(p.pop().is_some());
            assert_eq!(p.bytes(), 5, "popping gives the bytes back");
            assert!(p.pop().is_some());
            assert!(p.pop().is_none());
            assert_eq!(p.bytes(), 0);
        }
    }
}

#[cfg(test)]
mod transaction_tests {
    use super::*;

    fn ping() -> Command {
        let request = Value::Array(vec![Value::BulkString(Bytes::from_static(b"PING"))]);
        to_command(request).unwrap().unwrap()
    }

    fn queued() -> Value {
        Value::SimpleString(Bytes::from_static(b"QUEUED"))
    }

    #[test]
    fn a_queued_command_counts_its_bytes() {
        let mut t = Transaction::default();
        assert_eq!(queue_in(&mut t, Queued::Run(ping()), 14), queued());
        assert_eq!((t.queued.len(), t.bytes, t.failed), (1, 14, false));
    }

    #[test]
    fn an_error_waiting_for_exec_counts_too() {
        // SET k v ZZ: queued, and answered by EXEC in its slot.
        let mut t = Transaction::default();
        assert_eq!(
            queue_in(&mut t, Queued::Error(CommandError::Syntax), 20),
            queued()
        );
        assert_eq!((t.queued.len(), t.bytes, t.failed), (1, 20, false));
    }

    #[test]
    fn queue_time_failures_mark_the_transaction_and_hold_nothing() {
        let mut t = Transaction::default();
        let arity = CommandError::WrongArity(Bytes::from_static(b"get"));
        assert_eq!(
            queue_in(&mut t, Queued::Error(arity.clone()), 9),
            Value::Error(arity.to_resp())
        );
        let unknown = Command::Unknown {
            name: Bytes::from_static(b"NOSUCH"),
            args: vec![],
        };
        assert!(matches!(
            queue_in(&mut t, Queued::Run(unknown), 16),
            Value::Error(_)
        ));
        assert_eq!((t.queued.len(), t.bytes, t.failed), (0, 0, true));
    }
}
