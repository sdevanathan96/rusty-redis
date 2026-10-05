use std::sync::Arc;

use rusty_redis::config;
use rusty_redis::connection::handle_client;
use rusty_redis::db::{Db, SystemClock};
use rusty_redis::keyspace::{Request, keyspace_task};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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
