use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use rusty_redis::resp::{self, Value};
use rusty_redis::command::{to_command, execute};
use rusty_redis::db::Db;
use rusty_redis::db::SystemClock;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("listening on 127.0.0.1:6379");
    let clock = Arc::new(SystemClock);
    let db = Arc::new(Db::with_clock(clock));
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                println!("new connection from {peer}");
                let db = Arc::clone(&db);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, db).await {
                        eprintln!("connection {peer} ended: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept failed: {e}"),
        }
    }
}

async fn handle_client(mut stream: TcpStream, db: Arc<Db>) -> std::io::Result<()> {
    let mut inbuf = BytesMut::with_capacity(4096);
    let mut outbuf = BytesMut::with_capacity(4096);
    loop {
        loop {
            match resp::parse(&inbuf) {
                Ok(Some((consumed, value))) => {
                    inbuf.advance(consumed);
                    let reply = match to_command(value).and_then(|cmd| execute(cmd, &db)) {
                        Ok(v) => v,
                        Err(e) => Value::Error(e.to_resp()),
                    };
                    resp::encode(&reply, &mut outbuf);
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    let reply = resp::Value::Error(e.to_resp());
                    resp::encode(&reply, &mut outbuf);
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
        let n = stream.read_buf(&mut inbuf).await?;
        if n == 0 {
            return Ok(());
        }
    }  
}