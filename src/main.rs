#![allow(unused_imports)]
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop{
        let stream = listener.accept().await;
        match stream {
            Ok((mut stream, _)) => {
                println!("new connection accepted");
                tokio::spawn(async move {
                    handle_client(stream, b"+PONG\r\n").await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream, buffer: &[u8]) {
    let mut buf: [u8; 512] = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).await.unwrap();

        if bytes_read == 0 {
            return;
        }

        stream.write_all(buffer).await.unwrap();
    }
}
