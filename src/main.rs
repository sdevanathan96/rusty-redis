#![allow(unused_imports)]
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop{
        let stream = listener.accept().await;
        match stream {
            Ok(stream) => {
                println!("new connection accepted");
                tokio::spawn(async move {
                    handle_client(stream, b"+PONG\r\n");
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
        let bytes_read = stream.read(&mut buf).await.expect("Failed to read from client");

        if bytes_read == 0 {
            return;
        }

        stream.write_all(buffer).await.expect("Failed to write to client");
    }
}
