#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::Write;
use std::io::Read;

fn main() {

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buffer = [0;512];
                loop{
                    let bytes_read = stream.read(&mut buffer).unwrap();
                    if bytes_read == 0{
                        break;
                    }
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
                
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
