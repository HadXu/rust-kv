use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::io::{BufReader, BufWriter, Write};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";


#[derive(Serialize, Deserialize, Debug)]
struct Command {
    x: i32,
    y: i32,
}

fn main() {
    println!("start server.......");
    run();
}

fn run() {
    let listener = TcpListener::bind(DEFAULT_LISTENING_ADDRESS).unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer_addr = stream.peer_addr().unwrap();
                println!("new client! {}", peer_addr.ip());

                let reader = BufReader::new(&stream);
                let mut writer = BufWriter::new(&stream);

                let req_reader = Deserializer::from_reader(reader).into_iter::<Command>();
                for req in req_reader {
                    match req.unwrap() {
                        Command { x, y } => {
                            println!("x={},y={}", x, y)
                        }
                    }
                }
            }
            Err(e) => {}
        }
    }
}