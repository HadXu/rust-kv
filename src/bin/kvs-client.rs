use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

use serde::{Deserialize, Serialize};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn main() {
    println!("client");
    connect();
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    x: i32,
    y: i32,
}

fn connect() {
    let tcp_reader = TcpStream::connect(DEFAULT_LISTENING_ADDRESS).unwrap();
    let tcp_writer = tcp_reader.try_clone().unwrap();

    let c = Command { x: 3, y: 4 };

    serde_json::to_writer(&mut BufWriter::new(tcp_writer), &c);
}