use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::io::{BufReader, BufWriter, Write};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::path::Path;

use rust_kv::common::{Request, GetResponse, SetResponse, RemoveResponse};
use rust_kv::kv::KvStore;

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

                let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();


                for req in req_reader {
                    match req.unwrap() {
                        Request::Get { key } => {
                            println!("server get {}", key);
                            let mut store = KvStore::open(Path::new("")).unwrap();
                            let v_opt = store.get(key.to_string()).unwrap();
                            let resp = GetResponse::Ok(v_opt);
                            serde_json::to_writer(&mut writer, &resp).unwrap();
                            writer.flush().unwrap();
                        }

                        Request::Set { key, value } => {
                            let mut store = KvStore::open(Path::new("")).unwrap();
                            store.set(key, value);
                            let resp = SetResponse::Ok(());
                            serde_json::to_writer(&mut writer, &resp).unwrap();
                            writer.flush().unwrap();
                        }

                        Request::Remove { key } => {
                            let mut store = KvStore::open(Path::new("")).unwrap();
                            store.remove(key);
                            let resp = RemoveResponse::Ok(());
                            serde_json::to_writer(&mut writer, &resp).unwrap();
                            writer.flush().unwrap();
                        }
                    }
                }
            }
            Err(e) => {}
        }
    }
}