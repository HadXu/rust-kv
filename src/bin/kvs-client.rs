use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

use serde::{Deserialize, Serialize};
use serde_json::de::{Deserializer, IoRead};

use rust_kv::common::{Request, GetResponse, SetResponse, RemoveResponse};
use rust_kv::kv::KvStore;
use bincode::Error;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn main() {
    println!("client");
    let mut client = KvsClient::connect(DEFAULT_LISTENING_ADDRESS);
    client.set("12345".to_owned(), "hadxu123".to_owned());
    client.get("12345".to_owned());
    client.remove("12345".to_owned());
}

pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Self {
        let tcp_reader = TcpStream::connect(addr).unwrap();
        let tcp_writer = tcp_reader.try_clone().unwrap();
        KvsClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
            writer: BufWriter::new(tcp_writer),
        }
    }

    pub fn get(&mut self, key: String) {
        serde_json::to_writer(&mut self.writer, &Request::Get { key }).unwrap();
        self.writer.flush().unwrap();
        let resp = GetResponse::deserialize(&mut self.reader).unwrap();

        match resp {
            GetResponse::Ok(value) => {
                println!("{}", value.unwrap())
            }
            GetResponse::Err(msg) => Err("err").unwrap()
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value }).unwrap();
        self.writer.flush().unwrap();
        let resp = SetResponse::deserialize(&mut self.reader).unwrap();

        match resp {
            SetResponse::Ok(()) => {
                println!("set ok")
            }
            SetResponse::Err(e) => Err(e).unwrap()
        }
    }

    pub fn remove(&mut self, key: String) {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key }).unwrap();
        self.writer.flush().unwrap();


        let resp = RemoveResponse::deserialize(&mut self.reader).unwrap();

        match resp {
            RemoveResponse::Ok(()) => {
                println!("remove ok")
            }
            RemoveResponse::Err(e) => Err(e).unwrap()
        }
    }
}