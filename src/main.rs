use clap::{App, Arg};
use std::path::Path;
use kv::{KvsError,KvStore, Result};
use tempfile::TempDir;
use std::process::exit;

mod kv;

fn main() -> Result<()> {

    // let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    // let mut store = KvStore::open(Path::new("")).unwrap();
    // let mut store = KvStore::open(temp_dir.path()).unwrap();


    let matches = App::new("KV")
        .version("1.0")
        .author("hadxu123@gmail.com")
        .about("KV Store")
        .subcommand(
            App::new("get")
                .about("gets a key")
                .arg(Arg::with_name("key").required(true).help("Key to get")),
        )
        .subcommand(
            App::new("set")
                .about("sets a key")
                .arg(Arg::with_name("key").required(true).help("Key to set"))
                .arg(Arg::with_name("value").required(true).help("value to set")),
        )
        .subcommand(
            App::new("rm")
                .about("remove key")
                .arg(Arg::with_name("key").required(true).help("Key to remove")),
        )
        .get_matches();


    match matches.subcommand_name() {
        Some("get") => {
            let sub_matches = matches.subcommand_matches("get").unwrap();
            let key = sub_matches.value_of("key").unwrap();

            let mut store = KvStore::open(Path::new("")).unwrap();

            let v_opt = store.get(key.to_string()).unwrap();
            let msg = v_opt.unwrap_or("Key not found".to_string());
            println!("get {} value {}", key, msg);
            Ok(())

        }
        Some("set") => {
            let sub_matches = matches.subcommand_matches("set").unwrap();
            let key = sub_matches.value_of("key").unwrap();
            let value = sub_matches.value_of("value").unwrap();

            let mut store = KvStore::open(Path::new("")).unwrap();
            store.set(key.to_string(), value.to_string())

        }
        Some("rm") => {
            let sub_matches = matches.subcommand_matches("rm").unwrap();
            let key = sub_matches.value_of("key").unwrap();
            let mut store = KvStore::open(Path::new("")).unwrap();
            store.remove(key.to_string()).map_err(|e| match e {
                KvsError::NonExistentKey(_) => {
                    println!("Key not found");
                    exit(1);
                }
                _ => e,
            })

        }
        _ => panic!("Unrecognized subcommand"),
    }
}

