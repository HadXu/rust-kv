use clap::{App, Arg};
use std::path::Path;

mod kv;

fn main() {
    // let matches = App::new("KV")
    //     .version("1.0")
    //     .author("hadxu123@gmail.com")
    //     .about("KV Store")
    //     .subcommand(
    //         App::new("get")
    //             .about("gets a key")
    //             .arg(Arg::with_name("key").required(true).help("Key to get")),
    //     )
    //     .subcommand(
    //         App::new("set")
    //             .about("sets a key")
    //             .arg(Arg::with_name("key").required(true).help("Key to set"))
    //             .arg(Arg::with_name("value").required(true).help("value to set")),
    //     )
    //     .subcommand(
    //         App::new("rm")
    //             .about("remove key")
    //             .arg(Arg::with_name("key").required(true).help("Key to remove")),
    //     )
    //     .get_matches();
    //
    //
    // match matches.subcommand_name() {
    //     Some("get") => {
    //         let sub_matches = matches.subcommand_matches("get").unwrap();
    //         let key = sub_matches.value_of("key").unwrap();
    //         println!("get {}", key);
    //     }
    //     Some("set") => {
    //         let sub_matches = matches.subcommand_matches("set").unwrap();
    //         let key = sub_matches.value_of("key").unwrap();
    //         let value = sub_matches.value_of("value").unwrap();
    //         println!("set {}-{}", key, value);
    //     }
    //     Some("rm") => {
    //         let sub_matches = matches.subcommand_matches("rm").unwrap();
    //         let key = sub_matches.value_of("key").unwrap();
    //         println!("rm {}", key);
    //     }
    //     _ => panic!("Unrecognized subcommand"),
    // }

    let x = Path::new("").join(".kvs");

    println!("{:?}", x.to_str().unwrap());
}

