use std::collections::HashMap;
use std::sync::Arc;
use std::fs::{File, create_dir_all};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use log::debug;
use failure::Fail;

#[derive(Debug)]
struct LogPointer {
    file: Arc<File>,
    offset: u64,
    length: u64,
}


#[derive(Default)]
pub struct KvStore {
    dpath: String,
    // ???
    index: Option<HashMap<String, LogPointer>>,
    // ???
    file: Option<Arc<File>>,
    // ???
    uncompacted: u64,
}

#[derive(Fail, Debug)]
pub enum KvsError {
    /// Non-existent key.
    #[fail(display = "Non-existent key: {}", _0)]
    NonExistentKey(String),
    /// An IO error occurred.
    #[fail(display = "{}", _0)]
    IoError(#[fail(cause)] io::Error),
    /// A Bincode error occurred.
    #[fail(display = "Bincode error: {}", _0)]
    BincodeError(#[fail(cause)] bincode::Error),
    /// A SetLoggerError occurred.
    #[fail(display = "{}", _0)]
    SetLoggerError(#[fail(cause)] log::SetLoggerError),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IoError(err)
    }
}

impl From<bincode::Error> for KvsError {
    fn from(err: bincode::Error) -> KvsError {
        KvsError::BincodeError(err)
    }
}

impl From<log::SetLoggerError> for KvsError {
    fn from(err: log::SetLoggerError) -> KvsError {
        KvsError::SetLoggerError(err)
    }
}


pub type Result<T> = std::result::Result<T, KvsError>;

impl KvStore {
    pub fn open(dpath: &Path) -> Result<KvStore> {
        let dpath_full = dpath.join(".kvs");
        if !dpath_full.exists() {
            create_dir_all(&dpath_full)?;
        }
        let dpath_str = String::from(
            dpath_full
                // 对路径进行规范化
                .canonicalize()?
                .to_str()
                .expect("directory should exist"),
        );

        debug!("Opening KvStore, dpath: '{}'", dpath_str);

        Ok(KvStore {
            index: None,
            file: None,
            dpath: dpath_str,
            uncompacted: 0,
        })
    }
}


#[test]
fn open() {
    let mut store = KvStore::open(Path::new(""));
    assert_eq!(store.unwrap().dpath.as_str(), "/Users/haxu/IdeaProjects/rust-kv/.kvs");
}

