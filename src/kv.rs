use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, create_dir_all, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use log::debug;
use failure::Fail;
use tempfile::TempDir;
use walkdir::WalkDir;

#[derive(Debug)]
struct LogPointer {
    file: Arc<File>,
    offset: u64,
    length: u64,
}


#[derive(Serialize, Deserialize, Debug)]
enum CommandType {
    Set,
    Remove,
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    typ: CommandType,
    key: String,
    value: String,
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


fn read_value(key: &String, index: &HashMap<String, LogPointer>) -> Result<Option<String>> {
    match index.get(key).as_mut() {
        None => Ok(None),
        Some(lp) => {
            let mut file = lp.file.as_ref();

            file.seek(SeekFrom::Start(lp.offset))?;
            let cmd: Command = bincode::deserialize_from(file)?;
            Ok(Some(cmd.value))
        }
    }
}

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
    pub fn set(self: &mut KvStore, key: String, val: String) -> Result<()> {
        println!("Setting '{}' => '{}'", key, val);

        self.build_index()?;

        let index = self.index.as_ref().expect("index undefined");

        let existing_val = read_value(&key, index)?;

        if existing_val.as_ref() == Some(&val) {
            debug!("Doing nothing since the existing value is the same");
            return Ok(());
        }

        if existing_val.is_some() {
            let old_lp = index.get(&key).expect("key should be in index");
            debug!("Adding to compaction potential: {}", old_lp.length);
            self.uncompacted += old_lp.length;
        }

        let cmd = Command {
            typ: CommandType::Set,
            key: key.clone(),
            value: val.clone(),
        };

        println!("Writing set command: {}, {}", key, val);
        self.write_command(cmd)?;
        // println!("{:?}", self.index);


        Ok(())
    }

    pub fn get(self: &mut KvStore, key: String) -> Result<Option<String>> {
        println!("Getting key '{}'", key);
        self.build_index()?;
        let index = self.index.as_ref().unwrap();
        let val = read_value(&key, index)?;
        Ok(val)
    }

    pub fn remove(self: &mut KvStore, key: String) -> Result<()> {
        self.build_index()?;
        let index = self.index.as_mut().unwrap();
        if !index.contains_key(&key) {
            return Err(KvsError::NonExistentKey(key))?;
        }

        let old_cmd = index.remove(&key).expect("key not found");
        self.uncompacted += old_cmd.length;

        let cmd = Command {
            typ: CommandType::Remove,
            key: key.clone(),
            value: String::new(),
        };

        debug!("Writing remove command: {}", key);
        self.write_command(cmd)?;


        Ok(())
    }

    fn build_index(self: &mut KvStore) -> Result<()> {
        // 建立索引
        if self.index.is_some() {
            debug!("Index already defined");
            return Ok(());
        }

        self.index = Some(HashMap::new());
        let index = self.index.as_mut().expect("index should be defined");


        let mut uncompacted: u64 = 0;
        let mut i = 1;

        loop {
            let fpath = Path::new(&self.dpath).join(format!("log-{}", i));
            let file_rslt = File::open(fpath.clone());

            if file_rslt.is_err() {
                debug!("File '{}' doesn't exist, ending loop", fpath.to_str().unwrap());
                break;
            }

            let file = Arc::new(file_rslt.expect("file should be opened"));
            // println!("File '{}' exists, applying to index", fpath.to_str().unwrap());

            loop {
                let offset = file.as_ref().seek(SeekFrom::Current(0))?;
                let read_rslt = bincode::deserialize_from(file.as_ref());

                if read_rslt.is_err() {
                    break;
                }
                let cmd: Command = read_rslt.expect("read result should be OK");
                let cur_offset = file.as_ref().seek(SeekFrom::Current(0))?;
                let cmd_length: u64 = cur_offset - offset;

                match cmd.typ {
                    CommandType::Set => {
                        debug!("Read set command {} => {}", cmd.key, cmd.value);
                        if let Some(old_ptr) = index.insert(
                            cmd.key,
                            LogPointer {
                                file: file.clone(),
                                offset,
                                length: cmd_length,
                            },
                        ) {
                            debug!("Overridden command can be compacted: {}", old_ptr.length);
                            uncompacted += old_ptr.length;
                        }
                    }
                    CommandType::Remove => {
                        if let Some(old_ptr) = index.remove(&cmd.key) {
                            uncompacted += old_ptr.length;
                        }
                        uncompacted += cmd_length;
                    }
                }
            }
            i += 1;
        }

        self.uncompacted = uncompacted;
        Ok(())
    }


    fn write_command(self: &mut KvStore, cmd: Command) -> Result<()> {
        if self.file.is_none() {
            let mut i = 1;
            loop {
                let fpath = Path::new(&self.dpath).join(format!("log-{}", i));
                if !fpath.exists() {
                    debug!("Creating file at {}", fpath.to_str().unwrap());
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create_new(true)
                        .open(fpath)?;
                    self.file = Some(Arc::new(file));
                    break;
                }
                i += 1;
            }
        }


        let serialized = bincode::serialize(&cmd)?;
        let file = self.file.as_ref().expect("self.file");
        let offset = file.as_ref().seek(SeekFrom::End(0))?;
        file.as_ref().write(&serialized)?;
        let cur_offset = file.as_ref().seek(SeekFrom::End(0))?;


        match cmd.typ {
            CommandType::Remove => {}
            CommandType::Set => {
                debug!("Writing command to file {:?}", self.file.as_ref());
                let file = self.file.as_ref().unwrap().clone();

                let lp = LogPointer { file, offset, length: cur_offset - offset };

                self.index.as_mut()
                    .expect("self.index should be defined")
                    .insert(cmd.key, lp);
            }
        };

        if self.uncompacted > 1024 * 1024 {
            self.compact()?;
        }
        Ok(())
    }


    fn compact(&mut self) -> Result<()> {
        let index = self.index.as_ref().expect("self.index should be defined");
        let new_dpath = format!("{}.new", self.dpath);
        create_dir_all(&new_dpath)?;
        let mut file = File::create(Path::new(&new_dpath).join("log-1"))?;

        for (key, _) in self.index.as_ref().expect("self.index should be defined") {
            let value = read_value(key, index)?.expect("key should have value");
            let cmd = Command { typ: CommandType::Set, key: key.to_owned(), value };
            let serialized = bincode::serialize(&cmd)?;
            file.write(&serialized)?;
        }

        file.flush()?;
        self.index = None;
        self.file = None;

        let old_dpath = format!("{}.old", self.dpath);
        fs::rename(&self.dpath, &old_dpath)?;
        fs::rename(&new_dpath, &self.dpath)?;

        fs::remove_dir_all(&old_dpath)?;
        self.build_index()?;
        self.uncompacted = 0;
        Ok(())
    }
}


#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert!(store.remove("key1".to_owned()).is_ok());
    assert_eq!(store.get("key1".to_owned())?, None);
    Ok(())
}


#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    assert!(store.remove("key1".to_owned()).is_err());
    Ok(())
}


#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;


    println!("{:?}", store.index);

    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data
    println!("\nDropping store");
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
    store.set("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut current_size = dir_size();
    for iter in 0..10 {
        for key_id in 0..10 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(key, value)?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered

        drop(store);
        // reopen and check content
        let mut store = KvStore::open(temp_dir.path())?;
        for key_id in 0..10 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key)?, Some(format!("{}", iter)));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}




