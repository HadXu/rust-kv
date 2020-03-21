use std::collections::HashMap;

#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn get(self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}

#[test]
fn get_non_existent_value() {
    let mut store = KvStore::new();
    store.set("key1".to_owned(), "value1".to_owned());
    assert_eq!(store.get("key2".to_owned()), None);
}

#[test]
fn remove_key() {
    let mut store = KvStore::new();
    store.set("key1".to_owned(), "value1".to_owned());
    store.remove("key1".to_owned());
    assert_eq!(store.get("key1".to_owned()), None);
}





