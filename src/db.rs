use std::collections::HashMap;
use std::sync::Mutex;

pub struct Db {
    map: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl Db {
    pub fn new() -> Self {
        Db { map: Mutex::new(HashMap::new()) }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let guard = self.map.lock().unwrap();
        guard.get(key).cloned()
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        self.map.lock().unwrap().insert(key, value);
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        let mut guard = self.map.lock().unwrap();
        guard.remove(key).is_some()
    }
}