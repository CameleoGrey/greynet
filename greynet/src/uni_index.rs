//uni_index.rs
use super::arena::SafeTupleIndex;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub struct UniIndex<K: Eq + Hash> {
    map: HashMap<K, Vec<SafeTupleIndex>>,
}

impl<K: Eq + Hash> UniIndex<K> {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }

    pub fn put(&mut self, key: K, tuple_index: SafeTupleIndex) {
        self.map.entry(key).or_default().push(tuple_index);
    }

    pub fn get(&self, key: K) -> &[SafeTupleIndex] {
        self.map.get(&key).map_or(&[], |v| v.as_slice())
    }

    pub fn remove(&mut self, key: K, tuple_index: &SafeTupleIndex) {
        if let Some(tuples) = self.map.get_mut(&key) {
            tuples.retain(|x| x != tuple_index);
            if tuples.is_empty() {
                self.map.remove(&key);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.values().map(|v| v.len()).sum()
    }
}

impl<K: Eq + Hash> Default for UniIndex<K> {
    fn default() -> Self {
        Self::new()
    }
}