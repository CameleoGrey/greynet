//advanced_index.rs
use super::arena::SafeTupleIndex;
use super::joiner::JoinerType;
use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use rustc_hash::FxHashMap as HashMap;


#[derive(Debug)]
pub enum AdvancedIndex<K: Ord + Hash> {
    Range(BTreeMap<K, Vec<SafeTupleIndex>>),
    NotEqual {
        all_tuples: Vec<SafeTupleIndex>,
        key_map: HashMap<K, HashSet<SafeTupleIndex>>,
    },
}

impl<K: Ord + Hash + Copy> AdvancedIndex<K> {
    pub fn new(joiner_type: JoinerType) -> Self {
        match joiner_type {
            JoinerType::LessThan | JoinerType::LessThanOrEqual | JoinerType::GreaterThan | JoinerType::GreaterThanOrEqual => {
                AdvancedIndex::Range(BTreeMap::new())
            }
            JoinerType::NotEqual => AdvancedIndex::NotEqual {
                all_tuples: Vec::new(),
                key_map: HashMap::default(),
            },
            _ => panic!("AdvancedIndex does not support joiner type: {:?}", joiner_type),
        }
    }

    pub fn put(&mut self, key: K, tuple_index: SafeTupleIndex) {
        match self {
            AdvancedIndex::Range(map) => {
                map.entry(key).or_default().push(tuple_index);
            }
            AdvancedIndex::NotEqual { all_tuples, key_map } => {
                all_tuples.push(tuple_index);
                key_map.entry(key).or_default().insert(tuple_index);
            }
        }
    }

    pub fn remove(&mut self, key: K, tuple_index: &SafeTupleIndex) {
        match self {
            AdvancedIndex::Range(map) => {
                if let Some(tuples) = map.get_mut(&key) {
                    tuples.retain(|x| x != tuple_index);
                    if tuples.is_empty() {
                        map.remove(&key);
                    }
                }
            }
            AdvancedIndex::NotEqual { all_tuples, key_map } => {
                all_tuples.retain(|x| x != tuple_index);
                if let Some(tuples) = key_map.get_mut(&key) {
                    tuples.remove(tuple_index);
                    if tuples.is_empty() {
                        key_map.remove(&key);
                    }
                }
            }
        }
    }

    pub fn get_matches(&self, query_key: K, joiner_type: JoinerType) -> Vec<SafeTupleIndex> {
        match self {
            AdvancedIndex::Range(map) => {
                let mut results = Vec::new();
                let range_iter: Box<dyn Iterator<Item = (&K, &Vec<SafeTupleIndex>)>> = match joiner_type {
                    JoinerType::LessThan => Box::new(map.range(..query_key)),
                    JoinerType::LessThanOrEqual => Box::new(map.range(..=query_key)),
                    JoinerType::GreaterThan => Box::new(map.range((std::ops::Bound::Excluded(query_key), std::ops::Bound::Unbounded))),
                    JoinerType::GreaterThanOrEqual => Box::new(map.range(query_key..)),
                    _ => panic!("Unsupported joiner for Range index: {:?}", joiner_type),
                };
                for (_, tuples) in range_iter {
                    results.extend(tuples);
                }
                results
            }
            AdvancedIndex::NotEqual { all_tuples, key_map } => {
                let tuples_to_exclude = key_map.get(&query_key).cloned().unwrap_or_default();
                all_tuples.iter().filter(|&t| !tuples_to_exclude.contains(t)).cloned().collect()
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            AdvancedIndex::Range(map) => map.values().map(|v| v.len()).sum(),
            AdvancedIndex::NotEqual { all_tuples, .. } => all_tuples.len(),
        }
    }
}
