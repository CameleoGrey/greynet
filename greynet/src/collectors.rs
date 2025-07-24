// collectors.rs

use crate::tuple::FactIterator;
use crate::{AnyTuple, GreynetFact};
use rustc_hash::FxHashMap as HashMap;
use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::collections::hash_map::DefaultHasher;

// NEW: A receipt that can be used to undo an insertion. Its meaning is
// specific to each collector. For many, it's a unique ID for the insertion.
// For others, it can encode the value that was added.
pub type UndoReceipt = u64;

// MODIFIED: BaseCollector trait no longer returns a boxed closure.
// Instead, insert returns a receipt, and a new `remove` method accepts that receipt.
pub trait BaseCollector: std::fmt::Debug {
    /// Inserts an item into the collector and returns a receipt for undoing the operation.
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt;

    /// Removes a previously inserted item using its receipt.
    /// The original item is passed back to avoid storing data inside the receipt itself.
    fn remove(&mut self, item: &AnyTuple, receipt: UndoReceipt);

    /// Returns the aggregated result as a GreynetFact.
    fn result_as_fact(&self) -> Rc<dyn GreynetFact>;

    /// Checks if the collector is empty.
    fn is_empty(&self) -> bool;
}

// MODIFIED: FastCollector enum dispatch updated to the new trait methods.
#[derive(Debug)]
pub enum FastCollector {
    Count(CountCollector),
    Sum(SumCollector<fn(&AnyTuple) -> f64>),
    List(ListCollector),
    Custom(Box<dyn BaseCollector>),
}

impl BaseCollector for FastCollector {
    #[inline]
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        match self {
            FastCollector::Count(c) => c.insert(item),
            FastCollector::Sum(c) => c.insert(item),
            FastCollector::List(c) => c.insert(item),
            FastCollector::Custom(c) => c.insert(item),
        }
    }

    #[inline]
    fn remove(&mut self, item: &AnyTuple, receipt: UndoReceipt) {
        match self {
            FastCollector::Count(c) => c.remove(item, receipt),
            FastCollector::Sum(c) => c.remove(item, receipt),
            FastCollector::List(c) => c.remove(item, receipt),
            FastCollector::Custom(c) => c.remove(item, receipt),
        }
    }

    #[inline]
    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        match self {
            FastCollector::Count(c) => c.result_as_fact(),
            FastCollector::Sum(c) => c.result_as_fact(),
            FastCollector::List(c) => c.result_as_fact(),
            FastCollector::Custom(c) => c.result_as_fact(),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        match self {
            FastCollector::Count(c) => c.is_empty(),
            FastCollector::Sum(c) => c.is_empty(),
            FastCollector::List(c) => c.is_empty(),
            FastCollector::Custom(c) => c.is_empty(),
        }
    }
}

// MODIFIED: CountCollector no longer uses Rc<RefCell<...>>.
#[derive(Default, Debug)]
pub struct CountCollector {
    count: usize,
}

impl BaseCollector for CountCollector {
    #[inline]
    fn insert(&mut self, _item: &AnyTuple) -> UndoReceipt {
        self.count += 1;
        0 // Receipt is not used for this simple collector.
    }

    #[inline]
    fn remove(&mut self, _item: &AnyTuple, _receipt: UndoReceipt) {
        self.count = self.count.saturating_sub(1);
    }

    #[inline]
    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(self.count)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

// MODIFIED: SumCollector no longer uses Rc<RefCell<...>>.
pub struct SumCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    mapping_function: F,
    total: f64,
    count: usize,
}

impl<F> std::fmt::Debug for SumCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SumCollector")
            .field("mapping_function", &"<closure>")
            .field("total", &self.total)
            .field("count", &self.count)
            .finish()
    }
}

impl<F> BaseCollector for SumCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    #[inline]
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        let value = (self.mapping_function)(item);
        self.total += value;
        self.count += 1;
        // The receipt encodes the added value to be used for removal.
        value.to_bits()
    }

    #[inline]
    fn remove(&mut self, _item: &AnyTuple, receipt: UndoReceipt) {
        let value_to_remove = f64::from_bits(receipt);
        self.total -= value_to_remove;
        self.count = self.count.saturating_sub(1);
    }

    #[inline]
    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(self.total)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

// MODIFIED: AvgCollector no longer uses Rc<RefCell<...>>.
pub struct AvgCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    mapping_function: F,
    total: f64,
    count: usize,
}

impl<F> std::fmt::Debug for AvgCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvgCollector")
            .field("mapping_function", &"<closure>")
            .field("total", &self.total)
            .field("count", &self.count)
            .finish()
    }
}

impl<F> BaseCollector for AvgCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        let value = (self.mapping_function)(item);
        self.total += value;
        self.count += 1;
        value.to_bits()
    }

    fn remove(&mut self, _item: &AnyTuple, receipt: UndoReceipt) {
        let value_to_remove = f64::from_bits(receipt);
        self.total -= value_to_remove;
        self.count = self.count.saturating_sub(1);
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        if self.count == 0 {
            Rc::new(0.0)
        } else {
            Rc::new(self.total / self.count as f64)
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

// MODIFIED: ListCollector uses HashMap for O(1) removal and no longer uses Rc<RefCell<...>>.
#[derive(Default, Debug)]
pub struct ListCollector {
    items: HashMap<u64, AnyTuple>,
    // We still need insertion order for the result.
    insertion_order: Vec<u64>,
    next_id: u64,
}

impl BaseCollector for ListCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        let item_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);

        self.items.insert(item_id, item.clone());
        self.insertion_order.push(item_id);
        
        item_id
    }

    fn remove(&mut self, _item: &AnyTuple, receipt: UndoReceipt) {
        let item_id = receipt;
        if self.items.remove(&item_id).is_some() {
            self.insertion_order.retain(|id| *id != item_id);
        }
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let tuples: Vec<AnyTuple> = self.insertion_order
            .iter()
            .filter_map(|id| self.items.get(id).cloned())
            .collect();
        Rc::new(tuples)
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

// MODIFIED: MinCollector no longer uses Rc<RefCell<...>>.
pub struct MinCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    mapping_function: F,
    counts: BTreeMap<K, usize>,
}

impl<K, F> std::fmt::Debug for MinCollector<K, F>
where
    K: Ord + Clone + GreynetFact + std::fmt::Debug,
    F: Fn(&AnyTuple) -> K + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MinCollector")
            .field("mapping_function", &"<closure>")
            .field("counts", &self.counts)
            .finish()
    }
}

impl<K, F> BaseCollector for MinCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        let key = (self.mapping_function)(item);
        *self.counts.entry(key).or_insert(0) += 1;
        0 // Receipt is not used; key is recalculated on remove.
    }

    fn remove(&mut self, item: &AnyTuple, _receipt: UndoReceipt) {
        let key = (self.mapping_function)(item);
        if let Some(count) = self.counts.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&key);
            }
        }
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        if let Some(min_key) = self.counts.keys().next() {
            Rc::new(min_key.clone())
        } else {
            panic!("Collector result called on empty group")
        }
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }
}

// MODIFIED: MaxCollector no longer uses Rc<RefCell<...>>.
pub struct MaxCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    mapping_function: F,
    counts: BTreeMap<K, usize>,
}

impl<K, F> std::fmt::Debug for MaxCollector<K, F>
where
    K: Ord + Clone + GreynetFact + std::fmt::Debug,
    F: Fn(&AnyTuple) -> K + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaxCollector")
            .field("mapping_function", &"<closure>")
            .field("counts", &self.counts)
            .finish()
    }
}

impl<K, F> BaseCollector for MaxCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        let key = (self.mapping_function)(item);
        *self.counts.entry(key).or_insert(0) += 1;
        0 // Receipt is not used.
    }

    fn remove(&mut self, item: &AnyTuple, _receipt: UndoReceipt) {
        let key = (self.mapping_function)(item);
        if let Some(count) = self.counts.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&key);
            }
        }
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        if let Some(max_key) = self.counts.keys().last() {
            Rc::new(max_key.clone())
        } else {
            panic!("Collector result called on empty group")
        }
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }
}

// MODIFIED: SetCollector no longer uses Rc<RefCell<...>>.
#[derive(Default, Debug)]
pub struct SetCollector {
    items: HashSet<u64>,
    tuple_hashes: HashMap<u64, usize>,
}

impl BaseCollector for SetCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        let item_hash = Self::hash_tuple(item);
        self.items.insert(item_hash);
        *self.tuple_hashes.entry(item_hash).or_insert(0) += 1;
        item_hash
    }

    fn remove(&mut self, _item: &AnyTuple, receipt: UndoReceipt) {
        let item_hash = receipt;
        if let Some(count) = self.tuple_hashes.get_mut(&item_hash) {
            *count -= 1;
            if *count == 0 {
                self.tuple_hashes.remove(&item_hash);
                self.items.remove(&item_hash);
            }
        }
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(self.items.len())
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl SetCollector {
    fn hash_tuple(tuple: &AnyTuple) -> u64 {
        let mut hasher = DefaultHasher::new();
        for fact in tuple.facts_iter() {
            fact.hash_fact().hash(&mut hasher);
        }
        hasher.finish()
    }
}

// MODIFIED: DistinctCollector no longer uses Rc<RefCell<...>>.
#[derive(Default, Debug)]
pub struct DistinctCollector {
    items: HashMap<u64, AnyTuple>,
    insertion_order: Vec<u64>,
    counts: HashMap<u64, usize>,
}

impl BaseCollector for DistinctCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoReceipt {
        let item_hash = SetCollector::hash_tuple(item);
        if !self.items.contains_key(&item_hash) {
            self.items.insert(item_hash, item.clone());
            self.insertion_order.push(item_hash);
        }
        *self.counts.entry(item_hash).or_insert(0) += 1;
        item_hash
    }

    fn remove(&mut self, _item: &AnyTuple, receipt: UndoReceipt) {
        let item_hash = receipt;
        if let Some(count) = self.counts.get_mut(&item_hash) {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&item_hash);
                self.items.remove(&item_hash);
                self.insertion_order.retain(|h| h != &item_hash);
            }
        }
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let tuples: Vec<AnyTuple> = self
            .insertion_order
            .iter()
            .filter_map(|hash| self.items.get(hash).cloned())
            .collect();
        Rc::new(tuples)
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Factory for creating collectors
pub struct Collectors;

impl Collectors {
    pub fn count() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(CountCollector::default()))
    }

    pub fn sum<F>(mapping_function: F) -> Box<dyn Fn() -> Box<dyn BaseCollector>>
    where
        F: Fn(&AnyTuple) -> f64 + Clone + 'static,
    {
        Box::new(move || {
            Box::new(SumCollector {
                mapping_function: mapping_function.clone(),
                total: 0.0,
                count: 0,
            })
        })
    }

    pub fn avg<F>(mapping_function: F) -> Box<dyn Fn() -> Box<dyn BaseCollector>>
    where
        F: Fn(&AnyTuple) -> f64 + Clone + 'static,
    {
        Box::new(move || {
            Box::new(AvgCollector {
                mapping_function: mapping_function.clone(),
                total: 0.0,
                count: 0,
            })
        })
    }

    pub fn to_list() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(ListCollector::default()))
    }

    pub fn min<K, F>(mapping_function: F) -> Box<dyn Fn() -> Box<dyn BaseCollector>>
    where
        K: Ord + Clone + GreynetFact,
        F: Fn(&AnyTuple) -> K + Clone + 'static,
    {
        Box::new(move || {
            Box::new(MinCollector {
                mapping_function: mapping_function.clone(),
                counts: BTreeMap::new(),
            })
        })
    }

    pub fn max<K, F>(mapping_function: F) -> Box<dyn Fn() -> Box<dyn BaseCollector>>
    where
        K: Ord + Clone + GreynetFact,
        F: Fn(&AnyTuple) -> K + Clone + 'static,
    {
        Box::new(move || {
            Box::new(MaxCollector {
                mapping_function: mapping_function.clone(),
                counts: BTreeMap::new(),
            })
        })
    }

    pub fn to_set() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(SetCollector::default()))
    }

    pub fn distinct() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(DistinctCollector::default()))
    }

    pub fn list_with_capacity(capacity: usize) -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(move || {
            Box::new(ListCollector {
                items: HashMap::default(),
                insertion_order: Vec::with_capacity(capacity),
                next_id: 0,
            })
        })
    }

    pub fn set_with_capacity(capacity: usize) -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(move || {
            Box::new(SetCollector {
                items: HashSet::with_capacity(capacity),
                tuple_hashes: HashMap::with_capacity_and_hasher(
                    capacity,
                    Default::default(),
                ),
            })
        })
    }

    pub fn distinct_with_capacity(capacity: usize) -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(move || {
            Box::new(DistinctCollector {
                items: HashMap::with_capacity_and_hasher(
                    capacity,
                    Default::default(),
                ),
                insertion_order: Vec::with_capacity(capacity),
                counts: HashMap::with_capacity_and_hasher(
                    capacity,
                    Default::default(),
                ),
            })
        })
    }
}
