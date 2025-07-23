//collectors.rs
use crate::{AnyTuple, GreynetFact};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::collections::{HashSet, BTreeSet};
use std::hash::Hash;
use uuid::Uuid;
use std::hash::Hasher;
use std::hash::DefaultHasher;
use std::any::Any;

/// A trait for objects that can aggregate data within a `group_by` operation.
pub trait BaseCollector {
    /// Inserts an item and returns an undo function.
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction;
    /// Returns the current result of the aggregation as a `GreynetFact`.
    fn result_as_fact(&self) -> Rc<dyn GreynetFact>;
    /// Checks if the collector is empty.
    fn is_empty(&self) -> bool;
}

/// Safe undo function that captures state
pub struct UndoFunction {
    undo_fn: Box<dyn FnOnce()>,
}

impl UndoFunction {
    pub fn new<F: FnOnce() + 'static>(f: F) -> Self {
        Self { undo_fn: Box::new(f) }
    }

    pub fn execute(self) {
        (self.undo_fn)();
    }
}

#[derive(Default)]
pub struct CountCollector {
    count: Rc<RefCell<usize>>,
}

impl BaseCollector for CountCollector {
    fn insert(&mut self, _item: &AnyTuple) -> UndoFunction {
        *self.count.borrow_mut() += 1;
        let count = self.count.clone();
        UndoFunction::new(move || {
            *count.borrow_mut() -= 1;
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(*self.count.borrow())
    }

    fn is_empty(&self) -> bool {
        *self.count.borrow() == 0
    }
}

pub struct SumCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    mapping_function: F,
    total: Rc<RefCell<f64>>,
    count: Rc<RefCell<usize>>,
}

impl<F> BaseCollector for SumCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let value = (self.mapping_function)(item);
        *self.total.borrow_mut() += value;
        *self.count.borrow_mut() += 1;
        let total = self.total.clone();
        let count = self.count.clone();
        UndoFunction::new(move || {
            *total.borrow_mut() -= value;
            *count.borrow_mut() -= 1;
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(*self.total.borrow())
    }

    fn is_empty(&self) -> bool {
        *self.count.borrow() == 0
    }
}

pub struct AvgCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    mapping_function: F,
    total: Rc<RefCell<f64>>,
    count: Rc<RefCell<usize>>,
}

impl<F> BaseCollector for AvgCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let value = (self.mapping_function)(item);
        *self.total.borrow_mut() += value;
        *self.count.borrow_mut() += 1;
        let total = self.total.clone();
        let count = self.count.clone();
        UndoFunction::new(move || {
            *total.borrow_mut() -= value;
            *count.borrow_mut() -= 1;
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let total = *self.total.borrow();
        let count = *self.count.borrow();
        if count == 0 {
            Rc::new(0.0)
        } else {
            Rc::new(total / count as f64)
        }
    }

    fn is_empty(&self) -> bool {
        *self.count.borrow() == 0
    }
}

#[derive(Default)]
pub struct ListCollector {
    items: Rc<RefCell<Vec<AnyTuple>>>,
}

impl BaseCollector for ListCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let owned_item = item.clone();
        self.items.borrow_mut().push(owned_item.clone());
        let items = self.items.clone();
        UndoFunction::new(move || {
            let mut items_borrowed = items.borrow_mut();
            if let Some(pos) = items_borrowed.iter().position(|x| {
                if x.arity() != owned_item.arity() { return false; }
                let x_facts = x.facts();
                let item_facts = owned_item.facts();
                x_facts.iter().zip(item_facts.iter()).all(|(a, b)| a.eq_fact(&**b))
            }) {
                items_borrowed.remove(pos);
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(self.items.borrow().clone())
    }

    fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }
}

pub struct MinCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    mapping_function: F,
    counts: Rc<RefCell<BTreeMap<K, usize>>>,
}

impl<K, F> BaseCollector for MinCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let key = (self.mapping_function)(item);
        *self.counts.borrow_mut().entry(key.clone()).or_insert(0) += 1;
        let counts = self.counts.clone();
        UndoFunction::new(move || {
            let mut counts_borrowed = counts.borrow_mut();
            if let Some(count) = counts_borrowed.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    counts_borrowed.remove(&key);
                }
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let counts = self.counts.borrow();
        if let Some(min_key) = counts.keys().next() {
            Rc::new(min_key.clone())
        } else {
            panic!("Collector result called on empty group")
        }
    }

    fn is_empty(&self) -> bool {
        self.counts.borrow().is_empty()
    }
}

pub struct MaxCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    mapping_function: F,
    counts: Rc<RefCell<BTreeMap<K, usize>>>,
}

impl<K, F> BaseCollector for MaxCollector<K, F>
where
    K: Ord + Clone + GreynetFact,
    F: Fn(&AnyTuple) -> K + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let key = (self.mapping_function)(item);
        *self.counts.borrow_mut().entry(key.clone()).or_insert(0) += 1;
        let counts = self.counts.clone();
        UndoFunction::new(move || {
            let mut counts_borrowed = counts.borrow_mut();
            if let Some(count) = counts_borrowed.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    counts_borrowed.remove(&key);
                }
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let counts = self.counts.borrow();
        if let Some(max_key) = counts.keys().last() {
            Rc::new(max_key.clone())
        } else {
            panic!("Collector result called on empty group")
        }
    }

    fn is_empty(&self) -> bool {
        self.counts.borrow().is_empty()
    }
}

// FilteringCollector: Filters items based on a predicate before passing them to a downstream collector
pub struct FilteringCollector<P>
where
    P: Fn(&AnyTuple) -> bool + 'static,
{
    predicate: P,
    downstream: Box<dyn BaseCollector>,
    count: Rc<RefCell<usize>>,
}

impl<P> BaseCollector for FilteringCollector<P>
where
    P: Fn(&AnyTuple) -> bool + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        *self.count.borrow_mut() += 1;
        let count = self.count.clone();
        
        if (self.predicate)(item) {
            let downstream_undo = self.downstream.insert(item);
            UndoFunction::new(move || {
                downstream_undo.execute();
                *count.borrow_mut() -= 1;
            })
        } else {
            UndoFunction::new(move || {
                *count.borrow_mut() -= 1;
            })
        }
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        self.downstream.result_as_fact()
    }

    fn is_empty(&self) -> bool {
        *self.count.borrow() == 0
    }
}

// SetCollector: Aggregates items into a standard set
#[derive(Default)]
pub struct SetCollector {
    items: Rc<RefCell<HashSet<String>>>, // Using String hash for simplicity
    tuple_hashes: Rc<RefCell<Vec<String>>>, // Track insertion order for undo
}

impl BaseCollector for SetCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        // Create a deterministic hash for the tuple
        let item_hash = Self::hash_tuple(item);
        self.items.borrow_mut().insert(item_hash.clone());
        self.tuple_hashes.borrow_mut().push(item_hash.clone());
        
        let items = self.items.clone();
        let tuple_hashes = self.tuple_hashes.clone();
        
        UndoFunction::new(move || {
            let mut hashes = tuple_hashes.borrow_mut();
            if let Some(pos) = hashes.iter().rposition(|h| h == &item_hash) {
                hashes.remove(pos);
                
                // Only remove from set if this was the last occurrence
                if !hashes.contains(&item_hash) {
                    items.borrow_mut().remove(&item_hash);
                }
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let set_size = self.items.borrow().len();
        Rc::new(set_size)
    }

    fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }
}

impl SetCollector {
    fn hash_tuple(tuple: &AnyTuple) -> String {
        let facts = tuple.facts();
        let mut hash_parts = Vec::new();
        for fact in facts {
            hash_parts.push(fact.hash_fact().to_string());
        }
        hash_parts.join(":")
    }
}

// DistinctCollector: Aggregates unique items into a vec, preserving insertion order
#[derive(Default)]
pub struct DistinctCollector {
    items: Rc<RefCell<Vec<AnyTuple>>>,
    seen: Rc<RefCell<HashSet<String>>>,
    counts: Rc<RefCell<std::collections::HashMap<String, usize>>>,
}

impl BaseCollector for DistinctCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let item_hash = SetCollector::hash_tuple(item);
        let mut seen = self.seen.borrow_mut();
        let mut counts = self.counts.borrow_mut();
        let mut items = self.items.borrow_mut();
        
        let is_new = !seen.contains(&item_hash);
        if is_new {
            seen.insert(item_hash.clone());
            items.push(item.clone());
        }
        
        *counts.entry(item_hash.clone()).or_insert(0) += 1;
        
        let seen_ref = self.seen.clone();
        let counts_ref = self.counts.clone();
        let items_ref = self.items.clone();
        
        UndoFunction::new(move || {
            let mut counts_borrowed = counts_ref.borrow_mut();
            if let Some(count) = counts_borrowed.get_mut(&item_hash) {
                *count -= 1;
                if *count == 0 {
                    counts_borrowed.remove(&item_hash);
                    seen_ref.borrow_mut().remove(&item_hash);
                    
                    // Remove from items vec
                    let mut items_borrowed = items_ref.borrow_mut();
                    if let Some(pos) = items_borrowed.iter().position(|t| {
                        SetCollector::hash_tuple(t) == item_hash
                    }) {
                        items_borrowed.remove(pos);
                    }
                }
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(self.items.borrow().clone())
    }

    fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }
}

// CompositeCollector: Allows multiple aggregations to be performed on the same group simultaneously
pub struct CompositeCollector {
    collectors: Vec<Box<dyn BaseCollector>>,
    results: Rc<RefCell<Vec<Rc<dyn GreynetFact>>>>,
}

impl CompositeCollector {
    pub fn new(collectors: Vec<Box<dyn BaseCollector>>) -> Self {
        Self {
            collectors,
            results: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

impl BaseCollector for CompositeCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let mut undo_functions = Vec::new();
        
        for collector in &mut self.collectors {
            undo_functions.push(collector.insert(item));
        }
        
        UndoFunction::new(move || {
            for undo_fn in undo_functions {
                undo_fn.execute();
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let results: Vec<Rc<dyn GreynetFact>> = self.collectors
            .iter()
            .map(|c| c.result_as_fact())
            .collect();
        Rc::new(results)
    }

    fn is_empty(&self) -> bool {
        self.collectors.iter().all(|c| c.is_empty())
    }
}

// MappingCollector: Applies a mapping function to each item before passing it to a downstream collector
pub struct MappingCollector<F>
where
    F: Fn(&AnyTuple) -> AnyTuple + 'static,
{
    mapper: F,
    downstream: Box<dyn BaseCollector>,
    count: Rc<RefCell<usize>>,
}

impl<F> BaseCollector for MappingCollector<F>
where
    F: Fn(&AnyTuple) -> AnyTuple + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let mapped_item = (self.mapper)(item);
        *self.count.borrow_mut() += 1;
        let count = self.count.clone();
        
        let downstream_undo = self.downstream.insert(&mapped_item);
        
        UndoFunction::new(move || {
            downstream_undo.execute();
            *count.borrow_mut() -= 1;
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        self.downstream.result_as_fact()
    }

    fn is_empty(&self) -> bool {
        *self.count.borrow() == 0
    }
}

// StdDevCollector: Calculates the standard deviation of a group
pub struct StdDevCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    mapping_function: F,
    sum: Rc<RefCell<f64>>,
    sum_of_squares: Rc<RefCell<f64>>,
    count: Rc<RefCell<usize>>,
}

impl<F> BaseCollector for StdDevCollector<F>
where
    F: Fn(&AnyTuple) -> f64 + 'static,
{
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let value = (self.mapping_function)(item);
        let value_squared = value * value;
        
        *self.sum.borrow_mut() += value;
        *self.sum_of_squares.borrow_mut() += value_squared;
        *self.count.borrow_mut() += 1;
        
        let sum = self.sum.clone();
        let sum_of_squares = self.sum_of_squares.clone();
        let count = self.count.clone();
        
        UndoFunction::new(move || {
            *sum.borrow_mut() -= value;
            *sum_of_squares.borrow_mut() -= value_squared;
            *count.borrow_mut() -= 1;
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let count = *self.count.borrow();
        if count == 0 {
            return Rc::new(0.0);
        }
        
        let sum = *self.sum.borrow();
        let sum_of_squares = *self.sum_of_squares.borrow();
        let n = count as f64;
        
        let mean = sum / n;
        let variance = (sum_of_squares / n) - (mean * mean);
        let std_dev = variance.sqrt();
        
        Rc::new(std_dev)
    }

    fn is_empty(&self) -> bool {
        *self.count.borrow() == 0
    }
}

// Add these factory methods to the Collectors impl block:
impl Collectors {
    pub fn filtering<P>(
        predicate: P,
        downstream_supplier: Box<dyn Fn() -> Box<dyn BaseCollector>>,
    ) -> Box<dyn Fn() -> Box<dyn BaseCollector>>
    where
        P: Fn(&AnyTuple) -> bool + Clone + 'static,
    {
        Box::new(move || {
            Box::new(FilteringCollector {
                predicate: predicate.clone(),
                downstream: downstream_supplier(),
                count: Rc::new(RefCell::new(0)),
            })
        })
    }

    pub fn to_set() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(SetCollector::default()))
    }

    pub fn distinct() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(DistinctCollector::default()))
    }

    pub fn composite(
        suppliers: Vec<Box<dyn Fn() -> Box<dyn BaseCollector>>>,
    ) -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(move || {
            let collectors: Vec<Box<dyn BaseCollector>> = suppliers
                .iter()
                .map(|supplier| supplier())
                .collect();
            Box::new(CompositeCollector::new(collectors))
        })
    }

    pub fn mapping<F>(
        mapper: F,
        downstream_supplier: Box<dyn Fn() -> Box<dyn BaseCollector>>,
    ) -> Box<dyn Fn() -> Box<dyn BaseCollector>>
    where
        F: Fn(&AnyTuple) -> AnyTuple + Clone + 'static,
    {
        Box::new(move || {
            Box::new(MappingCollector {
                mapper: mapper.clone(),
                downstream: downstream_supplier(),
                count: Rc::new(RefCell::new(0)),
            })
        })
    }

    pub fn std_dev<F>(mapping_function: F) -> Box<dyn Fn() -> Box<dyn BaseCollector>>
    where
        F: Fn(&AnyTuple) -> f64 + Clone + 'static,
    {
        Box::new(move || {
            Box::new(StdDevCollector {
                mapping_function: mapping_function.clone(),
                sum: Rc::new(RefCell::new(0.0)),
                sum_of_squares: Rc::new(RefCell::new(0.0)),
                count: Rc::new(RefCell::new(0)),
            })
        })
    }
}

// Add GreynetFact implementation for Vec<Rc<dyn GreynetFact>> (needed for CompositeCollector)
impl GreynetFact for Vec<Rc<dyn GreynetFact>> {
    fn fact_id(&self) -> Uuid {
        let namespace = Uuid::parse_str("d9e2f3a4-c5b6-a7d8-e9f0-a1b2c3d4e5f6").unwrap();
        let mut hasher = DefaultHasher::new();
        for fact in self {
            fact.hash_fact().hash(&mut hasher);
        }
        Uuid::new_v5(&namespace, &hasher.finish().to_be_bytes())
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        if let Some(other_vec) = other.as_any().downcast_ref::<Vec<Rc<dyn GreynetFact>>>() {
            if self.len() != other_vec.len() {
                return false;
            }
            self.iter().zip(other_vec.iter()).all(|(f1, f2)| f1.eq_fact(&**f2))
        } else {
            false
        }
    }

    fn hash_fact(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for fact in self {
            fact.hash_fact().hash(&mut hasher);
        }
        hasher.finish()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A factory for creating collector suppliers.
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
                total: Rc::new(RefCell::new(0.0)),
                count: Rc::new(RefCell::new(0)),
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
                total: Rc::new(RefCell::new(0.0)),
                count: Rc::new(RefCell::new(0)),
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
                counts: Rc::new(RefCell::new(BTreeMap::new())),
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
                counts: Rc::new(RefCell::new(BTreeMap::new())),
            })
        })
    }
}