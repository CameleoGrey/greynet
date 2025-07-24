//collectors.rs
use crate::tuple::FactIterator;
use crate::{AnyTuple, GreynetFact};
use rustc_hash::FxHashMap as HashMap;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

pub trait BaseCollector: std::fmt::Debug {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction;
    fn result_as_fact(&self) -> Rc<dyn GreynetFact>;
    fn is_empty(&self) -> bool;
}

/// High-performance enum dispatch for common collectors
#[derive(Debug)]
pub enum FastCollector {
    Count(CountCollector),
    Sum(SumCollector<fn(&AnyTuple) -> f64>),
    List(ListCollector),
    Custom(Box<dyn BaseCollector>),
}

impl BaseCollector for FastCollector {
    #[inline]
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        match self {
            FastCollector::Count(c) => c.insert(item),
            FastCollector::Sum(c) => c.insert(item),
            FastCollector::List(c) => c.insert(item),
            FastCollector::Custom(c) => c.insert(item),
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

pub struct UndoFunction {
    undo_fn: Box<dyn FnOnce()>,
}

impl UndoFunction {
    #[inline]
    pub fn new<F: FnOnce() + 'static>(f: F) -> Self {
        Self {
            undo_fn: Box::new(f),
        }
    }

    #[inline]
    pub fn execute(self) {
        (self.undo_fn)();
    }
}

#[derive(Default, Debug)]
pub struct CountCollector {
    count: Rc<RefCell<usize>>,
}

impl BaseCollector for CountCollector {
    #[inline]
    fn insert(&mut self, _item: &AnyTuple) -> UndoFunction {
        *self.count.borrow_mut() += 1;
        let count = self.count.clone();
        UndoFunction::new(move || {
            *count.borrow_mut() -= 1;
        })
    }

    #[inline]
    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(*self.count.borrow())
    }

    #[inline]
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

    #[inline]
    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(*self.total.borrow())
    }

    #[inline]
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

// OPTIMIZATION (Guide 5.1): Update ListCollector to remove atomic dependency
// and use a simpler wrapping counter.
#[derive(Default, Debug)]
pub struct ListCollector {
    items: Rc<RefCell<Vec<(u64, AnyTuple)>>>,
    next_id: Rc<RefCell<u64>>,
}

impl BaseCollector for ListCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let mut next_id = self.next_id.borrow_mut();
        let item_id = *next_id;
        *next_id = next_id.wrapping_add(1);
        drop(next_id);

        self.items.borrow_mut().push((item_id, item.clone()));

        let items = self.items.clone();
        UndoFunction::new(move || {
            items.borrow_mut().retain(|(id, _)| *id != item_id);
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let items = self.items.borrow();
        let tuples: Vec<AnyTuple> = items.iter().map(|(_, tuple)| tuple.clone()).collect();
        Rc::new(tuples)
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

#[derive(Default)]
pub struct SetCollector {
    items: Rc<RefCell<HashSet<u64>>>,
    tuple_hashes: Rc<RefCell<HashMap<u64, usize>>>,
}

impl std::fmt::Debug for SetCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetCollector")
            .field("items", &self.items)
            .field("tuple_hashes", &self.tuple_hashes)
            .finish()
    }
}

impl BaseCollector for SetCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let item_hash = Self::hash_tuple(item);
        self.items.borrow_mut().insert(item_hash);
        *self.tuple_hashes.borrow_mut().entry(item_hash).or_insert(0) += 1;
        let items_ref = self.items.clone();
        let hashes_ref = self.tuple_hashes.clone();
        UndoFunction::new(move || {
            let mut hashes_borrowed = hashes_ref.borrow_mut();
            if let Some(count) = hashes_borrowed.get_mut(&item_hash) {
                *count -= 1;
                if *count == 0 {
                    hashes_borrowed.remove(&item_hash);
                    items_ref.borrow_mut().remove(&item_hash);
                }
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        Rc::new(self.items.borrow().len())
    }

    fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
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

#[derive(Default)]
pub struct DistinctCollector {
    items: Rc<RefCell<HashMap<u64, AnyTuple>>>,
    insertion_order: Rc<RefCell<Vec<u64>>>,
    counts: Rc<RefCell<HashMap<u64, usize>>>,
}

impl std::fmt::Debug for DistinctCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistinctCollector")
            .field("items", &self.items)
            .field("insertion_order", &self.insertion_order)
            .field("counts", &self.counts)
            .finish()
    }
}

impl BaseCollector for DistinctCollector {
    fn insert(&mut self, item: &AnyTuple) -> UndoFunction {
        let item_hash = SetCollector::hash_tuple(item);
        let mut items = self.items.borrow_mut();
        if !items.contains_key(&item_hash) {
            items.insert(item_hash, item.clone());
            self.insertion_order.borrow_mut().push(item_hash);
        }
        *self.counts.borrow_mut().entry(item_hash).or_insert(0) += 1;
        let items_ref = self.items.clone();
        let order_ref = self.insertion_order.clone();
        let counts_ref = self.counts.clone();
        UndoFunction::new(move || {
            let mut counts_borrowed = counts_ref.borrow_mut();
            if let Some(count) = counts_borrowed.get_mut(&item_hash) {
                *count -= 1;
                if *count == 0 {
                    counts_borrowed.remove(&item_hash);
                    items_ref.borrow_mut().remove(&item_hash);
                    order_ref.borrow_mut().retain(|h| h != &item_hash);
                }
            }
        })
    }

    fn result_as_fact(&self) -> Rc<dyn GreynetFact> {
        let items = self.items.borrow();
        let order = self.insertion_order.borrow();
        let tuples: Vec<AnyTuple> = order
            .iter()
            .filter_map(|hash| items.get(hash).cloned())
            .collect();
        Rc::new(tuples)
    }

    fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
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

    pub fn to_set() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(SetCollector::default()))
    }

    pub fn distinct() -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(|| Box::new(DistinctCollector::default()))
    }

    // OPTIMIZATION (Guide 5.2): Add factories that pre-size collectors.
    pub fn list_with_capacity(capacity: usize) -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(move || {
            Box::new(ListCollector {
                items: Rc::new(RefCell::new(Vec::with_capacity(capacity))),
                next_id: Rc::new(RefCell::new(0)),
            })
        })
    }

    pub fn set_with_capacity(capacity: usize) -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(move || {
            Box::new(SetCollector {
                items: Rc::new(RefCell::new(HashSet::with_capacity(capacity))),
                // FIX: Use with_capacity_and_hasher for FxHashMap
                tuple_hashes: Rc::new(RefCell::new(HashMap::with_capacity_and_hasher(
                    capacity,
                    Default::default(),
                ))),
            })
        })
    }

    pub fn distinct_with_capacity(capacity: usize) -> Box<dyn Fn() -> Box<dyn BaseCollector>> {
        Box::new(move || {
            Box::new(DistinctCollector {
                // FIX: Use with_capacity_and_hasher for FxHashMap
                items: Rc::new(RefCell::new(HashMap::with_capacity_and_hasher(
                    capacity,
                    Default::default(),
                ))),
                insertion_order: Rc::new(RefCell::new(Vec::with_capacity(capacity))),
                // FIX: Use with_capacity_and_hasher for FxHashMap
                counts: Rc::new(RefCell::new(HashMap::with_capacity_and_hasher(
                    capacity,
                    Default::default(),
                ))),
            })
        })
    }
}
