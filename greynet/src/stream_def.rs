//stream_def.rs
use super::factory::ConstraintFactory;
use super::joiner::JoinerType;
use super::nodes::{SharedKeyFn, SharedPredicate, SharedImpactFn, SharedMapperFn};
use super::collectors::BaseCollector;
use crate::{GreynetFact, Score, AnyTuple};
use std::any::TypeId;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct Arity1;
#[derive(Clone)]
pub struct Arity2;
#[derive(Clone)]
pub struct Arity3;
#[derive(Clone)]
pub struct Arity4;
#[derive(Clone)]
pub struct Arity5;

#[derive(Clone)]
pub struct ConstraintRecipe<S: Score> {
    pub stream_def: StreamDefinition<S>,
    pub penalty_function: SharedImpactFn<S>,
    pub constraint_id: String,
}

// IMPROVED: Replace unreliable function pointer comparison with unique IDs
static NEXT_COLLECTOR_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct CollectorSupplier {
    id: usize,
    supplier: Rc<dyn Fn() -> Box<dyn BaseCollector>>,
}

impl CollectorSupplier {
    pub fn new(supplier: impl Fn() -> Box<dyn BaseCollector> + 'static) -> Self {
        Self {
            id: NEXT_COLLECTOR_ID.fetch_add(1, Ordering::Relaxed),
            supplier: Rc::new(supplier),
        }
    }
    
    #[inline]
    pub fn create(&self) -> Box<dyn BaseCollector> {
        (self.supplier)()
    }
    
    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }
}

impl PartialEq for CollectorSupplier {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl std::hash::Hash for CollectorSupplier {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl std::fmt::Debug for CollectorSupplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectorSupplier")
            .field("id", &self.id)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum RetrievalId<S: Score> {
    From(TypeId, std::marker::PhantomData<S>),
    Filter {
        source: Box<RetrievalId<S>>,
        predicate_id: usize,
    },
    Join {
        left: Box<RetrievalId<S>>,
        right: Box<RetrievalId<S>>,
        joiner: JoinerType,
        left_key_id: usize,
        right_key_id: usize,
    },
    ConditionalJoin {
        source: Box<RetrievalId<S>>,
        other: Box<RetrievalId<S>>,
        should_exist: bool,
        left_key_id: usize,
        right_key_id: usize,
    },
    Group {
        source: Box<RetrievalId<S>>,
        key_fn_id: usize,
        collector_id: usize, // FIXED: Use reliable ID instead of pointer
    },
    FlatMap {
        source: Box<RetrievalId<S>>,
        mapper_fn_id: usize,
    },
}

impl<S: Score> std::hash::Hash for RetrievalId<S> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            RetrievalId::From(type_id, _) => {
                type_id.hash(state);
            }
            RetrievalId::Filter { source, predicate_id } => {
                source.hash(state);
                predicate_id.hash(state);
            }
            RetrievalId::Join { left, right, joiner, left_key_id, right_key_id } => {
                left.hash(state);
                right.hash(state);
                joiner.hash(state);
                left_key_id.hash(state);
                right_key_id.hash(state);
            }
            RetrievalId::ConditionalJoin { source, other, should_exist, left_key_id, right_key_id } => {
                source.hash(state);
                other.hash(state);
                should_exist.hash(state);
                left_key_id.hash(state);
                right_key_id.hash(state);
            }
            RetrievalId::Group { source, key_fn_id, collector_id } => {
                source.hash(state);
                key_fn_id.hash(state);
                collector_id.hash(state); // FIXED: Use ID instead of pointer
            }
            RetrievalId::FlatMap { source, mapper_fn_id } => {
                source.hash(state);
                mapper_fn_id.hash(state);
            }
        }
    }
}

impl<S: Score> PartialEq for RetrievalId<S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RetrievalId::From(a, _), RetrievalId::From(b, _)) => a == b,
            (RetrievalId::Filter { source: a1, predicate_id: a2 }, 
             RetrievalId::Filter { source: b1, predicate_id: b2 }) => a1 == b1 && a2 == b2,
            (RetrievalId::Join { left: a1, right: a2, joiner: a3, left_key_id: a4, right_key_id: a5 },
             RetrievalId::Join { left: b1, right: b2, joiner: b3, left_key_id: b4, right_key_id: b5 }) => {
                a1 == b1 && a2 == b2 && a3 == b3 && a4 == b4 && a5 == b5
            }
            (RetrievalId::ConditionalJoin { source: a1, other: a2, should_exist: a3, left_key_id: a4, right_key_id: a5 },
             RetrievalId::ConditionalJoin { source: b1, other: b2, should_exist: b3, left_key_id: b4, right_key_id: b5 }) => {
                a1 == b1 && a2 == b2 && a3 == b3 && a4 == b4 && a5 == b5
            }
            (RetrievalId::Group { source: a1, key_fn_id: a2, collector_id: a3 },
             RetrievalId::Group { source: b1, key_fn_id: b2, collector_id: b3 }) => {
                a1 == b1 && a2 == b2 && a3 == b3 // FIXED: Use ID comparison
            }
            (RetrievalId::FlatMap { source: a1, mapper_fn_id: a2 },
             RetrievalId::FlatMap { source: b1, mapper_fn_id: b2 }) => a1 == b1 && a2 == b2,
            _ => false,
        }
    }
}

impl<S: Score> Eq for RetrievalId<S> {}

unsafe impl<S: Score> Send for RetrievalId<S> {}
unsafe impl<S: Score> Sync for RetrievalId<S> {}

#[derive(Clone)]
pub enum StreamDefinition<S: Score> {
    From(FromDefinition),
    Filter(FilterDefinition<S>),
    Join(JoinDefinition<S>),
    ConditionalJoin(ConditionalJoinDefinition<S>),
    Group(GroupDefinition<S>),
    FlatMap(FlatMapDefinition<S>),
}

impl<S: Score> StreamDefinition<S> {
    pub fn get_retrieval_id(&self) -> RetrievalId<S> {
        match self {
            StreamDefinition::From(def) => RetrievalId::From(def.fact_type, std::marker::PhantomData),
            StreamDefinition::Filter(def) => RetrievalId::Filter {
                source: Box::new(def.source.get_retrieval_id()),
                predicate_id: def.predicate_id,
            },
            StreamDefinition::Join(def) => RetrievalId::Join {
                left: Box::new(def.left_source.get_retrieval_id()),
                right: Box::new(def.right_source.get_retrieval_id()),
                joiner: def.joiner_type,
                left_key_id: def.left_key_fn_id,
                right_key_id: def.right_key_fn_id,
            },
            StreamDefinition::ConditionalJoin(def) => RetrievalId::ConditionalJoin {
                source: Box::new(def.source.get_retrieval_id()),
                other: Box::new(def.other.get_retrieval_id()),
                should_exist: def.should_exist,
                left_key_id: def.left_key_fn_id,
                right_key_id: def.right_key_fn_id,
            },
            StreamDefinition::Group(def) => RetrievalId::Group {
                source: Box::new(def.source.get_retrieval_id()),
                key_fn_id: def.key_fn_id,
                collector_id: def.collector_supplier.id(), // FIXED: Use ID
            },
            StreamDefinition::FlatMap(def) => RetrievalId::FlatMap {
                source: Box::new(def.source.get_retrieval_id()),
                mapper_fn_id: def.mapper_fn_id,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct FromDefinition {
    pub fact_type: TypeId,
    pub fact_type_name: &'static str,
}

impl FromDefinition {
    pub fn new<T: GreynetFact>() -> Self {
        Self {
            fact_type: TypeId::of::<T>(),
            fact_type_name: std::any::type_name::<T>(),
        }
    }
}

#[derive(Clone)]
pub struct FilterDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub predicate_id: usize,
    _phantom: PhantomData<S>,
}

#[derive(Clone)]
pub struct JoinDefinition<S: Score> {
    pub left_source: Box<StreamDefinition<S>>,
    pub right_source: Box<StreamDefinition<S>>,
    pub joiner_type: JoinerType,
    pub left_key_fn_id: usize,
    pub right_key_fn_id: usize,
    _phantom: PhantomData<S>,
}

#[derive(Clone)]
pub struct ConditionalJoinDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub other: Box<StreamDefinition<S>>,
    pub should_exist: bool,
    pub left_key_fn_id: usize,
    pub right_key_fn_id: usize,
    _phantom: PhantomData<S>,
}

#[derive(Clone)]
pub struct GroupDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub key_fn_id: usize,
    pub collector_supplier: CollectorSupplier, // IMPROVED: Use reliable supplier
    _phantom: PhantomData<S>,
}

#[derive(Clone)]
pub struct FlatMapDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub mapper_fn_id: usize,
    _phantom: PhantomData<S>,
}

/// High-level stream API for building constraint pipelines
#[derive(Clone)]
pub struct Stream<A, S: Score> {
    pub(crate) definition: StreamDefinition<S>,
    pub(crate) factory: Weak<RefCell<ConstraintFactory<S>>>,
    _arity: PhantomData<A>,
    _score: PhantomData<S>,
}

impl<A, S: Score> Stream<A, S> {
    pub fn new(definition: StreamDefinition<S>, factory: Weak<RefCell<ConstraintFactory<S>>>) -> Self {
        Self {
            definition,
            factory,
            _arity: PhantomData,
            _score: PhantomData,
        }
    }

    pub fn penalize(self, constraint_id: &str, penalty_function: impl Fn(&AnyTuple) -> S + 'static) -> ConstraintRecipe<S> {
        ConstraintRecipe {
            stream_def: self.definition,
            penalty_function: Rc::new(penalty_function),
            constraint_id: constraint_id.to_string(),
        }
    }

    pub fn filter(self, predicate: SharedPredicate) -> Self {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let predicate_id = factory_rc.borrow_mut().register_predicate(predicate);
        let filter_def = FilterDefinition {
            source: Box::new(self.definition),
            predicate_id,
            _phantom: PhantomData,
        };
        Stream::new(StreamDefinition::Filter(filter_def), self.factory)
    }

    fn join_with<B, C>(self, other: Stream<B, S>, joiner_type: JoinerType, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<C, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        let left_key_fn_id = factory.register_key_fn(left_key_fn);
        let right_key_fn_id = factory.register_key_fn(right_key_fn);
        let join_def = JoinDefinition {
            left_source: Box::new(self.definition),
            right_source: Box::new(other.definition),
            joiner_type,
            left_key_fn_id,
            right_key_fn_id,
            _phantom: PhantomData,
        };
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    fn if_conditionally<B>(self, other: Stream<B, S>, should_exist: bool, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<A, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        let left_key_fn_id = factory.register_key_fn(left_key_fn);
        let right_key_fn_id = factory.register_key_fn(right_key_fn);
        let cond_def = ConditionalJoinDefinition {
            source: Box::new(self.definition),
            other: Box::new(other.definition),
            should_exist,
            left_key_fn_id,
            right_key_fn_id,
            _phantom: PhantomData,
        };
        Stream::new(StreamDefinition::ConditionalJoin(cond_def), self.factory)
    }
}

impl<S: Score> Stream<Arity1, S> {
    pub fn join(self, other: Stream<Arity1, S>, joiner_type: JoinerType, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity2, S> {
        self.join_with(other, joiner_type, left_key_fn, right_key_fn)
    }

    pub fn if_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity1, S> {
        self.if_conditionally(other, true, left_key_fn, right_key_fn)
    }

    pub fn if_not_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity1, S> {
        self.if_conditionally(other, false, left_key_fn, right_key_fn)
    }

    pub fn group_by(self, key_fn: SharedKeyFn, collector_supplier: Box<dyn Fn() -> Box<dyn BaseCollector>>) -> Stream<Arity2, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let key_fn_id = factory_rc.borrow_mut().register_key_fn(key_fn);
        let group_def = GroupDefinition {
            source: Box::new(self.definition),
            key_fn_id,
            collector_supplier: CollectorSupplier::new(collector_supplier),
            _phantom: PhantomData,
        };
        Stream::new(StreamDefinition::Group(group_def), self.factory)
    }

    pub fn flat_map(self, mapper_fn: SharedMapperFn) -> Stream<Arity1, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mapper_fn_id = factory_rc.borrow_mut().register_mapper(mapper_fn);
        let flatmap_def = FlatMapDefinition {
            source: Box::new(self.definition),
            mapper_fn_id,
            _phantom: PhantomData,
        };
        Stream::new(StreamDefinition::FlatMap(flatmap_def), self.factory)
    }

    pub fn map(self, mapper_fn: Rc<dyn Fn(&AnyTuple) -> Rc<dyn GreynetFact>>) -> Stream<Arity1, S> {
        let wrapped_mapper: SharedMapperFn = Rc::new(move |tuple: &AnyTuple| {
            vec![mapper_fn(tuple)]
        });
        self.flat_map(wrapped_mapper)
    }

    pub fn unique_pairs(self) -> Stream<Arity2, S>
    where
        S: 'static,
    {
        let other = Stream::new(self.definition.clone(), self.factory.clone());
        self.join(
            other,
            JoinerType::LessThan,
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    uni_tuple.fact_a.hash_fact()
                } else {
                    0
                }
            }),
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    uni_tuple.fact_a.hash_fact()
                } else {
                    0
                }
            }),
        )
    }
}

impl<S: Score> Stream<Arity2, S> {
    pub fn join(self, other: Stream<Arity1, S>, joiner_type: JoinerType, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity3, S> {
        self.join_with(other, joiner_type, left_key_fn, right_key_fn)
    }

    pub fn if_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity2, S> {
        self.if_conditionally(other, true, left_key_fn, right_key_fn)
    }

    pub fn if_not_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity2, S> {
        self.if_conditionally(other, false, left_key_fn, right_key_fn)
    }
}

impl<S: Score> Stream<Arity3, S> {
    pub fn join(self, other: Stream<Arity1, S>, joiner_type: JoinerType, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity4, S> {
        self.join_with(other, joiner_type, left_key_fn, right_key_fn)
    }

    pub fn if_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity3, S> {
        self.if_conditionally(other, true, left_key_fn, right_key_fn)
    }

    pub fn if_not_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity3, S> {
        self.if_conditionally(other, false, left_key_fn, right_key_fn)
    }
}

impl<S: Score> Stream<Arity4, S> {
    pub fn join(self, other: Stream<Arity1, S>, joiner_type: JoinerType, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity5, S> {
        self.join_with(other, joiner_type, left_key_fn, right_key_fn)
    }

    pub fn if_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity4, S> {
        self.if_conditionally(other, true, left_key_fn, right_key_fn)
    }

    pub fn if_not_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity4, S> {
        self.if_conditionally(other, false, left_key_fn, right_key_fn)
    }
}

impl<S: Score> Stream<Arity5, S> {
    pub fn if_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity5, S> {
        self.if_conditionally(other, true, left_key_fn, right_key_fn)
    }

    pub fn if_not_exists(self, other: Stream<Arity1, S>, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<Arity5, S> {
        self.if_conditionally(other, false, left_key_fn, right_key_fn)
    }
}