//! Fixed stream_def.rs with proper RetrievalId implementation and zero-copy integration

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

/// FIXED: Enhanced FunctionId with proper Hash and PartialEq implementations
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum FunctionId {
    Traditional(usize),
    ZeroCopy(usize),
}

impl FunctionId {
    /// Get the underlying ID regardless of function type
    #[inline]
    pub fn id(&self) -> usize {
        match self {
            FunctionId::Traditional(id) | FunctionId::ZeroCopy(id) => *id,
        }
    }
    
    /// Check if this is a zero-copy function
    #[inline]
    pub fn is_zero_copy(&self) -> bool {
        matches!(self, FunctionId::ZeroCopy(_))
    }
    
    /// Check if this is a traditional function
    #[inline]
    pub fn is_traditional(&self) -> bool {
        matches!(self, FunctionId::Traditional(_))
    }
}

#[derive(Clone, Debug)]  // Remove Hash, PartialEq, Eq
pub enum RetrievalId<S: Score> {
    From(TypeId, std::marker::PhantomData<S>),
    Filter {
        source: Box<RetrievalId<S>>,
        predicate_id: FunctionId,
    },
    Join {
        left: Box<RetrievalId<S>>,
        right: Box<RetrievalId<S>>,
        joiner: JoinerType,
        left_key_id: FunctionId,
        right_key_id: FunctionId,
    },
    ConditionalJoin {
        source: Box<RetrievalId<S>>,
        other: Box<RetrievalId<S>>,
        should_exist: bool,
        left_key_id: FunctionId,
        right_key_id: FunctionId,
    },
    Group {
        source: Box<RetrievalId<S>>,
        key_fn_id: FunctionId,
        collector_id: usize,
    },
    FlatMap {
        source: Box<RetrievalId<S>>,
        mapper_fn_id: usize,
    },
}

// Add manual implementations that ignore PhantomData
impl<S: Score> PartialEq for RetrievalId<S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RetrievalId::From(a, _), RetrievalId::From(b, _)) => a == b,
            (RetrievalId::Filter { source: a, predicate_id: b }, 
             RetrievalId::Filter { source: c, predicate_id: d }) => a == c && b == d,
            (RetrievalId::Join { left: a, right: b, joiner: c, left_key_id: d, right_key_id: e },
             RetrievalId::Join { left: f, right: g, joiner: h, left_key_id: i, right_key_id: j }) => {
                a == f && b == g && c == h && d == i && e == j
            },
            (RetrievalId::ConditionalJoin { source: a, other: b, should_exist: c, left_key_id: d, right_key_id: e },
             RetrievalId::ConditionalJoin { source: f, other: g, should_exist: h, left_key_id: i, right_key_id: j }) => {
                a == f && b == g && c == h && d == i && e == j
            },
            (RetrievalId::Group { source: a, key_fn_id: b, collector_id: c },
             RetrievalId::Group { source: d, key_fn_id: e, collector_id: f }) => {
                a == d && b == e && c == f
            },
            (RetrievalId::FlatMap { source: a, mapper_fn_id: b },
             RetrievalId::FlatMap { source: c, mapper_fn_id: d }) => {
                a == c && b == d
            },
            _ => false,
        }
    }
}

impl<S: Score> Eq for RetrievalId<S> {}

impl<S: Score> std::hash::Hash for RetrievalId<S> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            RetrievalId::From(type_id, _) => {
                0u8.hash(state);
                type_id.hash(state);
            },
            RetrievalId::Filter { source, predicate_id } => {
                1u8.hash(state);
                source.hash(state);
                predicate_id.hash(state);
            },
            RetrievalId::Join { left, right, joiner, left_key_id, right_key_id } => {
                2u8.hash(state);
                left.hash(state);
                right.hash(state);
                joiner.hash(state);
                left_key_id.hash(state);
                right_key_id.hash(state);
            },
            RetrievalId::ConditionalJoin { source, other, should_exist, left_key_id, right_key_id } => {
                3u8.hash(state);
                source.hash(state);
                other.hash(state);
                should_exist.hash(state);
                left_key_id.hash(state);
                right_key_id.hash(state);
            },
            RetrievalId::Group { source, key_fn_id, collector_id } => {
                4u8.hash(state);
                source.hash(state);
                key_fn_id.hash(state);
                collector_id.hash(state);
            },
            RetrievalId::FlatMap { source, mapper_fn_id } => {
                5u8.hash(state);
                source.hash(state);
                mapper_fn_id.hash(state);
            },
        }
    }
}

// NOTE: greynet is a single-threaded library - no thread safety needed

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
                predicate_id: def.predicate_id.clone(),
            },
            StreamDefinition::Join(def) => RetrievalId::Join {
                left: Box::new(def.left_source.get_retrieval_id()),
                right: Box::new(def.right_source.get_retrieval_id()),
                joiner: def.joiner_type,
                left_key_id: def.left_key_fn_id.clone(),
                right_key_id: def.right_key_fn_id.clone(),
            },
            StreamDefinition::ConditionalJoin(def) => RetrievalId::ConditionalJoin {
                source: Box::new(def.source.get_retrieval_id()),
                other: Box::new(def.other.get_retrieval_id()),
                should_exist: def.should_exist,
                left_key_id: def.left_key_fn_id.clone(),
                right_key_id: def.right_key_fn_id.clone(),
            },
            StreamDefinition::Group(def) => RetrievalId::Group {
                source: Box::new(def.source.get_retrieval_id()),
                key_fn_id: def.key_fn_id.clone(),
                collector_id: def.collector_supplier.id(),
            },
            StreamDefinition::FlatMap(def) => RetrievalId::FlatMap {
                source: Box::new(def.source.get_retrieval_id()),
                mapper_fn_id: def.mapper_fn_id,
            },
        }
    }
    
    /// Get the expected output arity of this stream definition
    pub fn output_arity(&self) -> usize {
        match self {
            StreamDefinition::From(_) => 1,
            StreamDefinition::Filter(def) => def.source.output_arity(),
            StreamDefinition::Join(def) => {
                def.left_source.output_arity() + def.right_source.output_arity()
            },
            StreamDefinition::ConditionalJoin(def) => def.source.output_arity(),
            StreamDefinition::Group(_) => 2,
            StreamDefinition::FlatMap(_) => 1,
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
    pub predicate_id: FunctionId,
    pub _phantom: PhantomData<S>,
}

impl<S: Score> FilterDefinition<S> {
    pub fn new(source: StreamDefinition<S>, predicate_id: FunctionId) -> Self {
        Self {
            source: Box::new(source),
            predicate_id,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct JoinDefinition<S: Score> {
    pub left_source: Box<StreamDefinition<S>>,
    pub right_source: Box<StreamDefinition<S>>,
    pub joiner_type: JoinerType,
    pub left_key_fn_id: FunctionId,
    pub right_key_fn_id: FunctionId,
    pub _phantom: PhantomData<S>,
}

impl<S: Score> JoinDefinition<S> {
    pub fn new(
        left_source: StreamDefinition<S>,
        right_source: StreamDefinition<S>,
        joiner_type: JoinerType,
        left_key_fn_id: FunctionId,
        right_key_fn_id: FunctionId,
    ) -> Self {
        Self {
            left_source: Box::new(left_source),
            right_source: Box::new(right_source),
            joiner_type,
            left_key_fn_id,
            right_key_fn_id,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct ConditionalJoinDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub other: Box<StreamDefinition<S>>,
    pub should_exist: bool,
    pub left_key_fn_id: FunctionId,
    pub right_key_fn_id: FunctionId,
    pub _phantom: PhantomData<S>,
}

impl<S: Score> ConditionalJoinDefinition<S> {
    pub fn new(
        source: StreamDefinition<S>,
        other: StreamDefinition<S>,
        should_exist: bool,
        left_key_fn_id: FunctionId,
        right_key_fn_id: FunctionId,
    ) -> Self {
        Self {
            source: Box::new(source),
            other: Box::new(other),
            should_exist,
            left_key_fn_id,
            right_key_fn_id,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct GroupDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub key_fn_id: FunctionId,
    pub collector_supplier: CollectorSupplier,
    pub _phantom: PhantomData<S>,
}

impl<S: Score> GroupDefinition<S> {
    pub fn new(
        source: StreamDefinition<S>,
        key_fn_id: FunctionId,
        collector_supplier: CollectorSupplier,
    ) -> Self {
        Self {
            source: Box::new(source),
            key_fn_id,
            collector_supplier,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct FlatMapDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub mapper_fn_id: usize,
    pub _phantom: PhantomData<S>,
}

impl<S: Score> FlatMapDefinition<S> {
    pub fn new(source: StreamDefinition<S>, mapper_fn_id: usize) -> Self {
        Self {
            source: Box::new(source),
            mapper_fn_id,
            _phantom: PhantomData,
        }
    }
}

/// FIXED: High-level stream API with enhanced error handling and zero-copy integration
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
        let filter_def = FilterDefinition::new(self.definition, FunctionId::Traditional(predicate_id));
        Stream::new(StreamDefinition::Filter(filter_def), self.factory)
    }

    fn join_with<B, C>(self, other: Stream<B, S>, joiner_type: JoinerType, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<C, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        let left_key_fn_id = factory.register_key_fn(left_key_fn);
        let right_key_fn_id = factory.register_key_fn(right_key_fn);
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            joiner_type,
            FunctionId::Traditional(left_key_fn_id),
            FunctionId::Traditional(right_key_fn_id),
        );
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    fn if_conditionally<B>(self, other: Stream<B, S>, should_exist: bool, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Stream<A, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        let left_key_fn_id = factory.register_key_fn(left_key_fn);
        let right_key_fn_id = factory.register_key_fn(right_key_fn);
        let cond_def = ConditionalJoinDefinition::new(
            self.definition,
            other.definition,
            should_exist,
            FunctionId::Traditional(left_key_fn_id),
            FunctionId::Traditional(right_key_fn_id),
        );
        Stream::new(StreamDefinition::ConditionalJoin(cond_def), self.factory)
    }
    
    /// Get the retrieval ID for this stream
    pub fn get_retrieval_id(&self) -> RetrievalId<S> {
        self.definition.get_retrieval_id()
    }
    
    /// Get the expected arity of this stream
    pub fn arity(&self) -> usize {
        self.definition.output_arity()
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
        let group_def = GroupDefinition::new(
            self.definition,
            FunctionId::Traditional(key_fn_id),
            CollectorSupplier::new(collector_supplier),
        );
        Stream::new(StreamDefinition::Group(group_def), self.factory)
    }

    pub fn flat_map(self, mapper_fn: SharedMapperFn) -> Stream<Arity1, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mapper_fn_id = factory_rc.borrow_mut().register_mapper(mapper_fn);
        let flatmap_def = FlatMapDefinition::new(self.definition, mapper_fn_id);
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