//stream_def.rs
//! Stream definition with unified API combining traditional and zero-copy operations.

use super::factory::ConstraintFactory;
use super::joiner::JoinerType;
use super::collectors::BaseCollector;
use crate::nodes::{ImpactFn, ZeroCopyKeyFn, ZeroCopyPredicate, ZeroCopyImpactFn, ZeroCopyMapperFn};
use crate::{GreynetFact, Score, AnyTuple};
use std::any::TypeId;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::hash::Hash;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use crate::prelude::ZeroCopyFacts;

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
    pub penalty_function: crate::nodes::ImpactFn<S>,
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

/// A simplified function identifier, now a newtype struct.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct FunctionId(pub usize);

// RetrievalId identifies a unique stream instance in the dataflow graph.
#[derive(Clone, Debug)]
pub enum RetrievalId<S: Score> {
    From(TypeId, PhantomData<S>),
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
        mapper_fn_id: FunctionId,
    },
    Scoring {
        source: Box<RetrievalId<S>>,
        impact_fn_id: FunctionId,
        constraint_id: String,
    },
}

// Manual implementation of PartialEq that ignores PhantomData
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
            (RetrievalId::Scoring { source: a, impact_fn_id: b, constraint_id: c },
             RetrievalId::Scoring { source: d, impact_fn_id: e, constraint_id: f }) => {
                a == d && b == e && c == f
            },
            _ => false,
        }
    }
}

impl<S: Score> Eq for RetrievalId<S> {}

// Manual implementation of Hash that ignores PhantomData
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
            RetrievalId::Scoring { source, impact_fn_id, constraint_id } => {
                6u8.hash(state);
                source.hash(state);
                impact_fn_id.hash(state);
                constraint_id.hash(state);
            },
        }
    }
}

#[derive(Clone)]
pub enum StreamDefinition<S: Score> {
    From(FromDefinition),
    Filter(FilterDefinition<S>),
    Join(JoinDefinition<S>),
    ConditionalJoin(ConditionalJoinDefinition<S>),
    Group(GroupDefinition<S>),
    FlatMap(FlatMapDefinition<S>),
    Scoring(ScoringDefinition<S>),
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
                mapper_fn_id: def.mapper_fn_id.clone(),
            },
            StreamDefinition::Scoring(def) => RetrievalId::Scoring {
                source: Box::new(def.source.get_retrieval_id()),
                impact_fn_id: def.impact_fn_id.clone(),
                constraint_id: def.constraint_id.clone(),
            },
        }
    }
    
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
            StreamDefinition::Scoring(def) => def.source.output_arity(),
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
pub struct ScoringDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub impact_fn_id: FunctionId,
    pub constraint_id: String,
    pub _phantom: PhantomData<S>,
}

impl<S: Score> ScoringDefinition<S> {
    pub fn new(
        source: StreamDefinition<S>,
        impact_fn_id: FunctionId,
        constraint_id: String,
    ) -> Self {
        Self {
            source: Box::new(source),
            impact_fn_id,
            constraint_id,
            _phantom: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct FlatMapDefinition<S: Score> {
    pub source: Box<StreamDefinition<S>>,
    pub mapper_fn_id: FunctionId,
    pub _phantom: PhantomData<S>,
}

impl<S: Score> FlatMapDefinition<S> {
    pub fn new(source: StreamDefinition<S>, mapper_fn_id: FunctionId) -> Self {
        Self {
            source: Box::new(source),
            mapper_fn_id,
            _phantom: PhantomData,
        }
    }
}

pub fn extract_fact<T: GreynetFact>(tuple: &dyn ZeroCopyFacts, index: usize) -> Option<&T> {
    tuple.get_fact_ref(index)
         .and_then(|fact| fact.as_any().downcast_ref::<T>())
}

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

    /// Unified penalize method that consumes the stream and registers the complete ConstraintRecipe
    pub fn penalize(
        self, 
        constraint_id: &str, 
        penalty_function: impl Fn(&AnyTuple) -> S + 'static
    ) -> ConstraintRecipe<S> {
        // Convert closure to zero-copy impact function
        let zero_copy_impact_fn: ZeroCopyImpactFn<S> = Rc::new(move |tuple| {
            // Convert ZeroCopyFacts to AnyTuple for the penalty function
            // This is a bridge between the old and new APIs
            if let Some(any_tuple) = tuple.as_any().downcast_ref::<AnyTuple>() {
                penalty_function(any_tuple)
            } else {
                S::null_score()
            }
        });

        // Register the function with the factory and get the recipe
        if let Some(factory_rc) = self.factory.upgrade() {
            let impact_fn_id = factory_rc.borrow_mut().register_zero_copy_impact_fn(zero_copy_impact_fn.clone());
            
            let recipe = ConstraintRecipe {
                stream_def: self.definition,
                penalty_function: ImpactFn(zero_copy_impact_fn),
                constraint_id: constraint_id.to_string(),
            };
            
            // Register the recipe with the factory
            factory_rc.borrow_mut().add_constraint_def(recipe.clone());
            
            recipe
        } else {
            panic!("ConstraintFactory has been dropped")
        }
    }
    
    /// Filters the stream using a predicate that can inspect the entire tuple.
    pub fn filter_tuple<F>(self, predicate: F) -> Self
    where
        F: Fn(&dyn ZeroCopyFacts) -> bool + 'static,
    {
        let zero_copy_predicate: ZeroCopyPredicate = Rc::new(predicate);

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let predicate_id = factory_rc.borrow_mut().register_zero_copy_predicate(zero_copy_predicate);
        
        let filter_def = FilterDefinition::new(self.definition, FunctionId(predicate_id));
        Stream::new(StreamDefinition::Filter(filter_def), self.factory)
    }

    /// Filters a stream of single facts based on a type-safe predicate.
    pub fn filter<T, F>(self, predicate: F) -> Self
    where
        T: GreynetFact,
        F: Fn(&T) -> bool + 'static,
    {
        let zero_copy_predicate: ZeroCopyPredicate = Rc::new(move |tuple| {
            // Use the `extract_fact` helper to simplify fact extraction.
            extract_fact::<T>(tuple, 0)
                .map_or(false, |typed_fact| predicate(typed_fact))
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let predicate_id = factory_rc.borrow_mut().register_zero_copy_predicate(zero_copy_predicate);
        
        let filter_def = FilterDefinition::new(self.definition, FunctionId(predicate_id));
        Stream::new(StreamDefinition::Filter(filter_def), self.factory)
    }

    pub(crate) fn join_flex(
        self,
        other: Stream<Arity1, S>,
        joiner_type: JoinerType,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Stream<Arity2, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            joiner_type,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Enhanced flat_map with type-safe closure support
    pub fn flat_map<T, F>(self, mapper: F) -> Stream<Arity1, S>
    where
        T: GreynetFact,
        F: Fn(&T) -> Vec<Rc<dyn GreynetFact>> + 'static,
    {
        let zero_copy_mapper: ZeroCopyMapperFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| mapper(typed_fact))
                .unwrap_or_else(|| Vec::new())
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mapper_fn_id = factory_rc.borrow_mut().register_zero_copy_mapper(zero_copy_mapper);
        
        let flatmap_def = FlatMapDefinition::new(self.definition, FunctionId(mapper_fn_id));
        Stream::new(StreamDefinition::FlatMap(flatmap_def), self.factory)
    }

    pub fn get_retrieval_id(&self) -> RetrievalId<S> {
        self.definition.get_retrieval_id()
    }

    pub fn arity(&self) -> usize {
        self.definition.output_arity()
    }
}

// Enhanced join operations for Arity1 streams
impl<S: Score + 'static> Stream<Arity1, S> {
    /// Type-safe join with automatic key extraction
    pub fn join_on<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity2, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        self.join_flex(other, JoinerType::Equal, left_zero_copy_key_fn, right_zero_copy_key_fn)
    }

    /// Conditional join - if exists
    pub fn if_exists<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.if_conditionally(other, true, left_key_fn, right_key_fn)
    }

    /// Conditional join - if not exists
    pub fn if_not_exists<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.if_conditionally(other, false, left_key_fn, right_key_fn)
    }

    /// Helper method for conditional joins
    fn if_conditionally<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        should_exist: bool,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let cond_def = ConditionalJoinDefinition::new(
            self.definition,
            other.definition,
            should_exist,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::ConditionalJoin(cond_def), self.factory)
    }

    /// Group by with type-safe key extraction
    pub fn group_by<T, F, K>(
        self,
        key_fn: F,
        collector_supplier: Box<dyn Fn() -> Box<dyn BaseCollector>>,
    ) -> Stream<Arity2, S>
    where
        T: GreynetFact,
        F: Fn(&T) -> K + 'static,
        K: Hash + 'static,
    {
        let zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let key_fn_id = factory_rc.borrow_mut().register_zero_copy_key_fn(zero_copy_key_fn);
        
        let group_def = GroupDefinition::new(
            self.definition,
            FunctionId(key_fn_id),
            CollectorSupplier::new(collector_supplier),
        );
        
        Stream::new(StreamDefinition::Group(group_def), self.factory)
    }
}

impl<S: Score + 'static> Stream<Arity2, S> {
    /// Conditional join - if not exists for Arity2 streams
    /// Extracts key from the first fact of the BiTuple
    pub fn if_not_exists_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.if_conditionally_first(other, false, left_key_fn, right_key_fn)
    }

    /// Conditional join - if not exists for Arity2 streams
    /// Extracts key from the second fact of the BiTuple
    pub fn if_not_exists_bi<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.if_conditionally_bi(other, false, left_key_fn, right_key_fn)
    }

    /// Conditional join - if not exists for Arity2 streams with composite key
    /// Extracts key from both facts of the BiTuple
    pub fn if_not_exists_composite<T1, T2, T3, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        T3: GreynetFact,
        F1: Fn(&T1, &T2) -> K + 'static,
        F2: Fn(&T3) -> K + 'static,
        K: Hash + 'static,
    {
        self.if_conditionally_composite(other, false, left_key_fn, right_key_fn)
    }

    /// Conditional join - if exists for Arity2 streams (first fact)
    pub fn if_exists_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.if_conditionally_first(other, true, left_key_fn, right_key_fn)
    }

    /// Conditional join - if exists for Arity2 streams (second fact)
    pub fn if_exists_bi<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.if_conditionally_bi(other, true, left_key_fn, right_key_fn)
    }

    // Helper methods for conditional joins

    fn if_conditionally_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        should_exist: bool,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        self.create_conditional_join(other, should_exist, left_zero_copy_key_fn, right_zero_copy_key_fn)
    }

    fn if_conditionally_bi<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        should_exist: bool,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(1) // Second fact (index 1)
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        self.create_conditional_join(other, should_exist, left_zero_copy_key_fn, right_zero_copy_key_fn)
    }

    fn if_conditionally_composite<T1, T2, T3, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        should_exist: bool,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self
    where
        T1: GreynetFact,
        T2: GreynetFact,
        T3: GreynetFact,
        F1: Fn(&T1, &T2) -> K + 'static,
        F2: Fn(&T3) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            if let (Some(fact1), Some(fact2)) = (tuple.get_fact_ref(0), tuple.get_fact_ref(1)) {
                if let (Some(typed_fact1), Some(typed_fact2)) = (
                    fact1.as_any().downcast_ref::<T1>(),
                    fact2.as_any().downcast_ref::<T2>(),
                ) {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact1, typed_fact2).hash(&mut hasher);
                    return hasher.finish();
                }
            }
            0
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T3>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        self.create_conditional_join(other, should_exist, left_zero_copy_key_fn, right_zero_copy_key_fn)
    }

    fn create_conditional_join(
        self,
        other: Stream<Arity1, S>,
        should_exist: bool,
        left_zero_copy_key_fn: ZeroCopyKeyFn,
        right_zero_copy_key_fn: ZeroCopyKeyFn,
    ) -> Self {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let cond_def = ConditionalJoinDefinition::new(
            self.definition,
            other.definition,
            should_exist,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::ConditionalJoin(cond_def), self.factory)
    }
}

// Add these implementations to the stream_def.rs file, after the existing Stream<Arity1, S> impl
// Also add this import to the top of stream_def.rs:
// use std::collections::hash_map::DefaultHasher;

impl<S: Score + 'static> Stream<Arity2, S> {
    /// Join using the first fact from the BiTuple as the key
    pub fn join_on_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Join using the second fact from the BiTuple as the key
    pub fn join_on_second<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(1) // Second fact (index 1)
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Join BiTuple with another BiTuple using first facts from both
    pub fn join_on_both_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity2, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Join using composite key from both facts in BiTuple
    pub fn join_on_composite<T1, T2, T3, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        T3: GreynetFact,
        F1: Fn(&T1, &T2) -> K + 'static,
        F2: Fn(&T3) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            if let (Some(fact1), Some(fact2)) = (tuple.get_fact_ref(0), tuple.get_fact_ref(1)) {
                if let (Some(typed_fact1), Some(typed_fact2)) = (
                    fact1.as_any().downcast_ref::<T1>(),
                    fact2.as_any().downcast_ref::<T2>(),
                ) {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact1, typed_fact2).hash(&mut hasher);
                    return hasher.finish();
                }
            }
            0
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T3>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }
}

impl<S: Score + 'static> Stream<Arity3, S> {
    /// Join using the first fact from the TriTuple as the key
    pub fn join_on_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_on_indexed(other, 0, left_key_fn, right_key_fn)
    }

    /// Join using the second fact from the TriTuple as the key
    pub fn join_on_second<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_on_indexed(other, 1, left_key_fn, right_key_fn)
    }

    /// Join using the third fact from the TriTuple as the key
    pub fn join_on_third<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_on_indexed(other, 2, left_key_fn, right_key_fn)
    }

    /// Join using any fact from TriTuple by index
    pub fn join_on_indexed<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        fact_index: usize,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(fact_index)
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Join TriTuple with BiTuple to create PentaTuple
    pub fn join_on_first_to_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity2, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity5, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }
}

impl<S: Score + 'static> Stream<Arity4, S> {
    /// Join QuadTuple with UniTuple using first fact as key
    pub fn join_on_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity5, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_on_indexed(other, 0, left_key_fn, right_key_fn)
    }

    /// Join using any fact from QuadTuple by index
    pub fn join_on_indexed<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        fact_index: usize,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity5, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(fact_index)
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }
}

impl<S: Score + 'static> Stream<Arity5, S> {
    // PentaTuple is the maximum arity, so joins from here would exceed the limit
    // We provide methods that return Result to handle the overflow case
    
    /// Attempt to join with overflow check - returns error if result would exceed Arity5
    pub fn try_join_on_indexed<T1, T2, F1, F2, K>(
        self,
        _other: Stream<Arity1, S>,
        _fact_index: usize,
        _left_key_fn: F1,
        _right_key_fn: F2,
    ) -> Result<(), crate::GreynetError>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        // Joining Arity5 + Arity1 would create Arity6, which exceeds the limit
        Err(crate::GreynetError::invalid_arity(5, 6))
    }
}

// Convenience methods for indexed joins across all arities
impl<S: Score + 'static> Stream<Arity2, S> {
    /// Generic indexed join method
    pub fn join_on_indexed<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        fact_index: usize,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        match fact_index {
            0 => self.join_on_first(other, left_key_fn, right_key_fn),
            1 => self.join_on_second(other, left_key_fn, right_key_fn),
            _ => panic!("Invalid fact index {} for Arity2 stream (valid: 0-1)", fact_index),
        }
    }
}

// =============================================================================
// REVERSE DIRECTION JOINS (Lower arity + Higher arity)
// =============================================================================

// Additional methods for Stream<Arity1, S> to join with higher arity streams
impl<S: Score + 'static> Stream<Arity1, S> {
    /// Join UniTuple with BiTuple using first fact from BiTuple
    pub fn join_with_bi_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity2, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Join UniTuple with BiTuple using second fact from BiTuple
    pub fn join_with_bi_second<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity2, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(1) // Second fact
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Join UniTuple with TriTuple using indexed fact from TriTuple
    pub fn join_with_tri_indexed<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity3, S>,
        fact_index: usize,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(fact_index)
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Join UniTuple with QuadTuple using indexed fact from QuadTuple
    pub fn join_with_quad_indexed<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity4, S>,
        fact_index: usize,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity5, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(fact_index)
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Try to join UniTuple with PentaTuple (would exceed max arity)
    pub fn try_join_with_penta<T1, T2, F1, F2, K>(
        self,
        _other: Stream<Arity5, S>,
        _fact_index: usize,
        _left_key_fn: F1,
        _right_key_fn: F2,
    ) -> Result<(), crate::GreynetError>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        // Joining Arity1 + Arity5 would create Arity6, which exceeds the limit
        Err(crate::GreynetError::invalid_arity(5, 6))
    }
}

// Additional methods for Stream<Arity2, S> to join with higher arity streams
impl<S: Score + 'static> Stream<Arity2, S> {
    /// Join BiTuple with TriTuple using indexed facts
    pub fn join_with_tri_indexed<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity3, S>,
        left_fact_index: usize,
        right_fact_index: usize,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity5, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        let left_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(left_fact_index)
                .and_then(|fact| fact.as_any().downcast_ref::<T1>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    left_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let right_zero_copy_key_fn: ZeroCopyKeyFn = Rc::new(move |tuple| {
            tuple.get_fact_ref(right_fact_index)
                .and_then(|fact| fact.as_any().downcast_ref::<T2>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    right_key_fn(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        });

        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_zero_copy_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_zero_copy_key_fn);
        
        let join_def = JoinDefinition::new(
            self.definition,
            other.definition,
            JoinerType::Equal,
            FunctionId(left_key_fn_id),
            FunctionId(right_key_fn_id),
        );
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }

    /// Try to join BiTuple with QuadTuple (would exceed max arity)
    pub fn try_join_with_quad<T1, T2, F1, F2, K>(
        self,
        _other: Stream<Arity4, S>,
        _left_fact_index: usize,
        _right_fact_index: usize,
        _left_key_fn: F1,
        _right_key_fn: F2,
    ) -> Result<(), crate::GreynetError>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        // Joining Arity2 + Arity4 would create Arity6, which exceeds the limit
        Err(crate::GreynetError::invalid_arity(5, 6))
    }
}

// Additional methods for Stream<Arity3, S> to join with higher arity streams
impl<S: Score + 'static> Stream<Arity3, S> {
    /// Try to join TriTuple with TriTuple (would exceed max arity)
    pub fn try_join_with_tri<T1, T2, F1, F2, K>(
        self,
        _other: Stream<Arity3, S>,
        _left_fact_index: usize,
        _right_fact_index: usize,
        _left_key_fn: F1,
        _right_key_fn: F2,
    ) -> Result<(), crate::GreynetError>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        // Joining Arity3 + Arity3 would create Arity6, which exceeds the limit
        Err(crate::GreynetError::invalid_arity(5, 6))
    }
}

// Convenience methods with descriptive names
impl<S: Score + 'static> Stream<Arity1, S> {
    /// Join with first fact of BiTuple
    pub fn join_with_bi_first_fact<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity2, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_with_bi_first(other, left_key_fn, right_key_fn)
    }

    /// Join with second fact of BiTuple
    pub fn join_with_bi_second_fact<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity2, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity3, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_with_bi_second(other, left_key_fn, right_key_fn)
    }

    /// Join with first fact of TriTuple
    pub fn join_with_tri_first<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity3, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_with_tri_indexed(other, 0, left_key_fn, right_key_fn)
    }

    /// Join with second fact of TriTuple
    pub fn join_with_tri_second<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity3, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_with_tri_indexed(other, 1, left_key_fn, right_key_fn)
    }

    /// Join with third fact of TriTuple
    pub fn join_with_tri_third<T1, T2, F1, F2, K>(
        self,
        other: Stream<Arity3, S>,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Stream<Arity4, S>
    where
        T1: GreynetFact,
        T2: GreynetFact,
        F1: Fn(&T1) -> K + 'static,
        F2: Fn(&T2) -> K + 'static,
        K: Hash + 'static,
    {
        self.join_with_tri_indexed(other, 2, left_key_fn, right_key_fn)
    }
}