//! Pure zero-copy API implementation

// =============================================================================
// streams_zero_copy.rs - Pure zero-copy stream operations
// =============================================================================

use crate::{UniTuple, BiTuple, TriTuple, QuadTuple, PentaTuple};
use crate::stream_def::{Stream, Arity1, Arity2, Arity3, Arity4, Arity5};
use crate::stream_def::{
    StreamDefinition, FilterDefinition, JoinDefinition, ConditionalJoinDefinition, 
    GroupDefinition, FlatMapDefinition, FunctionId
};
use crate::factory::ConstraintFactory;
use crate::joiner::JoinerType;
use crate::tuple::{AnyTuple, ZeroCopyFacts};
use crate::collectors::BaseCollector;
use crate::{Score, GreynetFact};
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// Zero-copy function types
pub type ZeroCopyKeyFn = Rc<dyn Fn(&dyn ZeroCopyFacts) -> u64>;
pub type ZeroCopyPredicate = Rc<dyn Fn(&dyn ZeroCopyFacts) -> bool>;
pub type ZeroCopyMapperFn = Rc<dyn Fn(&dyn ZeroCopyFacts) -> Vec<Rc<dyn GreynetFact>>>;

/// Extension trait for streams to use zero-copy operations
pub trait ZeroCopyStreamOps<A, S: Score + 'static> {
    /// Filter using zero-copy predicate (much faster than current filter)
    fn filter_zero_copy(self, predicate: ZeroCopyPredicate) -> Self;
    
}

impl<A, S: Score + 'static> ZeroCopyStreamOps<A, S> for Stream<A, S> {
    fn filter_zero_copy(self, predicate: ZeroCopyPredicate) -> Self {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        
        let predicate_id = factory_rc.borrow_mut().register_zero_copy_predicate(predicate);
        
        let filter_def = FilterDefinition {
            source: Box::new(self.definition),
            predicate_id: FunctionId(predicate_id),
            _phantom: PhantomData,
        };
        
        Stream::new(StreamDefinition::Filter(filter_def), self.factory)
    }
}

/// Extension trait for arity-1 streams with zero-copy join operations
pub trait ZeroCopyJoinOps<S: Score + 'static> {
    /// Join using zero-copy key functions (much faster than current join)
    fn join_zero_copy(
        self, 
        other: Stream<Arity1, S>, 
        joiner_type: JoinerType,
        left_key_fn: ZeroCopyKeyFn, 
        right_key_fn: ZeroCopyKeyFn
    ) -> Stream<Arity2, S>;
    
    /// Conditional join using zero-copy key functions
    fn if_exists_zero_copy(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self;
    
    /// Conditional join using zero-copy key functions
    fn if_not_exists_zero_copy(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self;
    
    /// Group by using zero-copy key function
    fn group_by_zero_copy(
        self,
        key_fn: ZeroCopyKeyFn,
        collector_supplier: Box<dyn Fn() -> Box<dyn BaseCollector>>,
    ) -> Stream<Arity2, S>;
    
    /// Convenience method for joining on field equality using zero-copy
    fn join_on_field<T, U, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_field: F1,
        right_field: F2,
    ) -> Stream<Arity2, S>
    where
        T: GreynetFact,
        U: GreynetFact,
        F1: Fn(&T) -> K + 'static,
        F2: Fn(&U) -> K + 'static,
        K: std::hash::Hash + 'static;
    
    /// Convenience method for filtering by field value using zero-copy
    fn filter_field<T, F, V>(
        self,
        field_extractor: F,
        predicate: impl Fn(&V) -> bool + 'static,
    ) -> Self
    where
        T: GreynetFact,
        F: Fn(&T) -> &V + 'static,
        V: 'static;
}


// Helper functions moved to the top for proper visibility
fn field_key_extractor<T, F, K>(field_extractor: F) -> ZeroCopyKeyFn 
where
    T: GreynetFact,
    F: Fn(&T) -> K + 'static,
    K: std::hash::Hash + 'static,
{
    Rc::new(move |tuple: &dyn ZeroCopyFacts| {
        tuple.first_fact()
            .and_then(|fact| fact.as_any().downcast_ref::<T>())
            .map(|typed_fact| {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                use std::hash::{Hash, Hasher};
                field_extractor(typed_fact).hash(&mut hasher);
                hasher.finish()
            })
            .unwrap_or(0)
    })
}

fn field_predicate<T, F, V>(field_extractor: F, predicate: impl Fn(&V) -> bool + 'static) -> ZeroCopyPredicate
where
    T: GreynetFact,
    F: Fn(&T) -> &V + 'static,
    V: 'static,
{
    Rc::new(move |tuple: &dyn ZeroCopyFacts| {
        tuple.first_fact()
            .and_then(|fact| fact.as_any().downcast_ref::<T>())
            .map(|typed_fact| predicate(field_extractor(typed_fact)))
            .unwrap_or(false)
    })
}

fn field_predicate_simple<T, F>(field_check: F) -> ZeroCopyPredicate
where
    T: GreynetFact,
    F: Fn(&T) -> bool + 'static,
{
    Rc::new(move |tuple: &dyn ZeroCopyFacts| {
        tuple.first_fact()
            .and_then(|fact| fact.as_any().downcast_ref::<T>())
            .map(|typed_fact| field_check(typed_fact))
            .unwrap_or(false)
    })
}

// Additional helper methods for Stream
impl<A, S: Score + 'static> Stream<A, S> {
    /// Enhanced filter with field access
    pub fn filter_by_field<T, F, V>(
        self,
        field_extractor: F,
        predicate: impl Fn(&V) -> bool + 'static,
    ) -> Self
    where
        T: GreynetFact,
        F: Fn(&T) -> &V + 'static,
        V: 'static,
    {
        let zero_copy_predicate = Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| predicate(field_extractor(typed_fact)))
                .unwrap_or(false)
        });
        self.filter_zero_copy(zero_copy_predicate)
    }
    
    /// Enhanced filter with simple field check
    pub fn filter_by_check<T>(
        self,
        field_check: impl Fn(&T) -> bool + 'static,
    ) -> Self
    where
        T: GreynetFact,
    {
        let zero_copy_predicate = zero_copy_ops::field_check(field_check);
        self.filter_zero_copy(zero_copy_predicate)
    }
}

/// Implementation for arity-1 streams
impl<S: Score + 'static> ZeroCopyJoinOps<S> for Stream<Arity1, S> {
    fn join_zero_copy(
        self, 
        other: Stream<Arity1, S>, 
        joiner_type: JoinerType,
        left_key_fn: ZeroCopyKeyFn, 
        right_key_fn: ZeroCopyKeyFn
    ) -> Stream<Arity2, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_key_fn);
        
        let join_def = JoinDefinition {
            left_source: Box::new(self.definition),
            right_source: Box::new(other.definition),
            joiner_type,
            left_key_fn_id: FunctionId(left_key_fn_id),
            right_key_fn_id: FunctionId(right_key_fn_id),
            _phantom: PhantomData,
        };
        
        Stream::new(StreamDefinition::Join(join_def), self.factory)
    }
    
    fn if_exists_zero_copy(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self {
        self.if_conditionally_zero_copy(other, true, left_key_fn, right_key_fn)
    }
    
    fn if_not_exists_zero_copy(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self {
        self.if_conditionally_zero_copy(other, false, left_key_fn, right_key_fn)
    }
    
    fn group_by_zero_copy(
        self,
        key_fn: ZeroCopyKeyFn,
        collector_supplier: Box<dyn Fn() -> Box<dyn BaseCollector>>,
    ) -> Stream<Arity2, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        
        let key_fn_id = factory_rc.borrow_mut().register_zero_copy_key_fn(key_fn);
        
        let group_def = GroupDefinition {
            source: Box::new(self.definition),
            key_fn_id: FunctionId(key_fn_id),
            collector_supplier: crate::stream_def::CollectorSupplier::new(collector_supplier),
            _phantom: PhantomData,
        };
        
        Stream::new(StreamDefinition::Group(group_def), self.factory)
    }
    
    fn join_on_field<T, U, F1, F2, K>(
        self,
        other: Stream<Arity1, S>,
        left_field: F1,
        right_field: F2,
    ) -> Stream<Arity2, S>
    where
        T: GreynetFact,
        U: GreynetFact,
        F1: Fn(&T) -> K + 'static,
        F2: Fn(&U) -> K + 'static,
        K: std::hash::Hash + 'static,
    {
        let left_key_fn = zero_copy_ops::field_key::<T, F1, K>(left_field);
        let right_key_fn = zero_copy_ops::field_key::<U, F2, K>(right_field);
        
        self.join_zero_copy(other, JoinerType::Equal, left_key_fn, right_key_fn)
    }
    
    fn filter_field<T, F, V>(
        self,
        field_extractor: F,
        predicate: impl Fn(&V) -> bool + 'static,
    ) -> Self
    where
        T: GreynetFact,
        F: Fn(&T) -> &V + 'static,
        V: 'static,
    {
        let zero_copy_predicate = Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| predicate(field_extractor(typed_fact)))
                .unwrap_or(false)
        });
        self.filter_zero_copy(zero_copy_predicate)
    }
}

// Helper method for conditional joins
impl<A, S: Score + 'static> Stream<A, S> {
    fn if_conditionally_zero_copy<B>(
        self,
        other: Stream<B, S>,
        should_exist: bool,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let left_key_fn_id = factory.register_zero_copy_key_fn(left_key_fn);
        let right_key_fn_id = factory.register_zero_copy_key_fn(right_key_fn);
        
        let cond_def = ConditionalJoinDefinition {
            source: Box::new(self.definition),
            other: Box::new(other.definition),
            should_exist,
            left_key_fn_id: FunctionId(left_key_fn_id),
            right_key_fn_id: FunctionId(right_key_fn_id),
            _phantom: PhantomData,
        };
        
        Stream::new(StreamDefinition::ConditionalJoin(cond_def), self.factory)
    }
}

/// Implementations for higher arity streams with proper join operations
macro_rules! impl_higher_arity_zero_copy {
    ($arity:ident, $next_arity:ident) => {
        impl<S: Score + 'static> Stream<$arity, S> {
            pub fn join_zero_copy(
                self, 
                other: Stream<Arity1, S>, 
                joiner_type: JoinerType,
                left_key_fn: ZeroCopyKeyFn, 
                right_key_fn: ZeroCopyKeyFn
            ) -> Stream<$next_arity, S> {
                let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
                let mut factory = factory_rc.borrow_mut();
                
                let left_key_fn_id = factory.register_zero_copy_key_fn(left_key_fn);
                let right_key_fn_id = factory.register_zero_copy_key_fn(right_key_fn);
                
                let join_def = JoinDefinition {
                    left_source: Box::new(self.definition),
                    right_source: Box::new(other.definition),
                    joiner_type,
                    left_key_fn_id: FunctionId(left_key_fn_id),
                    right_key_fn_id: FunctionId(right_key_fn_id),
                    _phantom: PhantomData,
                };
                
                Stream::new(StreamDefinition::Join(join_def), self.factory)
            }

            pub fn if_exists_zero_copy(
                self,
                other: Stream<Arity1, S>,
                left_key_fn: ZeroCopyKeyFn,
                right_key_fn: ZeroCopyKeyFn,
            ) -> Self {
                self.if_conditionally_zero_copy(other, true, left_key_fn, right_key_fn)
            }

            pub fn if_not_exists_zero_copy(
                self,
                other: Stream<Arity1, S>,
                left_key_fn: ZeroCopyKeyFn,
                right_key_fn: ZeroCopyKeyFn,
            ) -> Self {
                self.if_conditionally_zero_copy(other, false, left_key_fn, right_key_fn)
            }
        }
    };
}

impl_higher_arity_zero_copy!(Arity2, Arity3);
impl_higher_arity_zero_copy!(Arity3, Arity4);
impl_higher_arity_zero_copy!(Arity4, Arity5);

/// Specialized implementation for Arity5 (no more joins possible)
impl<S: Score + 'static> Stream<Arity5, S> {
    pub fn if_exists_zero_copy(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self {
        self.if_conditionally_zero_copy(other, true, left_key_fn, right_key_fn)
    }

    pub fn if_not_exists_zero_copy(
        self,
        other: Stream<Arity1, S>,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self {
        self.if_conditionally_zero_copy(other, false, left_key_fn, right_key_fn)
    }
}

/// Utility functions for creating common zero-copy operations
pub mod zero_copy_ops {
    use super::*;
    
    /// Create a zero-copy predicate that checks if all facts satisfy a condition
    pub fn all_facts_satisfy<F>(condition: F) -> ZeroCopyPredicate
    where
        F: Fn(&dyn GreynetFact) -> bool + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            for i in 0..tuple.arity() {
                if let Some(fact) = tuple.get_fact_ref(i) {
                    if !condition(fact) {
                        return false;
                    }
                }
            }
            true
        })
    }
    
    /// Create a zero-copy predicate that checks if any fact satisfies a condition
    pub fn any_fact_satisfies<F>(condition: F) -> ZeroCopyPredicate
    where
        F: Fn(&dyn GreynetFact) -> bool + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            for i in 0..tuple.arity() {
                if let Some(fact) = tuple.get_fact_ref(i) {
                    if condition(fact) {
                        return true;
                    }
                }
            }
            false
        })
    }

    /// Create a zero-copy key function from a field extractor
    pub fn field_key<T, F, K>(field_extractor: F) -> ZeroCopyKeyFn 
    where
        T: GreynetFact,
        F: Fn(&T) -> K + 'static,
        K: std::hash::Hash + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let mut hasher = DefaultHasher::new();
                    field_extractor(typed_fact).hash(&mut hasher);
                    hasher.finish()
                })
                .unwrap_or(0)
        })
    }
    
    /// Create a zero-copy predicate from a field check
    pub fn field_check<T, F>(field_check: F) -> ZeroCopyPredicate
    where
        T: GreynetFact,
        F: Fn(&T) -> bool + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| field_check(typed_fact))
                .unwrap_or(false)
        })
    }
    
    /// Create a zero-copy key function for the first fact's hash
    pub fn first_fact_hash() -> ZeroCopyKeyFn {
        Rc::new(|tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .map(|fact| fact.hash_fact())
                .unwrap_or(0)
        })
    }
    
    /// Create a zero-copy key function for the last fact's hash
    pub fn last_fact_hash() -> ZeroCopyKeyFn {
        Rc::new(|tuple: &dyn ZeroCopyFacts| {
            tuple.last_fact()
                .map(|fact| fact.hash_fact())
                .unwrap_or(0)
        })
    }
    
    /// Create a zero-copy key function for a specific fact index
    pub fn indexed_fact_hash(index: usize) -> ZeroCopyKeyFn {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.get_fact_ref(index)
                .map(|fact| fact.hash_fact())
                .unwrap_or(0)
        })
    }
    
    /// Create a composite key from multiple fact indices
    pub fn composite_key(indices: Vec<usize>) -> ZeroCopyKeyFn {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            let mut hasher = DefaultHasher::new();
            
            for &index in &indices {
                if let Some(fact) = tuple.get_fact_ref(index) {
                    fact.hash_fact().hash(&mut hasher);
                }
            }
            hasher.finish()
        })
    }
}

/// High-performance stream builder using zero-copy operations
pub struct ZeroCopyStreamBuilder<S: Score + 'static> {
    factory: Weak<RefCell<ConstraintFactory<S>>>,
}

impl<S: Score + 'static> ZeroCopyStreamBuilder<S> {
    pub fn new(factory: Weak<RefCell<ConstraintFactory<S>>>) -> Self {
        Self { factory }
    }
    
    /// Create a stream from a fact type with zero-copy optimizations enabled
    pub fn from<T: GreynetFact + 'static>(&self) -> Stream<Arity1, S> {
        if let Some(factory_rc) = self.factory.upgrade() {
            ConstraintFactory::from::<T>(&factory_rc)
        } else {
            panic!("ConstraintFactory has been dropped")
        }
    }
    
    /// Create optimized unique pairs stream using zero-copy
    pub fn unique_pairs<T: GreynetFact + 'static>(&self) -> Stream<Arity2, S> {
        let stream1 = self.from::<T>();
        let stream2 = self.from::<T>();
        
        stream1.join_zero_copy(
            stream2,
            JoinerType::LessThan,
            zero_copy_ops::first_fact_hash(),
            zero_copy_ops::first_fact_hash(),
        )
    }
    
    /// Create a filtered stream using zero-copy field access
    pub fn filtered_facts<T, F>(
        &self,
        field_check: F,
    ) -> Stream<Arity1, S>
    where
        T: GreynetFact + 'static,
        F: Fn(&T) -> bool + 'static,
    {
        self.from::<T>()
            .filter_zero_copy(zero_copy_ops::field_check(field_check))
    }
}

// =============================================================================
// Enhanced common operations with better type safety
// =============================================================================

/// Enhanced common operations for zero-copy API
pub mod enhanced_ops {
    use super::*;
    use std::collections::HashMap;
    
    /// Type-safe field key extractor with compile-time validation
    pub fn typed_field_key<T, F, K>(field_extractor: F) -> ZeroCopyKeyFn 
    where
        T: GreynetFact + 'static,
        F: Fn(&T) -> K + 'static,
        K: std::hash::Hash + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            for i in 0..tuple.arity() {
                if let Some(fact) = tuple.get_fact_ref(i) {
                    if let Some(typed_fact) = fact.as_any().downcast_ref::<T>() {
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        use std::hash::{Hash, Hasher};
                        field_extractor(typed_fact).hash(&mut hasher);
                        return hasher.finish();
                    }
                }
            }
            0
        })
    }
    
    /// Multi-type predicate for complex conditions
    pub fn multi_type_predicate<T1, T2, F>(condition: F) -> ZeroCopyPredicate
    where
        T1: GreynetFact + 'static,
        T2: GreynetFact + 'static,
        F: Fn(Option<&T1>, Option<&T2>) -> bool + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            let mut t1_ref = None;
            let mut t2_ref = None;
            
            for i in 0..tuple.arity() {
                if let Some(fact) = tuple.get_fact_ref(i) {
                    if t1_ref.is_none() {
                        t1_ref = fact.as_any().downcast_ref::<T1>();
                    }
                    if t2_ref.is_none() {
                        t2_ref = fact.as_any().downcast_ref::<T2>();
                    }
                }
            }
            
            condition(t1_ref, t2_ref)
        })
    }
    
    /// Range-based key function for numerical comparisons
    pub fn range_key<T, F>(field_extractor: F, bucket_size: f64) -> ZeroCopyKeyFn
    where
        T: GreynetFact + 'static,
        F: Fn(&T) -> f64 + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let value = field_extractor(typed_fact);
                    ((value / bucket_size).floor() as u64)
                })
                .unwrap_or(0)
        })
    }
    
    /// Cached key function for expensive computations
    pub fn cached_key<T, F, K>(field_extractor: F) -> ZeroCopyKeyFn
    where
        T: GreynetFact + 'static,
        F: Fn(&T) -> K + 'static + Clone,
        K: std::hash::Hash + Clone + 'static,
    {
        let cache = std::rc::Rc::new(std::cell::RefCell::new(HashMap::new()));
        
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            if let Some(fact) = tuple.first_fact() {
                if let Some(typed_fact) = fact.as_any().downcast_ref::<T>() {
                    let fact_id = fact.fact_id();
                    
                    // Check cache first
                    if let Some(&cached_key) = cache.borrow().get(&fact_id) {
                        return cached_key;
                    }
                    
                    // Compute and cache
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    use std::hash::{Hash, Hasher};
                    field_extractor(typed_fact).hash(&mut hasher);
                    let key = hasher.finish();
                    
                    cache.borrow_mut().insert(fact_id, key);
                    return key;
                }
            }
            0
        })
    }
}

pub mod enhanced_zero_copy_ops {
    use super::*;
    use crate::nodes::{ZeroCopyImpactFn, ZeroCopyMapperFn};
    use crate::{Score, GreynetFact};
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::rc::Rc;

    // =============================================================================
    // SCORING FUNCTIONS (for ScoringNode)
    // =============================================================================

    /// Create a zero-copy impact function for constraint scoring
    pub fn impact_function<S, F>(scoring_fn: F) -> ZeroCopyImpactFn<S>
    where
        S: Score,
        F: Fn(&dyn ZeroCopyFacts) -> S + 'static,
    {
        Rc::new(scoring_fn)
    }

    /// Create penalty function from field access with type safety
    pub fn field_penalty<T, F, S>(
        field_extractor: F,
        penalty_calculator: impl Fn(&T) -> S + 'static
    ) -> ZeroCopyImpactFn<S>
    where
        T: GreynetFact,
        F: Fn(&T) -> &T + 'static,
        S: Score,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| penalty_calculator(field_extractor(typed_fact)))
                .unwrap_or_else(|| S::null_score())
        })
    }

    /// Create penalty function based on numerical field violations
    pub fn numerical_violation_penalty<T, F, S>(
        field_extractor: F,
        min_value: f64,
        max_value: f64,
        penalty_per_unit: f64,
    ) -> ZeroCopyImpactFn<S>
    where
        T: GreynetFact,
        F: Fn(&T) -> f64 + 'static,
        S: Score + crate::score::FromSimple,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let value = field_extractor(typed_fact);
                    if value < min_value {
                        S::simple((min_value - value) * penalty_per_unit)
                    } else if value > max_value {
                        S::simple((value - max_value) * penalty_per_unit)
                    } else {
                        S::null_score()
                    }
                })
                .unwrap_or_else(|| S::null_score())
        })
    }

    /// Create hard constraint penalty (violates if condition is false)
    pub fn hard_constraint<T, F, S>(
        condition: F,
        penalty_value: f64,
    ) -> ZeroCopyImpactFn<S>
    where
        T: GreynetFact,
        F: Fn(&T) -> bool + 'static,
        S: Score + crate::score::FromHard,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    if condition(typed_fact) {
                        S::null_score()
                    } else {
                        S::hard(penalty_value)
                    }
                })
                .unwrap_or_else(|| S::hard(penalty_value))
        })
    }

    /// Create soft constraint penalty (penalizes violations gradually)
    pub fn soft_constraint<T, F, S>(
        constraint_fn: F,
        max_penalty: f64,
    ) -> ZeroCopyImpactFn<S>
    where
        T: GreynetFact,
        F: Fn(&T) -> f64 + 'static, // Returns violation amount (0.0 = no violation)
        S: Score + crate::score::FromSoft,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let violation = constraint_fn(typed_fact);
                    if violation > 0.0 {
                        S::soft(violation.min(max_penalty))
                    } else {
                        S::null_score()
                    }
                })
                .unwrap_or_else(|| S::null_score())
        })
    }

    // =============================================================================
    // MAPPING FUNCTIONS (for FlatMapNode)
    // =============================================================================

    /// Create a zero-copy mapper function
    pub fn mapping_function<F>(mapper_fn: F) -> ZeroCopyMapperFn
    where
        F: Fn(&dyn ZeroCopyFacts) -> Vec<Rc<dyn GreynetFact>> + 'static,
    {
        Rc::new(mapper_fn)
    }

    /// Create mapper that extracts field values as new facts
    pub fn field_extractor_mapper<T, F, V>(
        field_extractor: F,
    ) -> ZeroCopyMapperFn
    where
        T: GreynetFact,
        F: Fn(&T) -> V + 'static,
        V: GreynetFact + Clone,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let extracted_value = field_extractor(typed_fact);
                    vec![Rc::new(extracted_value) as Rc<dyn GreynetFact>]
                })
                .unwrap_or_else(|| Vec::new())
        })
    }

    /// Create mapper that generates derived facts based on computations
    pub fn computation_mapper<T, F, V>(
        computation: F,
    ) -> ZeroCopyMapperFn
    where
        T: GreynetFact,
        F: Fn(&T) -> Vec<V> + 'static,
        V: GreynetFact,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    computation(typed_fact)
                        .into_iter()
                        .map(|v| Rc::new(v) as Rc<dyn GreynetFact>)
                        .collect()
                })
                .unwrap_or_else(|| Vec::new())
        })
    }

    /// Create mapper for aggregation/grouping operations
    pub fn aggregation_mapper<T, K, V, F1, F2>(
        key_extractor: F1,
        value_extractor: F2,
    ) -> ZeroCopyMapperFn
    where
        T: GreynetFact,
        K: Hash + Eq + GreynetFact,
        V: GreynetFact,
        F1: Fn(&T) -> K + 'static,
        F2: Fn(&T) -> V + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let key = key_extractor(typed_fact);
                    let value = value_extractor(typed_fact);
                    vec![
                        Rc::new(key) as Rc<dyn GreynetFact>,
                        Rc::new(value) as Rc<dyn GreynetFact>,
                    ]
                })
                .unwrap_or_else(|| Vec::new())
        })
    }

    // =============================================================================
    // COMPOSITE OPERATIONS
    // =============================================================================

    /// Create mapper that processes multiple facts in tuple
    pub fn multi_fact_mapper<F>(
        processor: F,
    ) -> ZeroCopyMapperFn
    where
        F: Fn(&dyn ZeroCopyFacts) -> Vec<Rc<dyn GreynetFact>> + 'static,
    {
        Rc::new(processor)
    }

    /// Create conditional mapper (only maps if condition is met)
    pub fn conditional_mapper<T, C, M>(
        condition: C,
        mapper: M,
    ) -> ZeroCopyMapperFn
    where
        T: GreynetFact,
        C: Fn(&T) -> bool + 'static,
        M: Fn(&T) -> Vec<Rc<dyn GreynetFact>> + 'static,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .and_then(|typed_fact| {
                    if condition(typed_fact) {
                        Some(mapper(typed_fact))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| Vec::new())
        })
    }

    // =============================================================================
    // PERFORMANCE OPTIMIZED OPERATIONS
    // =============================================================================

    /// Create batch-optimized mapper for high-volume operations
    pub fn batch_mapper<T, F>(
        batch_processor: F,
    ) -> ZeroCopyMapperFn
    where
        T: GreynetFact,
        F: Fn(&[&T]) -> Vec<Vec<Rc<dyn GreynetFact>>> + 'static,
    {
        // Note: This is a simplified version. Full implementation would require 
        // coordination with the scheduling system for true batch processing.
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    // Process single item as batch of one
                    let results = batch_processor(&[typed_fact]);
                    results.into_iter().flatten().collect()
                })
                .unwrap_or_else(|| Vec::new())
        })
    }

    // =============================================================================
    // HELPER FUNCTIONS FOR COMMON PATTERNS
    // =============================================================================

    /// Create impact function that penalizes duplicate values
    pub fn uniqueness_constraint<T, F, K, S>(
        key_extractor: F,
        penalty_per_duplicate: f64,
    ) -> ZeroCopyImpactFn<S>
    where
        T: GreynetFact,
        F: Fn(&T) -> K + 'static,
        K: Hash + Eq + Clone + 'static,
        S: Score + crate::score::FromSoft,
    {
        let seen_values = Rc::new(std::cell::RefCell::new(HashMap::<K, usize>::new()));
        
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let key = key_extractor(typed_fact);
                    let mut seen = seen_values.borrow_mut();
                    let count = seen.entry(key).or_insert(0);
                    *count += 1;
                    
                    if *count > 1 {
                        S::soft(penalty_per_duplicate * (*count - 1) as f64)
                    } else {
                        S::null_score()
                    }
                })
                .unwrap_or_else(|| S::null_score())
        })
    }

    /// Create impact function for capacity constraints
    pub fn capacity_constraint<T, F, S>(
        capacity_checker: F,
        max_capacity: f64,
        penalty_per_unit_over: f64,
    ) -> ZeroCopyImpactFn<S>
    where
        T: GreynetFact,
        F: Fn(&T) -> f64 + 'static, // Returns current load/usage
        S: Score + crate::score::FromSoft,
    {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<T>())
                .map(|typed_fact| {
                    let current_load = capacity_checker(typed_fact);
                    if current_load > max_capacity {
                        let overflow = current_load - max_capacity;
                        S::soft(overflow * penalty_per_unit_over)
                    } else {
                        S::null_score()
                    }
                })
                .unwrap_or_else(|| S::null_score())
        })
    }
}