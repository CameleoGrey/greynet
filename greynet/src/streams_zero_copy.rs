//! Zero-copy stream operations for high-performance constraint building

use crate::{UniTuple, BiTuple, TriTuple, QuadTuple, PentaTuple};
use crate::stream_def::{Stream, Arity1, Arity2, Arity3, Arity4, Arity5};
use crate::stream_def::{StreamDefinition, FilterDefinition, JoinDefinition, ConditionalJoinDefinition, GroupDefinition};
use crate::factory::ConstraintFactory;
use crate::joiner::JoinerType;
use crate::tuple::{AnyTuple, ZeroCopyFacts};
use crate::collectors::BaseCollector;
use crate::{Score, GreynetFact};
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::any::Any;

//// High-performance key function type using zero-copy access
pub type ZeroCopyKeyFn = Rc<dyn Fn(&dyn ZeroCopyFacts) -> u64>;

/// High-performance predicate type using zero-copy access  
pub type ZeroCopyPredicate = Rc<dyn Fn(&dyn ZeroCopyFacts) -> bool>;

/// High-performance mapper function using zero-copy access
pub type ZeroCopyMapperFn = Rc<dyn Fn(&dyn ZeroCopyFacts) -> Vec<Rc<dyn GreynetFact>>>;

/// Extension trait for streams to use zero-copy operations
pub trait ZeroCopyStreamOps<A, S: Score + 'static> {
    /// Filter using zero-copy predicate (much faster than current filter)
    fn filter_zero_copy(self, predicate: ZeroCopyPredicate) -> Self;
    
    /// Map using zero-copy access (much faster than current map)
    fn map_zero_copy(self, mapper: Rc<dyn Fn(&dyn ZeroCopyFacts) -> Rc<dyn GreynetFact>>) -> Self;
    
    /// FlatMap using zero-copy access (much faster than current flat_map)
    fn flat_map_zero_copy(self, mapper: ZeroCopyMapperFn) -> Self;
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

/// Implementation for all stream arities
impl<A, S: Score + 'static> ZeroCopyStreamOps<A, S> for Stream<A, S> {
    fn filter_zero_copy(self, predicate: ZeroCopyPredicate) -> Self {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        
        // Convert zero-copy predicate to traditional SharedPredicate for factory registration
        let traditional_predicate = convert_zero_copy_predicate_to_traditional(predicate);
        let predicate_id = factory_rc.borrow_mut().register_predicate(traditional_predicate);
        
        let filter_def = FilterDefinition {
            source: Box::new(self.definition),
            predicate_id,
            _phantom: PhantomData,
        };
        
        Stream::new(StreamDefinition::Filter(filter_def), self.factory)
    }
    
    fn map_zero_copy(self, mapper: Rc<dyn Fn(&dyn ZeroCopyFacts) -> Rc<dyn GreynetFact>>) -> Self {
        let flat_mapper: ZeroCopyMapperFn = Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            vec![mapper(tuple)]
        });
        self.flat_map_zero_copy(flat_mapper)
    }
    
    fn flat_map_zero_copy(self, mapper: ZeroCopyMapperFn) -> Self {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        
        // Convert zero-copy mapper to traditional SharedMapperFn
        let traditional_mapper = convert_zero_copy_mapper_to_traditional(mapper);
        let mapper_id = factory_rc.borrow_mut().register_mapper(traditional_mapper);
        
        let flatmap_def = crate::stream_def::FlatMapDefinition {
            source: Box::new(self.definition),
            mapper_fn_id: mapper_id,
            _phantom: PhantomData,
        };
        
        Stream::new(StreamDefinition::FlatMap(flatmap_def), self.factory)
    }
}

/// Generic helpers for all stream arities
impl<A, S: Score + 'static> Stream<A, S> {
    /// Generic helper for conditional joins, available to all arities.
    fn if_conditionally_zero_copy<B>(
        self,
        other: Stream<B, S>,
        should_exist: bool,
        left_key_fn: ZeroCopyKeyFn,
        right_key_fn: ZeroCopyKeyFn,
    ) -> Self {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let traditional_left_key = convert_zero_copy_key_to_traditional(left_key_fn);
        let traditional_right_key = convert_zero_copy_key_to_traditional(right_key_fn);
        
        let left_key_fn_id = factory.register_key_fn(traditional_left_key);
        let right_key_fn_id = factory.register_key_fn(traditional_right_key);
        
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


/// Implementation for arity-1 streams (most common case)
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
        
        // Convert zero-copy key functions to traditional SharedKeyFn for factory registration
        let traditional_left_key = convert_zero_copy_key_to_traditional(left_key_fn);
        let traditional_right_key = convert_zero_copy_key_to_traditional(right_key_fn);
        
        let left_key_fn_id = factory.register_key_fn(traditional_left_key);
        let right_key_fn_id = factory.register_key_fn(traditional_right_key);
        
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
        
        // Convert zero-copy key function to traditional
        let traditional_key_fn = convert_zero_copy_key_to_traditional(key_fn);
        let key_fn_id = factory_rc.borrow_mut().register_key_fn(traditional_key_fn);
        
        let group_def = GroupDefinition {
            source: Box::new(self.definition),
            key_fn_id,
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
        let left_key_fn = field_key_extractor::<T, F1, K>(left_field);
        let right_key_fn = field_key_extractor::<U, F2, K>(right_field);
        
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
        let zero_copy_predicate = field_predicate::<T, F, V>(field_extractor, predicate);
        self.filter_zero_copy(zero_copy_predicate)
    }
}

/// Higher-arity stream implementations with zero-copy joins
impl<S: Score + 'static> Stream<Arity2, S> {
    pub fn join_zero_copy(
        self, 
        other: Stream<Arity1, S>, 
        joiner_type: JoinerType,
        left_key_fn: ZeroCopyKeyFn, 
        right_key_fn: ZeroCopyKeyFn
    ) -> Stream<Arity3, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let traditional_left_key = convert_zero_copy_key_to_traditional(left_key_fn);
        let traditional_right_key = convert_zero_copy_key_to_traditional(right_key_fn);
        
        let left_key_fn_id = factory.register_key_fn(traditional_left_key);
        let right_key_fn_id = factory.register_key_fn(traditional_right_key);
        
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

impl<S: Score + 'static> Stream<Arity3, S> {
    pub fn join_zero_copy(
        self, 
        other: Stream<Arity1, S>, 
        joiner_type: JoinerType,
        left_key_fn: ZeroCopyKeyFn, 
        right_key_fn: ZeroCopyKeyFn
    ) -> Stream<Arity4, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let traditional_left_key = convert_zero_copy_key_to_traditional(left_key_fn);
        let traditional_right_key = convert_zero_copy_key_to_traditional(right_key_fn);
        
        let left_key_fn_id = factory.register_key_fn(traditional_left_key);
        let right_key_fn_id = factory.register_key_fn(traditional_right_key);
        
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

impl<S: Score + 'static> Stream<Arity4, S> {
    pub fn join_zero_copy(
        self, 
        other: Stream<Arity1, S>, 
        joiner_type: JoinerType,
        left_key_fn: ZeroCopyKeyFn, 
        right_key_fn: ZeroCopyKeyFn
    ) -> Stream<Arity5, S> {
        let factory_rc = self.factory.upgrade().expect("ConstraintFactory has been dropped");
        let mut factory = factory_rc.borrow_mut();
        
        let traditional_left_key = convert_zero_copy_key_to_traditional(left_key_fn);
        let traditional_right_key = convert_zero_copy_key_to_traditional(right_key_fn);
        
        let left_key_fn_id = factory.register_key_fn(traditional_left_key);
        let right_key_fn_id = factory.register_key_fn(traditional_right_key);
        
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
    
    /// Create a zero-copy key function from a field extractor
    pub fn field_key<T, F, K>(field_extractor: F) -> ZeroCopyKeyFn 
    where
        T: GreynetFact,
        F: Fn(&T) -> K + 'static,
        K: std::hash::Hash + 'static,
    {
        field_key_extractor(field_extractor)
    }
    
    /// Create a zero-copy predicate from a field check
    pub fn field_check<T, F>(field_check: F) -> ZeroCopyPredicate
    where
        T: GreynetFact,
        F: Fn(&T) -> bool + 'static,
    {
        field_predicate_simple(field_check)
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
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::{Hash, Hasher};
            
            for &index in &indices {
                if let Some(fact) = tuple.get_fact_ref(index) {
                    fact.hash_fact().hash(&mut hasher);
                }
            }
            hasher.finish()
        })
    }
    
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
}

/// Helper functions for converting between zero-copy and traditional function types
fn convert_zero_copy_key_to_traditional(zero_copy_fn: ZeroCopyKeyFn) -> crate::nodes::SharedKeyFn {
    Rc::new(move |tuple: &AnyTuple| zero_copy_fn(tuple))
}

fn convert_zero_copy_predicate_to_traditional(zero_copy_fn: ZeroCopyPredicate) -> crate::nodes::SharedPredicate {
    Rc::new(move |tuple: &AnyTuple| zero_copy_fn(tuple))
}

fn convert_zero_copy_mapper_to_traditional(zero_copy_fn: ZeroCopyMapperFn) -> crate::nodes::SharedMapperFn {
    Rc::new(move |tuple: &AnyTuple| zero_copy_fn(tuple))
}

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

/// Migration utilities to help users transition from old to new API
pub mod migration {
    use super::*;
    
    // Convert traditional SharedKeyFn to zero-copy version with optimized concrete type handling
    pub fn upgrade_key_fn(traditional_fn: crate::nodes::SharedKeyFn) -> ZeroCopyKeyFn {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            // Try concrete types first for better performance
            if let Some(uni_tuple) = tuple.as_any().downcast_ref::<UniTuple>() {
                let any_tuple = AnyTuple::Uni(uni_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(bi_tuple) = tuple.as_any().downcast_ref::<BiTuple>() {
                let any_tuple = AnyTuple::Bi(bi_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(tri_tuple) = tuple.as_any().downcast_ref::<TriTuple>() {
                let any_tuple = AnyTuple::Tri(tri_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(quad_tuple) = tuple.as_any().downcast_ref::<QuadTuple>() {
                let any_tuple = AnyTuple::Quad(quad_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(penta_tuple) = tuple.as_any().downcast_ref::<PentaTuple>() {
                let any_tuple = AnyTuple::Penta(penta_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            // Fallback to AnyTuple if it's already that type
            if let Some(any_tuple) = tuple.as_any().downcast_ref::<AnyTuple>() {
                return traditional_fn(any_tuple);
            }
            
            // Last resort: reconstruct from facts (slower but works)
            let facts: Vec<Rc<dyn GreynetFact>> = (0..tuple.arity())
                .filter_map(|i| tuple.get_fact_ref(i))
                .map(|fact_ref| fact_ref.clone_fact().into())
                .collect();
            
            match crate::utils::TupleUtils::create_tuple_from_facts(facts) {
                Ok(any_tuple) => traditional_fn(&any_tuple),
                Err(_) => 0,
            }
        })
    }
    
    /// Convert traditional SharedPredicate to zero-copy version with optimized concrete type handling
    pub fn upgrade_predicate(traditional_fn: crate::nodes::SharedPredicate) -> ZeroCopyPredicate {
        Rc::new(move |tuple: &dyn ZeroCopyFacts| {
            // Try concrete types first for better performance
            if let Some(uni_tuple) = tuple.as_any().downcast_ref::<UniTuple>() {
                let any_tuple = AnyTuple::Uni(uni_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(bi_tuple) = tuple.as_any().downcast_ref::<BiTuple>() {
                let any_tuple = AnyTuple::Bi(bi_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(tri_tuple) = tuple.as_any().downcast_ref::<TriTuple>() {
                let any_tuple = AnyTuple::Tri(tri_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(quad_tuple) = tuple.as_any().downcast_ref::<QuadTuple>() {
                let any_tuple = AnyTuple::Quad(quad_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            if let Some(penta_tuple) = tuple.as_any().downcast_ref::<PentaTuple>() {
                let any_tuple = AnyTuple::Penta(penta_tuple.clone());
                return traditional_fn(&any_tuple);
            }
            
            // Fallback to AnyTuple if it's already that type
            if let Some(any_tuple) = tuple.as_any().downcast_ref::<AnyTuple>() {
                return traditional_fn(any_tuple);
            }
            
            // Last resort: reconstruct from facts (slower but works)
            let facts: Vec<Rc<dyn GreynetFact>> = (0..tuple.arity())
                .filter_map(|i| tuple.get_fact_ref(i))
                .map(|fact_ref| fact_ref.clone_fact().into())
                .collect();
            
            match crate::utils::TupleUtils::create_tuple_from_facts(facts) {
                Ok(any_tuple) => traditional_fn(&any_tuple),
                Err(_) => false,
            }
        })
    }
    
    /// Helper functions for zero-copy operations on specific tuple types
    pub mod concrete_ops {
        use super::*;
        
        /// Create a zero-copy key function that works optimally with UniTuple
        pub fn uni_tuple_key<F>(extractor: F) -> ZeroCopyKeyFn 
        where
            F: Fn(&dyn GreynetFact) -> u64 + 'static,
        {
            Rc::new(move |tuple: &dyn ZeroCopyFacts| {
                if let Some(uni_tuple) = tuple.as_any().downcast_ref::<UniTuple>() {
                    extractor(uni_tuple.fact_a.as_ref())
                } else if let Some(fact) = tuple.first_fact() {
                    extractor(fact)
                } else {
                    0
                }
            })
        }
        
        /// Create a zero-copy predicate that works optimally with UniTuple
        pub fn uni_tuple_predicate<F>(checker: F) -> ZeroCopyPredicate 
        where
            F: Fn(&dyn GreynetFact) -> bool + 'static,
        {
            Rc::new(move |tuple: &dyn ZeroCopyFacts| {
                if let Some(uni_tuple) = tuple.as_any().downcast_ref::<UniTuple>() {
                    checker(uni_tuple.fact_a.as_ref())
                } else if let Some(fact) = tuple.first_fact() {
                    checker(fact)
                } else {
                    false
                }
            })
        }
        
        /// Create a zero-copy key function that works optimally with BiTuple
        pub fn bi_tuple_composite_key<F1, F2>(
            first_extractor: F1, 
            second_extractor: F2
        ) -> ZeroCopyKeyFn 
        where
            F1: Fn(&dyn GreynetFact) -> u64 + 'static,
            F2: Fn(&dyn GreynetFact) -> u64 + 'static,
        {
            Rc::new(move |tuple: &dyn ZeroCopyFacts| {
                if let Some(bi_tuple) = tuple.as_any().downcast_ref::<BiTuple>() {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    use std::hash::{Hash, Hasher};
                    
                    first_extractor(bi_tuple.fact_a.as_ref()).hash(&mut hasher);
                    second_extractor(bi_tuple.fact_b.as_ref()).hash(&mut hasher);
                    hasher.finish()
                } else {
                    // Fallback for non-BiTuple
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    use std::hash::{Hash, Hasher};
                    
                    if let Some(first) = tuple.get_fact_ref(0) {
                        first_extractor(first).hash(&mut hasher);
                    }
                    if let Some(second) = tuple.get_fact_ref(1) {
                        second_extractor(second).hash(&mut hasher);
                    }
                    hasher.finish()
                }
            })
        }
    }
    
    /// Utility for checking if a ZeroCopyFacts is a specific concrete type
    pub trait ConcreteTypeCheck {
        fn is_uni_tuple(&self) -> bool;
        fn is_bi_tuple(&self) -> bool;
        fn is_tri_tuple(&self) -> bool;
        fn is_quad_tuple(&self) -> bool;
        fn is_penta_tuple(&self) -> bool;
        
        fn as_uni_tuple(&self) -> Option<&UniTuple>;
        fn as_bi_tuple(&self) -> Option<&BiTuple>;
        fn as_tri_tuple(&self) -> Option<&TriTuple>;
        fn as_quad_tuple(&self) -> Option<&QuadTuple>;
        fn as_penta_tuple(&self) -> Option<&PentaTuple>;
    }
    
    impl ConcreteTypeCheck for dyn ZeroCopyFacts {
        fn is_uni_tuple(&self) -> bool {
            self.as_any().downcast_ref::<UniTuple>().is_some()
        }
        
        fn is_bi_tuple(&self) -> bool {
            self.as_any().downcast_ref::<BiTuple>().is_some()
        }
        
        fn is_tri_tuple(&self) -> bool {
            self.as_any().downcast_ref::<TriTuple>().is_some()
        }
        
        fn is_quad_tuple(&self) -> bool {
            self.as_any().downcast_ref::<QuadTuple>().is_some()
        }
        
        fn is_penta_tuple(&self) -> bool {
            self.as_any().downcast_ref::<PentaTuple>().is_some()
        }
        
        fn as_uni_tuple(&self) -> Option<&UniTuple> {
            self.as_any().downcast_ref::<UniTuple>()
        }
        
        fn as_bi_tuple(&self) -> Option<&BiTuple> {
            self.as_any().downcast_ref::<BiTuple>()
        }
        
        fn as_tri_tuple(&self) -> Option<&TriTuple> {
            self.as_any().downcast_ref::<TriTuple>()
        }
        
        fn as_quad_tuple(&self) -> Option<&QuadTuple> {
            self.as_any().downcast_ref::<QuadTuple>()
        }
        
        fn as_penta_tuple(&self) -> Option<&PentaTuple> {
            self.as_any().downcast_ref::<PentaTuple>()
        }
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
            // Use the existing Rc<RefCell<ConstraintFactory<S>>> directly
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