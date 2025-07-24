//common_ops.rs
use super::*;
use std::any::Any;
use crate::prelude::Rc;
use crate::tuple::ZeroCopyFacts;
use crate::nodes::ZeroCopyPredicate;
use crate::nodes::ZeroCopyKeyFn;

/// Create a key function that extracts from the first fact's field
pub fn first_fact_field_key<T, F, K>(field_extractor: F) -> ZeroCopyKeyFn 
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
                std::hash::Hash::hash(&field_extractor(typed_fact), &mut hasher);
                std::hash::Hasher::finish(&hasher)
            })
            .unwrap_or(0)
    })
}

/// Create a predicate that checks a field in the first fact
pub fn first_fact_field_predicate<T, F>(field_check: F) -> ZeroCopyPredicate
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

/// Create a key function for composite keys from multiple facts
pub fn composite_key_fn(indices: Vec<usize>) -> ZeroCopyKeyFn {
    Rc::new(move |tuple: &dyn ZeroCopyFacts| {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for &index in &indices {
            if let Some(fact) = tuple.get_fact_ref(index) {
                std::hash::Hash::hash(&fact.hash_fact(), &mut hasher);
            }
        }
        std::hash::Hasher::finish(&hasher)
    })
}

/// Create a predicate that checks multiple facts
pub fn multi_fact_predicate<F>(check_fn: F) -> ZeroCopyPredicate
where
    F: Fn(&dyn ZeroCopyFacts) -> bool + 'static,
{
    Rc::new(move |tuple: &dyn ZeroCopyFacts| check_fn(tuple))
}