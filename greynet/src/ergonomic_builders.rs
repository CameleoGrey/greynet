use crate::stream_def::{Stream, Arity1, Arity2};
use crate::nodes::{SharedKeyFn, SharedPredicate};
use crate::joiner::JoinerType;
use crate::{GreynetFact, Score, AnyTuple};
use std::rc::Rc;

/// Ergonomic helper functions for common constraint patterns
impl<S: Score> Stream<Arity1, S> {
    /// Convenience method for filtering by a field value
    pub fn where_field<T, F, V>(self, field_extractor: F, predicate: impl Fn(&V) -> bool + 'static) -> Self
    where
        T: GreynetFact,
        F: Fn(&T) -> &V + 'static,
        V: 'static,
    {
        self.filter(Rc::new(move |tuple: &AnyTuple| {
            if let AnyTuple::Uni(uni_tuple) = tuple {
                if let Some(fact) = uni_tuple.fact_a.as_any().downcast_ref::<T>() {
                    predicate(field_extractor(fact))
                } else {
                    false
                }
            } else {
                false
            }
        }))
    }

    /// Join with another stream using a field extractor
    pub fn join_on<T, U, F1, F2, K>(
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
        K: std::hash::Hash + Eq + 'static,
    {
        self.join(
            other,
            JoinerType::Equal,
            Rc::new(move |tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(fact) = uni_tuple.fact_a.as_any().downcast_ref::<T>() {
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&left_field(fact), &mut hasher);
                        std::hash::Hasher::finish(&hasher)
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
            Rc::new(move |tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(fact) = uni_tuple.fact_a.as_any().downcast_ref::<U>() {
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&right_field(fact), &mut hasher);
                        std::hash::Hasher::finish(&hasher)
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
        )
    }
}