//constraint_builder.rs
use crate::factory::ConstraintFactory;
use crate::stream_def::{Stream, Arity1, ConstraintRecipe};
use crate::{GreynetFact, Score, constraint::ConstraintWeights, Result, ResourceLimits};
use crate::session::Session;
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use crate::stream_def::Arity2;
use crate::JoinerType;
use crate::AnyTuple;
use crate::zero_copy_ops;
use crate::streams_zero_copy::ZeroCopyJoinOps;
use crate::streams_zero_copy::ZeroCopyStreamOps;

/// High-level builder for constraint satisfaction problems with comprehensive error handling
#[derive(Debug)]
pub struct ConstraintBuilder<S: Score> {
    pub factory: Rc<RefCell<ConstraintFactory<S>>>,
    pub weights: Rc<RefCell<ConstraintWeights>>,
    pub _phantom: PhantomData<S>,
}

impl<S: Score + 'static> ConstraintBuilder<S> {
    pub fn new() -> Self {
        Self::with_limits(ResourceLimits::default())
    }
    
    pub fn with_limits(limits: ResourceLimits) -> Self {
        let weights = Rc::new(RefCell::new(ConstraintWeights::new()));
        let factory = Rc::new(RefCell::new(ConstraintFactory::with_limits(Rc::clone(&weights), limits)));
        Self {
            factory,
            weights,
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn for_each<T: GreynetFact + 'static>(&self) -> Stream<Arity1, S> {
        ConstraintFactory::from::<T>(&self.factory)
    }

    pub fn for_each_unique_pair<T: GreynetFact + 'static>(&self) -> Stream<Arity2, S> {
        let stream1 = self.for_each::<T>();
        let stream2 = self.for_each::<T>();
        
        stream1.join(
            stream2,
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

    /// Add SIMD configuration option
    pub fn with_simd_optimization(self, enable: bool) -> Self {
        // This could store configuration in the factory for SIMD optimization
        // For now, we'll use this as a marker for SIMD-aware building
        self
    }
    
    /// Enhanced build method that uses SIMD-optimized session
    pub fn build(self) -> Result<Session<S>> {
        let factory = Rc::try_unwrap(self.factory)
            .map_err(|_| crate::GreynetError::constraint_builder_error("ConstraintFactory has multiple owners"))?
            .into_inner();
        
        let mut session = factory.build_session()?;
        
        // Pre-warm SIMD if available
        #[cfg(feature = "simd")]
        {
            // Could do SIMD feature detection here
            if cfg!(target_arch = "x86_64") {
            }
        }
        
        Ok(session)
    }

    /// Fixed constraint method to return Self for proper chaining
    #[inline]
    pub fn constraint(mut self, id: &str, weight: f64, recipe_fn: impl Fn() -> ConstraintRecipe<S>) -> Self {
        self.weights.borrow_mut().set_weight(id.to_string(), weight);
        let recipe = recipe_fn();
        self.factory.borrow_mut().add_constraint_def(recipe);
        self
    }

    /// Fixed bulk_constraints method
    pub fn bulk_constraints(
        mut self,
        constraints: Vec<(&str, f64, Box<dyn Fn() -> ConstraintRecipe<S>>)>,
    ) -> Self {
        for (id, weight, recipe_fn) in constraints {
            self.weights.borrow_mut().set_weight(id.to_string(), weight);
            let recipe = recipe_fn();
            self.factory.borrow_mut().add_constraint_def(recipe);
        }
        self
    }
    
    /// Alternative bulk_constraints with closure support
    pub fn bulk_constraints_with_closures<F>(
        mut self,
        constraints: Vec<(&str, f64, F)>,
    ) -> Self 
    where
        F: Fn() -> ConstraintRecipe<S> + Clone,
    {
        for (id, weight, recipe_fn) in constraints {
            self.weights.borrow_mut().set_weight(id.to_string(), weight);
            let recipe = recipe_fn();
            self.factory.borrow_mut().add_constraint_def(recipe);
        }
        self
    }
    
    /// Convenience method for creating filtered fact streams
    pub fn filtered_facts<T: GreynetFact + 'static, F>(
        &self, 
        predicate: F
    ) -> Stream<Arity1, S>
    where
        F: Fn(&T) -> bool + 'static,
    {
        use crate::streams_zero_copy::{ZeroCopyStreamOps, zero_copy_ops};
        self.for_each::<T>()
            .filter_zero_copy(zero_copy_ops::field_check(predicate))
    }
    
    /// Convenience method for creating joined streams with field access
    pub fn join_on_fields<T, U, F1, F2, K>(
        &self,
        left_field: F1,
        right_field: F2,
    ) -> Stream<Arity2, S>
    where
        T: GreynetFact + 'static,
        U: GreynetFact + 'static,
        F1: Fn(&T) -> K + 'static,
        F2: Fn(&U) -> K + 'static,
        K: std::hash::Hash + 'static,
    {
        use crate::streams_zero_copy::ZeroCopyJoinOps;
        self.for_each::<T>()
            .join_on_field(self.for_each::<U>(), left_field, right_field)
    }
    
    /// Create a high-performance unique pairs stream
    pub fn unique_pairs<T: GreynetFact + 'static>(&self) -> Stream<Arity2, S> {
        use crate::streams_zero_copy::{ZeroCopyJoinOps, zero_copy_ops};
        let stream1 = self.for_each::<T>();
        let stream2 = self.for_each::<T>();
        
        stream1.join_zero_copy(
            stream2,
            JoinerType::LessThan,
            zero_copy_ops::first_fact_hash(),
            zero_copy_ops::first_fact_hash(),
        )
    }
}

impl<S: Score> Default for ConstraintBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}