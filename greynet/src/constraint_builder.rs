// constraint_builder.rs

use crate::factory::ConstraintFactory;
use crate::stream_def::{Stream, Arity1, ConstraintRecipe};
use crate::{GreynetFact, Score, constraint::{ConstraintWeights, ConstraintId}, Result, ResourceLimits};
use crate::session::Session;
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;

/// Fluent constraint stream builder that enables a chained API for constraint definition.
/// This builder is created by `ConstraintBuilder::add_constraint` and holds the
/// context (the `ConstraintId`) for the stream that will be created.
pub struct ConstraintStreamBuilder<'a, S: Score> {
    factory: Rc<RefCell<ConstraintFactory<S>>>,
    constraint_id: ConstraintId,
    _phantom: PhantomData<&'a S>,
}

impl<'a, S: Score + 'static> ConstraintStreamBuilder<'a, S> {
    /// Creates a new `ConstraintStreamBuilder`.
    pub fn new(
        factory: Rc<RefCell<ConstraintFactory<S>>>,
        constraint_id: ConstraintId, 
    ) -> Self {
        Self {
            factory,
            constraint_id,
            _phantom: PhantomData,
        }
    }

    /// Initiates a new `Stream` from a fact type, injecting the constraint ID
    /// into the stream's context. This allows `penalize()` to be called later
    /// without arguments.
    pub fn for_each<T: GreynetFact + 'static>(self) -> Stream<Arity1, S> {
        let mut stream = ConstraintFactory::from::<T>(&self.factory);
        // Set the constraint ID in the stream's context to be used by penalize() later.
        stream.constraint_id_context = Some(self.constraint_id);
        stream
    }
}

/// High-level builder for constraint satisfaction problems.
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

    /// Starts a stream definition for any fact type, not tied to a specific constraint.
    #[inline]
    pub fn for_each<T: GreynetFact + 'static>(&self) -> Stream<Arity1, S> {
        ConstraintFactory::from::<T>(&self.factory)
    }
    
    /// Consumes the builder and constructs the `Session`.
    pub fn build(self) -> Result<Session<S>> {
        let factory = Rc::try_unwrap(self.factory)
            .map_err(|_| crate::GreynetError::constraint_builder_error("ConstraintFactory has multiple owners"))?
            .into_inner();
        
        factory.build_session()
    }

    /// Starts the definition of a new constraint with a fluent API.
    /// This is the recommended way to define constraints.
    pub fn add_constraint(&mut self, id: &str, weight: f64) -> ConstraintStreamBuilder<S> {
        // Set the weight and get a performant ID for the constraint name.
        self.weights.borrow().set_weight(id, weight);
        let constraint_id = self.weights.borrow().get_or_create_id(id);
        
        // Return the stream builder with the new context.
        ConstraintStreamBuilder::new(
            self.factory.clone(),
            constraint_id,
        )
    }

    /// Defines a constraint using a closure. This method is kept for backward
    /// compatibility but `add_constraint` is preferred.
    /// The closure now receives the `ConstraintId` to pass to `penalize()`.
    pub fn constraint(mut self, id: &str, weight: f64, recipe_fn: impl Fn(ConstraintId) -> ConstraintRecipe<S>) -> Self {
        self.weights.borrow().set_weight(id, weight);
        let constraint_id = self.weights.borrow().get_or_create_id(id);
        let recipe = recipe_fn(constraint_id);
        self.factory.borrow_mut().add_constraint_def(recipe);
        self
    }

    /// Defines a batch of constraints using closures.
    pub fn bulk_constraints(
        mut self,
        constraints: Vec<(&str, f64, Box<dyn Fn(ConstraintId) -> ConstraintRecipe<S>>)>,
    ) -> Self {
        for (id, weight, recipe_fn) in constraints {
            self.weights.borrow().set_weight(id, weight);
            let constraint_id = self.weights.borrow().get_or_create_id(id);
            let recipe = recipe_fn(constraint_id);
            self.factory.borrow_mut().add_constraint_def(recipe);
        }
        self
    }
}

impl<S: Score> Default for ConstraintBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}
