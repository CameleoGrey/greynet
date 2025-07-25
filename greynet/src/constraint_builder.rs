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
    
    pub fn build(self) -> Result<Session<S>> {
        let factory = Rc::try_unwrap(self.factory)
            .map_err(|_| crate::GreynetError::constraint_builder_error("ConstraintFactory has multiple owners"))?
            .into_inner();
        
        let mut session = factory.build_session()?;
        
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
}

impl<S: Score> Default for ConstraintBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}