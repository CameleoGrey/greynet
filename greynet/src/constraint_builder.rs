//constraint_builder.rs
use crate::factory::ConstraintFactory;
use crate::stream_def::{Stream, Arity1, ConstraintRecipe};
use crate::{GreynetFact, Score, constraint::ConstraintWeights};
use crate::session::Session;
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use crate::stream_def::Arity2;
use crate::JoinerType;
use crate::AnyTuple;

#[derive(Debug)]
pub struct ConstraintBuilder<S: Score> {
    factory: Rc<RefCell<ConstraintFactory<S>>>,
    weights: Rc<RefCell<ConstraintWeights>>,
    _phantom: PhantomData<S>,
}

impl<S: Score + 'static> ConstraintBuilder<S> {
    pub fn new() -> Self {
        let weights = Rc::new(RefCell::new(ConstraintWeights::new()));
        let factory = Rc::new(RefCell::new(ConstraintFactory::new(Rc::clone(&weights))));
        Self {
            factory,
            weights,
            _phantom: PhantomData,
        }
    }

    pub fn for_each<T: GreynetFact + 'static>(&self) -> Stream<Arity1, S> {
        ConstraintFactory::from::<T>(&self.factory)
    }

    pub fn for_each_unique_pair<T: GreynetFact + 'static>(&self) -> Stream<Arity2, S> {
        let stream1 = self.for_each::<T>();
        let stream2 = self.for_each::<T>();
        
        stream1.join(
            stream2,
            JoinerType::LessThan, // Ensures (a,b) is processed but not (b,a)
            Rc::new(|tuple: &AnyTuple| {
                // Use the fact's unique hash for comparison
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

    pub fn constraint(&self, id: &str, weight: f64, recipe_fn: impl Fn() -> ConstraintRecipe<S>) -> &Self {
        self.weights.borrow().set_weight(id.to_string(), weight);
        let recipe = recipe_fn();
        self.factory.borrow_mut().add_constraint_def(recipe);
        self
    }

    pub fn build(self) -> Session<S> {
        let factory = Rc::try_unwrap(self.factory)
            .expect("ConstraintFactory has multiple owners")
            .into_inner();
        factory.build_session()
    }
}

impl<S: Score> Default for ConstraintBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}