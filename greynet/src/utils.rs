// utils.rs - Tuple manipulation utilities
use crate::{
    AnyTuple, BiTuple, GreynetFact, PentaTuple, QuadTuple, TriTuple, TupleArity, UniTuple,
};
use std::rc::Rc;

/// Utilities for working with tuples
pub struct TupleUtils;

impl TupleUtils {
    /// Gets the arity of a tuple
    pub fn get_arity(tuple: &AnyTuple) -> usize {
        tuple.arity()
    }

    /// Extracts all facts from a tuple
    pub fn get_facts(tuple: &AnyTuple) -> Vec<Rc<dyn GreynetFact>> {
        tuple.facts()
    }

    /// Creates a tuple from a list of facts
    pub fn create_tuple_from_facts(facts: Vec<Rc<dyn GreynetFact>>) -> Result<AnyTuple, String> {
        match facts.len() {
            1 => Ok(AnyTuple::Uni(UniTuple::new(facts[0].clone()))),
            2 => Ok(AnyTuple::Bi(BiTuple::new(
                facts[0].clone(),
                facts[1].clone(),
            ))),
            3 => Ok(AnyTuple::Tri(TriTuple::new(
                facts[0].clone(),
                facts[1].clone(),
                facts[2].clone(),
            ))),
            4 => Ok(AnyTuple::Quad(QuadTuple::new(
                facts[0].clone(),
                facts[1].clone(),
                facts[2].clone(),
                facts[3].clone(),
            ))),
            5 => Ok(AnyTuple::Penta(PentaTuple::new(
                facts[0].clone(),
                facts[1].clone(),
                facts[2].clone(),
                facts[3].clone(),
                facts[4].clone(),
            ))),
            n => Err(format!(
                "Cannot create tuple with arity {}. Supported arities are 1-5.",
                n
            )),
        }
    }

    /// Combines two tuples into a larger tuple
    pub fn combine_tuples(left: &AnyTuple, right: &AnyTuple) -> Result<AnyTuple, String> {
        let mut combined_facts = left.facts();
        combined_facts.extend(right.facts());
        Self::create_tuple_from_facts(combined_facts)
    }

    /// Checks if two tuples have the same arity
    pub fn same_arity(tuple1: &AnyTuple, tuple2: &AnyTuple) -> bool {
        tuple1.arity() == tuple2.arity()
    }

    /// Gets the tuple arity as an enum
    pub fn get_tuple_arity(tuple: &AnyTuple) -> TupleArity {
        tuple.tuple_arity()
    }
}