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

#[cfg(test)]
mod utils_tests {
    use super::*;

    use uuid::Uuid;

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct TestFact {
        id: Uuid,
        value: i32,
    }

    impl TestFact {
        fn new(value: i32) -> Self {
            Self {
                id: Uuid::new_v4(),
                value,
            }
        }

        fn with_id(id: Uuid, value: i32) -> Self {
            Self { id, value }
        }
    }

    impl GreynetFact for TestFact {
        fn fact_id(&self) -> Uuid {
            self.id
        }

        fn clone_fact(&self) -> Box<dyn GreynetFact> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct OtherFact {
        id: Uuid,
        name: String,
    }

    impl OtherFact {
        fn new(name: String) -> Self {
            Self {
                id: Uuid::new_v4(),
                name,
            }
        }
    }

    impl GreynetFact for OtherFact {
        fn fact_id(&self) -> Uuid {
            self.id
        }

        fn clone_fact(&self) -> Box<dyn GreynetFact> {
            Box::new(self.clone())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn create_test_facts() -> (
        Rc<dyn GreynetFact>,
        Rc<dyn GreynetFact>,
        Rc<dyn GreynetFact>,
    ) {
        (
            Rc::new(TestFact::new(1)),
            Rc::new(TestFact::new(2)),
            Rc::new(OtherFact::new("test".to_string())),
        )
    }

    #[test]
    fn test_tuple_utils_get_arity() {
        let (fact1, fact2, fact3) = create_test_facts();

        let uni_tuple = AnyTuple::Uni(UniTuple::new(fact1.clone()));
        assert_eq!(TupleUtils::get_arity(&uni_tuple), 1);

        let bi_tuple = AnyTuple::Bi(BiTuple::new(fact1.clone(), fact2.clone()));
        assert_eq!(TupleUtils::get_arity(&bi_tuple), 2);

        let tri_tuple = AnyTuple::Tri(TriTuple::new(fact1, fact2, fact3));
        assert_eq!(TupleUtils::get_arity(&tri_tuple), 3);
    }

    #[test]
    fn test_tuple_utils_get_facts() {
        let (fact1, fact2, _) = create_test_facts();

        let uni_tuple = AnyTuple::Uni(UniTuple::new(fact1.clone()));
        let facts = TupleUtils::get_facts(&uni_tuple);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].fact_id(), fact1.fact_id());

        let bi_tuple = AnyTuple::Bi(BiTuple::new(fact1.clone(), fact2.clone()));
        let facts = TupleUtils::get_facts(&bi_tuple);
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].fact_id(), fact1.fact_id());
        assert_eq!(facts[1].fact_id(), fact2.fact_id());
    }

    #[test]
    fn test_tuple_utils_create_tuple_from_facts() {
        let (fact1, fact2, fact3) = create_test_facts();

        // Test creating UniTuple
        let uni_result = TupleUtils::create_tuple_from_facts(vec![fact1.clone()]);
        assert!(uni_result.is_ok());
        assert_eq!(uni_result.unwrap().arity(), 1);

        // Test creating BiTuple
        let bi_result = TupleUtils::create_tuple_from_facts(vec![fact1.clone(), fact2.clone()]);
        assert!(bi_result.is_ok());
        assert_eq!(bi_result.unwrap().arity(), 2);

        // Test creating TriTuple
        let tri_result = TupleUtils::create_tuple_from_facts(vec![fact1, fact2, fact3]);
        assert!(tri_result.is_ok());
        assert_eq!(tri_result.unwrap().arity(), 3);
    }

    #[test]
    fn test_tuple_utils_create_tuple_empty_facts() {
        let result = TupleUtils::create_tuple_from_facts(vec![]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Cannot create tuple with arity 0"));
    }

    #[test]
    fn test_tuple_utils_create_tuple_unsupported_arity() {
        let facts: Vec<Rc<dyn GreynetFact>> = (0..6)
            .map(|i| Rc::new(TestFact::new(i)) as Rc<dyn GreynetFact>)
            .collect();

        let result = TupleUtils::create_tuple_from_facts(facts);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Cannot create tuple with arity 6"));
    }

    #[test]
    fn test_tuple_utils_create_all_supported_arities() {
        let facts: Vec<Rc<dyn GreynetFact>> = (0..5)
            .map(|i| Rc::new(TestFact::new(i)) as Rc<dyn GreynetFact>)
            .collect();

        // Test all supported arities (1-5)
        for arity in 1..=5 {
            let subset = facts[0..arity].to_vec();
            let result = TupleUtils::create_tuple_from_facts(subset);
            assert!(result.is_ok());
            assert_eq!(result.unwrap().arity(), arity);
        }
    }

    #[test]
    fn test_tuple_utils_combine_tuples() {
        let (fact1, fact2, fact3) = create_test_facts();

        let uni_tuple1 = AnyTuple::Uni(UniTuple::new(fact1));
        let uni_tuple2 = AnyTuple::Uni(UniTuple::new(fact2));
        let bi_tuple = AnyTuple::Bi(BiTuple::new(fact3.clone(), fact3.clone()));

        // Combine two UniTuples to get BiTuple
        let result = TupleUtils::combine_tuples(&uni_tuple1, &uni_tuple2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().arity(), 2);

        // Combine UniTuple and BiTuple to get TriTuple
        let result = TupleUtils::combine_tuples(&uni_tuple1, &bi_tuple);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().arity(), 3);
    }

    #[test]
    fn test_tuple_utils_combine_tuples_exceeds_max_arity() {
        let facts: Vec<Rc<dyn GreynetFact>> = (0..3)
            .map(|i| Rc::new(TestFact::new(i)) as Rc<dyn GreynetFact>)
            .collect();

        let tri_tuple1 = AnyTuple::Tri(TriTuple::new(
            facts[0].clone(),
            facts[1].clone(),
            facts[2].clone(),
        ));
        let tri_tuple2 = AnyTuple::Tri(TriTuple::new(
            facts[0].clone(),
            facts[1].clone(),
            facts[2].clone(),
        ));

        // Combining two TriTuples would result in arity 6, which is unsupported
        let result = TupleUtils::combine_tuples(&tri_tuple1, &tri_tuple2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Cannot create tuple with arity 6"));
    }

    #[test]
    fn test_tuple_utils_same_arity() {
        let (fact1, fact2, fact3) = create_test_facts();

        let uni_tuple1 = AnyTuple::Uni(UniTuple::new(fact1.clone()));
        let uni_tuple2 = AnyTuple::Uni(UniTuple::new(fact2.clone()));
        let bi_tuple = AnyTuple::Bi(BiTuple::new(fact1, fact2));
        let tri_tuple = AnyTuple::Tri(TriTuple::new(fact3.clone(), fact3.clone(), fact3));

        assert!(TupleUtils::same_arity(&uni_tuple1, &uni_tuple2));
        assert!(!TupleUtils::same_arity(&uni_tuple1, &bi_tuple));
        assert!(!TupleUtils::same_arity(&bi_tuple, &tri_tuple));
    }

    #[test]
    fn test_tuple_utils_get_tuple_arity() {
        let (fact1, fact2, fact3) = create_test_facts();

        let uni_tuple = AnyTuple::Uni(UniTuple::new(fact1.clone()));
        assert_eq!(TupleUtils::get_tuple_arity(&uni_tuple), TupleArity::One);

        let bi_tuple = AnyTuple::Bi(BiTuple::new(fact1.clone(), fact2.clone()));
        assert_eq!(TupleUtils::get_tuple_arity(&bi_tuple), TupleArity::Two);

        let tri_tuple = AnyTuple::Tri(TriTuple::new(fact1, fact2, fact3));
        assert_eq!(TupleUtils::get_tuple_arity(&tri_tuple), TupleArity::Three);
    }

    #[test]
    fn test_tuple_utils_preserve_fact_identity() {
        let fact1 = Rc::new(TestFact::new(42));
        let fact2 = Rc::new(TestFact::new(24));

        // Create tuple from facts
        let tuple =
            TupleUtils::create_tuple_from_facts(vec![fact1.clone(), fact2.clone()]).unwrap();

        // Extract facts back
        let extracted_facts = TupleUtils::get_facts(&tuple);

        // Verify that fact identities are preserved
        assert_eq!(extracted_facts.len(), 2);
        assert_eq!(extracted_facts[0].fact_id(), fact1.fact_id());
        assert_eq!(extracted_facts[1].fact_id(), fact2.fact_id());

        // Verify that the Rc pointers are the same (not just equal facts)
        assert!(Rc::ptr_eq(
            &extracted_facts[0],
            &(fact1 as Rc<dyn GreynetFact>)
        ));
        assert!(Rc::ptr_eq(
            &extracted_facts[1],
            &(fact2 as Rc<dyn GreynetFact>)
        ));
    }
}
