// fact.rs - GreynetFact trait
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

/// Core trait for all facts in the Greynet system.
///
/// This trait must be implemented by all facts that can be used in the constraint system.
/// The trait is object-safe to allow storage in `Rc<dyn GreynetFact>`.
pub trait GreynetFact: Debug + 'static {
    /// Returns a unique identifier for the fact instance.
    fn fact_id(&self) -> Uuid;
    /// Creates a boxed clone of the fact.
    fn clone_fact(&self) -> Box<dyn GreynetFact>;
    /// Provides a hash for the fact, defaulting to hashing the `fact_id`.
    fn hash_fact(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.fact_id().hash(&mut hasher);
        hasher.finish()
    }
    /// Compares this fact to another, defaulting to comparing `fact_id`.
    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        self.fact_id() == other.fact_id()
    }
    /// Returns the fact as a `&dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;
}

/// Helper functions for working with GreynetFact trait objects
pub mod fact_utils {
    use super::*;
    use std::rc::Rc;

    /// Clone a trait object into a new Rc
    pub fn clone_fact_rc(fact: &Rc<dyn GreynetFact>) -> Rc<dyn GreynetFact> {
        Rc::from(fact.clone_fact())
    }

    /// Downcast a trait object to a concrete type
    pub fn downcast_fact<T: GreynetFact>(fact: &dyn GreynetFact) -> Option<&T> {
        fact.as_any().downcast_ref::<T>()
    }

    /// Create a hash-compatible wrapper for trait objects
    #[derive(Hash, PartialEq, Eq, Clone)]
    pub struct FactKey {
        pub id: uuid::Uuid,
        pub hash: u64,
    }

    impl FactKey {
        pub fn new(fact: &dyn GreynetFact) -> Self {
            Self {
                id: fact.fact_id(),
                hash: fact.hash_fact(),
            }
        }
    }
}

// state.rs - Tuple state management
/// Represents the lifecycle state of a tuple in the RETE network.
///
/// This enum tracks whether a tuple is being created, is stable,
/// is being updated, or is being removed from the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TupleState {
    /// Tuple is being created and hasn't been processed yet
    Creating,
    /// Tuple is stable and active in the network
    Ok,
    /// Tuple is being updated (retract + reinsert)
    Updating,
    /// Tuple is being removed from the network
    Dying,
    /// Tuple creation was aborted before processing
    Aborting,
    /// Tuple is dead and can be recycled
    Dead,
}

impl TupleState {
    /// Returns true if the tuple is in a transitional state that requires processing.
    pub fn is_dirty(&self) -> bool {
        matches!(
            self,
            TupleState::Creating | TupleState::Updating | TupleState::Dying
        )
    }
}

impl Default for TupleState {
    fn default() -> Self {
        TupleState::Dead
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        AnyTuple, BiTuple, PentaTuple, QuadTuple, TriTuple, TupleArity, TupleState, UniTuple,
    };
    use super::*;
    use std::rc::Rc;
    use uuid::Uuid;

    // Test fact types implementing GreynetFact manually
    #[derive(Clone, Debug)]
    struct TestFact {
        id: Uuid,
        value: i32,
    }

    impl PartialEq for TestFact {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    impl Eq for TestFact {}

    impl std::hash::Hash for TestFact {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.id.hash(state);
        }
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

    #[derive(Clone, Debug)]
    struct OtherFact {
        id: Uuid,
        name: String,
    }

    impl PartialEq for OtherFact {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    impl Eq for OtherFact {}

    impl std::hash::Hash for OtherFact {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.id.hash(state);
        }
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

    #[test]
    fn test_tuple_creation() {
        let fact1 = Rc::new(TestFact::new(42));
        let fact2 = Rc::new(OtherFact::new("test".to_string()));

        // Test UniTuple
        let uni = UniTuple::new(fact1.clone());
        assert_eq!(uni.facts().len(), 1);
        assert_eq!(uni.arity(), 1);

        // Test BiTuple
        let bi = BiTuple::new(fact1.clone(), fact2.clone());
        assert_eq!(bi.facts().len(), 2);
        assert_eq!(bi.arity(), 2);
    }

    #[test]
    fn test_any_tuple() {
        let fact1 = Rc::new(TestFact::new(42));
        let fact2 = Rc::new(OtherFact::new("test".to_string()));

        // Test pattern matching
        let uni = AnyTuple::Uni(UniTuple::new(fact1.clone()));
        assert_eq!(uni.arity(), 1);
        assert_eq!(uni.facts().len(), 1);

        let bi = AnyTuple::Bi(BiTuple::new(fact1.clone(), fact2.clone()));
        assert_eq!(bi.arity(), 2);
        assert_eq!(bi.facts().len(), 2);

        // Test state management
        let mut any_tuple = uni;
        assert_eq!(any_tuple.state(), TupleState::Dead);

        any_tuple.set_state(TupleState::Creating);
        assert_eq!(any_tuple.state(), TupleState::Creating);
    }

    #[test]
    fn test_tuple_state() {
        let state = TupleState::Creating;
        assert!(state.is_dirty());

        let state = TupleState::Ok;
        assert!(!state.is_dirty());

        let state = TupleState::Dying;
        assert!(state.is_dirty());
    }

    #[test]
    fn test_tuple_reset() {
        let fact1 = Rc::new(TestFact::new(42));

        let mut tuple = AnyTuple::Uni(UniTuple::new(fact1));
        tuple.set_state(TupleState::Creating);

        tuple.reset();
        assert_eq!(tuple.state(), TupleState::Dead);
        assert!(tuple.node().is_none());
    }

    #[test]
    fn test_fact_equality_and_hashing() {
        // Facts with same values but different IDs should not be equal
        let fact1 = TestFact::new(42);
        let fact2 = TestFact::new(42);
        assert_ne!(fact1, fact2);
        assert_ne!(fact1.fact_id(), fact2.fact_id());

        // Facts with same ID should be equal
        let id = Uuid::new_v4();
        let fact3 = TestFact::with_id(id, 100);
        let fact4 = TestFact::with_id(id, 200); // Different value, same ID
        assert_eq!(fact3, fact4); // Equal because same ID
        assert_eq!(fact3.fact_id(), fact4.fact_id());

        // Test the trait object methods
        let fact1_trait: &dyn GreynetFact = &fact1;
        let fact2_trait: &dyn GreynetFact = &fact2;
        let fact3_trait: &dyn GreynetFact = &fact3;
        let fact4_trait: &dyn GreynetFact = &fact4;

        assert!(!fact1_trait.eq_fact(fact2_trait));
        assert!(fact3_trait.eq_fact(fact4_trait));

        // Test hashing
        assert_ne!(fact1_trait.hash_fact(), fact2_trait.hash_fact());
        assert_eq!(fact3_trait.hash_fact(), fact4_trait.hash_fact());

        // Test that facts can be used in HashSet using a wrapper
        use super::super::fact::fact_utils::FactKey;
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(FactKey::new(fact1_trait));
        set.insert(FactKey::new(fact2_trait));
        assert_eq!(set.len(), 2);

        // Adding same fact again should not increase size
        set.insert(FactKey::new(fact1_trait));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_complex_fact_types() {
        // Test with more complex field types
        #[derive(Clone, Debug)]
        struct ComplexFact {
            id: Uuid,
            numbers: Vec<i32>,
            nested: Option<String>,
        }

        impl PartialEq for ComplexFact {
            fn eq(&self, other: &Self) -> bool {
                self.id == other.id
            }
        }

        impl Eq for ComplexFact {}

        impl std::hash::Hash for ComplexFact {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.id.hash(state);
            }
        }

        impl GreynetFact for ComplexFact {
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

        impl ComplexFact {
            fn new(numbers: Vec<i32>, nested: Option<String>) -> Self {
                Self {
                    id: Uuid::new_v4(),
                    numbers,
                    nested,
                }
            }
        }

        let complex = ComplexFact::new(vec![1, 2, 3], Some("nested".to_string()));

        assert_eq!(complex.numbers, vec![1, 2, 3]);
        assert_eq!(complex.nested, Some("nested".to_string()));

        // Test that the fact ID is properly generated
        let complex2 = ComplexFact::new(vec![4, 5, 6], None);
        assert_ne!(complex.fact_id(), complex2.fact_id());
    }
}
