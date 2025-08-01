// fact.rs - GreynetFact trait
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

/// Core trait for all facts in the Greynet system.
///
/// This trait must be implemented by all facts that can be used in the constraint system.
/// The trait is object-safe to allow storage in `Rc<dyn GreynetFact>`.
pub trait GreynetFact: Debug + 'static {
    /// Returns a unique identifier for the fact instance.
    fn fact_id(&self) -> i64;
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
        pub id: i64,
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