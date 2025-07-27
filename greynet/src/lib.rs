// lib.rs - Main library file
//!
//! This crate implements the core foundation types for the Greynet constraint
//! satisfaction engine, translated from Python to Rust.

// Declare all public modules of the crate.
pub mod advanced_index;
pub mod analysis;
pub mod arena;
pub mod collectors;
pub mod common_ops;
pub mod constraint;
pub mod constraint_builder;
pub mod error;
pub mod fact;
pub mod fact_impls;
pub mod factory;
pub mod join_adapters;
pub mod joiner;
pub mod node_operations;
pub mod node_sharing;
pub mod nodes;
pub mod packed_indices;
pub mod resource_limits;
pub mod scheduler;
pub mod score;
pub mod session;
pub mod state;
pub mod stream_def;
pub mod tuple;
pub mod uni_index;
pub mod utils;
// The `sparse_set` module has been removed as it is no longer needed with the `slotmap` implementation.

// Re-export core types for easier access from outside the crate.
pub use analysis::{ConstraintAnalysis, ConstraintViolationReport, NetworkStatistics};
pub use collectors::{BaseCollector, Collectors};
pub use constraint::ConstraintWeights;
pub use constraint_builder::ConstraintBuilder;
pub use error::{GreynetError, Result};
pub use fact::GreynetFact;
pub use joiner::{Comparator, JoinerType, RangeUtils};
pub use resource_limits::ResourceLimits;
pub use score::{FromHard, FromMedium, FromSimple, FromSoft, HardMediumSoftScore, HardSoftScore, Score, SimpleScore};
pub use session::Session;
pub use state::TupleState;
pub use stream_def::{
    extract_fact, Arity1, Arity2, Arity3, Arity4, Arity5, ConstraintRecipe, Stream,
};
pub use tuple::{AnyTuple, BiTuple, FactIterator, PentaTuple, QuadTuple, TriTuple, TupleArity, UniTuple};
pub use utils::TupleUtils;

/// A macro for asserting that no memory leaks (e.g., dangling tuples) exist
/// in the tuple arena during debug builds.
#[macro_export]
macro_rules! assert_no_leaks {
    ($arena:expr) => {
        #[cfg(debug_assertions)]
        if let Err(e) = $arena.check_for_leaks() {
            panic!("Memory leak detected: {}", e);
        }
    };
}

/// A macro to generate boilerplate accessor methods for `AnyTuple`, ensuring
/// efficient, inlined implementations for each tuple variant.
#[macro_export]
macro_rules! impl_any_tuple_accessors {
    ($($variant:ident),*) => {
        #[inline]
        pub fn node(&self) -> Option<NodeId> {
            match self {
                $(AnyTuple::$variant(t) => t.node,)*
            }
        }

        #[inline]
        pub fn set_node(&mut self, node_id: NodeId) {
            match self {
                $(AnyTuple::$variant(t) => t.node = Some(node_id),)*
            }
        }

        #[inline]
        pub fn state(&self) -> TupleState {
            match self {
                $(AnyTuple::$variant(t) => t.state,)*
            }
        }

        #[inline]
        pub fn set_state(&mut self, state: TupleState) {
            match self {
                $(AnyTuple::$variant(t) => t.state = state,)*
            }
        }

        pub fn reset(&mut self) {
            match self {
                $(AnyTuple::$variant(t) => t.reset(),)*
            }
        }
    };
}

/// Convenience function to create a new constraint builder with default limits.
pub fn builder<S: Score + 'static>() -> ConstraintBuilder<S> {
    ConstraintBuilder::new()
}

/// Convenience function to create a constraint builder with custom resource limits.
pub fn builder_with_limits<S: Score + 'static>(limits: ResourceLimits) -> ConstraintBuilder<S> {
    ConstraintBuilder::with_limits(limits)
}

/// A "prelude" module for easily importing the most commonly used types.
pub mod prelude {
    pub use crate::{
        builder, builder_with_limits, AnyTuple, Arity1, Arity2, Collectors, ConstraintBuilder,
        ConstraintRecipe, GreynetError, GreynetFact, HardMediumSoftScore, HardSoftScore,
        JoinerType, ResourceLimits, Result, Score, Session, SimpleScore, Stream, TupleState,
        extract_fact
    };
    
    // Re-export the zero-copy trait for easy use in function definitions.
    pub use crate::tuple::ZeroCopyFacts;
    
    pub use std::rc::Rc;
}
