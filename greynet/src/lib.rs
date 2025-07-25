// lib.rs - Main library file
//!
//! This crate implements the core foundation types for the Greynet constraint
//! satisfaction engine, translated from Python to Rust.

pub mod advanced_index;
pub mod analysis;
pub mod arena;
pub mod collectors;
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
pub mod resource_limits;
pub mod scheduler;
pub mod score;
pub mod session;
pub mod state;
pub mod stream_def;
pub mod tuple;
pub mod uni_index;
pub mod utils;
pub mod sparse_set;
pub mod packed_indices;
pub mod common_ops;
pub mod streams_zero_copy;

pub use error::{GreynetError, Result};
pub use fact::GreynetFact;
pub use joiner::{Comparator, JoinerType, RangeUtils};
pub use resource_limits::ResourceLimits;
pub use score::{Score, SimpleScore, HardSoftScore, HardMediumSoftScore};
pub use score::{FromSimple, FromHard, FromMedium, FromSoft};
pub use state::TupleState;
pub use tuple::{AnyTuple, BiTuple, PentaTuple, QuadTuple, TriTuple, TupleArity, UniTuple};
pub use tuple::FactIterator;
pub use utils::TupleUtils;
pub use session::Session;
pub use constraint_builder::ConstraintBuilder;
pub use constraint::ConstraintWeights;
pub use stream_def::{Stream, Arity1, Arity2, Arity3, Arity4, Arity5, ConstraintRecipe};
pub use collectors::{BaseCollector, Collectors};
pub use analysis::{ConstraintAnalysis, ConstraintViolationReport, NetworkStatistics};

/// Memory leak detection macro for debug builds
#[macro_export]
macro_rules! assert_no_leaks {
    ($arena:expr) => {
        #[cfg(debug_assertions)]
        if let Err(e) = $arena.check_for_leaks() {
            panic!("Memory leak detected: {}", e);
        }
    };
}

#[macro_export]
// OPTIMIZATION: Use a macro to generate repetitive accessor methods,
    // ensuring identical, inlined implementations for each tuple variant.
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

/// Convenience function to create a new constraint builder
pub fn builder<S: Score + 'static>() -> ConstraintBuilder<S> {
    ConstraintBuilder::new()
}

/// Convenience function to create a constraint builder with custom limits
pub fn builder_with_limits<S: Score + 'static>(limits: ResourceLimits) -> ConstraintBuilder<S> {
    ConstraintBuilder::with_limits(limits)
}


/// Performance-optimized prelude for common imports
pub mod prelude {
    pub use crate::{
        GreynetFact, Score, SimpleScore, HardSoftScore, HardMediumSoftScore,
        AnyTuple, TupleState, Session, ConstraintBuilder, Stream,
        Arity1, Arity2, JoinerType, Collectors, builder, builder_with_limits,
        Result, GreynetError, ResourceLimits, ConstraintRecipe
    };
    
    // Tuple zero-copy traits
    pub use crate::tuple::ZeroCopyFacts;
    
    pub use std::rc::Rc;
}

// Enhanced convenience functions
/// Create a zero-copy optimized constraint builder
pub fn zero_copy_builder<S: Score + 'static>() -> ConstraintBuilder<S> {
    ConstraintBuilder::new()
}

/// Create a zero-copy optimized constraint builder with custom limits
pub fn zero_copy_builder_with_limits<S: Score + 'static>(limits: ResourceLimits) -> ConstraintBuilder<S> {
    ConstraintBuilder::with_limits(limits)
}