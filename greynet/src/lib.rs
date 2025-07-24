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
pub mod ergonomic_builders;
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
pub use stream_def::{Stream, Arity1, Arity2, Arity3, Arity4, Arity5};
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
        Result, GreynetError, ResourceLimits
    };
    pub use std::rc::Rc;
}