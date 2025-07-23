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
pub mod fact;
pub mod fact_impls;
pub mod factory;
pub mod join_adapters;
pub mod joiner;
pub mod node_operations;
pub mod node_sharing;
pub mod nodes;
pub mod scheduler;
pub mod score;
pub mod session;
pub mod state;
pub mod stream_def;
pub mod tuple;
pub mod uni_index;
pub mod utils;

pub use fact::GreynetFact;
pub use joiner::{Comparator, JoinerType};
pub use score::{HardMediumSoftScore, HardSoftScore, Score, SimpleScore};
pub use state::TupleState;
pub use tuple::{AnyTuple, BiTuple, PentaTuple, QuadTuple, TriTuple, TupleArity, UniTuple};
pub use utils::TupleUtils;

// Test macro for leak detection
#[macro_export]
macro_rules! assert_no_leaks {
    ($arena:expr) => {
        #[cfg(debug_assertions)]
        if let Err(e) = $arena.check_for_leaks() {
            panic!("Memory leak detected: {}", e);
        }
    };
}