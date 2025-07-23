// lib.rs - Main library file
//!
//! This crate implements the core foundation types for the Greynet constraint
//! satisfaction engine, translated from Python to Rust.

pub mod arena;
pub mod constraint;
pub mod fact;
pub mod joiner;
pub mod score;
pub mod state;
pub mod tuple;
pub mod utils;
pub mod factory;
pub mod node_operations;
pub mod node_sharing;
pub mod nodes;
pub mod scheduler;
pub mod stream_def;
pub mod advanced_index;
pub mod join_adapters;
pub mod uni_index;
pub mod collectors;
pub mod fact_impls;
pub mod constraint_builder;
pub mod session;
pub mod ergonomic_builders;
pub mod analysis;

pub use fact::GreynetFact;
pub use joiner::{Comparator, JoinerType};
pub use score::{HardMediumSoftScore, HardSoftScore, Score, SimpleScore};
pub use state::TupleState;
pub use tuple::{AnyTuple, BiTuple, PentaTuple, QuadTuple, TriTuple, TupleArity, UniTuple};
pub use utils::TupleUtils;
