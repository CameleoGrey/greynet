//error.rs
//! Error types for the Greynet constraint satisfaction engine

use std::fmt;
use thiserror::Error;
use uuid::Uuid;

/// Result type alias for Greynet operations
pub type Result<T> = std::result::Result<T, GreynetError>;

/// Main error type for Greynet operations
#[derive(Debug, Error)]
pub enum GreynetError {
    #[error("Fact with ID {0} already exists")]
    DuplicateFact(Uuid),
    
    #[error("Fact with ID {0} not found")]
    FactNotFound(Uuid),
    
    #[error("No from node registered for type {type_name}")]
    UnregisteredType { type_name: String },
    
    #[error("Resource limit exceeded: {limit_type} - {details}")]
    ResourceLimit { 
        limit_type: String, 
        details: String 
    },
    
    #[error("Operation cascade exceeded maximum iterations ({max_iterations}). Possible infinite loop detected")]
    InfiniteLoop { max_iterations: usize },
    
    #[error("Invalid tuple index: {reason}")]
    InvalidIndex { reason: String },
    
    #[error("Arena error: {details}")]
    Arena { details: String },
    
    #[error("Scheduler error: {details}")]
    Scheduler { details: String },
    
    #[error("Constraint building error: {details}")]
    ConstraintBuilder { details: String },
    
    #[error("Memory allocation failed: {details}")]
    Memory { details: String },
    
    #[error("Invalid tuple arity: expected {expected}, got {actual}")]
    InvalidArity { expected: usize, actual: usize },
    
    #[error("Type mismatch: {details}")]
    TypeMismatch { details: String },
    
    #[error("Consistency check failed: {details}")]
    ConsistencyViolation { details: String },
    
    #[error("{0}")]
    Other(String),
}

impl GreynetError {
    pub fn duplicate_fact(id: Uuid) -> Self {
        Self::DuplicateFact(id)
    }
    
    pub fn fact_not_found(id: Uuid) -> Self {
        Self::FactNotFound(id)
    }
    
    pub fn unregistered_type(type_name: impl Into<String>) -> Self {
        Self::UnregisteredType { type_name: type_name.into() }
    }
    
    pub fn resource_limit(limit_type: impl Into<String>, details: impl Into<String>) -> Self {
        Self::ResourceLimit { 
            limit_type: limit_type.into(), 
            details: details.into() 
        }
    }
    
    pub fn infinite_loop(max_iterations: usize) -> Self {
        Self::InfiniteLoop { max_iterations }
    }
    
    pub fn invalid_index(reason: impl Into<String>) -> Self {
        Self::InvalidIndex { reason: reason.into() }
    }

    pub fn invalid_arity(expected: usize, actual: usize ) -> Self {
        Self::InvalidArity { expected, actual }
    }

    pub fn constraint_builder_error(details: impl Into<String>) -> Self {
        Self::ConstraintBuilder { details: details.into() }
    }
    
    pub fn arena_error(details: impl Into<String>) -> Self {
        Self::Arena { details: details.into() }
    }
    
    pub fn scheduler_error(details: impl Into<String>) -> Self {
        Self::Scheduler { details: details.into() }
    }

    pub fn type_mismatch(details: impl Into<String>) -> Self {
        Self::TypeMismatch { details: details.into() }
    }

    pub fn consistency_violation(details: impl Into<String>) -> Self {
        Self::ConsistencyViolation { details: details.into() }
    }
}

impl From<String> for GreynetError {
    fn from(msg: String) -> Self {
        Self::Other(msg)
    }
}

impl From<&str> for GreynetError {
    fn from(msg: &str) -> Self {
        Self::Other(msg.to_string())
    }
}