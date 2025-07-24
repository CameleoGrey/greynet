//resource_limits.rs
//! Resource management and limits for the Greynet engine

/// Resource limits to prevent unbounded growth and ensure system stability
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum number of tuples in the system
    pub max_tuples: usize,
    /// Maximum operations processed in a single batch
    pub max_operations_per_batch: usize,
    /// Maximum memory usage in megabytes (estimated)
    pub max_memory_mb: usize,
    /// Maximum depth for operation cascades
    pub max_cascade_depth: usize,
    /// Maximum facts per type
    pub max_facts_per_type: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_tuples: 10_000_000,
            max_operations_per_batch: 100_000,
            max_memory_mb: 2048,
            max_cascade_depth: 1000,
            max_facts_per_type: 1_000_000,
        }
    }
}

impl ResourceLimits {
    /// Create conservative limits for small systems
    pub fn conservative() -> Self {
        Self {
            max_tuples: 100_000,
            max_operations_per_batch: 10_000,
            max_memory_mb: 256,
            max_cascade_depth: 100,
            max_facts_per_type: 50_000,
        }
    }
    
    /// Create aggressive limits for high-performance systems
    pub fn aggressive() -> Self {
        Self {
            max_tuples: 100_000_000,
            max_operations_per_batch: 1_000_000,
            max_memory_mb: 8192,
            max_cascade_depth: 10_000,
            max_facts_per_type: 10_000_000,
        }
    }
    
    /// Estimate memory usage for current tuple count
    #[inline]
    pub fn estimate_memory_usage(&self, tuple_count: usize, node_count: usize) -> usize {
        // Rough estimation: 200 bytes per tuple + 1KB per node
        (tuple_count * 200 + node_count * 1024) / (1024 * 1024)
    }
    
    /// Check if operation would exceed limits
    pub fn check_operation_limit(&self, current_ops: usize) -> crate::error::Result<()> {
        if current_ops > self.max_operations_per_batch {
            return Err(crate::error::GreynetError::resource_limit(
                "operations_per_batch",
                format!("Current: {}, Limit: {}", current_ops, self.max_operations_per_batch)
            ));
        }
        Ok(())
    }
    
    /// Check if tuple count would exceed limits
    pub fn check_tuple_limit(&self, current_tuples: usize) -> crate::error::Result<()> {
        if current_tuples > self.max_tuples {
            return Err(crate::error::GreynetError::resource_limit(
                "max_tuples",
                format!("Current: {}, Limit: {}", current_tuples, self.max_tuples)
            ));
        }
        Ok(())
    }
}