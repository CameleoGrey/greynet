// scheduler.rs

use crate::arena::{NodeArena, SafeTupleIndex, TupleArena, NodeOperation};
use crate::state::TupleState;
use crate::score::Score;
use crate::{Result, GreynetError, ResourceLimits};
use std::collections::VecDeque;

/// Object pool for reducing allocations of temporary vectors during scheduling.
#[derive(Debug)]
pub struct OperationPool {
    operation_buffers: Vec<Vec<NodeOperation>>,
    index_buffers: Vec<Vec<SafeTupleIndex>>,
}

impl OperationPool {
    pub fn new() -> Self {
        Self {
            operation_buffers: Vec::new(),
            index_buffers: Vec::new(),
        }
    }

    /// Acquires a buffer for node operations from the pool, or creates one if none are available.
    #[inline]
    pub fn get_operation_buffer(&mut self) -> Vec<NodeOperation> {
        self.operation_buffers.pop().unwrap_or_else(|| Vec::with_capacity(128))
    }
    
    /// Returns an operation buffer to the pool for reuse.
    #[inline]
    pub fn return_operation_buffer(&mut self, mut buffer: Vec<NodeOperation>) {
        buffer.clear();
        // Only pool reasonably-sized buffers to avoid holding onto large allocations.
        if buffer.capacity() <= 512 {
            self.operation_buffers.push(buffer);
        }
    }

    /// Acquires a buffer for tuple indices from the pool.
    #[inline]
    pub fn get_index_buffer(&mut self) -> Vec<SafeTupleIndex> {
        self.index_buffers.pop().unwrap_or_else(|| Vec::with_capacity(64))
    }
    
    /// Returns an index buffer to the pool for reuse.
    #[inline]
    pub fn return_index_buffer(&mut self, mut buffer: Vec<SafeTupleIndex>) {
        buffer.clear();
        if buffer.capacity() <= 256 {
            self.index_buffers.push(buffer);
        }
    }
}

/// High-performance batch scheduler with object pooling and safety guarantees.
/// It processes tuple insertions and retractions, generating a cascade of
/// node operations that are executed until the network reaches a stable state.
#[derive(Debug)]
pub struct BatchScheduler {
    pending_queue: VecDeque<SafeTupleIndex>,
    operation_queue: VecDeque<NodeOperation>,
    pool: OperationPool,
    limits: ResourceLimits,
}

impl BatchScheduler {
    /// Creates a new scheduler with default resource limits.
    pub fn new() -> Self {
        Self::with_limits(ResourceLimits::default())
    }
    
    /// Creates a new scheduler with custom resource limits.
    pub fn with_limits(limits: ResourceLimits) -> Self {
        Self {
            pending_queue: VecDeque::new(),
            operation_queue: VecDeque::new(),
            pool: OperationPool::new(),
            limits,
        }
    }

    /// Schedules a tuple for insertion into the network.
    #[inline]
    pub fn schedule_insert(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_mut_checked(tuple_index) {
            if !tuple.state().is_dirty() {
                tuple.set_state(TupleState::Creating);
                self.pending_queue.push_back(tuple_index);
                Ok(())
            } else {
                Err(GreynetError::scheduler_error("Tuple is already in a dirty state"))
            }
        } else {
            Err(GreynetError::invalid_index("Invalid tuple index for scheduling"))
        }
    }

    /// Schedules a tuple for retraction from the network.
    #[inline]
    pub fn schedule_retract(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_mut_checked(tuple_index) {
            match tuple.state() {
                TupleState::Creating => {
                    // If the tuple is still being created, abort its creation.
                    tuple.set_state(TupleState::Aborting);
                    Ok(())
                }
                TupleState::Ok => {
                    // If the tuple is stable, mark it for retraction.
                    tuple.set_state(TupleState::Dying);
                    self.pending_queue.push_back(tuple_index);
                    Ok(())
                }
                state => Err(GreynetError::scheduler_error(
                    format!("Tuple cannot be retracted in state: {:?}", state)
                )),
            }
        } else {
            Err(GreynetError::invalid_index("Invalid tuple index for retraction"))
        }
    }

    /// Executes all pending operations until the network is stable.
    pub fn execute_all<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) -> Result<()> {
        self.execute_all_scalar(nodes, tuples)
    }

    // The main execution loop delays tuple release until the entire batch is processed.
    // This prevents use-after-free errors by ensuring a tuple is not deallocated
    // while other operations in the same batch may still need to reference it.
    pub fn execute_all_scalar<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) -> Result<()> {
        let mut iteration_count = 0;
        // Collect all tuples that are marked as dead during the batch execution.
        // They will be released only at the very end of the process.
        let mut final_release_list = self.pool.get_index_buffer();

        while !self.pending_queue.is_empty() {
            iteration_count += 1;
            if iteration_count > self.limits.max_cascade_depth {
                self.pool.return_index_buffer(final_release_list); // Avoid leaking the buffer on error
                return Err(GreynetError::infinite_loop(self.limits.max_cascade_depth));
            }
            
            self.limits.check_operation_limit(self.operation_queue.len())?;
            
            // `prepare_operations` returns a list of tuples that have been marked as 'Dead'.
            // We collect them here but do not release them from the arena yet.
            let newly_dead = self.prepare_operations(nodes, tuples)?;
            final_release_list.extend_from_slice(&newly_dead);
            
            let ops_to_execute: Vec<NodeOperation> = self.operation_queue.drain(..).collect();
            
            // Execute the cascaded operations. The initial tuples are still alive in the arena,
            // preventing any use-after-free errors.
            NodeArena::execute_operations(ops_to_execute, nodes, tuples)?;
        }

        // After all operations and their cascades are complete, it is now safe to release the initial tuples.
        for tuple_index in final_release_list.iter() {
            tuples.release_tuple(*tuple_index);
        }
        self.pool.return_index_buffer(final_release_list);

        Ok(())
    }

    // This function returns a list of tuples that should be released by the caller
    // once all operations for the current batch are complete.
    fn prepare_operations<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) -> Result<Vec<SafeTupleIndex>> {
        let mut dead_tuples_for_release = self.pool.get_index_buffer();
        let mut processed_tuples = Vec::new();
        let mut new_operations = self.pool.get_operation_buffer();

        while let Some(tuple_index) = self.pending_queue.pop_front() {
            let (current_state, node_id) = if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
                (tuple.state(), tuple.node())
            } else {
                // Skip invalid indices, as they may have been released in a sub-cascade.
                continue; 
            };

            processed_tuples.push((tuple_index, current_state));

            if let Some(node_id) = node_id {
                if let Some(node) = nodes.get_node_mut(node_id) {
                    match current_state {
                        TupleState::Creating => {
                            node.collect_insert_ops(tuple_index, tuples, &mut new_operations)?;
                        }
                        TupleState::Dying => {
                            node.collect_retract_ops(tuple_index, tuples, &mut new_operations)?;
                        }
                        _ => {}
                    }
                }
            }
        }

        self.operation_queue.extend(new_operations.drain(..));
        self.pool.return_operation_buffer(new_operations);

        // Update tuple states and collect indices of dead tuples for later release.
        for (tuple_index, previous_state) in processed_tuples {
            match previous_state {
                TupleState::Creating => {
                    if let Ok(tuple) = tuples.get_tuple_mut_checked(tuple_index) {
                        tuple.set_state(TupleState::Ok);
                    }
                }
                TupleState::Dying | TupleState::Aborting => {
                    if let Ok(tuple) = tuples.get_tuple_mut_checked(tuple_index) {
                        tuple.set_state(TupleState::Dead);
                    }
                    // Add to the list to be returned; do not release from the arena here.
                    dead_tuples_for_release.push(tuple_index);
                }
                _ => {}
            }
        }

        // Return the list of dead tuples to the caller.
        Ok(dead_tuples_for_release)
    }
    
    /// Gets current statistics about the scheduler's state.
    pub fn stats(&self) -> SchedulerStats {
        SchedulerStats {
            pending_operations: self.pending_queue.len(),
            queued_operations: self.operation_queue.len(),
            operation_buffers_pooled: self.pool.operation_buffers.len(),
            index_buffers_pooled: self.pool.index_buffers.len(),
        }
    }
    
}

/// Contains statistics about the scheduler's internal queues and pools.
#[derive(Debug, Clone)]
pub struct SchedulerStats {
    pub pending_operations: usize,
    pub queued_operations: usize,
    pub operation_buffers_pooled: usize,
    pub index_buffers_pooled: usize,
}

impl Default for BatchScheduler {
    fn default() -> Self {
        Self::new()
    }
}
