//scheduler.rs
use crate::arena::{NodeArena, SafeTupleIndex, TupleArena, NodeOperation};
use crate::state::TupleState;
use crate::score::Score;
use crate::{Result, GreynetError, ResourceLimits};
use std::collections::VecDeque;

/// Object pool for reducing allocations
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

    #[inline]
    pub fn get_operation_buffer(&mut self) -> Vec<NodeOperation> {
        self.operation_buffers.pop().unwrap_or_else(|| Vec::with_capacity(128))
    }
    
    #[inline]
    pub fn return_operation_buffer(&mut self, mut buffer: Vec<NodeOperation>) {
        buffer.clear();
        if buffer.capacity() <= 512 {
            self.operation_buffers.push(buffer);
        }
    }

    #[inline]
    pub fn get_index_buffer(&mut self) -> Vec<SafeTupleIndex> {
        self.index_buffers.pop().unwrap_or_else(|| Vec::with_capacity(64))
    }
    
    #[inline]
    pub fn return_index_buffer(&mut self, mut buffer: Vec<SafeTupleIndex>) {
        buffer.clear();
        if buffer.capacity() <= 256 {
            self.index_buffers.push(buffer);
        }
    }
}

/// High-performance batch scheduler with object pooling and safety guarantees
#[derive(Debug)]
pub struct BatchScheduler {
    pending_queue: VecDeque<SafeTupleIndex>,
    operation_queue: VecDeque<NodeOperation>,
    pool: OperationPool,
    limits: ResourceLimits,
}

impl BatchScheduler {
    pub fn new() -> Self {
        Self::with_limits(ResourceLimits::default())
    }
    
    pub fn with_limits(limits: ResourceLimits) -> Self {
        Self {
            pending_queue: VecDeque::new(),
            operation_queue: VecDeque::new(),
            pool: OperationPool::new(),
            limits,
        }
    }

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

    #[inline]
    pub fn schedule_retract(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_mut_checked(tuple_index) {
            match tuple.state() {
                TupleState::Creating => {
                    tuple.set_state(TupleState::Aborting);
                    Ok(())
                }
                TupleState::Ok => {
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

    pub fn execute_all<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) -> Result<()> {

        self.execute_all_scalar(nodes, tuples)
    }

    // IMPROVED: Add infinite loop prevention and resource limits
    pub fn execute_all_scalar<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) -> Result<()> {
        let mut iteration_count = 0;
        
        while !self.pending_queue.is_empty() {
            iteration_count += 1;
            if iteration_count > self.limits.max_cascade_depth {
                return Err(GreynetError::infinite_loop(self.limits.max_cascade_depth));
            }
            
            // Check operation limits
            self.limits.check_operation_limit(self.operation_queue.len())?;
            
            self.prepare_operations(nodes, tuples)?;
            let ops_to_execute: Vec<NodeOperation> = self.operation_queue.drain(..).collect();
            NodeArena::execute_operations(ops_to_execute, nodes, tuples)?;
        }
        Ok(())
    }

    fn prepare_operations<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) -> Result<()> {
        let mut dead_tuples = self.pool.get_index_buffer();
        let mut processed_tuples = Vec::new();
        let mut new_operations = self.pool.get_operation_buffer();

        while let Some(tuple_index) = self.pending_queue.pop_front() {
            let (current_state, node_id) = if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
                (tuple.state(), tuple.node())
            } else {
                continue; // Skip invalid indices
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

        // Update states and collect dead tuples
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
                    dead_tuples.push(tuple_index);
                }
                _ => {}
            }
        }

        // Release dead tuples
        for tuple_index in dead_tuples.iter() {
            tuples.release_tuple(*tuple_index);
        }
        
        self.pool.return_index_buffer(dead_tuples);
        Ok(())
    }
    
    /// Get current scheduler statistics
    pub fn stats(&self) -> SchedulerStats {
        SchedulerStats {
            pending_operations: self.pending_queue.len(),
            queued_operations: self.operation_queue.len(),
            operation_buffers_pooled: self.pool.operation_buffers.len(),
            index_buffers_pooled: self.pool.index_buffers.len(),
        }
    }
    
}

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