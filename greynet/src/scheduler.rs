//scheduler.rs
use crate::arena::{NodeArena, SafeTupleIndex, TupleArena, NodeOperation};
use crate::state::TupleState;
use crate::score::Score;
use std::collections::VecDeque;

pub struct BatchScheduler {
    pending_queue: VecDeque<SafeTupleIndex>,
    operation_queue: VecDeque<NodeOperation>,
}

impl BatchScheduler {
    pub fn new() -> Self {
        Self {
            pending_queue: VecDeque::new(),
            operation_queue: VecDeque::new(),
        }
    }

    pub fn schedule_insert(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena) -> Result<(), String> {
        if let Some(tuple) = tuples.get_tuple_mut(tuple_index) {
            if !tuple.state().is_dirty() {
                tuple.set_state(TupleState::Creating);
                self.pending_queue.push_back(tuple_index);
                Ok(())
            } else {
                Err("Tuple is already in a dirty state".to_string())
            }
        } else {
            Err("Invalid tuple index".to_string())
        }
    }

    pub fn schedule_retract(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena) -> Result<(), String> {
        if let Some(tuple) = tuples.get_tuple_mut(tuple_index) {
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
                _ => Err("Tuple cannot be retracted in current state".to_string()),
            }
        } else {
            Err("Invalid tuple index".to_string())
        }
    }

    pub fn execute_all<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) -> Result<(), String> {
        while !self.pending_queue.is_empty() {
            self.prepare_operations(nodes, tuples);
            let ops_to_execute: Vec<NodeOperation> = self.operation_queue.drain(..).collect();
            NodeArena::execute_operations(ops_to_execute, nodes, tuples);
        }
        Ok(())
    }

    fn prepare_operations<S: Score>(&mut self, nodes: &mut NodeArena<S>, tuples: &mut TupleArena) {
        let mut dead_tuples = Vec::new();
        let mut processed_tuples = Vec::new();
        let mut new_operations = Vec::new();

        while let Some(tuple_index) = self.pending_queue.pop_front() {
            let (current_state, node_id) = if let Some(tuple) = tuples.get_tuple(tuple_index) {
                (tuple.state(), tuple.node())
            } else {
                continue;
            };

            processed_tuples.push((tuple_index, current_state));

            if let Some(node_id) = node_id {
                if let Some(node) = nodes.get_node_mut(node_id) {
                    match current_state {
                        TupleState::Creating => {
                            node.collect_insert_ops(tuple_index, tuples, &mut new_operations);
                        }
                        TupleState::Dying => {
                            node.collect_retract_ops(tuple_index, tuples, &mut new_operations);
                        }
                        _ => {}
                    }
                }
            }
        }

        self.operation_queue.extend(new_operations);

        for (tuple_index, previous_state) in processed_tuples {
            match previous_state {
                TupleState::Creating => {
                    if let Some(tuple) = tuples.get_tuple_mut(tuple_index) {
                        tuple.set_state(TupleState::Ok);
                    }
                }
                TupleState::Dying | TupleState::Aborting => {
                    if let Some(tuple) = tuples.get_tuple_mut(tuple_index) {
                        tuple.set_state(TupleState::Dead);
                    }
                    dead_tuples.push(tuple_index);
                }
                _ => {}
            }
        }

        for tuple_index in dead_tuples {
            tuples.release_tuple(tuple_index);
        }
    }
}

impl std::fmt::Debug for BatchScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchScheduler")
            .field("pending_queue_size", &self.pending_queue.len())
            .field("operation_queue_size", &self.operation_queue.len())
            .finish()
    }
}

impl Default for BatchScheduler {
    fn default() -> Self {
        Self::new()
    }
}
