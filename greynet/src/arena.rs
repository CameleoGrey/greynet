// arena.rs

use crate::join_adapters::*;
use crate::nodes::{
    ConditionalNode, FilterNode, FlatMapNode, FromNode, GroupNode, JoinNode, ScoringNode,
};
use crate::score::Score;
use crate::state::TupleState;
use crate::tuple::AnyTuple;
use crate::{GreynetError, Result, ResourceLimits};
use rustc_hash::FxHashMap as HashMap;
use slotmap::{DefaultKey, Key, SlotMap};

/// A unique, stable identifier for a node in the `NodeArena`.
/// It is an alias for `slotmap::DefaultKey`.
pub type NodeId = DefaultKey;

/// A stable, generational reference to a tuple in the `TupleArena`.
/// This is a wrapper around `slotmap::DefaultKey` to provide a type-safe index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SafeTupleIndex(pub(crate) DefaultKey);

impl SafeTupleIndex {
    /// Returns the underlying `DefaultKey` for this index.
    #[inline]
    pub fn key(&self) -> DefaultKey {
        self.0
    }
}

/// High-performance tuple arena using `SlotMap` for memory safety and efficiency.
/// This implementation replaces the previous combination of `generational-arena` and a custom `SparseSet`,
/// fixing the unbounded memory growth issue in long-running sessions.
pub struct TupleArena {
    /// The core storage for all tuples, managed by `slotmap`.
    pub arena: SlotMap<DefaultKey, AnyTuple>,
    /// Resource limits to prevent excessive memory usage.
    limits: ResourceLimits,
}

/// Helper for marking unlikely branches for compiler optimization.
#[inline(always)]
#[cold]
fn unlikely<T>(val: T) -> T {
    val
}

impl TupleArena {
    /// Creates a new `TupleArena` with default resource limits.
    pub fn new() -> Self {
        Self::with_limits(ResourceLimits::default())
    }

    /// Creates a new `TupleArena` with the specified resource limits.
    pub fn with_limits(limits: ResourceLimits) -> Self {
        Self {
            arena: SlotMap::new(),
            limits,
        }
    }

    /// Acquires a slot in the arena for a new tuple.
    /// This is an O(1) operation. `SlotMap` handles recycling of removed slots internally.
    #[inline]
    pub fn acquire_tuple(&mut self, tuple: AnyTuple) -> Result<SafeTupleIndex> {
        if unlikely(self.arena.len() >= self.limits.max_tuples) {
            return Err(GreynetError::resource_limit(
                "max_tuples",
                format!("Current: {}, Limit: {}", self.arena.len(), self.limits.max_tuples),
            ));
        }
        let key = self.arena.insert(tuple);
        Ok(SafeTupleIndex(key))
    }

    /// An alias for `acquire_tuple` for API consistency.
    #[inline]
    pub fn acquire_tuple_fast(&mut self, tuple: AnyTuple) -> Result<SafeTupleIndex> {
        self.acquire_tuple(tuple)
    }

    /// Gets an immutable reference to a tuple if the index is valid and the slot is occupied.
    #[inline]
    pub fn get_tuple(&self, safe_index: SafeTupleIndex) -> Option<&AnyTuple> {
        self.arena.get(safe_index.0)
    }

    /// Gets a mutable reference to a tuple if the index is valid and the slot is occupied.
    #[inline]
    pub fn get_tuple_mut(&mut self, safe_index: SafeTupleIndex) -> Option<&mut AnyTuple> {
        self.arena.get_mut(safe_index.0)
    }

    /// Gets an immutable reference, returning a `GreynetError` if the index is invalid or stale.
    pub fn get_tuple_checked(&self, safe_index: SafeTupleIndex) -> Result<&AnyTuple> {
        self.arena
            .get(safe_index.0)
            .ok_or_else(|| GreynetError::invalid_index("Invalid or stale tuple index"))
    }

    /// Gets a mutable reference, returning a `GreynetError` if the index is invalid or stale.
    pub fn get_tuple_mut_checked(&mut self, safe_index: SafeTupleIndex) -> Result<&mut AnyTuple> {
        self.arena
            .get_mut(safe_index.0)
            .ok_or_else(|| GreynetError::invalid_index("Invalid or stale tuple index"))
    }

    /// Releases a tuple from the arena, making its slot available for reuse.
    /// This is an O(1) operation.
    pub fn release_tuple(&mut self, safe_index: SafeTupleIndex) {
        self.arena.remove(safe_index.0);
    }

    /// In debug builds, checks for logical inconsistencies like dangling tuple states.
    #[cfg(debug_assertions)]
    pub fn check_for_leaks(&self) -> Result<()> {
        // With SlotMap, the primary leak vector (unbounded sparse array) is gone.
        // This check now ensures no tuples are left in a transient "Dying" state.
        let dying_count = self.arena.values().filter(|t| t.state() == TupleState::Dying).count();

        if dying_count > 0 {
            return Err(GreynetError::consistency_violation(format!(
                "Found {} tuples stuck in Dying state. They should have been released by the scheduler.",
                dying_count
            )));
        }

        Ok(())
    }

    /// Estimates the current memory usage of the tuple arena in megabytes.
    pub fn memory_usage_estimate(&self) -> usize {
        self.limits.estimate_memory_usage(self.arena.len(), 0)
    }

    /// Gathers statistics about the current state of the arena.
    pub fn stats(&self) -> ArenaStats {
        let (live_tuples, dead_tuples) = self.arena.values().fold((0, 0), |(live, dead), tuple| {
            if tuple.state() == TupleState::Dead {
                (live, dead + 1)
            } else {
                (live + 1, dead)
            }
        });

        ArenaStats {
            total_slots: self.arena.capacity(),
            live_tuples,
            dead_tuples,
            // These fields are no longer relevant with SlotMap but are kept for API compatibility.
            pooled_tuples: 0,
            generation_counter: 0,
        }
    }

    /// Finds all tuples marked as 'Dying' and releases them from the arena.
    pub fn cleanup_dying_tuples(&mut self) -> usize {
        let keys_to_clean: Vec<DefaultKey> = self.arena
            .iter()
            .filter(|(_, tuple)| tuple.state() == TupleState::Dying)
            .map(|(key, _)| key)
            .collect();
        
        let cleaned_count = keys_to_clean.len();
        for key in keys_to_clean {
            self.arena.remove(key);
        }
        cleaned_count
    }

    /// Efficiently transitions all tuples from a given state to another.
    pub fn bulk_transition_states(&mut self, from: TupleState, to: TupleState) -> usize {
        let mut updated_count = 0;
        for tuple in self.arena.values_mut() {
            if tuple.state() == from {
                tuple.set_state(to);
                updated_count += 1;
            }
        }
        updated_count
    }

    /// Pre-allocates memory for a specified number of additional tuples.
    pub fn reserve_capacity(&mut self, additional: usize) {
        self.arena.reserve(additional);
    }

    /// Triggers a cleanup of dying tuples if memory usage exceeds a certain threshold.
    pub fn cleanup_if_memory_pressure(&mut self) -> usize {
        let memory_estimate = self.memory_usage_estimate();
        
        if memory_estimate > self.limits.max_memory_mb / 2 {
            // High memory pressure - aggressive cleanup
            self.cleanup_dying_tuples()
        } else {
            0
        }
    }
}

/// A snapshot of `TupleArena` statistics.
#[derive(Debug, Clone)]
pub struct ArenaStats {
    pub total_slots: usize,
    pub live_tuples: usize,
    pub dead_tuples: usize,
    pub pooled_tuples: usize,
    pub generation_counter: u64,
}

impl std::fmt::Debug for TupleArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TupleArena")
            .field("live_tuples", &self.arena.len())
            .field("capacity", &self.arena.capacity())
            .finish()
    }
}

impl Default for TupleArena {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents an operation to be performed on a node in the network.
#[derive(Debug, Clone)]
pub enum NodeOperation {
    Insert(NodeId, SafeTupleIndex),
    Retract(NodeId, SafeTupleIndex),
    InsertLeft(NodeId, SafeTupleIndex),
    InsertRight(NodeId, SafeTupleIndex),
    RetractLeft(NodeId, SafeTupleIndex),
    RetractRight(NodeId, SafeTupleIndex),
    ReleaseTuple(SafeTupleIndex),
}

/// An enum that holds the data for any type of node in the network.
#[derive(Debug)]
pub enum NodeData<S: Score> {
    From(FromNode),
    Filter(FilterNode),
    Join(JoinNode),
    Conditional(ConditionalNode),
    JoinLeftAdapter(JoinLeftAdapter),
    JoinRightAdapter(JoinRightAdapter),
    Group(GroupNode),
    FlatMap(FlatMapNode),
    Scoring(ScoringNode<S>),
}

impl<S: Score> NodeData<S> {
    /// Collects the operations that should be triggered by an insertion into this node.
    #[inline]
    pub fn collect_insert_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        match self {
            NodeData::From(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::Filter(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::Group(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::FlatMap(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::Join(_) | NodeData::Conditional(_) => Ok(()), // Handled by adapters
            NodeData::JoinLeftAdapter(adapter) => {
                operations.push(NodeOperation::InsertLeft(
                    adapter.parent_join_node,
                    tuple_index,
                ));
                Ok(())
            }
            NodeData::JoinRightAdapter(adapter) => {
                operations.push(NodeOperation::InsertRight(
                    adapter.parent_join_node,
                    tuple_index,
                ));
                Ok(())
            }
            NodeData::Scoring(node) => node.insert_collect_ops(tuple_index, tuples, operations),
        }
    }

    /// Collects the operations that should be triggered by a retraction from this node.
    #[inline]
    pub fn collect_retract_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        match self {
            NodeData::From(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::Filter(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::Group(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::FlatMap(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::Join(_) | NodeData::Conditional(_) => Ok(()), // Handled by adapters
            NodeData::JoinLeftAdapter(adapter) => {
                operations.push(NodeOperation::RetractLeft(
                    adapter.parent_join_node,
                    tuple_index,
                ));
                Ok(())
            }
            NodeData::JoinRightAdapter(adapter) => {
                operations.push(NodeOperation::RetractRight(
                    adapter.parent_join_node,
                    tuple_index,
                ));
                Ok(())
            }
            NodeData::Scoring(node) => node.retract_collect_ops(tuple_index, tuples, operations),
        }
    }

    /// Adds a child node to this node's list of children, if applicable.
    pub fn add_child(&mut self, child_id: NodeId) {
        let children = match self {
            NodeData::From(n) => &mut n.children,
            NodeData::Filter(n) => &mut n.children,
            NodeData::Join(n) => &mut n.children,
            NodeData::Conditional(n) => &mut n.children,
            NodeData::Group(n) => &mut n.children,
            NodeData::FlatMap(n) => &mut n.children,
            // Scoring and Adapter nodes are terminal or have special connections
            NodeData::Scoring(_) => return,
            NodeData::JoinLeftAdapter(_) | NodeData::JoinRightAdapter(_) => return,
        };
        if !children.contains(&child_id) {
            children.push(child_id);
        }
    }
}

/// The storage arena for all nodes in the constraint network.
pub struct NodeArena<S: Score> {
    pub(crate) nodes: SlotMap<NodeId, NodeData<S>>,
}

impl<S: Score> NodeArena<S> {
    pub fn new() -> Self {
        Self {
            nodes: SlotMap::new(),
        }
    }

    #[inline]
    pub fn insert_node(&mut self, node_data: NodeData<S>) -> NodeId {
        self.nodes.insert(node_data)
    }

    #[inline]
    pub fn get_node(&self, node_id: NodeId) -> Option<&NodeData<S>> {
        self.nodes.get(node_id)
    }

    #[inline]
    pub fn get_node_mut(&mut self, node_id: NodeId) -> Option<&mut NodeData<S>> {
        self.nodes.get_mut(node_id)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Executes a list of `NodeOperation`s, potentially creating new operations
    /// in a cascading fashion until the network stabilizes.
    pub fn execute_operations(
        mut operations: Vec<NodeOperation>,
        nodes: &mut NodeArena<S>,
        tuples: &mut TupleArena,
    ) -> Result<()> {
        let mut node_ops = Vec::with_capacity(operations.len());
        let mut release_ops = Vec::new();

        // Separate node operations from release operations for deferred execution.
        for op in operations.drain(..) {
            match op {
                NodeOperation::ReleaseTuple(idx) => release_ops.push(idx),
                other => node_ops.push(other),
            }
        }

        // Process node operations in batches.
        while !node_ops.is_empty() {
            let current_batch = std::mem::take(&mut node_ops);
            for operation in current_batch {
                let mut new_operations = Vec::new();

                let result = match operation {
                    NodeOperation::Insert(node_id, tuple_index) => {
                        if let Some(node) = nodes.get_node_mut(node_id) {
                            node.collect_insert_ops(tuple_index, tuples, &mut new_operations)
                        } else {
                            Ok(())
                        }
                    }
                    NodeOperation::Retract(node_id, tuple_index) => {
                        if let Some(node) = nodes.get_node_mut(node_id) {
                            node.collect_retract_ops(tuple_index, tuples, &mut new_operations)
                        } else {
                            Ok(())
                        }
                    }
                    NodeOperation::InsertLeft(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.insert_left_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.insert_left_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else {
                            Ok(())
                        }
                    }
                    NodeOperation::InsertRight(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.insert_right_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.insert_right_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else {
                            Ok(())
                        }
                    }
                    NodeOperation::RetractLeft(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.retract_left_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.retract_left_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else {
                            Ok(())
                        }
                    }
                    NodeOperation::RetractRight(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.retract_right_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.retract_right_collect_ops(tuple_index, tuples, &mut new_operations)
                        } else {
                            Ok(())
                        }
                    }
                    NodeOperation::ReleaseTuple(idx) => {
                        release_ops.push(idx);
                        Ok(())
                    }
                };

                if let Err(e) = result {
                    return Err(e);
                }

                // Queue new operations generated by the current batch.
                for new_op in new_operations {
                    match new_op {
                        NodeOperation::ReleaseTuple(idx) => release_ops.push(idx),
                        other => node_ops.push(other),
                    }
                }
            }
        }

        // Release all collected tuples at the very end to avoid use-after-free issues.
        for tuple_idx in release_ops {
            tuples.release_tuple(tuple_idx);
        }

        Ok(())
    }
}

impl<S: Score> std::fmt::Debug for NodeArena<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeArena")
            .field("total_nodes", &self.nodes.len())
            .finish()
    }
}

impl<S: Score> Default for NodeArena<S> {
    fn default() -> Self {
        Self::new()
    }
}
