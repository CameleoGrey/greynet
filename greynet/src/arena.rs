// arena.rs
use crate::join_adapters::*;
use crate::nodes::{ConditionalNode, FilterNode, FlatMapNode, FromNode, GroupNode, JoinNode, ScoringNode};
use crate::score::Score;
use crate::state::TupleState;
use crate::tuple::{AnyTuple, TupleArity};
use generational_arena::{Arena, Index as TupleIndex};
use slotmap::{DefaultKey, SlotMap};
use std::collections::HashMap;

pub type NodeId = DefaultKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SafeTupleIndex {
    pub(crate) index: TupleIndex,
    pub(crate) generation: u64,
}

impl SafeTupleIndex {
    pub fn new(index: TupleIndex, generation: u64) -> Self {
        Self { index, generation }
    }

    pub fn index(&self) -> TupleIndex {
        self.index
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

pub struct TupleArena {
    pub arena: Arena<AnyTuple>,
    generation_counter: u64,
    type_pools: HashMap<TupleArity, Vec<SafeTupleIndex>>,
    active_refs: HashMap<TupleIndex, u64>,
}

impl TupleArena {
    pub fn new() -> Self {
        Self {
            arena: Arena::new(),
            generation_counter: 0,
            type_pools: HashMap::new(),
            active_refs: HashMap::new(),
        }
    }

    pub fn acquire_tuple(&mut self, tuple: AnyTuple) -> SafeTupleIndex {
        let arity = tuple.tuple_arity();

        // Try to reuse from pool
        if let Some(pool) = self.type_pools.get_mut(&arity) {
            if let Some(safe_index) = pool.pop() {
                // Verify generation is still valid
                if let Some(&expected_gen) = self.active_refs.get(&safe_index.index) {
                    if expected_gen == safe_index.generation {
                        if let Some(slot) = self.arena.get_mut(safe_index.index) {
                            *slot = tuple;
                            return safe_index;
                        }
                    }
                }
            }
        }

        // Create new tuple
        self.generation_counter += 1;
        let index = self.arena.insert(tuple);
        let safe_index = SafeTupleIndex::new(index, self.generation_counter);
        self.active_refs.insert(index, self.generation_counter);
        safe_index
    }

    pub fn get_tuple(&self, safe_index: SafeTupleIndex) -> Option<&AnyTuple> {
        // Verify generation
        if self
            .active_refs
            .get(&safe_index.index)
            .map_or(false, |&gen| gen == safe_index.generation)
        {
            self.arena.get(safe_index.index)
        } else {
            None
        }
    }

    pub fn get_tuple_mut(&mut self, safe_index: SafeTupleIndex) -> Option<&mut AnyTuple> {
        // Verify generation
        if self
            .active_refs
            .get(&safe_index.index)
            .map_or(false, |&gen| gen == safe_index.generation)
        {
            self.arena.get_mut(safe_index.index)
        } else {
            None
        }
    }

    pub fn release_tuple(&mut self, safe_index: SafeTupleIndex) {
        // Verify the tuple exists and is in the expected state
        if let Some(tuple) = self.get_tuple_mut(safe_index) {
            // Only release if it's in a dying or dead state
            if !matches!(tuple.state(), TupleState::Dying | TupleState::Dead) {
                eprintln!(
                    "Warning: Releasing tuple in unexpected state: {:?}",
                    tuple.state()
                );
            }

            let arity = tuple.tuple_arity();
            tuple.reset(); // This sets state to Dead and clears node reference

            // Return to pool for reuse
            self.type_pools.entry(arity).or_default().push(safe_index);
            self.active_refs.remove(&safe_index.index);
        } else {
            eprintln!(
                "Warning: Attempted to release invalid tuple: {:?}",
                safe_index
            );
        }
    }

    // FIXED: Add method to force cleanup of all dying tuples
    pub fn cleanup_dying_tuples(&mut self) -> usize {
        let mut cleaned_count = 0;
        let mut indices_to_clean = Vec::new();

        // Find all dying tuples
        for (index, tuple) in self.arena.iter() {
            if matches!(tuple.state(), TupleState::Dying) {
                if let Some(&generation) = self.active_refs.get(&index) {
                    indices_to_clean.push(SafeTupleIndex::new(index, generation));
                }
            }
        }

        // Clean them up
        for safe_index in indices_to_clean {
            self.release_tuple(safe_index);
            cleaned_count += 1;
        }

        cleaned_count
    }
}

#[cfg(debug_assertions)]
impl TupleArena {
    pub fn check_for_leaks(&self) -> Result<(), String> {
        let mut live_count = 0;
        let mut dead_count = 0;
        let mut dying_count = 0;

        for (_, tuple) in self.arena.iter() {
            match tuple.state() {
                TupleState::Dead => dead_count += 1,
                TupleState::Dying => dying_count += 1,
                _ => live_count += 1,
            }
        }

        let pooled_count: usize = self.type_pools.values().map(|v| v.len()).sum();

        if dying_count > 0 {
            return Err(format!(
                "Found {} tuples in Dying state that should be cleaned up",
                dying_count
            ));
        }

        if dead_count != pooled_count {
            return Err(format!(
                "Inconsistency: {} dead tuples but {} pooled tuples",
                dead_count, pooled_count
            ));
        }

        Ok(())
    }

    pub fn get_memory_stats(&self) -> String {
        let mut live_count = 0;
        let mut dead_count = 0;
        let mut dying_count = 0;

        for (_, tuple) in self.arena.iter() {
            match tuple.state() {
                TupleState::Dead => dead_count += 1,
                TupleState::Dying => dying_count += 1,
                _ => live_count += 1,
            }
        }

        let pooled_count: usize = self.type_pools.values().map(|v| v.len()).sum();

        format!(
            "Arena Stats: {} live, {} dead, {} dying, {} pooled, {} total slots",
            live_count,
            dead_count,
            dying_count,
            pooled_count,
            self.arena.len()
        )
    }
}

impl std::fmt::Debug for TupleArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut arity_counts = std::collections::HashMap::new();
        for arity in self.type_pools.keys() {
            let pool_size = self.type_pools.get(arity).map(|v| v.len()).unwrap_or(0);
            arity_counts.insert(format!("Arity{:?}", arity), pool_size);
        }

        f.debug_struct("TupleArena")
            .field("total_arena_slots", &self.arena.len())
            .field("generation_counter", &self.generation_counter)
            .field("type_pools", &arity_counts)
            .field("active_refs_count", &self.active_refs.len())
            .finish()
    }
}

impl Default for TupleArena {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub enum NodeOperation {
    Insert(NodeId, SafeTupleIndex),
    Retract(NodeId, SafeTupleIndex),
    InsertLeft(NodeId, SafeTupleIndex),
    InsertRight(NodeId, SafeTupleIndex),
    RetractLeft(NodeId, SafeTupleIndex),
    RetractRight(NodeId, SafeTupleIndex),
    // NEW: Explicit tuple release operation
    ReleaseTuple(SafeTupleIndex),
}

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
    pub fn collect_insert_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) {
        match self {
            NodeData::From(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::Filter(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::Group(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::FlatMap(node) => node.insert_collect_ops(tuple_index, tuples, operations),
            NodeData::Join(_) | NodeData::Conditional(_) => {
                // Join and Conditional nodes cannot be inserted directly
            }
            NodeData::JoinLeftAdapter(adapter) => {
                operations.push(NodeOperation::InsertLeft(
                    adapter.parent_join_node,
                    tuple_index,
                ));
            }
            NodeData::JoinRightAdapter(adapter) => {
                operations.push(NodeOperation::InsertRight(
                    adapter.parent_join_node,
                    tuple_index,
                ));
            }
            NodeData::Scoring(node) => node.insert_collect_ops(tuple_index, tuples, operations),
        }
    }

    pub fn collect_retract_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) {
        match self {
            NodeData::From(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::Filter(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::Group(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::FlatMap(node) => node.retract_collect_ops(tuple_index, tuples, operations),
            NodeData::Join(_) | NodeData::Conditional(_) => {
                // Join and Conditional nodes cannot be retracted directly
            }
            NodeData::JoinLeftAdapter(adapter) => {
                operations.push(NodeOperation::RetractLeft(
                    adapter.parent_join_node,
                    tuple_index,
                ));
            }
            NodeData::JoinRightAdapter(adapter) => {
                operations.push(NodeOperation::RetractRight(
                    adapter.parent_join_node,
                    tuple_index,
                ));
            }
            NodeData::Scoring(node) => node.retract_collect_ops(tuple_index, tuples, operations),
        }
    }

    pub fn add_child(&mut self, child_id: NodeId) {
        let children = match self {
            NodeData::From(n) => &mut n.children,
            NodeData::Filter(n) => &mut n.children,
            NodeData::Join(n) => &mut n.children,
            NodeData::Conditional(n) => &mut n.children,
            NodeData::Group(n) => &mut n.children,
            NodeData::FlatMap(n) => &mut n.children,
            NodeData::Scoring(_)
            | NodeData::JoinLeftAdapter(_)
            | NodeData::JoinRightAdapter(_) => return,
        };
        if !children.contains(&child_id) {
            children.push(child_id);
        }
    }
}

pub struct NodeArena<S: Score> {
    pub(crate) nodes: SlotMap<NodeId, NodeData<S>>,
}

impl<S: Score> NodeArena<S> {
    pub fn new() -> Self {
        Self {
            nodes: SlotMap::new(),
        }
    }

    pub fn insert_node(&mut self, node_data: NodeData<S>) -> NodeId {
        self.nodes.insert(node_data)
    }

    pub fn get_node(&self, node_id: NodeId) -> Option<&NodeData<S>> {
        self.nodes.get(node_id)
    }

    pub fn get_node_mut(&mut self, node_id: NodeId) -> Option<&mut NodeData<S>> {
        self.nodes.get_mut(node_id)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn execute_operations(
        mut operations: Vec<NodeOperation>,
        nodes: &mut NodeArena<S>,
        tuples: &mut TupleArena,
    ) {
        // Separate node operations from release operations for proper ordering
        let mut node_ops = Vec::new();
        let mut release_ops = Vec::new();

        // Initial separation
        for op in operations.drain(..) {
            match op {
                NodeOperation::ReleaseTuple(idx) => release_ops.push(idx),
                other => node_ops.push(other),
            }
        }

        // Process all node operations first (these may generate more operations)
        while !node_ops.is_empty() {
            let current_batch = std::mem::take(&mut node_ops);
            for operation in current_batch {
                let mut new_operations = Vec::new();
                match operation {
                    NodeOperation::Insert(node_id, tuple_index) => {
                        if let Some(node) = nodes.get_node_mut(node_id) {
                            node.collect_insert_ops(tuple_index, tuples, &mut new_operations);
                        }
                    }
                    NodeOperation::Retract(node_id, tuple_index) => {
                        if let Some(node) = nodes.get_node_mut(node_id) {
                            node.collect_retract_ops(tuple_index, tuples, &mut new_operations);
                        }
                    }
                    NodeOperation::InsertLeft(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.insert_left_collect_ops(tuple_index, tuples, &mut new_operations);
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.insert_left_collect_ops(tuple_index, tuples, &mut new_operations);
                        }
                    }
                    NodeOperation::InsertRight(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.insert_right_collect_ops(tuple_index, tuples, &mut new_operations);
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.insert_right_collect_ops(tuple_index, tuples, &mut new_operations);
                        }
                    }
                    NodeOperation::RetractLeft(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.retract_left_collect_ops(tuple_index, tuples, &mut new_operations);
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.retract_left_collect_ops(tuple_index, tuples, &mut new_operations);
                        }
                    }
                    NodeOperation::RetractRight(node_id, tuple_index) => {
                        if let Some(NodeData::Join(n)) = nodes.get_node_mut(node_id) {
                            n.retract_right_collect_ops(tuple_index, tuples, &mut new_operations);
                        } else if let Some(NodeData::Conditional(n)) = nodes.get_node_mut(node_id) {
                            n.retract_right_collect_ops(tuple_index, tuples, &mut new_operations);
                        }
                    }
                    NodeOperation::ReleaseTuple(idx) => {
                        // This shouldn't happen here, but handle it
                        release_ops.push(idx);
                    }
                }

                // Separate new operations
                for new_op in new_operations {
                    match new_op {
                        NodeOperation::ReleaseTuple(idx) => release_ops.push(idx),
                        other => node_ops.push(other),
                    }
                }
            }
        }

        // CRITICAL: Now release all marked tuples after all operations complete
        for tuple_idx in release_ops {
            tuples.release_tuple(tuple_idx);
        }
    }
}

impl<S: Score> std::fmt::Debug for NodeArena<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut node_counts = std::collections::HashMap::new();
        for (_, node_data) in self.nodes.iter() {
            let node_type = match node_data {
                NodeData::From(_) => "From",
                NodeData::Filter(_) => "Filter",
                NodeData::Join(_) => "Join",
                NodeData::Conditional(_) => "Conditional",
                NodeData::JoinLeftAdapter(_) => "JoinLeftAdapter",
                NodeData::JoinRightAdapter(_) => "JoinRightAdapter",
                NodeData::Group(_) => "Group",
                NodeData::FlatMap(_) => "FlatMap",
                NodeData::Scoring(_) => "Scoring",
            };
            *node_counts.entry(node_type).or_insert(0) += 1;
        }

        f.debug_struct("NodeArena")
            .field("total_nodes", &self.nodes.len())
            .field("node_type_counts", &node_counts)
            .finish()
    }
}

impl<S: Score> Default for NodeArena<S> {
    fn default() -> Self {
        Self::new()
    }
}