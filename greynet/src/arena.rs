//arena.rs
use crate::join_adapters::*;
use crate::nodes::{
    ConditionalNode, FilterNode, FlatMapNode, FromNode, GroupNode, JoinNode, ScoringNode,
};
use crate::score::Score;
use crate::sparse_set::SparseSet;
use crate::state::TupleState;
use crate::tuple::{AnyTuple, TupleArity};
use crate::{GreynetError, Result, ResourceLimits};
use generational_arena::{Arena, Index as TupleIndex};
use rustc_hash::FxHashMap as HashMap;
use slotmap::{DefaultKey, SlotMap};
use smallvec::SmallVec;
use crate::SimdOps;

pub type NodeId = DefaultKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SafeTupleIndex {
    pub(crate) index: TupleIndex,
    pub(crate) generation: u64,
}

impl SafeTupleIndex {
    #[inline]
    pub fn new(index: TupleIndex, generation: u64) -> Self {
        Self { index, generation }
    }
    #[inline]
    pub fn index(&self) -> TupleIndex {
        self.index
    }
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// High-performance tuple arena with optimized memory layout and safety guarantees
pub struct TupleArena {
    pub arena: Arena<AnyTuple>,
    generation_counter: u64,
    type_pools: HashMap<TupleArity, SmallVec<[SafeTupleIndex; 4]>>,
    // OPTIMIZATION (Guide 2.1): Replace Vec<Option<u64>> with a SparseSet for O(1) operations.
    active_generations: SparseSet<u64>,
    limits: ResourceLimits,
}

// Helper for likely/unlikely branch hints (Guide 2.2)
#[inline(always)]
#[cold]
fn unlikely<T>(val: T) -> T {
    val
}

impl TupleArena {
    pub fn new() -> Self {
        Self::with_limits(ResourceLimits::default())
    }

    pub fn with_limits(limits: ResourceLimits) -> Self {
        Self {
            arena: Arena::new(),
            generation_counter: 0,
            type_pools: HashMap::default(),
            // OPTIMIZATION (Guide 2.1): Initialize SparseSet
            active_generations: SparseSet::new(),
            limits,
        }
    }

    // OPTIMIZATION (Guide 2.2): Fast path for tuple acquisition.
    #[inline(always)]
    pub fn acquire_tuple_fast(&mut self, tuple: AnyTuple) -> Result<SafeTupleIndex> {
        let current_count = self.arena.len();
        if unlikely(current_count >= self.limits.max_tuples) {
            return Err(GreynetError::resource_limit(
                "max_tuples",
                format!("Current: {}, Limit: {}", current_count, self.limits.max_tuples),
            ));
        }

        let arity = tuple.tuple_arity();

        if let Some(pool) = self.type_pools.get_mut(&arity) {
            if let Some(safe_index) = pool.pop() {
                let raw_index = safe_index.index.into_raw_parts().0;

                // OPTIMIZATION (Guide 2.1): Use SparseSet for generation check
                if self.active_generations.get(raw_index) == Some(safe_index.generation) {
                    if let Some(slot) = self.arena.get_mut(safe_index.index) {
                        *slot = tuple;
                        return Ok(safe_index);
                    }
                }
                // Return to pool on validation failure
                pool.push(safe_index);
            }
        }

        // Allocate new
        self.generation_counter = self.generation_counter.wrapping_add(1);
        let index = self.arena.insert(tuple);
        let safe_index = SafeTupleIndex::new(index, self.generation_counter);
        // OPTIMIZATION (Guide 2.1): Use SparseSet for insertion
        self.active_generations
            .insert(index.into_raw_parts().0, self.generation_counter);

        Ok(safe_index)
    }

    // OPTIMIZATION (Guide 2.2): Update acquire_tuple to use the fast path.
    #[inline]
    pub fn acquire_tuple(&mut self, tuple: AnyTuple) -> Result<SafeTupleIndex> {
        self.acquire_tuple_fast(tuple)
    }

    #[inline]
    pub fn get_tuple(&self, safe_index: SafeTupleIndex) -> Option<&AnyTuple> {
        self.get_tuple_checked(safe_index).ok()
    }

    #[inline]
    pub fn get_tuple_mut(&mut self, safe_index: SafeTupleIndex) -> Option<&mut AnyTuple> {
        self.get_tuple_mut_checked(safe_index).ok()
    }

    pub fn get_tuple_checked(&self, safe_index: SafeTupleIndex) -> Result<&AnyTuple> {
        let raw_index = safe_index.index.into_raw_parts().0;
        // OPTIMIZATION (Guide 2.1): Use SparseSet for O(1) lookup.
        if let Some(gen) = self.active_generations.get(raw_index) {
            if gen == safe_index.generation {
                return self
                    .arena
                    .get(safe_index.index)
                    .ok_or_else(|| GreynetError::arena_error("Arena slot empty after generation check"));
            }
        }
        Err(GreynetError::invalid_index(
            "Invalid generation or inactive index",
        ))
    }

    pub fn get_tuple_mut_checked(&mut self, safe_index: SafeTupleIndex) -> Result<&mut AnyTuple> {
        let raw_index = safe_index.index.into_raw_parts().0;
        // OPTIMIZATION (Guide 2.1): Use SparseSet for O(1) lookup.
        if let Some(gen) = self.active_generations.get(raw_index) {
            if gen == safe_index.generation {
                return self
                    .arena
                    .get_mut(safe_index.index)
                    .ok_or_else(|| GreynetError::arena_error("Arena slot empty after generation check"));
            }
        }
        Err(GreynetError::invalid_index(
            "Invalid generation or inactive index",
        ))
    }

    pub fn release_tuple(&mut self, safe_index: SafeTupleIndex) {
        if let Ok(tuple) = self.get_tuple_mut_checked(safe_index) {
            let arity = tuple.tuple_arity();
            tuple.reset();

            self.type_pools.entry(arity).or_default().push(safe_index);

            let raw_index = safe_index.index.into_raw_parts().0;
            // OPTIMIZATION (Guide 2.1): Use SparseSet for O(1) removal.
            self.active_generations.remove(raw_index);
        }
    }

    pub fn cleanup_dying_tuples(&mut self) -> usize {
        // Use SIMD version if available, fallback to original if not
        if cfg!(feature = "simd") {
            self.cleanup_dying_tuples_simd()
        } else {
            self.cleanup_dying_tuples_scalar()
        }
    }

    pub fn cleanup_dying_tuples_scalar(&mut self) -> usize {
        let mut cleaned_count = 0;
        let mut indices_to_clean = Vec::new();

        for (index, tuple) in self.arena.iter() {
            if matches!(tuple.state(), TupleState::Dying) {
                let raw_index = index.into_raw_parts().0;
                if let Some(generation) = self.active_generations.get(raw_index) {
                    indices_to_clean.push(SafeTupleIndex::new(index, generation));
                }
            }
        }

        for safe_index in indices_to_clean {
            self.release_tuple(safe_index);
            cleaned_count += 1;
        }

        cleaned_count
    }

    pub fn cleanup_dying_tuples_simd(&mut self) -> usize {
        let mut states: Vec<TupleState> = Vec::with_capacity(self.arena.len());
        let mut indices: Vec<(generational_arena::Index, u64)> = Vec::with_capacity(self.arena.len());
        
        // Collect states and indices
        for (index, tuple) in self.arena.iter() {
            states.push(tuple.state());
            let raw_index = index.into_raw_parts().0;
            if let Some(generation) = self.active_generations.get(raw_index) {
                indices.push((index, generation));
            }
        }
        
        // Use SIMD to count dying tuples
        let dying_count = SimdOps::count_matching_tuples_simd(&states, TupleState::Dying);
        
        if dying_count == 0 {
            return 0;
        }
        
        // Clean up dying tuples
        let mut cleaned = 0;
        for (i, (&state, &(index, generation))) in states.iter().zip(indices.iter()).enumerate() {
            if state == TupleState::Dying {
                let safe_index = SafeTupleIndex::new(index, generation);
                self.release_tuple(safe_index);
                cleaned += 1;
            }
        }
        
        cleaned
    }

    /// Bulk state transition with SIMD optimization
    pub fn bulk_transition_states(&mut self, from: TupleState, to: TupleState) -> usize {
        let mut states: Vec<TupleState> = Vec::with_capacity(self.arena.len());
        let mut indices: Vec<SafeTupleIndex> = Vec::with_capacity(self.arena.len());
        
        // Collect current states
        for (index, tuple) in self.arena.iter() {
            states.push(tuple.state());
            let raw_index = index.into_raw_parts().0;
            if let Some(generation) = self.active_generations.get(raw_index) {
                indices.push(SafeTupleIndex::new(index, generation));
            }
        }
        
        // Use SIMD to update states
        let updated_count = SimdOps::bulk_update_states_simd(&mut states, from, to);
        
        // Apply the updates back to tuples
        for (i, &safe_index) in indices.iter().enumerate() {
            if i < states.len() {
                if let Ok(tuple) = self.get_tuple_mut_checked(safe_index) {
                    tuple.set_state(states[i]);
                }
            }
        }
        
        updated_count
    }

    #[cfg(debug_assertions)]
    pub fn check_for_leaks(&self) -> Result<()> {
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
            return Err(GreynetError::consistency_violation(format!(
                "Found {} tuples in Dying state",
                dying_count
            )));
        }

        if dead_count != pooled_count {
            return Err(GreynetError::consistency_violation(format!(
                "Inconsistency: {} dead vs {} pooled",
                dead_count, pooled_count
            )));
        }

        Ok(())
    }

    /// Get current memory usage estimate
    pub fn memory_usage_estimate(&self) -> usize {
        self.limits.estimate_memory_usage(self.arena.len(), 0)
    }

    /// Get statistics about the arena
    pub fn stats(&self) -> ArenaStats {
        let mut live_count = 0;
        let mut dead_count = 0;

        for (_, tuple) in self.arena.iter() {
            match tuple.state() {
                TupleState::Dead => dead_count += 1,
                _ => live_count += 1,
            }
        }

        ArenaStats {
            total_slots: self.arena.len(),
            live_tuples: live_count,
            dead_tuples: dead_count,
            pooled_tuples: self.type_pools.values().map(|v| v.len()).sum(),
            generation_counter: self.generation_counter,
        }
    }
}

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
            .field("total_arena_slots", &self.arena.len())
            .field("generation_counter", &self.generation_counter)
            .field("active_generations_len", &self.active_generations.dense.len())
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
            NodeData::Join(_) | NodeData::Conditional(_) => Ok(()),
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
            NodeData::Join(_) | NodeData::Conditional(_) => Ok(()),
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

/// High-performance node arena
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

    pub fn execute_operations(
        mut operations: Vec<NodeOperation>,
        nodes: &mut NodeArena<S>,
        tuples: &mut TupleArena,
    ) -> Result<()> {
        let mut node_ops = Vec::with_capacity(operations.len());
        let mut release_ops = Vec::new();

        // Separate node operations from release operations
        for op in operations.drain(..) {
            match op {
                NodeOperation::ReleaseTuple(idx) => release_ops.push(idx),
                other => node_ops.push(other),
            }
        }

        // Process node operations in batches with proper error handling
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

                // Queue new operations
                for new_op in new_operations {
                    match new_op {
                        NodeOperation::ReleaseTuple(idx) => release_ops.push(idx),
                        other => node_ops.push(other),
                    }
                }
            }
        }

        // Release tuples at the end
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
