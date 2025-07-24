//nodes.rs
use super::advanced_index::AdvancedIndex;
use super::arena::{NodeId, NodeOperation, SafeTupleIndex, TupleArena};
use super::collectors::{BaseCollector, UndoFunction};
use super::joiner::JoinerType;
use super::tuple::BiTuple;
use super::uni_index::UniIndex;
use super::stream_def::CollectorSupplier;
use crate::constraint::ConstraintWeights;
use crate::score::Score;
use crate::state::TupleState;
use crate::tuple::{AnyTuple, FactIterator};
use crate::{Result, GreynetError};
use std::any::TypeId;
use std::cell::RefCell;
use rustc_hash::FxHashMap as HashMap;
use std::rc::Rc;

/// Optimized enum dispatch for key functions
#[derive(Clone)]
pub enum KeyFunction {
    HashBased(fn(&AnyTuple) -> u64),
    FieldExtractor(fn(&AnyTuple) -> u64),
    Custom(Rc<dyn Fn(&AnyTuple) -> u64>),
}

impl KeyFunction {
    #[inline]
    pub fn extract(&self, tuple: &AnyTuple) -> u64 {
        match self {
            KeyFunction::HashBased(f) => f(tuple),
            KeyFunction::FieldExtractor(f) => f(tuple),
            KeyFunction::Custom(f) => f(tuple),
        }
    }
}

pub type SharedImpactFn<S> = Rc<dyn Fn(&AnyTuple) -> S>;
pub type SharedKeyFn = Rc<dyn Fn(&AnyTuple) -> u64>;
pub type SharedPredicate = Rc<dyn Fn(&AnyTuple) -> bool>;
pub type SharedMapperFn = Rc<dyn Fn(&AnyTuple) -> Vec<Rc<dyn crate::fact::GreynetFact>>>;

#[derive(Debug)]
pub struct FromNode {
    pub children: Vec<NodeId>,
    pub fact_type: TypeId,
}

impl FromNode {
    pub fn new(fact_type: TypeId) -> Self {
        Self {
            children: Vec::new(),
            fact_type,
        }
    }

    #[inline]
    pub fn insert_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        _tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        for &child_id in &self.children {
            operations.push(NodeOperation::Insert(child_id, tuple_index));
        }
        Ok(())
    }

    #[inline]
    pub fn retract_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        _tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        for &child_id in &self.children {
            operations.push(NodeOperation::Retract(child_id, tuple_index));
        }
        Ok(())
    }
}

pub struct FilterNode {
    pub children: Vec<NodeId>,
    pub predicate: SharedPredicate,
}

impl FilterNode {
    pub fn new(predicate: SharedPredicate) -> Self {
        Self {
            children: Vec::new(),
            predicate,
        }
    }

    #[inline]
    pub fn insert_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            if (self.predicate)(tuple) {
                for &child_id in &self.children {
                    operations.push(NodeOperation::Insert(child_id, tuple_index));
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn retract_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            if (self.predicate)(tuple) {
                for &child_id in &self.children {
                    operations.push(NodeOperation::Retract(child_id, tuple_index));
                }
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for FilterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterNode")
            .field("children", &self.children)
            .field("predicate", &"<function>")
            .finish()
    }
}

#[derive(Debug)]
enum JoinIndex {
    Uni(UniIndex<u64>),
    Advanced(AdvancedIndex<u64>),
}

impl JoinIndex {
    fn new(joiner_type: JoinerType) -> Self {
        match joiner_type {
            JoinerType::Equal => JoinIndex::Uni(UniIndex::new()),
            _ => JoinIndex::Advanced(AdvancedIndex::new(joiner_type)),
        }
    }

    #[inline]
    fn put(&mut self, key: u64, tuple_index: SafeTupleIndex) {
        match self {
            JoinIndex::Uni(i) => i.put(key, tuple_index),
            JoinIndex::Advanced(i) => i.put(key, tuple_index),
        }
    }

    #[inline]
    fn remove(&mut self, key: u64, tuple_index: &SafeTupleIndex) {
        match self {
            JoinIndex::Uni(i) => i.remove(key, tuple_index),
            JoinIndex::Advanced(i) => i.remove(key, tuple_index),
        }
    }

    #[inline]
    fn get_matches(&self, key: u64, joiner_type: JoinerType) -> Vec<SafeTupleIndex> {
        match self {
            JoinIndex::Uni(i) => i.get(key).to_vec(),
            JoinIndex::Advanced(i) => i.get_matches(key, joiner_type),
        }
    }
}

pub struct JoinNode {
    pub children: Vec<NodeId>,
    pub joiner_type: JoinerType,
    pub left_index: JoinIndex,
    pub right_index: JoinIndex,
    pub left_key_fn: SharedKeyFn,
    pub right_key_fn: SharedKeyFn,
    pub beta_memory: HashMap<(SafeTupleIndex, SafeTupleIndex), SafeTupleIndex>,
}

impl JoinNode {
    pub fn new(
        joiner_type: JoinerType,
        left_key_fn: SharedKeyFn,
        right_key_fn: SharedKeyFn,
    ) -> Self {
        Self {
            children: Vec::new(),
            joiner_type,
            left_index: JoinIndex::new(joiner_type),
            right_index: JoinIndex::new(joiner_type.inverse()),
            left_key_fn,
            right_key_fn,
            beta_memory: HashMap::default(),
        }
    }

    pub fn insert_left_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let left_tuple = tuples.get_tuple_checked(tuple_index)?.clone();
        let key = (self.left_key_fn)(&left_tuple);
        self.left_index.put(key, tuple_index);

        let right_matches = self.right_index.get_matches(key, self.joiner_type.inverse());
        for &right_match_idx in &right_matches {
            let right_tuple = tuples.get_tuple_checked(right_match_idx)?.clone();

            match left_tuple.combine(&right_tuple) {
                Ok(combined) => {
                    let child_idx = tuples.acquire_tuple(combined)?;
                    self.beta_memory.insert((tuple_index, right_match_idx), child_idx);
                    for &child_id in self.children.iter() {
                        operations.push(NodeOperation::Insert(child_id, child_idx));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn insert_right_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let right_tuple = tuples.get_tuple_checked(tuple_index)?.clone();
        let key = (self.right_key_fn)(&right_tuple);
        self.right_index.put(key, tuple_index);

        let left_matches = self.left_index.get_matches(key, self.joiner_type);
        for &left_match_idx in &left_matches {
            let left_tuple = tuples.get_tuple_checked(left_match_idx)?.clone();

            match left_tuple.combine(&right_tuple) {
                Ok(combined) => {
                    let child_idx = tuples.acquire_tuple(combined)?;
                    self.beta_memory.insert((left_match_idx, tuple_index), child_idx);
                    for &child_id in self.children.iter() {
                        operations.push(NodeOperation::Insert(child_id, child_idx));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn retract_left_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            let key = (self.left_key_fn)(tuple);
            self.left_index.remove(key, &tuple_index);
        }

        let pairs_to_remove: Vec<_> = self
            .beta_memory
            .keys()
            .filter(|(left, _)| *left == tuple_index)
            .cloned()
            .collect();

        for (left_idx, right_idx) in pairs_to_remove {
            if let Some(child_idx) = self.beta_memory.remove(&(left_idx, right_idx)) {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, child_idx));
                }

                if let Ok(child_tuple) = tuples.get_tuple_mut_checked(child_idx) {
                    child_tuple.set_state(TupleState::Dying);
                }

                operations.push(NodeOperation::ReleaseTuple(child_idx));
            }
        }
        Ok(())
    }

    pub fn retract_right_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            let key = (self.right_key_fn)(tuple);
            self.right_index.remove(key, &tuple_index);
        }

        let pairs_to_remove: Vec<_> = self
            .beta_memory
            .keys()
            .filter(|(_, right)| *right == tuple_index)
            .cloned()
            .collect();

        for (left_idx, right_idx) in pairs_to_remove {
            if let Some(child_idx) = self.beta_memory.remove(&(left_idx, right_idx)) {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, child_idx));
                }

                if let Ok(child_tuple) = tuples.get_tuple_mut_checked(child_idx) {
                    child_tuple.set_state(TupleState::Dying);
                }

                operations.push(NodeOperation::ReleaseTuple(child_idx));
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for JoinNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinNode")
            .field("children", &self.children)
            .field("joiner_type", &self.joiner_type)
            .field("beta_memory", &self.beta_memory)
            .field("left_key_fn", &"<function>")
            .field("right_key_fn", &"<function>")
            .finish()
    }
}

pub struct ConditionalNode {
    pub children: Vec<NodeId>,
    should_exist: bool,
    left_index: UniIndex<u64>,
    right_index: UniIndex<u64>,
    left_key_fn: SharedKeyFn,
    right_key_fn: SharedKeyFn,
    propagation_map: HashMap<SafeTupleIndex, u64>,
}

impl ConditionalNode {
    pub fn new(
        should_exist: bool,
        left_key_fn: SharedKeyFn,
        right_key_fn: SharedKeyFn,
    ) -> Self {
        Self {
            children: Vec::new(),
            should_exist,
            left_index: UniIndex::new(),
            right_index: UniIndex::new(),
            left_key_fn,
            right_key_fn,
            propagation_map: HashMap::default(),
        }
    }

    pub fn insert_left_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let key = if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            (self.left_key_fn)(tuple)
        } else {
            return Ok(());
        };

        self.left_index.put(key, tuple_index);
        let right_match_count = self.right_index.get(key).len() as u64;
        let has_matches = right_match_count > 0;
        self.propagation_map.insert(tuple_index, right_match_count);

        if has_matches == self.should_exist {
            for &child_id in self.children.iter() {
                operations.push(NodeOperation::Insert(child_id, tuple_index));
            }
        }
        Ok(())
    }

    pub fn insert_right_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let key = if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            (self.right_key_fn)(tuple)
        } else {
            return Ok(());
        };

        self.right_index.put(key, tuple_index);
        let left_matches = self.left_index.get(key).to_vec();

        for &left_match_idx in &left_matches {
            let match_count = self.propagation_map.entry(left_match_idx).or_insert(0);
            let was_propagated = (*match_count > 0) == self.should_exist;
            *match_count += 1;
            let is_now_propagated = (*match_count > 0) == self.should_exist;

            if !was_propagated && is_now_propagated {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Insert(child_id, left_match_idx));
                }
            } else if was_propagated && !is_now_propagated {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, left_match_idx));
                }
            }
        }
        Ok(())
    }

    pub fn retract_left_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let key = if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            (self.left_key_fn)(tuple)
        } else {
            return Ok(());
        };

        self.left_index.remove(key, &tuple_index);

        if let Some(match_count) = self.propagation_map.remove(&tuple_index) {
            if (match_count > 0) == self.should_exist {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, tuple_index));
                }
            }
        }
        Ok(())
    }

    pub fn retract_right_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let key = if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            (self.right_key_fn)(tuple)
        } else {
            return Ok(());
        };

        self.right_index.remove(key, &tuple_index);
        let left_matches = self.left_index.get(key).to_vec();

        for &left_match_idx in &left_matches {
            if let Some(match_count) = self.propagation_map.get_mut(&left_match_idx) {
                let was_propagated = (*match_count > 0) == self.should_exist;
                *match_count = match_count.saturating_sub(1);
                let is_now_propagated = (*match_count > 0) == self.should_exist;

                if was_propagated && !is_now_propagated {
                    for &child_id in self.children.iter() {
                        operations.push(NodeOperation::Retract(child_id, left_match_idx));
                    }
                } else if !was_propagated && is_now_propagated {
                    for &child_id in self.children.iter() {
                        operations.push(NodeOperation::Insert(child_id, left_match_idx));
                    }
                }
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for ConditionalNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConditionalNode")
            .field("children", &self.children)
            .field("should_exist", &self.should_exist)
            .field("propagation_map", &self.propagation_map)
            .field("left_key_fn", &"<function>")
            .field("right_key_fn", &"<function>")
            .finish()
    }
}

pub struct GroupNode {
    pub children: Vec<NodeId>,
    key_fn: SharedKeyFn,
    collector_supplier: CollectorSupplier,
    groups: HashMap<u64, Box<dyn BaseCollector>>,
    tuple_to_undo: HashMap<SafeTupleIndex, (u64, UndoFunction)>,
    group_key_to_tuple: HashMap<u64, SafeTupleIndex>,
}

impl GroupNode {
    pub fn new(
        key_fn: SharedKeyFn,
        collector_supplier: CollectorSupplier,
    ) -> Self {
        Self {
            children: Vec::new(),
            key_fn,
            collector_supplier,
            groups: HashMap::default(),
            tuple_to_undo: HashMap::default(),
            group_key_to_tuple: HashMap::default(),
        }
    }

    pub fn insert_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let parent_tuple = tuples.get_tuple_checked(tuple_index)?;
        let key = (self.key_fn)(parent_tuple);
        
        let collector = self
            .groups
            .entry(key)
            .or_insert_with(|| self.collector_supplier.create());
        let undo_fn = collector.insert(parent_tuple);
        self.tuple_to_undo.insert(tuple_index, (key, undo_fn));
        self.update_or_create_child(key, tuples, operations)?;
        Ok(())
    }

    pub fn retract_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        if let Some((key, undo_fn)) = self.tuple_to_undo.remove(&tuple_index) {
            undo_fn.execute();
            let is_empty = self.groups.get(&key).map_or(true, |c| c.is_empty());

            if is_empty {
                if let Some(child_tuple_index) = self.group_key_to_tuple.remove(&key) {
                    for &child_id in self.children.iter() {
                        operations.push(NodeOperation::Retract(child_id, child_tuple_index));
                    }

                    if let Ok(child_tuple) = tuples.get_tuple_mut_checked(child_tuple_index) {
                        child_tuple.set_state(TupleState::Dying);
                    }

                    operations.push(NodeOperation::ReleaseTuple(child_tuple_index));
                }
                self.groups.remove(&key);
            } else {
                self.update_or_create_child(key, tuples, operations)?;
            }
        }
        Ok(())
    }

    fn update_or_create_child(
        &mut self,
        key: u64,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let new_result_fact = {
            let collector = self.groups.get_mut(&key)
                .ok_or_else(|| GreynetError::arena_error("Collector not found for key"))?;
            collector.result_as_fact()
        };

        if let Some(&old_child_index) = self.group_key_to_tuple.get(&key) {
            let old_tuple = tuples.get_tuple_checked(old_child_index)?;
            let old_result_fact = match old_tuple {
                AnyTuple::Bi(t) => t.fact_b.clone(),
                _ => return Err(GreynetError::type_mismatch("GroupNode child should be BiTuple")),
            };

            if old_result_fact.eq_fact(&*new_result_fact) {
                return Ok(());
            }

            for &child_id in self.children.iter() {
                operations.push(NodeOperation::Retract(child_id, old_child_index));
            }

            if let Ok(old_tuple_mut) = tuples.get_tuple_mut_checked(old_child_index) {
                old_tuple_mut.set_state(TupleState::Dying);
            }

            operations.push(NodeOperation::ReleaseTuple(old_child_index));
        }

        let key_fact: Rc<dyn crate::fact::GreynetFact> = Rc::new(key);
        let new_child_tuple = AnyTuple::Bi(BiTuple::new(key_fact, new_result_fact));
        let new_child_index = tuples.acquire_tuple(new_child_tuple)?;
        self.group_key_to_tuple.insert(key, new_child_index);

        for &child_id in self.children.iter() {
            operations.push(NodeOperation::Insert(child_id, new_child_index));
        }
        Ok(())
    }
}

impl std::fmt::Debug for GroupNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupNode")
            .field("children", &self.children)
            .field("groups", &self.groups.len())
            .field("key_fn", &"<function>")
            .field("collector_supplier", &self.collector_supplier)
            .finish()
    }
}

pub struct FlatMapNode {
    pub children: Vec<NodeId>,
    mapper_fn: SharedMapperFn,
    parent_to_children_map: HashMap<SafeTupleIndex, Vec<SafeTupleIndex>>,
}

impl FlatMapNode {
    pub fn new(mapper_fn: SharedMapperFn) -> Self {
        Self {
            children: Vec::new(),
            mapper_fn,
            parent_to_children_map: HashMap::default(),
        }
    }

    pub fn insert_collect_ops(
        &mut self,
        parent_tuple_idx: SafeTupleIndex,
        tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        let parent_tuple = tuples.get_tuple_checked(parent_tuple_idx)?;
        let new_facts = (self.mapper_fn)(parent_tuple);
        
        if new_facts.is_empty() {
            return Ok(());
        }

        let mut child_indices = Vec::new();
        for fact in new_facts {
            let new_tuple = AnyTuple::Uni(crate::tuple::UniTuple::new(fact));
            let child_idx = tuples.acquire_tuple(new_tuple)?;
            child_indices.push(child_idx);
            for &child_id in self.children.iter() {
                operations.push(NodeOperation::Insert(child_id, child_idx));
            }
        }
        self.parent_to_children_map.insert(parent_tuple_idx, child_indices);
        Ok(())
    }

    pub fn retract_collect_ops(
        &mut self,
        parent_tuple_idx: SafeTupleIndex,
        _tuples: &mut TupleArena,
        operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        if let Some(child_indices) = self.parent_to_children_map.remove(&parent_tuple_idx) {
            for child_idx in child_indices {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, child_idx));
                }
                operations.push(NodeOperation::ReleaseTuple(child_idx));
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for FlatMapNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatMapNode")
            .field("children", &self.children)
            .field("mapper_fn", &"<function>")
            .finish()
    }
}

pub struct ScoringNode<S: Score> {
    pub constraint_id: String,
    penalty_function: SharedImpactFn<S>,
    weights: Rc<RefCell<ConstraintWeights>>,
    matches: HashMap<SafeTupleIndex, S>,
}

impl<S: Score> ScoringNode<S> {
    pub fn new(
        constraint_id: String,
        penalty_function: SharedImpactFn<S>,
        weights: Rc<RefCell<ConstraintWeights>>,
    ) -> Self {
        Self {
            constraint_id,
            penalty_function,
            weights,
            matches: HashMap::default(),
        }
    }

    #[inline]
    pub fn insert_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        tuples: &mut TupleArena,
        _operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        if let Ok(tuple) = tuples.get_tuple_checked(tuple_index) {
            let base_score = (self.penalty_function)(tuple);
            let weight = self.weights.borrow().get_weight(&self.constraint_id);
            let weighted_score = base_score.mul(weight);
            self.matches.insert(tuple_index, weighted_score);
        }
        Ok(())
    }

    #[inline]
    pub fn retract_collect_ops(
        &mut self,
        tuple_index: SafeTupleIndex,
        _tuples: &mut TupleArena,
        _operations: &mut Vec<NodeOperation>,
    ) -> Result<()> {
        self.matches.remove(&tuple_index);
        Ok(())
    }

    #[inline]
    pub fn get_total_score(&self) -> S {
        self.matches
            .values()
            .fold(S::null_score(), |acc, score| acc + score.clone())
    }

    pub fn recalculate_scores(&mut self, tuples: &TupleArena) -> Result<()> {
        let weight = self.weights.borrow().get_weight(&self.constraint_id);
        for (tuple_idx, score_val) in self.matches.iter_mut() {
            if let Ok(tuple) = tuples.get_tuple_checked(*tuple_idx) {
                let base_score = (self.penalty_function)(tuple);
                *score_val = base_score.mul(weight);
            }
        }
        Ok(())
    }

    pub fn match_indices(&self) -> impl Iterator<Item = &SafeTupleIndex> {
        self.matches.keys()
    }
}

impl<S: Score> std::fmt::Debug for ScoringNode<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScoringNode")
            .field("constraint_id", &self.constraint_id)
            .field("matches", &self.matches.len())
            .field("penalty_function", &"<function>")
            .finish()
    }
}