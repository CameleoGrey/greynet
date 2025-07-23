//nodes.rs
use super::advanced_index::AdvancedIndex;
use super::arena::{NodeId, NodeOperation, SafeTupleIndex, TupleArena};
use super::collectors::BaseCollector;
use super::joiner::JoinerType;
use super::tuple::BiTuple;
use super::uni_index::UniIndex;
use crate::AnyTuple;
use std::any::TypeId;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use crate::{Score, constraint::ConstraintWeights};
use crate::collectors::UndoFunction;

pub type SharedImpactFn<S> = Rc<dyn Fn(&crate::tuple::AnyTuple) -> S>;
pub type SharedKeyFn = Rc<dyn Fn(&AnyTuple) -> u64>;
pub type SharedPredicate = Rc<dyn Fn(&AnyTuple) -> bool>;
pub type SharedMapperFn = Rc<dyn Fn(&crate::tuple::AnyTuple) -> Vec<Rc<dyn crate::fact::GreynetFact>>>;

#[derive(Debug)]
pub struct FromNode {
    pub children: Vec<NodeId>,
    pub fact_type: TypeId,
}

impl FromNode {
    pub fn new(fact_type: TypeId) -> Self {
        Self { children: Vec::new(), fact_type }
    }

    pub fn insert_collect_ops(&mut self, tuple_index: SafeTupleIndex, _tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        for &child_id in &self.children {
            operations.push(NodeOperation::Insert(child_id, tuple_index));
        }
    }

    pub fn retract_collect_ops(&mut self, tuple_index: SafeTupleIndex, _tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        for &child_id in &self.children {
            operations.push(NodeOperation::Retract(child_id, tuple_index));
        }
    }
}

pub struct FilterNode {
    pub children: Vec<NodeId>,
    pub predicate: SharedPredicate,
}

impl FilterNode {
    pub fn new(predicate: SharedPredicate) -> Self {
        Self { children: Vec::new(), predicate }
    }

    pub fn insert_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        if let Some(tuple) = tuples.get_tuple(tuple_index) {
            if (self.predicate)(tuple) {
                for &child_id in &self.children {
                    operations.push(NodeOperation::Insert(child_id, tuple_index));
                }
            }
        }
    }

    pub fn retract_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        if let Some(tuple) = tuples.get_tuple(tuple_index) {
            if (self.predicate)(tuple) {
                for &child_id in &self.children {
                    operations.push(NodeOperation::Retract(child_id, tuple_index));
                }
            }
        }
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

    fn put(&mut self, key: u64, tuple_index: SafeTupleIndex) {
        match self {
            JoinIndex::Uni(i) => i.put(key, tuple_index),
            JoinIndex::Advanced(i) => i.put(key, tuple_index),
        }
    }

    fn remove(&mut self, key: u64, tuple_index: &SafeTupleIndex) {
        match self {
            JoinIndex::Uni(i) => i.remove(key, tuple_index),
            JoinIndex::Advanced(i) => i.remove(key, tuple_index),
        }
    }

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
    pub fn new(joiner_type: JoinerType, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Self {
        Self {
            children: Vec::new(),
            joiner_type,
            left_index: JoinIndex::new(joiner_type),
            right_index: JoinIndex::new(joiner_type.inverse()),
            left_key_fn,
            right_key_fn,
            beta_memory: HashMap::new(),
        }
    }

    pub fn insert_left_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let left_tuple = if let Some(t) = tuples.get_tuple(tuple_index) {
            t.clone()
        } else {
            return;
        };

        let key = (self.left_key_fn)(&left_tuple);
        self.left_index.put(key, tuple_index);

        let right_matches = self.right_index.get_matches(key, self.joiner_type.inverse());
        for &right_match_idx in &right_matches {
            let right_tuple = if let Some(t) = tuples.get_tuple(right_match_idx) {
                t.clone()
            } else {
                continue;
            };

            if let Ok(combined) = left_tuple.combine(&right_tuple) {
                let child_idx = tuples.acquire_tuple(combined);
                self.beta_memory.insert((tuple_index, right_match_idx), child_idx);
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Insert(child_id, child_idx));
                }
            }
        }
    }

    pub fn insert_right_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let right_tuple = if let Some(t) = tuples.get_tuple(tuple_index) {
            t.clone()
        } else {
            return;
        };

        let key = (self.right_key_fn)(&right_tuple);
        self.right_index.put(key, tuple_index);

        let left_matches = self.left_index.get_matches(key, self.joiner_type);
        for &left_match_idx in &left_matches {
            let left_tuple = if let Some(t) = tuples.get_tuple(left_match_idx) {
                t.clone()
            } else {
                continue;
            };

            if let Ok(combined) = left_tuple.combine(&right_tuple) {
                let child_idx = tuples.acquire_tuple(combined);
                self.beta_memory.insert((left_match_idx, tuple_index), child_idx);
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Insert(child_id, child_idx));
                }
            }
        }
    }

    pub fn retract_left_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        if let Some(tuple) = tuples.get_tuple(tuple_index) {
            let key = (self.left_key_fn)(tuple);
            self.left_index.remove(key, &tuple_index);
        }

        let pairs_to_remove: Vec<_> = self.beta_memory.keys()
            .filter(|(left, _)| *left == tuple_index)
            .cloned()
            .collect();

        for (left_idx, right_idx) in pairs_to_remove {
            if let Some(child_idx) = self.beta_memory.remove(&(left_idx, right_idx)) {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, child_idx));
                }
            }
        }
    }

    pub fn retract_right_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        if let Some(tuple) = tuples.get_tuple(tuple_index) {
            let key = (self.right_key_fn)(tuple);
            self.right_index.remove(key, &tuple_index);
        }

        let pairs_to_remove: Vec<_> = self.beta_memory.keys()
            .filter(|(_, right)| *right == tuple_index)
            .cloned()
            .collect();

        for (left_idx, right_idx) in pairs_to_remove {
            if let Some(child_idx) = self.beta_memory.remove(&(left_idx, right_idx)) {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, child_idx));
                }
            }
        }
    }
}

impl std::fmt::Debug for JoinNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinNode")
            .field("children", &self.children)
            .field("joiner_type", &self.joiner_type)
            .field("left_index", &self.left_index)
            .field("right_index", &self.right_index)
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
    pub fn new(should_exist: bool, left_key_fn: SharedKeyFn, right_key_fn: SharedKeyFn) -> Self {
        Self {
            children: Vec::new(),
            should_exist,
            left_index: UniIndex::new(),
            right_index: UniIndex::new(),
            left_key_fn,
            right_key_fn,
            propagation_map: HashMap::new(),
        }
    }

    pub fn insert_left_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let key = if let Some(tuple) = tuples.get_tuple(tuple_index) {
            (self.left_key_fn)(tuple)
        } else {
            return;
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
    }

    pub fn insert_right_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let key = if let Some(tuple) = tuples.get_tuple(tuple_index) {
            (self.right_key_fn)(tuple)
        } else {
            return;
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
    }

    pub fn retract_left_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let key = if let Some(tuple) = tuples.get_tuple(tuple_index) {
            (self.left_key_fn)(tuple)
        } else {
            return;
        };

        self.left_index.remove(key, &tuple_index);

        if let Some(match_count) = self.propagation_map.remove(&tuple_index) {
            if (match_count > 0) == self.should_exist {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, tuple_index));
                }
            }
        }
    }

    pub fn retract_right_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let key = if let Some(tuple) = tuples.get_tuple(tuple_index) {
            (self.right_key_fn)(tuple)
        } else {
            return;
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
    }
}

impl std::fmt::Debug for ConditionalNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConditionalNode")
            .field("children", &self.children)
            .field("should_exist", &self.should_exist)
            .field("left_index", &self.left_index)
            .field("right_index", &self.right_index)
            .field("propagation_map", &self.propagation_map)
            .field("left_key_fn", &"<function>")
            .field("right_key_fn", &"<function>")
            .finish()
    }
}

pub struct GroupNode {
    pub children: Vec<NodeId>,
    key_fn: SharedKeyFn,
    collector_supplier: Rc<dyn Fn() -> Box<dyn BaseCollector>>,
    groups: HashMap<u64, Box<dyn BaseCollector>>,
    tuple_to_undo: HashMap<SafeTupleIndex, (u64, UndoFunction)>,
    group_key_to_tuple: HashMap<u64, SafeTupleIndex>,
}

impl GroupNode {
    pub fn new(key_fn: SharedKeyFn, collector_supplier: Rc<dyn Fn() -> Box<dyn BaseCollector>>) -> Self {
        Self {
            children: Vec::new(),
            key_fn,
            collector_supplier,
            groups: HashMap::new(),
            tuple_to_undo: HashMap::new(),
            group_key_to_tuple: HashMap::new(),
        }
    }

    pub fn insert_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let parent_tuple = if let Some(t) = tuples.get_tuple(tuple_index) {
            t
        } else {
            return;
        };

        let key = (self.key_fn)(parent_tuple);
        let collector = self.groups.entry(key).or_insert_with(|| (self.collector_supplier)());
        let undo_fn = collector.insert(parent_tuple);
        self.tuple_to_undo.insert(tuple_index, (key, undo_fn));
        self.update_or_create_child(key, tuples, operations);
    }

    pub fn retract_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        if let Some((key, undo_fn)) = self.tuple_to_undo.remove(&tuple_index) {
            undo_fn.execute();
            let is_empty = self.groups.get(&key).map_or(true, |c| c.is_empty());

            if is_empty {
                if let Some(child_tuple_index) = self.group_key_to_tuple.remove(&key) {
                    for &child_id in self.children.iter() {
                        operations.push(NodeOperation::Retract(child_id, child_tuple_index));
                    }
                }
                self.groups.remove(&key);
            } else {
                self.update_or_create_child(key, tuples, operations);
            }
        }
    }

    fn update_or_create_child(&mut self, key: u64, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let new_result_fact = {
            let collector = if let Some(c) = self.groups.get_mut(&key) {
                c
            } else {
                return;
            };
            collector.result_as_fact()
        };

        if let Some(&child_tuple_index) = self.group_key_to_tuple.get(&key) {
            let child_tuple = tuples.get_tuple(child_tuple_index).unwrap();
            let old_result_fact = match child_tuple {
                AnyTuple::Bi(t) => t.fact_b.clone(),
                _ => panic!("GroupNode child should be BiTuple"),
            };

            if old_result_fact.eq_fact(&*new_result_fact) {
                return;
            }

            for &child_id in self.children.iter() {
                operations.push(NodeOperation::Retract(child_id, child_tuple_index));
            }
        }

        let key_fact: Rc<dyn crate::fact::GreynetFact> = Rc::new(key);
        let new_child_tuple = AnyTuple::Bi(BiTuple::new(key_fact, new_result_fact));
        let new_child_index = tuples.acquire_tuple(new_child_tuple);
        self.group_key_to_tuple.insert(key, new_child_index);

        for &child_id in self.children.iter() {
            operations.push(NodeOperation::Insert(child_id, new_child_index));
        }
    }
}

impl std::fmt::Debug for GroupNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupNode")
            .field("children", &self.children)
            .field("groups", &self.groups.len())
            .field("key_fn", &"<function>")
            .field("collector_supplier", &"<function>")
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
            parent_to_children_map: HashMap::new(),
        }
    }

    pub fn insert_collect_ops(&mut self, parent_tuple_idx: SafeTupleIndex, tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        let parent_tuple = if let Some(tuple) = tuples.get_tuple(parent_tuple_idx) {
            tuple
        } else {
            return;
        };

        let new_facts = (self.mapper_fn)(parent_tuple);
        if new_facts.is_empty() {
            return;
        }

        let mut child_indices = Vec::new();
        for fact in new_facts {
            let new_tuple = AnyTuple::Uni(crate::tuple::UniTuple::new(fact));
            let child_idx = tuples.acquire_tuple(new_tuple);
            child_indices.push(child_idx);
            for &child_id in self.children.iter() {
                operations.push(NodeOperation::Insert(child_id, child_idx));
            }
        }
        self.parent_to_children_map.insert(parent_tuple_idx, child_indices);
    }

    pub fn retract_collect_ops(&mut self, parent_tuple_idx: SafeTupleIndex, _tuples: &mut TupleArena, operations: &mut Vec<NodeOperation>) {
        if let Some(child_indices) = self.parent_to_children_map.remove(&parent_tuple_idx) {
            for child_idx in child_indices {
                for &child_id in self.children.iter() {
                    operations.push(NodeOperation::Retract(child_id, child_idx));
                }
            }
        }
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
    pub fn new(constraint_id: String, penalty_function: SharedImpactFn<S>, weights: Rc<RefCell<ConstraintWeights>>) -> Self {
        Self {
            constraint_id,
            penalty_function,
            weights,
            matches: HashMap::new(),
        }
    }

    pub fn insert_collect_ops(&mut self, tuple_index: SafeTupleIndex, tuples: &mut TupleArena, _operations: &mut Vec<NodeOperation>) {
        if let Some(tuple) = tuples.get_tuple(tuple_index) {
            let base_score = (self.penalty_function)(tuple);
            let weight = self.weights.borrow().get_weight(&self.constraint_id);
            let weighted_score = base_score.mul(weight);
            self.matches.insert(tuple_index, weighted_score);
        }
    }

    pub fn retract_collect_ops(&mut self, tuple_index: SafeTupleIndex, _tuples: &mut TupleArena, _operations: &mut Vec<NodeOperation>) {
        self.matches.remove(&tuple_index);
    }

    pub fn get_total_score(&self) -> S {
        self.matches.values().fold(S::null_score(), |acc, score| acc + score.clone())
    }

    pub fn recalculate_scores(&mut self, tuples: &TupleArena) {
        let weight = self.weights.borrow().get_weight(&self.constraint_id);
        for (tuple_idx, score_val) in self.matches.iter_mut() {
            if let Some(tuple) = tuples.get_tuple(*tuple_idx) {
                let base_score = (self.penalty_function)(tuple);
                *score_val = base_score.mul(weight);
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::{NodeArena, NodeOperation, TupleArena};
    use crate::collectors::Collectors;
    use crate::fact::GreynetFact;
    use crate::joiner::JoinerType;
    use crate::tuple::{AnyTuple, BiTuple, UniTuple};
    use std::cell::RefCell;
    use std::collections::HashSet;
    use uuid::Uuid;

    // --- Test Setup ---

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestFact {
        id: Uuid,
        value: u64,
    }

    impl TestFact {
        fn new(value: u64) -> Self {
            Self {
                id: Uuid::new_v4(),
                value,
            }
        }
    }

    impl GreynetFact for TestFact {
        fn fact_id(&self) -> Uuid {
            self.id
        }
        fn clone_fact(&self) -> Box<dyn GreynetFact> {
            Box::new(self.clone())
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    // Helper to get the value from a TestFact inside an AnyTuple
    fn get_fact_value(tuple: &AnyTuple) -> u64 {
        let facts = tuple.facts();
        let fact = facts[0].as_any().downcast_ref::<TestFact>().unwrap();
        fact.value
    }

    // Helper to create a key function based on the TestFact's value
    fn key_fn_by_value() -> SharedKeyFn {
        Rc::new(|tuple: &AnyTuple| get_fact_value(tuple))
    }

    // Helper to create a predicate based on the TestFact's value
    fn predicate_is_even() -> SharedPredicate {
        Rc::new(|tuple: &AnyTuple| get_fact_value(tuple) % 2 == 0)
    }

    // Helper to create a tuple in the arena
    fn create_tuple(value: u64, tuples: &mut TupleArena) -> SafeTupleIndex {
        tuples.acquire_tuple(AnyTuple::Uni(UniTuple::new(Rc::new(TestFact::new(value)))))
    }

    // --- FilterNode Tests ---

    // In greynet/src/nodes.rs, within the `tests` module:

    #[test]
    fn test_filter_node() {
        let mut tuples = TupleArena::new();
        let mut operations = Vec::new();

        let mut node = FilterNode::new(predicate_is_even());
        let child_id = NodeId::default();
        node.children.push(child_id);

        // Test with a tuple that passes the filter
        let even_tuple = create_tuple(10, &mut tuples);
        node.insert_collect_ops(even_tuple, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        assert!(
            matches!(operations[0], NodeOperation::Insert(id, t) if id == child_id && t == even_tuple)
        );
        operations.clear();

        // Test with a tuple that fails the filter
        let odd_tuple = create_tuple(9, &mut tuples);
        node.insert_collect_ops(odd_tuple, &mut tuples, &mut operations);
        assert!(operations.is_empty());

        // Test retract
        // FIX: Called retract_collect_ops instead of insert_collect_ops to correctly test retraction.
        node.retract_collect_ops(even_tuple, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        assert!(
            matches!(operations[0], NodeOperation::Retract(id, t) if id == child_id && t == even_tuple)
        );
    }

    // --- JoinNode Tests ---

    #[test]
    fn test_join_node_equal() {
        let mut tuples = TupleArena::new();
        let mut operations = Vec::new();

        let mut node = JoinNode::new(JoinerType::Equal, key_fn_by_value(), key_fn_by_value());
        let child_id = NodeId::default();
        node.children.push(child_id);

        // Insert left and right tuples that should match
        let left_tuple = create_tuple(10, &mut tuples);
        let right_tuple = create_tuple(10, &mut tuples);

        node.insert_left_collect_ops(left_tuple, &mut tuples, &mut operations);
        assert!(operations.is_empty(), "Should not join yet");

        node.insert_right_collect_ops(right_tuple, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1, "Should create one joined tuple");
        if let NodeOperation::Insert(_, joined_idx) = operations[0] {
            let joined_tuple = tuples.get_tuple(joined_idx).unwrap();
            assert_eq!(joined_tuple.arity(), 2);
        } else {
            panic!("Expected Insert operation");
        }

        // Retract left tuple
        operations.clear();
        node.retract_left_collect_ops(left_tuple, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1, "Should retract the joined tuple");
        assert!(matches!(operations[0], NodeOperation::Retract(_, _)));
    }

    // --- ConditionalNode Tests ---

    #[test]
    fn test_conditional_node_if_exists() {
        let mut tuples = TupleArena::new();
        let mut operations = Vec::new();
        let mut node = ConditionalNode::new(true, key_fn_by_value(), key_fn_by_value());
        node.children.push(NodeId::default());

        let left1 = create_tuple(10, &mut tuples);
        let right1 = create_tuple(10, &mut tuples);

        // Insert left, no right exists yet, should not propagate
        node.insert_left_collect_ops(left1, &mut tuples, &mut operations);
        assert!(operations.is_empty());

        // Insert right, now a match exists, should propagate left
        node.insert_right_collect_ops(right1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        assert!(matches!(operations[0], NodeOperation::Insert(_, t) if t == left1));

        // Retract right, no more matches, should retract left
        operations.clear();
        node.retract_right_collect_ops(right1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        assert!(matches!(operations[0], NodeOperation::Retract(_, t) if t == left1));
    }

    #[test]
    fn test_conditional_node_if_not_exists() {
        let mut tuples = TupleArena::new();
        let mut operations = Vec::new();
        let mut node = ConditionalNode::new(false, key_fn_by_value(), key_fn_by_value());
        node.children.push(NodeId::default());

        let left1 = create_tuple(10, &mut tuples);
        let right1 = create_tuple(10, &mut tuples);

        // Insert left, no right exists, should propagate
        node.insert_left_collect_ops(left1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        assert!(matches!(operations[0], NodeOperation::Insert(_, t) if t == left1));

        // Insert right, now a match exists, should retract left
        operations.clear();
        node.insert_right_collect_ops(right1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        assert!(matches!(operations[0], NodeOperation::Retract(_, t) if t == left1));

        // Retract right, no more matches, should re-insert left
        operations.clear();
        node.retract_right_collect_ops(right1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        assert!(matches!(operations[0], NodeOperation::Insert(_, t) if t == left1));
    }

    // --- GroupNode Tests ---

    #[test]
    fn test_group_node_count() {
        let mut tuples = TupleArena::new();
        let mut operations = Vec::new();
        let mut node = GroupNode::new(key_fn_by_value(), Rc::from(Collectors::count()));
        node.children.push(NodeId::default());

        let t1 = create_tuple(10, &mut tuples); // group 10
        let t2 = create_tuple(20, &mut tuples); // group 20
        let t3 = create_tuple(10, &mut tuples); // group 10

        // Insert first tuple for group 10
        node.insert_collect_ops(t1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);
        let agg_idx1 = if let NodeOperation::Insert(_, idx) = operations[0] {
            let agg_tuple = tuples.get_tuple(idx).unwrap();
            assert_eq!(
                agg_tuple.facts()[1].as_any().downcast_ref::<usize>(),
                Some(&1)
            );
            idx
        } else {
            panic!()
        };

        // Insert tuple for group 20
        operations.clear();
        node.insert_collect_ops(t2, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1);

        // Insert second tuple for group 10, should update
        operations.clear();
        node.insert_collect_ops(t3, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 2); // Retract old, Insert new
        let (retract_op, insert_op) = (&operations[0], &operations[1]);
        assert!(matches!(retract_op, NodeOperation::Retract(_, t) if *t == agg_idx1));
        if let NodeOperation::Insert(_, new_agg_idx) = insert_op {
            let agg_tuple = tuples.get_tuple(*new_agg_idx).unwrap();
            assert_eq!(
                agg_tuple.facts()[1].as_any().downcast_ref::<usize>(),
                Some(&2)
            );
        } else {
            panic!()
        };

        // Retract one from group 10
        operations.clear();
        node.retract_collect_ops(t1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 2); // Retract old, Insert new
        if let NodeOperation::Insert(_, new_agg_idx) = operations[1] {
            let agg_tuple = tuples.get_tuple(new_agg_idx).unwrap();
            assert_eq!(
                agg_tuple.facts()[1].as_any().downcast_ref::<usize>(),
                Some(&1)
            );
        } else {
            panic!()
        };

        // Retract last from group 10
        operations.clear();
        node.retract_collect_ops(t3, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 1); // Just retract
        assert!(matches!(operations[0], NodeOperation::Retract(_, _)));
    }

    // --- FlatMapNode Tests ---

    #[test]
    fn test_flat_map_node() {
        let mut tuples = TupleArena::new();
        let mut operations = Vec::new();
        // Mapper fn creates N new facts based on the value of the input fact
        let mapper_fn: SharedMapperFn = Rc::new(|tuple: &AnyTuple| {
            let val = get_fact_value(tuple);
            (0..val)
                .map(|i| Rc::new(TestFact::new(i)) as Rc<dyn GreynetFact>)
                .collect()
        });
        let mut node = FlatMapNode::new(mapper_fn);
        node.children.push(NodeId::default());

        // Insert a parent tuple that should map to 3 children
        let parent1 = create_tuple(3, &mut tuples);
        node.insert_collect_ops(parent1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 3, "Should create 3 child tuples");

        let child_indices: HashSet<_> = operations
            .iter()
            .map(|op| match op {
                NodeOperation::Insert(_, idx) => *idx,
                _ => panic!(),
            })
            .collect();
        assert_eq!(child_indices.len(), 3);

        // Retract the parent tuple
        operations.clear();
        node.retract_collect_ops(parent1, &mut tuples, &mut operations);
        assert_eq!(operations.len(), 3, "Should retract all 3 children");

        let retracted_indices: HashSet<_> = operations
            .iter()
            .map(|op| match op {
                NodeOperation::Retract(_, idx) => *idx,
                _ => panic!(),
            })
            .collect();
        assert_eq!(retracted_indices, child_indices);
    }
}
