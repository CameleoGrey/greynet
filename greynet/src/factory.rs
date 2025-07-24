//factory.rs
use super::arena::{NodeArena, NodeData, NodeId};
use super::node_sharing::NodeSharingManager;
use super::join_adapters::{JoinLeftAdapter, JoinRightAdapter};
use super::nodes::{SharedKeyFn, SharedPredicate, SharedMapperFn, ScoringNode};
use super::nodes::{FromNode, FilterNode, JoinNode, ConditionalNode, GroupNode, FlatMapNode};
use super::stream_def::*;
use crate::{GreynetFact, Score, constraint::ConstraintWeights, Result, GreynetError, ResourceLimits};
use crate::session::Session;
use crate::scheduler::BatchScheduler;
use crate::arena::TupleArena;
use std::any::TypeId;
use std::cell::RefCell;
use rustc_hash::FxHashMap as HashMap;
use std::marker::PhantomData;
use std::rc::Rc;

/// High-performance constraint factory with optimized hash maps and error handling
pub struct ConstraintFactory<S: Score> {
    pub node_sharer: Rc<RefCell<NodeSharingManager<S>>>,
    pub key_fns: HashMap<usize, SharedKeyFn>,
    pub predicates: HashMap<usize, SharedPredicate>,
    pub mappers: HashMap<usize, SharedMapperFn>,
    pub collector_suppliers: HashMap<usize, CollectorSupplier>,
    constraint_defs: Vec<ConstraintRecipe<S>>,
    weights: Rc<RefCell<ConstraintWeights>>,
    limits: ResourceLimits,
    next_key_fn_id: usize,
    next_predicate_id: usize,
    next_mapper_id: usize,
    _phantom: PhantomData<S>,
}

impl<S: Score + 'static> ConstraintFactory<S> {
    pub fn new(weights: Rc<RefCell<ConstraintWeights>>) -> Self {
        Self::with_limits(weights, ResourceLimits::default())
    }
    
    pub fn with_limits(weights: Rc<RefCell<ConstraintWeights>>, limits: ResourceLimits) -> Self {
        Self {
            node_sharer: Rc::new(RefCell::new(NodeSharingManager::new())),
            key_fns: HashMap::default(),
            predicates: HashMap::default(),
            mappers: HashMap::default(),
            collector_suppliers: HashMap::default(),
            constraint_defs: Vec::new(),
            weights,
            limits,
            next_key_fn_id: 0,
            next_predicate_id: 0,
            next_mapper_id: 0,
            _phantom: PhantomData,
        }
    }

    pub fn from<T: GreynetFact + 'static>(factory: &Rc<RefCell<Self>>) -> Stream<Arity1, S> {
        let from_def = FromDefinition::new::<T>();
        let stream_def = StreamDefinition::From(from_def);
        Stream::new(stream_def, Rc::downgrade(factory))
    }

    pub fn add_constraint_def(&mut self, recipe: ConstraintRecipe<S>) {
        self.constraint_defs.push(recipe);
    }

    #[inline]
    pub fn register_key_fn(&mut self, key_fn: SharedKeyFn) -> usize {
        let id = self.next_key_fn_id;
        self.key_fns.insert(id, key_fn);
        self.next_key_fn_id += 1;
        id
    }

    #[inline]
    pub fn register_predicate(&mut self, predicate: SharedPredicate) -> usize {
        let id = self.next_predicate_id;
        self.predicates.insert(id, predicate);
        self.next_predicate_id += 1;
        id
    }

    #[inline]
    pub fn register_mapper(&mut self, mapper: SharedMapperFn) -> usize {
        let id = self.next_mapper_id;
        self.mappers.insert(id, mapper);
        self.next_mapper_id += 1;
        id
    }
    
    #[inline]
    pub fn register_collector_supplier(&mut self, supplier: CollectorSupplier) -> usize {
        let id = supplier.id();
        self.collector_suppliers.insert(id, supplier);
        id
    }

    pub fn build_stream(&mut self, stream_def: &StreamDefinition<S>, nodes: &mut NodeArena<S>) -> Result<NodeId> {
        let retrieval_id = stream_def.get_retrieval_id();
        if let Some(node_id) = self.node_sharer.borrow().get_node(&retrieval_id) {
            return Ok(node_id);
        }

        let new_node_data = match stream_def {
            StreamDefinition::From(def) => NodeData::From(FromNode::new(def.fact_type)),
            StreamDefinition::Filter(def) => {
                let predicate = self.predicates.get(&def.predicate_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Predicate not found"))?;
                NodeData::Filter(FilterNode::new(predicate))
            }
            StreamDefinition::Join(def) => {
                let left_key = self.key_fns.get(&def.left_key_fn_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Left key fn not found"))?;
                let right_key = self.key_fns.get(&def.right_key_fn_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Right key fn not found"))?;
                NodeData::Join(JoinNode::new(def.joiner_type, left_key, right_key))
            }
            StreamDefinition::ConditionalJoin(def) => {
                let left_key = self.key_fns.get(&def.left_key_fn_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Left key fn not found"))?;
                let right_key = self.key_fns.get(&def.right_key_fn_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Right key fn not found"))?;
                NodeData::Conditional(ConditionalNode::new(def.should_exist, left_key, right_key))
            }
            StreamDefinition::Group(def) => {
                let key_fn = self.key_fns.get(&def.key_fn_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Key fn not found"))?;
                NodeData::Group(GroupNode::new(key_fn, def.collector_supplier.clone()))
            }
            StreamDefinition::FlatMap(def) => {
                let mapper_fn = self.mappers.get(&def.mapper_fn_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Mapper fn not found"))?;
                NodeData::FlatMap(FlatMapNode::new(mapper_fn))
            }
        };

        let new_node_id = nodes.insert_node(new_node_data);
        self.node_sharer.borrow_mut().register_node(retrieval_id, new_node_id)?;

        match stream_def {
            StreamDefinition::Filter(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            StreamDefinition::Join(def) => {
                let left_parent_id = self.build_stream(&def.left_source, nodes)?;
                let right_parent_id = self.build_stream(&def.right_source, nodes)?;
                
                let left_adapter = nodes.insert_node(NodeData::JoinLeftAdapter(JoinLeftAdapter::new(new_node_id)));
                let right_adapter = nodes.insert_node(NodeData::JoinRightAdapter(JoinRightAdapter::new(new_node_id)));
                
                if let Some(left_parent) = nodes.get_node_mut(left_parent_id) {
                    left_parent.add_child(left_adapter);
                }
                if let Some(right_parent) = nodes.get_node_mut(right_parent_id) {
                    right_parent.add_child(right_adapter);
                }
            }
            StreamDefinition::ConditionalJoin(def) => {
                let source_id = self.build_stream(&def.source, nodes)?;
                let other_id = self.build_stream(&def.other, nodes)?;
                
                let left_adapter = nodes.insert_node(NodeData::JoinLeftAdapter(JoinLeftAdapter::new(new_node_id)));
                let right_adapter = nodes.insert_node(NodeData::JoinRightAdapter(JoinRightAdapter::new(new_node_id)));
                
                if let Some(source_node) = nodes.get_node_mut(source_id) {
                    source_node.add_child(left_adapter);
                }
                if let Some(other_node) = nodes.get_node_mut(other_id) {
                    other_node.add_child(right_adapter);
                }
            }
            StreamDefinition::Group(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            StreamDefinition::FlatMap(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            StreamDefinition::From(_) => {}
        }

        Ok(new_node_id)
    }

    pub fn build_session(mut self) -> Result<Session<S>> {
        let mut nodes = NodeArena::<S>::new();
        let tuples = TupleArena::with_limits(self.limits.clone());
        let scheduler = BatchScheduler::with_limits(self.limits.clone());
        let mut from_nodes = HashMap::default();
        let mut scoring_nodes = Vec::new();

        // Build constraint network
        for recipe in self.constraint_defs.clone() {
            let parent_node_id = self.build_stream(&recipe.stream_def, &mut nodes)?;
            let scoring_node = ScoringNode::new(
                recipe.constraint_id.clone(),
                recipe.penalty_function.clone(),
                self.weights.clone(),
            );
            let scoring_node_id = nodes.insert_node(NodeData::Scoring(scoring_node));
            if let Some(parent_node) = nodes.get_node_mut(parent_node_id) {
                parent_node.add_child(scoring_node_id);
            }
            scoring_nodes.push(scoring_node_id);
        }

        // Collect from nodes and scoring nodes
        for (id, node_data) in nodes.nodes.iter() {
            match node_data {
                NodeData::From(from_node) => {
                    from_nodes.insert(from_node.fact_type, id);
                }
                _ => {}
            }
        }

        Ok(Session::new(
            nodes,
            tuples,
            scheduler,
            HashMap::default(),
            from_nodes,
            scoring_nodes,
            self.weights,
            self.limits,
        ))
    }
}

impl<S: Score> std::fmt::Debug for ConstraintFactory<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConstraintFactory")
            .field("key_fns_count", &self.key_fns.len())
            .field("predicates_count", &self.predicates.len())
            .field("mappers_count", &self.mappers.len())
            .field("constraint_defs_count", &self.constraint_defs.len())
            .finish()
    }
}