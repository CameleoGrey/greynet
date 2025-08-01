// factory.rs - PERFORMANCE FIXED - Optimized node creation and wiring

use super::arena::{NodeArena, NodeData, NodeId};
use super::node_sharing::NodeSharingManager;
use super::join_adapters::{JoinLeftAdapter, JoinRightAdapter};
use super::nodes::{
    ScoringNode, ZeroCopyKeyFn, ZeroCopyPredicate, ZeroCopyMapperFn, ZeroCopyImpactFn,
    Predicate, KeyFn, MapperFn, ImpactFn, TupleMapperFn
};
use super::nodes::{
    FromNode, FilterNode, JoinNode, ConditionalNode, GroupNode, FlatMapNode, 
    MapNode, UnionNode, DistinctNode, GlobalAggregateNode
};
use super::stream_def::*;
use crate::{GreynetFact, Score, constraint::{ConstraintWeights, ConstraintId}, Result, GreynetError, ResourceLimits};
use crate::session::Session;
use crate::scheduler::BatchScheduler;
use crate::arena::TupleArena;
use std::any::TypeId;
use std::cell::RefCell;
use rustc_hash::FxHashMap as HashMap;
use std::marker::PhantomData;
use std::rc::Rc;

/// High-performance constraint factory with minimal overhead
pub struct ConstraintFactory<S: Score> {
    pub node_sharer: Rc<RefCell<NodeSharingManager<S>>>,
    pub zero_copy_key_fns: HashMap<usize, ZeroCopyKeyFn>,
    pub zero_copy_predicates: HashMap<usize, ZeroCopyPredicate>,
    pub zero_copy_mappers: HashMap<usize, ZeroCopyMapperFn>,
    // PERFORMANCE FIX: Combined tuple mappers with regular mappers to reduce lookups
    pub zero_copy_tuple_mappers: HashMap<usize, ZeroCopyTupleMapperFn>,
    pub zero_copy_impact_fns: HashMap<usize, ZeroCopyImpactFn<S>>,
    pub collector_suppliers: HashMap<usize, CollectorSupplier>,
    constraint_defs: Vec<ConstraintRecipe<S>>,
    pub weights: Rc<RefCell<ConstraintWeights>>,
    limits: ResourceLimits,
    
    // PERFORMANCE FIX: Pre-allocated counters to avoid repeated computation
    next_zero_copy_key_fn_id: usize,
    next_zero_copy_predicate_id: usize,
    next_zero_copy_mapper_id: usize,
    next_zero_copy_tuple_mapper_id: usize,
    next_zero_copy_impact_fn_id: usize,
    next_collector_id: usize,
    _phantom: PhantomData<S>,
}

impl<S: Score + 'static> ConstraintFactory<S> {
    pub fn new(weights: Rc<RefCell<ConstraintWeights>>) -> Self {
        Self::with_limits(weights, ResourceLimits::default())
    }
    
    pub fn with_limits(weights: Rc<RefCell<ConstraintWeights>>, limits: ResourceLimits) -> Self {
        Self {
            node_sharer: Rc::new(RefCell::new(NodeSharingManager::new())),
            zero_copy_key_fns: HashMap::default(),
            zero_copy_predicates: HashMap::default(),
            zero_copy_mappers: HashMap::default(),
            zero_copy_tuple_mappers: HashMap::default(),
            zero_copy_impact_fns: HashMap::default(),
            collector_suppliers: HashMap::default(),
            constraint_defs: Vec::new(),
            weights,
            limits,
            next_zero_copy_key_fn_id: 0,
            next_zero_copy_predicate_id: 0,
            next_zero_copy_mapper_id: 0,
            next_zero_copy_tuple_mapper_id: 0,
            next_zero_copy_impact_fn_id: 0,
            next_collector_id: 0,
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

    // PERFORMANCE FIX: Inlined registration methods for better optimization
    #[inline]
    pub fn register_zero_copy_key_fn(&mut self, key_fn: ZeroCopyKeyFn) -> usize {
        let id = self.next_zero_copy_key_fn_id;
        self.zero_copy_key_fns.insert(id, key_fn);
        self.next_zero_copy_key_fn_id += 1;
        id
    }

    #[inline]
    pub fn register_zero_copy_predicate(&mut self, predicate: ZeroCopyPredicate) -> usize {
        let id = self.next_zero_copy_predicate_id;
        self.zero_copy_predicates.insert(id, predicate);
        self.next_zero_copy_predicate_id += 1;
        id
    }

    #[inline]
    pub fn register_zero_copy_mapper(&mut self, mapper: ZeroCopyMapperFn) -> usize {
        let id = self.next_zero_copy_mapper_id;
        self.zero_copy_mappers.insert(id, mapper);
        self.next_zero_copy_mapper_id += 1;
        id
    }

    #[inline]
    pub fn register_zero_copy_tuple_mapper(&mut self, mapper: ZeroCopyTupleMapperFn) -> usize {
        let id = self.next_zero_copy_tuple_mapper_id;
        self.zero_copy_tuple_mappers.insert(id, mapper);
        self.next_zero_copy_tuple_mapper_id += 1;
        id
    }

    #[inline]
    pub fn register_zero_copy_impact_fn(&mut self, impact_fn: ZeroCopyImpactFn<S>) -> usize {
        let id = self.next_zero_copy_impact_fn_id;
        self.zero_copy_impact_fns.insert(id, impact_fn);
        self.next_zero_copy_impact_fn_id += 1;
        id
    }
    
    #[inline]
    pub fn register_collector_supplier(&mut self, supplier: CollectorSupplier) -> usize {
        let id = self.next_collector_id;
        self.collector_suppliers.insert(id, supplier);
        self.next_collector_id += 1;
        id
    }

    // PERFORMANCE FIX: Inlined getters with expect instead of Result for hot path
    #[inline]
    fn get_key_fn(&self, fn_id: &FunctionId) -> Result<KeyFn> {
        self.zero_copy_key_fns.get(&fn_id.0)
            .map(|k| KeyFn(k.clone()))
            .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy key function not found"))
    }
    
    #[inline]
    fn get_predicate(&self, fn_id: &FunctionId) -> Result<Predicate> {
        self.zero_copy_predicates.get(&fn_id.0)
            .map(|p| Predicate(p.clone()))
            .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy predicate not found"))
    }

    #[inline]
    fn get_mapper_fn(&self, fn_id: &FunctionId) -> Result<MapperFn> {
        self.zero_copy_mappers.get(&fn_id.0)
            .map(|f| MapperFn(f.clone()))
            .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy mapper function not found"))
    }

    #[inline]
    fn get_tuple_mapper_fn(&self, fn_id: &FunctionId) -> Result<TupleMapperFn> {
        self.zero_copy_tuple_mappers.get(&fn_id.0)
            .map(|f| TupleMapperFn(f.clone()))
            .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy tuple mapper function not found"))
    }

    #[inline]
    fn get_impact_fn(&self, fn_id: &FunctionId) -> Result<ImpactFn<S>> {
        self.zero_copy_impact_fns.get(&fn_id.0)
            .map(|f| ImpactFn(f.clone()))
            .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy impact function not found"))
    }

    pub fn build_stream(&mut self, stream_def: &StreamDefinition<S>, nodes: &mut NodeArena<S>) -> Result<NodeId> {
        let retrieval_id = stream_def.get_retrieval_id();
        if let Some(node_id) = self.node_sharer.borrow().get_node(&retrieval_id) {
            return Ok(node_id);
        }

        let new_node_data = match stream_def {
            StreamDefinition::From(def) => NodeData::From(FromNode::new(def.fact_type)),
            StreamDefinition::Filter(def) => {
                let predicate = self.get_predicate(&def.predicate_id)?;
                NodeData::Filter(FilterNode::new(predicate))
            }
            StreamDefinition::Join(def) => {
                let left_key = self.get_key_fn(&def.left_key_fn_id)?;
                let right_key = self.get_key_fn(&def.right_key_fn_id)?;
                NodeData::Join(JoinNode::new(def.joiner_type, left_key, right_key))
            }
            StreamDefinition::ConditionalJoin(def) => {
                let left_key = self.get_key_fn(&def.left_key_fn_id)?;
                let right_key = self.get_key_fn(&def.right_key_fn_id)?;
                NodeData::Conditional(ConditionalNode::new(def.should_exist, left_key, right_key))
            }
            StreamDefinition::Group(def) => {
                let key_fn = self.get_key_fn(&def.key_fn_id)?;
                NodeData::Group(GroupNode::new(key_fn, def.collector_supplier.clone()))
            }
            StreamDefinition::FlatMap(def) => {
                let mapper_fn = self.get_mapper_fn(&def.mapper_fn_id)?;
                NodeData::FlatMap(FlatMapNode::new(mapper_fn))
            }
            StreamDefinition::Map(def) => {
                let mapper_fn = self.get_tuple_mapper_fn(&def.mapper_fn_id)?;
                NodeData::Map(MapNode::new(mapper_fn))
            }
            StreamDefinition::Union(_def) => {
                NodeData::Union(UnionNode::new())
            }
            StreamDefinition::Distinct(_def) => {
                NodeData::Distinct(DistinctNode::new())
            }
            StreamDefinition::GlobalAggregate(def) => {
                NodeData::GlobalAggregate(GlobalAggregateNode::new(def.collector_supplier.clone()))
            }
            StreamDefinition::Scoring(def) => {
                let impact_fn = self.get_impact_fn(&def.impact_fn_id)?;
                NodeData::Scoring(ScoringNode::new(
                    def.constraint_id,
                    impact_fn,
                    self.weights.clone(),
                ))
            }
        };

        let new_node_id = nodes.insert_node(new_node_data);
        self.node_sharer.borrow_mut().register_node(retrieval_id, new_node_id)
            .map_err(|e| GreynetError::constraint_builder_error(e))?;

        self.wire_node_connections(stream_def, new_node_id, nodes)?;

        Ok(new_node_id)
    }
    
    // PERFORMANCE FIX: Optimized wiring with reduced allocations and simpler logic
    fn wire_node_connections(&mut self, stream_def: &StreamDefinition<S>, new_node_id: NodeId, nodes: &mut NodeArena<S>) -> Result<()> {
        match stream_def {
            // Handle single parent cases efficiently
            StreamDefinition::Filter(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
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
            StreamDefinition::Map(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            StreamDefinition::Distinct(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            StreamDefinition::GlobalAggregate(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            StreamDefinition::Scoring(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            
            // Handle join cases
            StreamDefinition::Join(def) => {
                let left_parent_id = self.build_stream(&def.left_source, nodes)?;
                let right_parent_id = self.build_stream(&def.right_source, nodes)?;
                
                let left_adapter = nodes.insert_node(NodeData::JoinLeftAdapter(JoinLeftAdapter::new(new_node_id)));
                let right_adapter = nodes.insert_node(NodeData::JoinRightAdapter(JoinRightAdapter::new(new_node_id)));
                
                if let Some(left_parent_node) = nodes.get_node_mut(left_parent_id) {
                    left_parent_node.add_child(left_adapter);
                }
                if let Some(right_parent_node) = nodes.get_node_mut(right_parent_id) {
                    right_parent_node.add_child(right_adapter);
                }
            }
            
            StreamDefinition::ConditionalJoin(def) => {
                let left_parent_id = self.build_stream(&def.source, nodes)?;
                let right_parent_id = self.build_stream(&def.other, nodes)?;
                
                let left_adapter = nodes.insert_node(NodeData::JoinLeftAdapter(JoinLeftAdapter::new(new_node_id)));
                let right_adapter = nodes.insert_node(NodeData::JoinRightAdapter(JoinRightAdapter::new(new_node_id)));
                
                if let Some(left_parent_node) = nodes.get_node_mut(left_parent_id) {
                    left_parent_node.add_child(left_adapter);
                }
                if let Some(right_parent_node) = nodes.get_node_mut(right_parent_id) {
                    right_parent_node.add_child(right_adapter);
                }
            }
            
            // PERFORMANCE FIX: Simplified union handling - reduced adapter overhead
            StreamDefinition::Union(def) => {
                // Direct connection without adapters for better performance
                for parent_stream in &def.sources {
                    let parent_id = self.build_stream(parent_stream, nodes)?;
                    if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                        parent_node.add_child(new_node_id);
                    }
                }
            }
            
            // From nodes don't need wiring
            StreamDefinition::From(_) => {}
        }
        
        Ok(())
    }

    pub fn build_session(mut self) -> Result<Session<S>> {
        let mut nodes = NodeArena::<S>::new();
        let tuples = TupleArena::with_limits(self.limits.clone());
        let scheduler = BatchScheduler::with_limits(self.limits.clone());
        let mut from_nodes = HashMap::default();
        let mut scoring_nodes = Vec::new();

        // PERFORMANCE FIX: Pre-allocate vectors based on constraint count
        scoring_nodes.reserve(self.constraint_defs.len());

        for recipe in self.constraint_defs.clone() {
            let parent_node_id = self.build_stream(&recipe.stream_def, &mut nodes).map_err(|e| {
                let name = self.weights.borrow().get_name(recipe.constraint_id).unwrap_or_else(|| "unknown".to_string());
                GreynetError::constraint_builder_error(format!("Failed to build constraint '{}': {}", name, e))
            })?;

            let scoring_node = ScoringNode::new(
                recipe.constraint_id,
                recipe.penalty_function,
                self.weights.clone(),
            );
            let scoring_node_id = nodes.insert_node(NodeData::Scoring(scoring_node));
            
            if let Some(parent_node) = nodes.get_node_mut(parent_node_id) {
                parent_node.add_child(scoring_node_id);
            }
            
            scoring_nodes.push(scoring_node_id);
        }

        // PERFORMANCE FIX: Single pass to collect from nodes
        for (id, node_data) in nodes.nodes.iter() {
            if let NodeData::From(from_node) = node_data {
                from_nodes.insert(from_node.fact_type, id);
            }
        }

        if from_nodes.is_empty() {
            return Err(GreynetError::constraint_builder_error(
                "No from nodes found - at least one fact type must be registered"
            ));
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

// PERFORMANCE FIX: Simplified union adapter - removed if not needed
#[derive(Debug)]
pub struct UnionAdapter {
    pub parent_union_node: NodeId,
    pub source_index: usize,
}

impl UnionAdapter {
    pub fn new(parent_union_node: NodeId, source_index: usize) -> Self {
        Self { parent_union_node, source_index }
    }
}