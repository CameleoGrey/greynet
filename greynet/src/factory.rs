//! Fully refactored factory.rs with comprehensive zero-copy API integration

use super::arena::{NodeArena, NodeData, NodeId};
use super::node_sharing::NodeSharingManager;
use super::join_adapters::{JoinLeftAdapter, JoinRightAdapter};
use super::nodes::{
    SharedKeyFn, SharedPredicate, SharedMapperFn, SharedImpactFn,
    ScoringNode, ZeroCopyKeyFn, ZeroCopyPredicate, ZeroCopyMapperFn, ZeroCopyImpactFn,
    Predicate, KeyFn, MapperFn, ImpactFn
};
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

/// High-performance constraint factory with comprehensive zero-copy API support
pub struct ConstraintFactory<S: Score> {
    pub node_sharer: Rc<RefCell<NodeSharingManager<S>>>,
    
    // Traditional function registries (for backward compatibility)
    pub key_fns: HashMap<usize, SharedKeyFn>,
    pub predicates: HashMap<usize, SharedPredicate>,
    pub mappers: HashMap<usize, SharedMapperFn>,
    pub impact_fns: HashMap<usize, SharedImpactFn<S>>,
    
    // Zero-copy function registries (primary API)
    pub zero_copy_key_fns: HashMap<usize, ZeroCopyKeyFn>,
    pub zero_copy_predicates: HashMap<usize, ZeroCopyPredicate>,
    pub zero_copy_mappers: HashMap<usize, ZeroCopyMapperFn>,
    pub zero_copy_impact_fns: HashMap<usize, ZeroCopyImpactFn<S>>,
    
    // Collector registry
    pub collector_suppliers: HashMap<usize, CollectorSupplier>,
    
    // Constraint definitions and weights
    constraint_defs: Vec<ConstraintRecipe<S>>,
    weights: Rc<RefCell<ConstraintWeights>>,
    limits: ResourceLimits,
    
    // ID counters for traditional functions
    next_key_fn_id: usize,
    next_predicate_id: usize,
    next_mapper_id: usize,
    next_impact_fn_id: usize,
    
    // ID counters for zero-copy functions
    next_zero_copy_key_fn_id: usize,
    next_zero_copy_predicate_id: usize,
    next_zero_copy_mapper_id: usize,
    next_zero_copy_impact_fn_id: usize,
    
    // Collector ID counter
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
            
            // Traditional function registries
            key_fns: HashMap::default(),
            predicates: HashMap::default(),
            mappers: HashMap::default(),
            impact_fns: HashMap::default(),
            
            // Zero-copy function registries
            zero_copy_key_fns: HashMap::default(),
            zero_copy_predicates: HashMap::default(),
            zero_copy_mappers: HashMap::default(),
            zero_copy_impact_fns: HashMap::default(),
            
            // Collector registry
            collector_suppliers: HashMap::default(),
            
            constraint_defs: Vec::new(),
            weights,
            limits,
            
            // Traditional ID counters
            next_key_fn_id: 0,
            next_predicate_id: 0,
            next_mapper_id: 0,
            next_impact_fn_id: 0,
            
            // Zero-copy ID counters
            next_zero_copy_key_fn_id: 0,
            next_zero_copy_predicate_id: 0,
            next_zero_copy_mapper_id: 0,
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

    // =============================================================================
    // TRADITIONAL FUNCTION REGISTRATION (Backward Compatibility)
    // =============================================================================

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
    pub fn register_impact_fn(&mut self, impact_fn: SharedImpactFn<S>) -> usize {
        let id = self.next_impact_fn_id;
        self.impact_fns.insert(id, impact_fn);
        self.next_impact_fn_id += 1;
        id
    }

    // =============================================================================
    // ZERO-COPY FUNCTION REGISTRATION (Primary API)
    // =============================================================================
    
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

    // =============================================================================
    // BATCH REGISTRATION METHODS
    // =============================================================================

    /// Register a batch of zero-copy functions for better performance
    pub fn register_zero_copy_batch(
        &mut self,
        key_fns: Vec<ZeroCopyKeyFn>,
        predicates: Vec<ZeroCopyPredicate>,
        mappers: Vec<ZeroCopyMapperFn>,
        impact_fns: Vec<ZeroCopyImpactFn<S>>,
    ) -> (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>) {
        let key_ids = key_fns.into_iter()
            .map(|f| self.register_zero_copy_key_fn(f))
            .collect();
        
        let predicate_ids = predicates.into_iter()
            .map(|f| self.register_zero_copy_predicate(f))
            .collect();
            
        let mapper_ids = mappers.into_iter()
            .map(|f| self.register_zero_copy_mapper(f))
            .collect();
            
        let impact_ids = impact_fns.into_iter()
            .map(|f| self.register_zero_copy_impact_fn(f))
            .collect();
        
        (key_ids, predicate_ids, mapper_ids, impact_ids)
    }

    /// Register traditional functions in batch (backward compatibility)
    pub fn register_traditional_batch(
        &mut self,
        key_fns: Vec<SharedKeyFn>,
        predicates: Vec<SharedPredicate>,
        mappers: Vec<SharedMapperFn>,
        impact_fns: Vec<SharedImpactFn<S>>,
    ) -> (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>) {
        let key_ids = key_fns.into_iter()
            .map(|f| self.register_key_fn(f))
            .collect();
        
        let predicate_ids = predicates.into_iter()
            .map(|f| self.register_predicate(f))
            .collect();
            
        let mapper_ids = mappers.into_iter()
            .map(|f| self.register_mapper(f))
            .collect();
            
        let impact_ids = impact_fns.into_iter()
            .map(|f| self.register_impact_fn(f))
            .collect();
        
        (key_ids, predicate_ids, mapper_ids, impact_ids)
    }

    // =============================================================================
    // FUNCTION RETRIEVAL HELPERS
    // =============================================================================

    fn get_key_fn(&self, fn_id: &FunctionId) -> Result<KeyFn> {
        match fn_id {
            FunctionId::Traditional(id) => {
                self.key_fns.get(id)
                    .map(|k| KeyFn::Traditional(k.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Traditional key function not found"))
            }
            FunctionId::ZeroCopy(id) => {
                self.zero_copy_key_fns.get(id)
                    .map(|k| KeyFn::ZeroCopy(k.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy key function not found"))
            }
        }
    }
    
    fn get_predicate(&self, fn_id: &FunctionId) -> Result<Predicate> {
        match fn_id {
            FunctionId::Traditional(id) => {
                self.predicates.get(id)
                    .map(|p| Predicate::Traditional(p.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Traditional predicate not found"))
            }
            FunctionId::ZeroCopy(id) => {
                self.zero_copy_predicates.get(id)
                    .map(|p| Predicate::ZeroCopy(p.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy predicate not found"))
            }
        }
    }

    fn get_mapper_fn(&self, fn_id: &FunctionId) -> Result<MapperFn> {
        match fn_id {
            FunctionId::Traditional(id) => {
                self.mappers.get(id)
                    .map(|f| MapperFn::Traditional(f.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Traditional mapper function not found"))
            }
            FunctionId::ZeroCopy(id) => {
                self.zero_copy_mappers.get(id)
                    .map(|f| MapperFn::ZeroCopy(f.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy mapper function not found"))
            }
        }
    }

    fn get_impact_fn(&self, fn_id: &FunctionId) -> Result<ImpactFn<S>> {
        match fn_id {
            FunctionId::Traditional(id) => {
                self.impact_fns.get(id)
                    .map(|f| ImpactFn::Traditional(f.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Traditional impact function not found"))
            }
            FunctionId::ZeroCopy(id) => {
                self.zero_copy_impact_fns.get(id)
                    .map(|f| ImpactFn::ZeroCopy(f.clone()))
                    .ok_or_else(|| GreynetError::constraint_builder_error("Zero-copy impact function not found"))
            }
        }
    }

    // =============================================================================
    // STREAM BUILDING WITH COMPREHENSIVE ZERO-COPY SUPPORT
    // =============================================================================

    pub fn build_stream(&mut self, stream_def: &StreamDefinition<S>, nodes: &mut NodeArena<S>) -> Result<NodeId> {
        let retrieval_id = stream_def.get_retrieval_id();
        if let Some(node_id) = self.node_sharer.borrow().get_node(&retrieval_id) {
            return Ok(node_id);
        }

        // Enhanced node construction with zero-copy support
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
            
            StreamDefinition::Scoring(def) => {
                let impact_fn = self.get_impact_fn(&def.impact_fn_id)?;
                NodeData::Scoring(ScoringNode::new(
                    def.constraint_id.clone(),
                    impact_fn,
                    self.weights.clone(),
                ))
            }
        };

        let new_node_id = nodes.insert_node(new_node_data);
        self.node_sharer.borrow_mut().register_node(retrieval_id, new_node_id)
            .map_err(|e| GreynetError::constraint_builder_error(e))?;

        // Comprehensive wiring logic with better error handling
        self.wire_node_connections(stream_def, new_node_id, nodes)?;

        Ok(new_node_id)
    }
    
    fn wire_node_connections(
        &mut self, 
        stream_def: &StreamDefinition<S>, 
        new_node_id: NodeId, 
        nodes: &mut NodeArena<S>
    ) -> Result<()> {
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
            
            StreamDefinition::Scoring(def) => {
                let parent_id = self.build_stream(&def.source, nodes)?;
                if let Some(parent_node) = nodes.get_node_mut(parent_id) {
                    parent_node.add_child(new_node_id);
                }
            }
            
            StreamDefinition::From(_) => {
                // From nodes don't need parent connections
            }
        }
        
        Ok(())
    }

    // =============================================================================
    // SESSION BUILDING WITH ENHANCED ERROR HANDLING
    // =============================================================================

    pub fn build_session(mut self) -> Result<Session<S>> {
        let mut nodes = NodeArena::<S>::new();
        let tuples = TupleArena::with_limits(self.limits.clone());
        let scheduler = BatchScheduler::with_limits(self.limits.clone());
        let mut from_nodes = HashMap::default();
        let mut scoring_nodes = Vec::new();

        // Build constraint network with comprehensive error handling
        for recipe in self.constraint_defs.clone() {
            match self.build_stream(&recipe.stream_def, &mut nodes) {
                Ok(parent_node_id) => {
                    // Register impact function and create scoring node
                    let impact_fn = recipe.penalty_function;
                    let scoring_node = ScoringNode::new(
                        recipe.constraint_id.clone(),
                        impact_fn,
                        self.weights.clone(),
                    );
                    let scoring_node_id = nodes.insert_node(NodeData::Scoring(scoring_node));
                    
                    if let Some(parent_node) = nodes.get_node_mut(parent_node_id) {
                        parent_node.add_child(scoring_node_id);
                    }
                    
                    scoring_nodes.push(scoring_node_id);
                }
                Err(e) => {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Failed to build constraint '{}': {}", recipe.constraint_id, e)
                    ));
                }
            }
        }

        // Collect from nodes with validation
        for (id, node_data) in nodes.nodes.iter() {
            if let NodeData::From(from_node) = node_data {
                from_nodes.insert(from_node.fact_type, id);
            }
        }

        // Validate that we have at least one from node
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

    // =============================================================================
    // FACTORY MANAGEMENT AND UTILITIES
    // =============================================================================
    
    /// Get comprehensive statistics about registered functions
    pub fn get_function_stats(&self) -> FactoryStats {
        FactoryStats {
            traditional_key_fns: self.key_fns.len(),
            traditional_predicates: self.predicates.len(),
            traditional_mappers: self.mappers.len(),
            traditional_impact_fns: self.impact_fns.len(),
            zero_copy_key_fns: self.zero_copy_key_fns.len(),
            zero_copy_predicates: self.zero_copy_predicates.len(),
            zero_copy_mappers: self.zero_copy_mappers.len(),
            zero_copy_impact_fns: self.zero_copy_impact_fns.len(),
            collector_suppliers: self.collector_suppliers.len(),
            constraint_definitions: self.constraint_defs.len(),
        }
    }
    
    /// Clear all registered functions and constraints
    pub fn clear(&mut self) {
        // Clear traditional functions
        self.key_fns.clear();
        self.predicates.clear();
        self.mappers.clear();
        self.impact_fns.clear();
        
        // Clear zero-copy functions
        self.zero_copy_key_fns.clear();
        self.zero_copy_predicates.clear();
        self.zero_copy_mappers.clear();
        self.zero_copy_impact_fns.clear();
        
        // Clear other registries
        self.collector_suppliers.clear();
        self.constraint_defs.clear();
        
        // Reset traditional ID counters
        self.next_key_fn_id = 0;
        self.next_predicate_id = 0;
        self.next_mapper_id = 0;
        self.next_impact_fn_id = 0;
        
        // Reset zero-copy ID counters
        self.next_zero_copy_key_fn_id = 0;
        self.next_zero_copy_predicate_id = 0;
        self.next_zero_copy_mapper_id = 0;
        self.next_zero_copy_impact_fn_id = 0;
        
        // Reset collector counter
        self.next_collector_id = 0;
    }
    
    /// Validate that all referenced functions exist
    pub fn validate(&self) -> Result<()> {
        for recipe in &self.constraint_defs {
            self.validate_stream_def(&recipe.stream_def)?;
        }
        Ok(())
    }
    
    fn validate_stream_def(&self, stream_def: &StreamDefinition<S>) -> Result<()> {
        match stream_def {
            StreamDefinition::From(_) => Ok(()),
            
            StreamDefinition::Filter(def) => {
                self.validate_predicate_id(&def.predicate_id)?;
                self.validate_stream_def(&def.source)
            }
            
            StreamDefinition::Join(def) => {
                self.validate_key_fn_id(&def.left_key_fn_id)?;
                self.validate_key_fn_id(&def.right_key_fn_id)?;
                self.validate_stream_def(&def.left_source)?;
                self.validate_stream_def(&def.right_source)
            }
            
            StreamDefinition::ConditionalJoin(def) => {
                self.validate_key_fn_id(&def.left_key_fn_id)?;
                self.validate_key_fn_id(&def.right_key_fn_id)?;
                self.validate_stream_def(&def.source)?;
                self.validate_stream_def(&def.other)
            }
            
            StreamDefinition::Group(def) => {
                self.validate_key_fn_id(&def.key_fn_id)?;
                self.validate_stream_def(&def.source)
            }
            
            StreamDefinition::FlatMap(def) => {
                self.validate_mapper_fn_id(&def.mapper_fn_id)?;
                self.validate_stream_def(&def.source)
            }
            
            StreamDefinition::Scoring(def) => {
                self.validate_impact_fn_id(&def.impact_fn_id)?;
                self.validate_stream_def(&def.source)
            }
        }
    }
    
    fn validate_predicate_id(&self, fn_id: &FunctionId) -> Result<()> {
        match fn_id {
            FunctionId::Traditional(id) => {
                if !self.predicates.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Traditional predicate with ID {} not found", id)
                    ));
                }
            }
            FunctionId::ZeroCopy(id) => {
                if !self.zero_copy_predicates.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Zero-copy predicate with ID {} not found", id)
                    ));
                }
            }
        }
        Ok(())
    }
    
    fn validate_key_fn_id(&self, fn_id: &FunctionId) -> Result<()> {
        match fn_id {
            FunctionId::Traditional(id) => {
                if !self.key_fns.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Traditional key function with ID {} not found", id)
                    ));
                }
            }
            FunctionId::ZeroCopy(id) => {
                if !self.zero_copy_key_fns.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Zero-copy key function with ID {} not found", id)
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_mapper_fn_id(&self, fn_id: &FunctionId) -> Result<()> {
        match fn_id {
            FunctionId::Traditional(id) => {
                if !self.mappers.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Traditional mapper function with ID {} not found", id)
                    ));
                }
            }
            FunctionId::ZeroCopy(id) => {
                if !self.zero_copy_mappers.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Zero-copy mapper function with ID {} not found", id)
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_impact_fn_id(&self, fn_id: &FunctionId) -> Result<()> {
        match fn_id {
            FunctionId::Traditional(id) => {
                if !self.impact_fns.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Traditional impact function with ID {} not found", id)
                    ));
                }
            }
            FunctionId::ZeroCopy(id) => {
                if !self.zero_copy_impact_fns.contains_key(id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Zero-copy impact function with ID {} not found", id)
                    ));
                }
            }
        }
        Ok(())
    }
    
    /// Get memory usage estimate for the factory
    pub fn memory_usage_estimate(&self) -> usize {
        // Enhanced memory estimation
        let traditional_function_count = self.key_fns.len() + self.predicates.len() + 
                                       self.mappers.len() + self.impact_fns.len();
        let zero_copy_function_count = self.zero_copy_key_fns.len() + self.zero_copy_predicates.len() +
                                     self.zero_copy_mappers.len() + self.zero_copy_impact_fns.len();
        
        let base_size = std::mem::size_of::<Self>();
        let traditional_overhead = traditional_function_count * 64;
        let zero_copy_overhead = zero_copy_function_count * 48; // Slightly smaller overhead
        let constraint_overhead = self.constraint_defs.len() * 128;
        
        base_size + traditional_overhead + zero_copy_overhead + constraint_overhead
    }

    /// Check if factory prefers zero-copy operations
    pub fn is_zero_copy_optimized(&self) -> bool {
        let zero_copy_count = self.zero_copy_key_fns.len() + self.zero_copy_predicates.len() +
                             self.zero_copy_mappers.len() + self.zero_copy_impact_fns.len();
        let traditional_count = self.key_fns.len() + self.predicates.len() +
                               self.mappers.len() + self.impact_fns.len();
        
        zero_copy_count >= traditional_count
    }

    /// Get optimization recommendations
    pub fn get_optimization_report(&self) -> OptimizationReport {
        let stats = self.get_function_stats();
        let is_zero_copy_optimized = self.is_zero_copy_optimized();
        
        let mut recommendations = Vec::new();
        
        if stats.traditional_key_fns > 0 && stats.zero_copy_key_fns == 0 {
            recommendations.push("Consider migrating key functions to zero-copy API for better performance".to_string());
        }
        
        if stats.traditional_predicates > 0 && stats.zero_copy_predicates == 0 {
            recommendations.push("Consider migrating predicates to zero-copy API for reduced allocations".to_string());
        }
        
        if stats.constraint_definitions > 100 && !is_zero_copy_optimized {
            recommendations.push("High constraint count detected - zero-copy API recommended for optimal performance".to_string());
        }
        
        OptimizationReport {
            is_zero_copy_optimized,
            memory_usage_estimate: self.memory_usage_estimate(),
            recommendations,
            performance_score: self.calculate_performance_score(),
        }
    }

    fn calculate_performance_score(&self) -> f64 {
        let stats = self.get_function_stats();
        let total_functions = stats.traditional_key_fns + stats.traditional_predicates + 
                             stats.traditional_mappers + stats.traditional_impact_fns +
                             stats.zero_copy_key_fns + stats.zero_copy_predicates +
                             stats.zero_copy_mappers + stats.zero_copy_impact_fns;
        
        if total_functions == 0 {
            return 1.0;
        }
        
        let zero_copy_functions = stats.zero_copy_key_fns + stats.zero_copy_predicates +
                                 stats.zero_copy_mappers + stats.zero_copy_impact_fns;
        
        // Performance score based on zero-copy adoption
        (zero_copy_functions as f64 / total_functions as f64) * 100.0
    }
}

#[derive(Debug, Clone)]
pub struct FactoryStats {
    pub traditional_key_fns: usize,
    pub traditional_predicates: usize,
    pub traditional_mappers: usize,
    pub traditional_impact_fns: usize,
    pub zero_copy_key_fns: usize,
    pub zero_copy_predicates: usize,
    pub zero_copy_mappers: usize,
    pub zero_copy_impact_fns: usize,
    pub collector_suppliers: usize,
    pub constraint_definitions: usize,
}

#[derive(Debug, Clone)]
pub struct OptimizationReport {
    pub is_zero_copy_optimized: bool,
    pub memory_usage_estimate: usize,
    pub recommendations: Vec<String>,
    pub performance_score: f64, // 0-100 based on zero-copy adoption
}

impl<S: Score> std::fmt::Debug for ConstraintFactory<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.get_function_stats();
        f.debug_struct("ConstraintFactory")
            .field("traditional_functions", &(stats.traditional_key_fns + stats.traditional_predicates + 
                                             stats.traditional_mappers + stats.traditional_impact_fns))
            .field("zero_copy_functions", &(stats.zero_copy_key_fns + stats.zero_copy_predicates + 
                                           stats.zero_copy_mappers + stats.zero_copy_impact_fns))
            .field("collectors", &stats.collector_suppliers)
            .field("constraints", &stats.constraint_definitions)
            .field("zero_copy_optimized", &self.is_zero_copy_optimized())
            .finish()
    }
}