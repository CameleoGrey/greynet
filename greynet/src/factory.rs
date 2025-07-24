//! Fixed factory.rs with proper zero-copy API integration

use super::arena::{NodeArena, NodeData, NodeId};
use super::node_sharing::NodeSharingManager;
use super::join_adapters::{JoinLeftAdapter, JoinRightAdapter};
use super::nodes::{SharedKeyFn, SharedPredicate, SharedMapperFn, ScoringNode, ZeroCopyKeyFn, ZeroCopyPredicate, Predicate, KeyFn};
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

/// High-performance constraint factory with optimized hash maps, error handling, and zero-copy API support
pub struct ConstraintFactory<S: Score> {
    pub node_sharer: Rc<RefCell<NodeSharingManager<S>>>,
    
    // Traditional function registries
    pub key_fns: HashMap<usize, SharedKeyFn>,
    pub predicates: HashMap<usize, SharedPredicate>,
    pub mappers: HashMap<usize, SharedMapperFn>,
    
    // Zero-copy function registries (FIXED: Better organization)
    pub zero_copy_key_fns: HashMap<usize, ZeroCopyKeyFn>,
    pub zero_copy_predicates: HashMap<usize, ZeroCopyPredicate>,
    
    // Collector registry
    pub collector_suppliers: HashMap<usize, CollectorSupplier>,
    
    // Constraint definitions and weights
    constraint_defs: Vec<ConstraintRecipe<S>>,
    weights: Rc<RefCell<ConstraintWeights>>,
    limits: ResourceLimits,
    
    // ID counters (FIXED: Separate counters for zero-copy functions)
    next_key_fn_id: usize,
    next_predicate_id: usize,
    next_mapper_id: usize,
    next_zero_copy_key_fn_id: usize,
    next_zero_copy_predicate_id: usize,
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
            key_fns: HashMap::default(),
            predicates: HashMap::default(),
            mappers: HashMap::default(),
            zero_copy_key_fns: HashMap::default(),
            zero_copy_predicates: HashMap::default(),
            collector_suppliers: HashMap::default(),
            constraint_defs: Vec::new(),
            weights,
            limits,
            next_key_fn_id: 0,
            next_predicate_id: 0,
            next_mapper_id: 0,
            next_zero_copy_key_fn_id: 0,
            next_zero_copy_predicate_id: 0,
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

    // --- FIXED: Function Registration with proper ID management ---

    #[inline]
    pub fn register_key_fn(&mut self, key_fn: SharedKeyFn) -> usize {
        let id = self.next_key_fn_id;
        self.key_fns.insert(id, key_fn);
        self.next_key_fn_id += 1;
        id
    }
    
    #[inline]
    pub fn register_zero_copy_key_fn(&mut self, key_fn: ZeroCopyKeyFn) -> usize {
        let id = self.next_zero_copy_key_fn_id;
        self.zero_copy_key_fns.insert(id, key_fn);
        self.next_zero_copy_key_fn_id += 1;
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
    pub fn register_zero_copy_predicate(&mut self, predicate: ZeroCopyPredicate) -> usize {
        let id = self.next_zero_copy_predicate_id;
        self.zero_copy_predicates.insert(id, predicate);
        self.next_zero_copy_predicate_id += 1;
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
        let id = self.next_collector_id;
        self.collector_suppliers.insert(id, supplier);
        self.next_collector_id += 1;
        id
    }

    // --- FIXED: Stream building with comprehensive zero-copy support ---

    pub fn build_stream(&mut self, stream_def: &StreamDefinition<S>, nodes: &mut NodeArena<S>) -> Result<NodeId> {
        let retrieval_id = stream_def.get_retrieval_id();
        if let Some(node_id) = self.node_sharer.borrow().get_node(&retrieval_id) {
            return Ok(node_id);
        }

        // FIXED: Enhanced node construction with better error handling
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
                let mapper_fn = self.mappers.get(&def.mapper_fn_id)
                    .cloned()
                    .ok_or_else(|| GreynetError::constraint_builder_error("Mapper fn not found"))?;
                NodeData::FlatMap(FlatMapNode::new(mapper_fn))
            }
        };

        let new_node_id = nodes.insert_node(new_node_data);
        self.node_sharer.borrow_mut().register_node(retrieval_id, new_node_id)
            .map_err(|e| GreynetError::constraint_builder_error(e))?;

        // FIXED: Comprehensive wiring logic with better error handling
        self.wire_node_connections(stream_def, new_node_id, nodes)?;

        Ok(new_node_id)
    }
    
    // FIXED: Separate method for wiring to improve readability and error handling
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
            
            StreamDefinition::From(_) => {
                // From nodes don't need parent connections
            }
        }
        
        Ok(())
    }
    
    // FIXED: Enhanced helper methods for function retrieval with zero-copy support
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

    // FIXED: Enhanced session building with comprehensive error handling and cleanup
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
    
    // FIXED: Additional utility methods for factory management
    
    /// Get statistics about registered functions
    pub fn get_function_stats(&self) -> FactoryStats {
        FactoryStats {
            traditional_key_fns: self.key_fns.len(),
            traditional_predicates: self.predicates.len(),
            traditional_mappers: self.mappers.len(),
            zero_copy_key_fns: self.zero_copy_key_fns.len(),
            zero_copy_predicates: self.zero_copy_predicates.len(),
            collector_suppliers: self.collector_suppliers.len(),
            constraint_definitions: self.constraint_defs.len(),
        }
    }
    
    /// Clear all registered functions and constraints
    pub fn clear(&mut self) {
        self.key_fns.clear();
        self.predicates.clear();
        self.mappers.clear();
        self.zero_copy_key_fns.clear();
        self.zero_copy_predicates.clear();
        self.collector_suppliers.clear();
        self.constraint_defs.clear();
        
        // Reset ID counters
        self.next_key_fn_id = 0;
        self.next_predicate_id = 0;
        self.next_mapper_id = 0;
        self.next_zero_copy_key_fn_id = 0;
        self.next_zero_copy_predicate_id = 0;
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
                if !self.mappers.contains_key(&def.mapper_fn_id) {
                    return Err(GreynetError::constraint_builder_error(
                        format!("Mapper function with ID {} not found", def.mapper_fn_id)
                    ));
                }
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
}

#[derive(Debug, Clone)]
pub struct FactoryStats {
    pub traditional_key_fns: usize,
    pub traditional_predicates: usize,
    pub traditional_mappers: usize,
    pub zero_copy_key_fns: usize,
    pub zero_copy_predicates: usize,
    pub collector_suppliers: usize,
    pub constraint_definitions: usize,
}

impl<S: Score> std::fmt::Debug for ConstraintFactory<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.get_function_stats();
        f.debug_struct("ConstraintFactory")
            .field("traditional_functions", &(stats.traditional_key_fns + stats.traditional_predicates + stats.traditional_mappers))
            .field("zero_copy_functions", &(stats.zero_copy_key_fns + stats.zero_copy_predicates))
            .field("collectors", &stats.collector_suppliers)
            .field("constraints", &stats.constraint_definitions)
            .finish()
    }
}

// FIXED: Enhanced constraint builder integration with zero-copy API
impl<S: Score + 'static> ConstraintFactory<S> {
    /// Create a zero-copy optimized stream builder
    pub fn zero_copy_builder(factory: &Rc<RefCell<Self>>) -> crate::streams_zero_copy::ZeroCopyStreamBuilder<S> {
        crate::streams_zero_copy::ZeroCopyStreamBuilder::new(Rc::downgrade(factory))
    }
    
    /// Register a batch of zero-copy functions for better performance
    pub fn register_zero_copy_batch(
        &mut self,
        key_fns: Vec<ZeroCopyKeyFn>,
        predicates: Vec<ZeroCopyPredicate>,
    ) -> (Vec<usize>, Vec<usize>) {
        let key_ids = key_fns.into_iter()
            .map(|f| self.register_zero_copy_key_fn(f))
            .collect();
        
        let predicate_ids = predicates.into_iter()
            .map(|f| self.register_zero_copy_predicate(f))
            .collect();
        
        (key_ids, predicate_ids)
    }
    
    /// Get memory usage estimate for the factory
    pub fn memory_usage_estimate(&self) -> usize {
        // Rough estimate based on function counts and constraint definitions
        let function_count = self.key_fns.len() + self.predicates.len() + self.mappers.len() +
                           self.zero_copy_key_fns.len() + self.zero_copy_predicates.len();
        
        let base_size = std::mem::size_of::<Self>();
        let function_overhead = function_count * 64; // Rough estimate per function
        let constraint_overhead = self.constraint_defs.len() * 128; // Rough estimate per constraint
        
        base_size + function_overhead + constraint_overhead
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SimpleScore, GreynetFact};
    use uuid::Uuid;
    
    #[derive(Debug, Clone)]
    struct TestFact {
        id: Uuid,
        value: i32,
    }
    
    impl TestFact {
        fn new(value: i32) -> Self {
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
    
    #[test]
    fn test_factory_creation() {
        let weights = Rc::new(RefCell::new(ConstraintWeights::new()));
        let factory = ConstraintFactory::<SimpleScore>::new(weights);
        
        let stats = factory.get_function_stats();
        assert_eq!(stats.traditional_key_fns, 0);
        assert_eq!(stats.zero_copy_key_fns, 0);
        assert_eq!(stats.constraint_definitions, 0);
    }
    
    #[test]
    fn test_function_registration() {
        let weights = Rc::new(RefCell::new(ConstraintWeights::new()));
        let mut factory = ConstraintFactory::<SimpleScore>::new(weights);
        
        // Register traditional functions
        let traditional_key = Rc::new(|_: &crate::AnyTuple| 42u64);
        let traditional_pred = Rc::new(|_: &crate::AnyTuple| true);
        
        let key_id = factory.register_key_fn(traditional_key);
        let pred_id = factory.register_predicate(traditional_pred);
        
        assert_eq!(key_id, 0);
        assert_eq!(pred_id, 0);
        
        // Register zero-copy functions
        let zero_copy_key = Rc::new(|_: &dyn crate::tuple::ZeroCopyFacts| 24u64);
        let zero_copy_pred = Rc::new(|_: &dyn crate::tuple::ZeroCopyFacts| false);
        
        let zc_key_id = factory.register_zero_copy_key_fn(zero_copy_key);
        let zc_pred_id = factory.register_zero_copy_predicate(zero_copy_pred);
        
        assert_eq!(zc_key_id, 0);
        assert_eq!(zc_pred_id, 0);
        
        let stats = factory.get_function_stats();
        assert_eq!(stats.traditional_key_fns, 1);
        assert_eq!(stats.traditional_predicates, 1);
        assert_eq!(stats.zero_copy_key_fns, 1);
        assert_eq!(stats.zero_copy_predicates, 1);
    }
    
    #[test]
    fn test_factory_validation() {
        let weights = Rc::new(RefCell::new(ConstraintWeights::new()));
        let factory = ConstraintFactory::<SimpleScore>::new(weights);
        
        // Empty factory should validate successfully
        assert!(factory.validate().is_ok());
    }
    
    #[test]
    fn test_factory_clear() {
        let weights = Rc::new(RefCell::new(ConstraintWeights::new()));
        let mut factory = ConstraintFactory::<SimpleScore>::new(weights);
        
        // Add some functions
        let key_fn = Rc::new(|_: &crate::AnyTuple| 0u64);
        factory.register_key_fn(key_fn);
        
        let zc_key_fn = Rc::new(|_: &dyn crate::tuple::ZeroCopyFacts| 0u64);
        factory.register_zero_copy_key_fn(zc_key_fn);
        
        // Verify they were added
        let stats_before = factory.get_function_stats();
        assert_eq!(stats_before.traditional_key_fns, 1);
        assert_eq!(stats_before.zero_copy_key_fns, 1);
        
        // Clear and verify they're gone
        factory.clear();
        let stats_after = factory.get_function_stats();
        assert_eq!(stats_after.traditional_key_fns, 0);
        assert_eq!(stats_after.zero_copy_key_fns, 0);
    }
}