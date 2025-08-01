// session.rs - PERFORMANCE FIXED - Removed hot path overhead

use crate::arena::{NodeArena, NodeData, NodeId, SafeTupleIndex, TupleArena};
use crate::constraint::{ConstraintId, ConstraintWeights};
use crate::scheduler::BatchScheduler;
use crate::score::Score;
use crate::state::TupleState;
use crate::tuple::{AnyTuple, FactIterator, UniTuple, ZeroCopyFacts};
use crate::{GreynetError, GreynetFact, ResourceLimits, Result, collectors::BaseCollector};
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;
use std::any::TypeId;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

/// Information about a constraint violation involving a specific fact.
#[derive(Debug, Clone)]
pub struct FactConstraintMatch<S: Score> {
    pub constraint_id: String,
    pub violating_tuple: AnyTuple,
    pub fact_role: FactRole,
    pub violation_score: S,
}

/// Describes the role of a fact within a violating tuple.
#[derive(Debug, Clone)]
pub enum FactRole {
    /// Fact is the only fact in a unary tuple.
    Primary,
    /// Fact is at a specific position in a multi-fact tuple.
    Positional { index: usize, total_facts: usize },
}

/// A comprehensive report of all constraint violations, organized by fact.
#[derive(Debug, Clone)]
pub struct FactConstraintReport<S: Score> {
    pub matches_by_fact: HashMap<i64, Vec<FactConstraintMatch<S>>>,
    pub total_involved_facts: usize,
    pub total_violations: usize,
}

/// Statistics about the current state of the session.
/// PERFORMANCE FIX: Made statistics collection opt-in and cached
#[derive(Debug, Clone)]
pub struct SessionStatistics {
    pub total_facts: usize,
    pub total_nodes: usize,
    pub scoring_nodes: usize,
    pub arena_stats: crate::arena::ArenaStats,
    pub memory_usage_mb: usize,
    // Enhanced statistics - only computed when explicitly requested
    pub node_type_breakdown: Option<HashMap<String, usize>>,
    pub active_tuples_by_arity: Option<HashMap<usize, usize>>,
    pub constraint_match_counts: Option<HashMap<String, usize>>,
}

/// Optional stream processing statistics (only when enabled)
#[cfg(feature = "detailed-stats")]
#[derive(Debug, Clone, Default)]
pub struct StreamProcessingStats {
    pub tuples_processed: usize,
    pub tuples_filtered: usize,
    pub joins_performed: usize,
    pub aggregations_computed: usize,
    pub transformations_applied: usize,
    pub deduplication_events: usize,
}

/// Optional performance metrics (only when enabled)
#[cfg(feature = "detailed-stats")]
#[derive(Debug, Clone, Default)]
pub struct PerformanceMetrics {
    pub avg_constraint_evaluation_time_ns: f64,
    pub total_memory_allocations: usize,
    pub cache_hit_ratio: f64,
    pub network_propagation_depth: usize,
}

/// High-performance session with minimal overhead in hot paths
#[derive(Debug)]
pub struct Session<S: Score> {
    pub nodes: NodeArena<S>,
    pub tuples: TupleArena,
    scheduler: BatchScheduler,
    fact_to_tuple_map: HashMap<i64, SafeTupleIndex>,
    from_nodes: HashMap<TypeId, NodeId>,
    scoring_nodes: Vec<NodeId>,
    weights: Rc<RefCell<ConstraintWeights>>,
    limits: ResourceLimits,
    
    // PERFORMANCE FIX: Optional statistics only when feature enabled
    #[cfg(feature = "detailed-stats")]
    stream_processing_stats: StreamProcessingStats,
    #[cfg(feature = "detailed-stats")]
    performance_metrics: PerformanceMetrics,
    
    // Cached statistics to avoid recomputation
    cached_statistics: RefCell<Option<SessionStatistics>>,
    
    _phantom: PhantomData<S>,
}

impl<S: Score + 'static> Session<S> {
    /// Internal constructor for creating a new session.
    pub(crate) fn new(
        nodes: NodeArena<S>,
        tuples: TupleArena,
        scheduler: BatchScheduler,
        fact_to_tuple_map: HashMap<i64, SafeTupleIndex>,
        from_nodes: HashMap<TypeId, NodeId>,
        scoring_nodes: Vec<NodeId>,
        weights: Rc<RefCell<ConstraintWeights>>,
        limits: ResourceLimits,
    ) -> Self {
        Self {
            nodes,
            tuples,
            scheduler,
            fact_to_tuple_map,
            from_nodes,
            scoring_nodes,
            weights,
            limits,
            
            #[cfg(feature = "detailed-stats")]
            stream_processing_stats: StreamProcessingStats::default(),
            #[cfg(feature = "detailed-stats")]
            performance_metrics: PerformanceMetrics::default(),
            
            cached_statistics: RefCell::new(None),
            _phantom: PhantomData,
        }
    }

    /// CRITICAL PERFORMANCE FIX: Removed statistics tracking from hot path
    #[inline]
    pub fn insert<T: GreynetFact + 'static>(&mut self, fact: T) -> Result<()> {
        let fact_id = fact.fact_id();
        let fact_type_id = TypeId::of::<T>();

        if self.fact_to_tuple_map.contains_key(&fact_id) {
            return Err(GreynetError::duplicate_fact(fact_id));
        }

        self.limits.check_tuple_limit(self.tuples.arena.len())?;

        let from_node_id = *self
            .from_nodes
            .get(&fact_type_id)
            .ok_or_else(|| GreynetError::unregistered_type(std::any::type_name::<T>()))?;

        let tuple = AnyTuple::Uni(UniTuple::new(Rc::new(fact)));
        let tuple_index = self.tuples.acquire_tuple(tuple)?;

        if let Ok(t) = self.tuples.get_tuple_mut_checked(tuple_index) {
            t.set_node(from_node_id);
        }

        match self.scheduler.schedule_insert(tuple_index, &mut self.tuples) {
            Ok(()) => {
                self.fact_to_tuple_map.insert(fact_id, tuple_index);
                
                // PERFORMANCE FIX: Only track stats when feature enabled
                #[cfg(feature = "detailed-stats")]
                {
                    self.stream_processing_stats.tuples_processed += 1;
                }
                
                // Invalidate cached statistics
                *self.cached_statistics.borrow_mut() = None;
                
                Ok(())
            }
            Err(e) => {
                self.tuples.release_tuple(tuple_index);
                Err(e)
            }
        }
    }

    /// Inserts a collection of facts into the session.
    pub fn insert_batch<T: GreynetFact + 'static>(&mut self, facts: impl IntoIterator<Item = T>) -> Result<()> {
        for fact in facts {
            self.insert(fact)?;
        }
        Ok(())
    }

    /// Bulk insert with performance optimization
    pub fn insert_bulk<T: GreynetFact + 'static>(&mut self, facts: Vec<T>) -> Result<()> {
        let batch_size = facts.len();
        self.tuples.reserve_capacity(batch_size);
        
        for fact in facts {
            self.insert(fact)?;
        }
        
        self.flush()?;
        Ok(())
    }

    /// Retracts a fact from the session, scheduling it for removal.
    #[inline]
    pub fn retract<T: GreynetFact>(&mut self, fact: &T) -> Result<()> {
        let fact_id = fact.fact_id();
        let tuple_index = self
            .fact_to_tuple_map
            .remove(&fact_id)
            .ok_or_else(|| GreynetError::fact_not_found(fact_id))?;
            
        // Invalidate cached statistics
        *self.cached_statistics.borrow_mut() = None;
        
        self.scheduler.schedule_retract(tuple_index, &mut self.tuples)
    }

    /// Retracts a collection of facts from the session.
    pub fn retract_batch<'a, T: GreynetFact + 'a>(&mut self, facts: impl IntoIterator<Item = &'a T>) -> Result<()> {
        for fact in facts {
            self.retract(fact)?;
        }
        Ok(())
    }

    /// Removes all facts from the session.
    pub fn clear(&mut self) -> Result<()> {
        let tuple_indices_to_retract: Vec<SafeTupleIndex> = self.fact_to_tuple_map.values().cloned().collect();
        self.fact_to_tuple_map.clear();
        for index in tuple_indices_to_retract {
            self.scheduler.schedule_retract(index, &mut self.tuples)?;
        }
        
        // Invalidate cached statistics
        *self.cached_statistics.borrow_mut() = None;
        
        self.flush()
    }

    /// Processes all pending insertions and retractions until the network is stable.
    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        self.scheduler.execute_all(&mut self.nodes, &mut self.tuples)
    }

    /// Flush with additional optimizations for large datasets
    pub fn flush_with_optimizations(&mut self) -> Result<()> {
        let cleaned = self.tuples.cleanup_if_memory_pressure();
        
        #[cfg(feature = "detailed-stats")]
        if cleaned > 0 {
            self.stream_processing_stats.deduplication_events += cleaned;
        }
        
        self.flush()?;
        
        #[cfg(feature = "detailed-stats")]
        self.update_performance_metrics();
        
        Ok(())
    }

    /// Calculates and returns the total score for the current state of the network.
    pub fn get_score(&mut self) -> Result<S> {
        self.flush()?;
        let mut total_accumulator = S::Accumulator::default();
        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                let node_score = scoring_node.get_total_score();
                S::accumulate_into(&mut total_accumulator, &node_score);
            }
        }
        Ok(S::from_accumulator(&total_accumulator))
    }

    /// Updates a constraint's weight and triggers a score recalculation.
    pub fn update_constraint_weight(&mut self, constraint_id_str: &str, new_weight: f64) -> Result<()> {
        self.weights.borrow().set_weight(constraint_id_str, new_weight);

        if let Some(constraint_id) = self.weights.borrow().get_id(constraint_id_str) {
            for &node_id in &self.scoring_nodes {
                if let Some(NodeData::Scoring(node)) = self.nodes.get_node_mut(node_id) {
                    if node.constraint_id == constraint_id {
                        node.recalculate_scores(&self.tuples)?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Bulk update multiple constraint weights
    pub fn update_constraint_weights(&mut self, weight_updates: HashMap<String, f64>) -> Result<()> {
        let mut affected_constraints = HashSet::default();
        
        for (constraint_name, new_weight) in weight_updates {
            self.weights.borrow().set_weight(&constraint_name, new_weight);
            if let Some(constraint_id) = self.weights.borrow().get_id(&constraint_name) {
                affected_constraints.insert(constraint_id);
            }
        }
        
        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(node)) = self.nodes.get_node_mut(node_id) {
                if affected_constraints.contains(&node.constraint_id) {
                    node.recalculate_scores(&self.tuples)?;
                }
            }
        }
        Ok(())
    }

    /// Retrieves all tuples that are currently violating any constraints.
    pub fn get_constraint_matches(&mut self) -> Result<HashMap<String, Vec<AnyTuple>>> {
        self.flush()?;
        let mut all_matches = HashMap::default();
        let weights_ref = self.weights.borrow();

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                if let Some(constraint_name) = weights_ref.get_name(scoring_node.constraint_id) {
                    let matches_for_constraint: Result<Vec<AnyTuple>> = scoring_node
                        .match_indices()
                        .map(|tuple_idx| self.tuples.get_tuple_checked(*tuple_idx).cloned())
                        .collect();

                    if let Ok(matches) = matches_for_constraint {
                        if !matches.is_empty() {
                            all_matches.insert(constraint_name, matches);
                        }
                    }
                }
            }
        }
        Ok(all_matches)
    }

    /// Get constraint matches with detailed statistics
    pub fn get_constraint_matches_with_stats(&mut self) -> Result<HashMap<String, (Vec<AnyTuple>, usize, S)>> {
        self.flush()?;
        let mut all_matches = HashMap::default();
        let weights_ref = self.weights.borrow();

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                if let Some(constraint_name) = weights_ref.get_name(scoring_node.constraint_id) {
                    let matches_for_constraint: Result<Vec<AnyTuple>> = scoring_node
                        .match_indices()
                        .map(|tuple_idx| self.tuples.get_tuple_checked(*tuple_idx).cloned())
                        .collect();

                    if let Ok(matches) = matches_for_constraint {
                        let count = matches.len();
                        let total_score = scoring_node.get_total_score();
                        all_matches.insert(constraint_name, (matches, count, total_score));
                    }
                }
            }
        }
        Ok(all_matches)
    }

    /// Retrieves all constraint violations that involve a specific fact.
    pub fn get_fact_constraint_matches(&mut self, fact_id: i64) -> Result<Vec<FactConstraintMatch<S>>> {
        self.flush()?;
        let mut matches = Vec::new();
        let weights_ref = self.weights.borrow();

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                if let Some(constraint_name) = weights_ref.get_name(scoring_node.constraint_id) {
                    for &tuple_idx in scoring_node.match_indices() {
                        if let Ok(tuple) = self.tuples.get_tuple_checked(tuple_idx) {
                            if let Some(fact_role) = self.find_fact_in_tuple(tuple, fact_id) {
                                let base_score = scoring_node.penalty_function.execute(tuple);
                                let weight = weights_ref.get_weight(scoring_node.constraint_id);
                                let weighted_score = base_score.mul(weight);

                                matches.push(FactConstraintMatch {
                                    constraint_id: constraint_name.clone(),
                                    violating_tuple: tuple.clone(),
                                    fact_role,
                                    violation_score: weighted_score,
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(matches)
    }

    /// Helper function to find a fact within a tuple and determine its role.
    fn find_fact_in_tuple(&self, tuple: &AnyTuple, target_fact_id: i64) -> Option<FactRole> {
        for (index, fact) in tuple.facts_iter().enumerate() {
            if fact.fact_id() == target_fact_id {
                return Some(if tuple.arity() == 1 {
                    FactRole::Primary
                } else {
                    FactRole::Positional {
                        index,
                        total_facts: tuple.arity(),
                    }
                });
            }
        }
        None
    }

    /// Retrieves constraint matches for a specific fact by the fact object itself.
    pub fn get_fact_constraint_matches_by_fact<T: GreynetFact>(&mut self, fact: &T) -> Result<Vec<FactConstraintMatch<S>>> {
        self.get_fact_constraint_matches(fact.fact_id())
    }

    /// Generates a comprehensive report of all constraint violations, grouped by fact.
    pub fn get_all_fact_constraint_matches(&mut self) -> Result<FactConstraintReport<S>> {
        self.flush()?;

        let mut matches_by_fact: HashMap<i64, Vec<FactConstraintMatch<S>>> = HashMap::default();
        let mut total_violations = 0;
        let weights_ref = self.weights.borrow();

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                if let Some(constraint_name) = weights_ref.get_name(scoring_node.constraint_id) {
                    for &tuple_idx in scoring_node.match_indices() {
                        if let Ok(tuple) = self.tuples.get_tuple_checked(tuple_idx) {
                            total_violations += 1;
                            
                            let base_score = scoring_node.penalty_function.execute(tuple);
                            let weight = weights_ref.get_weight(scoring_node.constraint_id);
                            let weighted_score = base_score.mul(weight);

                            for (index, fact) in tuple.facts_iter().enumerate() {
                                let fact_id = fact.fact_id();
                                let fact_role = if tuple.arity() == 1 {
                                    FactRole::Primary
                                } else {
                                    FactRole::Positional { index, total_facts: tuple.arity() }
                                };
                                let match_info = FactConstraintMatch {
                                    constraint_id: constraint_name.clone(),
                                    violating_tuple: tuple.clone(),
                                    fact_role,
                                    violation_score: weighted_score.clone(),
                                };
                                matches_by_fact.entry(fact_id).or_default().push(match_info);
                            }
                        }
                    }
                }
            }
        }

        let total_involved_facts = matches_by_fact.len();
        Ok(FactConstraintReport {
            matches_by_fact,
            total_involved_facts,
            total_violations,
        })
    }

    /// Gets comprehensive statistics about the session's state.
    /// PERFORMANCE FIX: Cached and lazy computation
    pub fn get_statistics(&self) -> SessionStatistics {
        // Check cache first
        if let Some(ref cached) = *self.cached_statistics.borrow() {
            return cached.clone();
        }
        
        let arena_stats = self.tuples.stats();
        
        let stats = SessionStatistics {
            total_facts: self.fact_to_tuple_map.len(),
            total_nodes: self.nodes.len(),
            scoring_nodes: self.scoring_nodes.len(),
            arena_stats,
            memory_usage_mb: self.tuples.memory_usage_estimate(),
            // Basic stats only - enhanced stats computed on demand
            node_type_breakdown: None,
            active_tuples_by_arity: None,
            constraint_match_counts: None,
        };
        
        // Cache the result
        *self.cached_statistics.borrow_mut() = Some(stats.clone());
        stats
    }

    /// PERFORMANCE FIX: Expensive statistics computed only on explicit request
    pub fn get_detailed_statistics(&self) -> SessionStatistics {
        let mut stats = self.get_statistics();
        
        // Enhanced node type breakdown
        let mut node_type_breakdown = HashMap::default();
        for (_, node_data) in self.nodes.nodes.iter() {
            let node_type = match node_data {
                NodeData::From(_) => "From",
                NodeData::Filter(_) => "Filter", 
                NodeData::Join(_) => "Join",
                NodeData::Conditional(_) => "Conditional",
                NodeData::Group(_) => "Group",
                NodeData::FlatMap(_) => "FlatMap",
                NodeData::Map(_) => "Map",
                NodeData::Union(_) => "Union",
                NodeData::Distinct(_) => "Distinct",
                NodeData::GlobalAggregate(_) => "GlobalAggregate",
                NodeData::Scoring(_) => "Scoring",
                NodeData::JoinLeftAdapter(_) => "JoinLeftAdapter",
                NodeData::JoinRightAdapter(_) => "JoinRightAdapter",
                NodeData::UnionAdapter(_) => "UnionAdapter",
            };
            *node_type_breakdown.entry(node_type.to_string()).or_insert(0) += 1;
        }
        
        // Arity breakdown
        let mut active_tuples_by_arity = HashMap::default();
        for (_, tuple) in self.tuples.arena.iter() {
            if tuple.state() != TupleState::Dead {
                *active_tuples_by_arity.entry(tuple.arity()).or_insert(0) += 1;
            }
        }
        
        // Constraint match counts
        let mut constraint_match_counts = HashMap::default();
        let weights_ref = self.weights.borrow();
        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                if let Some(constraint_name) = weights_ref.get_name(scoring_node.constraint_id) {
                    constraint_match_counts.insert(constraint_name, scoring_node.match_count());
                }
            }
        }
        
        stats.node_type_breakdown = Some(node_type_breakdown);
        stats.active_tuples_by_arity = Some(active_tuples_by_arity);
        stats.constraint_match_counts = Some(constraint_match_counts);
        
        stats
    }

    // Optional detailed stats methods (only available with feature)
    #[cfg(feature = "detailed-stats")]
    pub fn get_stream_processing_stats(&self) -> &StreamProcessingStats {
        &self.stream_processing_stats
    }

    #[cfg(feature = "detailed-stats")]
    pub fn get_performance_metrics(&self) -> &PerformanceMetrics {
        &self.performance_metrics
    }

    #[cfg(feature = "detailed-stats")]
    pub fn reset_statistics(&mut self) {
        self.stream_processing_stats = StreamProcessingStats::default();
        self.performance_metrics = PerformanceMetrics::default();
    }

    #[cfg(feature = "detailed-stats")]
    fn update_performance_metrics(&mut self) {
        let mut max_depth = 0;
        for (_, node_data) in self.nodes.nodes.iter() {
            if let NodeData::Scoring(_) = node_data {
                max_depth = max_depth.max(self.nodes.len() / self.from_nodes.len());
            }
        }
        self.performance_metrics.network_propagation_depth = max_depth;
        self.performance_metrics.total_memory_allocations = self.tuples.arena.capacity();
        
        let total_retrieval_attempts = self.nodes.len();
        let unique_nodes = self.nodes.len();
        self.performance_metrics.cache_hit_ratio = if total_retrieval_attempts > 0 {
            1.0 - (unique_nodes as f64 / total_retrieval_attempts as f64)
        } else {
            0.0
        };
    }

    // Remaining methods stay the same...
    pub fn validate_consistency(&self) -> Result<()> {
        let map_count = self.fact_to_tuple_map.len();
        let mut arena_live_count = 0;

        for (_, tuple) in self.tuples.arena.iter() {
            if !matches!(tuple.state(), TupleState::Dead) {
                arena_live_count += 1;
            }
        }

        if map_count != arena_live_count {
            return Err(GreynetError::consistency_violation(format!(
                "Fact map contains {} facts, but arena has {} live tuples",
                map_count, arena_live_count
            )));
        }

        #[cfg(debug_assertions)]
        self.tuples.check_for_leaks()?;

        Ok(())
    }

    pub fn check_resource_limits(&self) -> Result<()> {
        self.limits.check_tuple_limit(self.tuples.arena.len())?;
        let memory_usage = self.tuples.memory_usage_estimate();
        if memory_usage > self.limits.max_memory_mb {
            return Err(GreynetError::resource_limit(
                "memory",
                format!("Current: {}MB, Limit: {}MB", memory_usage, self.limits.max_memory_mb)
            ));
        }
        Ok(())
    }

    pub fn bulk_transition_tuple_states(&mut self, from: TupleState, to: TupleState) -> Result<usize> {
        *self.cached_statistics.borrow_mut() = None;
        Ok(self.tuples.bulk_transition_states(from, to))
    }

    pub fn cleanup_dying_tuples(&mut self) -> Result<usize> {
        let cleaned = self.tuples.cleanup_dying_tuples();
        
        #[cfg(feature = "detailed-stats")]
        {
            self.stream_processing_stats.deduplication_events += cleaned;
        }
        
        *self.cached_statistics.borrow_mut() = None;
        Ok(cleaned)
    }
}