//session.rs
use crate::arena::{NodeArena, NodeData, NodeId, SafeTupleIndex, TupleArena};
use crate::constraint::ConstraintWeights;
use crate::scheduler::BatchScheduler;
use crate::score::Score;
use crate::state::TupleState;
use crate::tuple::{AnyTuple, UniTuple};
use crate::{GreynetFact, Result, GreynetError, ResourceLimits};
use std::any::TypeId;
use rustc_hash::FxHashMap as HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::cell::RefCell;

/// High-performance session with direct ownership and comprehensive error handling
#[derive(Debug)]
pub struct Session<S: Score> {
    pub nodes: NodeArena<S>,
    pub tuples: TupleArena,
    scheduler: BatchScheduler,
    fact_to_tuple_map: HashMap<uuid::Uuid, SafeTupleIndex>,
    from_nodes: HashMap<TypeId, NodeId>,
    scoring_nodes: Vec<NodeId>,
    weights: Rc<RefCell<ConstraintWeights>>,
    limits: ResourceLimits,
    _phantom: PhantomData<S>,
}

impl<S: Score + 'static> Session<S> {
    pub(crate) fn new(
        nodes: NodeArena<S>,
        tuples: TupleArena,
        scheduler: BatchScheduler,
        fact_to_tuple_map: HashMap<uuid::Uuid, SafeTupleIndex>,
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
            _phantom: PhantomData,
        }
    }

    // IMPROVED: Better error handling with proper cleanup
    #[inline]
    pub fn insert<T: GreynetFact + 'static>(&mut self, fact: T) -> Result<()> {
        let fact_id = fact.fact_id();
        let fact_type_id = TypeId::of::<T>();

        // Check if fact already exists
        if self.fact_to_tuple_map.contains_key(&fact_id) {
            return Err(GreynetError::duplicate_fact(fact_id));
        }

        // Check resource limits
        self.limits.check_tuple_limit(self.tuples.arena.len())?;

        // Find the from node for this type
        let from_node_id = *self
            .from_nodes
            .get(&fact_type_id)
            .ok_or_else(|| GreynetError::unregistered_type(std::any::type_name::<T>()))?;

        // Create and acquire tuple
        let tuple = AnyTuple::Uni(UniTuple::new(Rc::new(fact)));
        let tuple_index = self.tuples.acquire_tuple(tuple)?;

        // Set the node reference
        if let Ok(t) = self.tuples.get_tuple_mut_checked(tuple_index) {
            t.set_node(from_node_id);
        }

        // FIXED: Only add to map after successful scheduling
        match self.scheduler.schedule_insert(tuple_index, &mut self.tuples) {
            Ok(()) => {
                self.fact_to_tuple_map.insert(fact_id, tuple_index);
                Ok(())
            }
            Err(e) => {
                // Clean up the acquired tuple on failure
                self.tuples.release_tuple(tuple_index);
                Err(e)
            }
        }
    }

    #[inline]
    pub fn retract<T: GreynetFact>(&mut self, fact: &T) -> Result<()> {
        let fact_id = fact.fact_id();

        let tuple_index = self
            .fact_to_tuple_map
            .get(&fact_id)
            .copied()
            .ok_or_else(|| GreynetError::fact_not_found(fact_id))?;

        self.scheduler.schedule_retract(tuple_index, &mut self.tuples)?;
        self.fact_to_tuple_map.remove(&fact_id);

        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        let tuple_indices_to_retract: Vec<SafeTupleIndex> =
            self.fact_to_tuple_map.values().cloned().collect();

        self.fact_to_tuple_map.clear();

        for index in tuple_indices_to_retract {
            self.scheduler.schedule_retract(index, &mut self.tuples)?;
        }

        self.flush()?;
        Ok(())
    }

    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        self.scheduler.execute_all(&mut self.nodes, &mut self.tuples)
    }

    pub fn update_constraint_weight(&mut self, constraint_id: &str, new_weight: f64) -> Result<()> {
        self.weights
            .borrow_mut()
            .set_weight(constraint_id.to_string(), new_weight);

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(node)) = self.nodes.get_node_mut(node_id) {
                if node.constraint_id == constraint_id {
                    node.recalculate_scores(&self.tuples)?;
                }
            }
        }
        Ok(())
    }

    pub fn get_constraint_matches(&mut self) -> Result<HashMap<String, Vec<AnyTuple>>> {
        self.flush()?;

        let mut all_matches = HashMap::default();

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                let constraint_id = scoring_node.constraint_id.clone();
                let matches_for_constraint: Result<Vec<AnyTuple>> = scoring_node
                    .match_indices()
                    .map(|tuple_idx| {
                        self.tuples.get_tuple_checked(*tuple_idx)
                            .map(|t| t.clone())
                    })
                    .collect();

                match matches_for_constraint {
                    Ok(matches) if !matches.is_empty() => {
                        all_matches.insert(constraint_id, matches);
                    }
                    Ok(_) => {}, // Empty matches, skip
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(all_matches)
    }

    pub fn retract_batch<'a, T: GreynetFact + 'a>(
        &mut self,
        facts: impl IntoIterator<Item = &'a T>,
    ) -> Result<()> {
        for fact in facts {
            self.retract(fact)?;
        }
        Ok(())
    }

    pub fn validate_consistency(&self) -> Result<()> {
        let map_count = self.fact_to_tuple_map.len();
        let mut arena_live_count = 0;

        for (_, tuple) in self.tuples.arena.iter() {
            if !matches!(tuple.state(), TupleState::Dead) {
                arena_live_count += 1;
            }
        }

        if map_count != arena_live_count {
            return Err(GreynetError::consistency_violation(
                format!("Fact map contains {} facts, but arena has {} live tuples", 
                       map_count, arena_live_count)
            ));
        }

        // Additional consistency checks
        #[cfg(debug_assertions)]
        self.tuples.check_for_leaks()?;

        Ok(())
    }
    
    /// Get comprehensive session statistics
    pub fn get_statistics(&self) -> SessionStatistics {
        let arena_stats = self.tuples.stats();
        
        SessionStatistics {
            total_facts: self.fact_to_tuple_map.len(),
            total_nodes: self.nodes.len(),
            scoring_nodes: self.scoring_nodes.len(),
            arena_stats,
            memory_usage_mb: self.tuples.memory_usage_estimate(),
        }
    }
    
    /// Check if session is within resource limits
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

    pub fn insert_batch<T: GreynetFact + 'static>(
        &mut self,
        facts: impl IntoIterator<Item = T>,
    ) -> Result<()> {

        for fact in facts {
            self.insert(fact)?;
        }
        Ok(())
    }

    pub fn get_score(&mut self) -> Result<S> {
        self.flush()?;
        
        let mut total_accumulator = S::Accumulator::default();
        
        // Accumulate scores from all scoring nodes - no iteration overhead!
        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = self.nodes.get_node(node_id) {
                let node_score = scoring_node.get_total_score();
                S::accumulate_into(&mut total_accumulator, &node_score);
            }
        }
        
        Ok(S::from_accumulator(&total_accumulator))
    }

    pub fn bulk_transition_tuple_states(&mut self, from: TupleState, to: TupleState) -> Result<usize> {
        Ok(self.tuples.bulk_transition_states(from, to))
    }

    pub fn cleanup_dying_tuples(&mut self) -> Result<usize> {
        Ok(self.tuples.cleanup_dying_tuples())
    }
}

#[derive(Debug, Clone)]
pub struct SessionStatistics {
    pub total_facts: usize,
    pub total_nodes: usize,
    pub scoring_nodes: usize,
    pub arena_stats: crate::arena::ArenaStats,
    pub memory_usage_mb: usize,
}