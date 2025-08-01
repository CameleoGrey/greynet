// session.rs
use crate::arena::{NodeArena, NodeData, NodeId, SafeTupleIndex, TupleArena};
use crate::constraint::{ConstraintId, ConstraintWeights};
use crate::scheduler::BatchScheduler;
use crate::score::Score;
use crate::state::TupleState;
use crate::tuple::{AnyTuple, FactIterator, UniTuple};
use crate::{GreynetError, GreynetFact, ResourceLimits, Result};
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
#[derive(Debug, Clone)]
pub struct SessionStatistics {
    pub total_facts: usize,
    pub total_nodes: usize,
    pub scoring_nodes: usize,
    pub arena_stats: crate::arena::ArenaStats,
    pub memory_usage_mb: usize,
}

/// High-performance session with direct ownership and comprehensive error handling.
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
            _phantom: PhantomData,
        }
    }

    /// Inserts a fact into the session, scheduling it for processing.
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

    /// Retracts a fact from the session, scheduling it for removal.
    #[inline]
    pub fn retract<T: GreynetFact>(&mut self, fact: &T) -> Result<()> {
        let fact_id = fact.fact_id();
        let tuple_index = self
            .fact_to_tuple_map
            .remove(&fact_id)
            .ok_or_else(|| GreynetError::fact_not_found(fact_id))?;
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
        self.flush()
    }

    /// Processes all pending insertions and retractions until the network is stable.
    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        self.scheduler.execute_all(&mut self.nodes, &mut self.tuples)
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

    /// Retrieves all constraint violations that involve a specific fact, including score impact.
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
                                // Calculate the score contribution of this specific tuple
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
                            
                            // Calculate score for this tuple
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
    
    /// Retrieves constraint matches only for facts directly inserted by the user.
    pub fn get_inserted_fact_constraint_matches(&mut self) -> Result<HashMap<i64, Vec<FactConstraintMatch<S>>>> {
        self.flush()?;
        let mut matches_by_fact: HashMap<i64, Vec<FactConstraintMatch<S>>> = HashMap::default();
        let fact_ids: Vec<i64> = self.fact_to_tuple_map.keys().copied().collect();
        for fact_id in fact_ids {
            let fact_matches = self.get_fact_constraint_matches(fact_id)?;
            if !fact_matches.is_empty() {
                matches_by_fact.insert(fact_id, fact_matches);
            }
        }
        Ok(matches_by_fact)
    }

    /// Checks if a specific fact is currently involved in any constraint violations.
    pub fn is_fact_violating_constraints(&mut self, fact_id: i64) -> Result<bool> {
        let matches = self.get_fact_constraint_matches(fact_id)?;
        Ok(!matches.is_empty())
    }

    /// Gets the count of constraint violations involving a specific fact.
    pub fn get_fact_violation_count(&mut self, fact_id: i64) -> Result<usize> {
        let matches = self.get_fact_constraint_matches(fact_id)?;
        Ok(matches.len())
    }

    /// Gets the facts that are involved in the most constraint violations.
    pub fn get_most_problematic_facts(&mut self, limit: usize) -> Result<Vec<(i64, usize)>> {
        let report = self.get_all_fact_constraint_matches()?;
        let mut fact_violation_counts: Vec<(i64, usize)> = report
            .matches_by_fact
            .into_iter()
            .map(|(fact_id, matches)| (fact_id, matches.len()))
            .collect();

        fact_violation_counts.sort_by(|a, b| b.1.cmp(&a.1));
        fact_violation_counts.truncate(limit);
        Ok(fact_violation_counts)
    }

    /// Checks for internal consistency between the fact map and tuple arena.
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

    /// Gets comprehensive statistics about the session's state.
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

    /// Checks if the session is within its resource limits.
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

    /// Efficiently transitions all tuples from a given state to another.
    pub fn bulk_transition_tuple_states(&mut self, from: TupleState, to: TupleState) -> Result<usize> {
        Ok(self.tuples.bulk_transition_states(from, to))
    }

    /// Finds all tuples marked as 'Dying' and releases them from the arena.
    pub fn cleanup_dying_tuples(&mut self) -> Result<usize> {
        Ok(self.tuples.cleanup_dying_tuples())
    }
}
