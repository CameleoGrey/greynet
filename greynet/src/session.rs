//session.rs
use crate::arena::{NodeArena, NodeData, NodeId, SafeTupleIndex, TupleArena};
use crate::constraint::ConstraintWeights;
use crate::scheduler::BatchScheduler;
use crate::score::Score;
use crate::state::TupleState;
use crate::tuple::{AnyTuple, UniTuple};
use crate::GreynetFact;
use std::any::TypeId;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;

#[derive(Debug)]
pub struct Session<S: Score> {
    pub nodes: Rc<RefCell<NodeArena<S>>>,
    pub tuples: Rc<RefCell<TupleArena>>,
    scheduler: Rc<RefCell<BatchScheduler>>,
    fact_to_tuple_map: Rc<RefCell<HashMap<uuid::Uuid, SafeTupleIndex>>>,
    from_nodes: HashMap<TypeId, NodeId>,
    scoring_nodes: Vec<NodeId>,
    weights: Rc<RefCell<ConstraintWeights>>,
    _phantom: PhantomData<S>,
}

impl<S: Score + 'static> Session<S> {
    pub(crate) fn new(
        nodes: Rc<RefCell<NodeArena<S>>>,
        tuples: Rc<RefCell<TupleArena>>,
        scheduler: Rc<RefCell<BatchScheduler>>,
        fact_to_tuple_map: Rc<RefCell<HashMap<uuid::Uuid, SafeTupleIndex>>>,
        from_nodes: HashMap<TypeId, NodeId>,
        scoring_nodes: Vec<NodeId>,
        weights: Rc<RefCell<ConstraintWeights>>,
    ) -> Self {
        Self {
            nodes,
            tuples,
            scheduler,
            fact_to_tuple_map,
            from_nodes,
            scoring_nodes,
            weights,
            _phantom: PhantomData,
        }
    }

    pub fn insert<T: GreynetFact + 'static>(&mut self, fact: T) -> Result<(), String> {
        let fact_id = fact.fact_id();
        let fact_type_id = TypeId::of::<T>();

        // Check if already exists
        if self.fact_to_tuple_map.borrow().contains_key(&fact_id) {
            return Err(format!("Fact with ID {} already exists.", fact_id));
        }

        // Find appropriate from node
        let from_node_id = *self
            .from_nodes
            .get(&fact_type_id)
            .ok_or_else(|| format!("No 'from' node registered for type {:?}", fact_type_id))?;

        // Create tuple
        let tuple = AnyTuple::Uni(UniTuple::new(Rc::new(fact)));
        let tuple_index = self.tuples.borrow_mut().acquire_tuple(tuple);

        // Set node on tuple
        if let Some(t) = self.tuples.borrow_mut().get_tuple_mut(tuple_index) {
            t.set_node(from_node_id);
        }

        // Register fact mapping
        self.fact_to_tuple_map
            .borrow_mut()
            .insert(fact_id, tuple_index);

        // Schedule insertion
        self.scheduler
            .borrow_mut()
            .schedule_insert(tuple_index, &mut self.tuples.borrow_mut())?;

        Ok(())
    }

    pub fn retract<T: GreynetFact>(&mut self, fact: &T) -> Result<(), String> {
        let fact_id = fact.fact_id();

        // FIXED: Don't remove from map until we're sure scheduling succeeds
        let tuple_index = self
            .fact_to_tuple_map
            .borrow()
            .get(&fact_id)
            .copied()
            .ok_or_else(|| format!("Fact with ID {} not found.", fact_id))?;

        // Try to schedule retraction first
        self.scheduler
            .borrow_mut()
            .schedule_retract(tuple_index, &mut self.tuples.borrow_mut())?;

        // FIXED: Only remove from map if scheduling succeeded
        self.fact_to_tuple_map.borrow_mut().remove(&fact_id);

        Ok(())
    }

    // FIXED: Better clear method with proper cleanup
    pub fn clear(&mut self) -> Result<(), String> {
        // Collect all tuple indices before clearing the map
        let tuple_indices_to_retract: Vec<SafeTupleIndex> =
            self.fact_to_tuple_map.borrow().values().cloned().collect();

        // Clear the map first to prevent new operations
        self.fact_to_tuple_map.borrow_mut().clear();

        // Schedule all retractions
        for index in tuple_indices_to_retract {
            // This schedules the tuple to be marked as Dying and processed by the scheduler
            self.scheduler
                .borrow_mut()
                .schedule_retract(index, &mut self.tuples.borrow_mut())?;
        }

        // Process all the pending retractions
        self.flush()?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), String> {
        self.scheduler.borrow_mut().execute_all(
            &mut self.nodes.borrow_mut(),
            &mut self.tuples.borrow_mut(),
        )
    }

    pub fn get_score(&mut self) -> Result<S, String> {
        self.flush()?;

        let nodes = self.nodes.borrow();
        let total_score = self.scoring_nodes.iter().fold(S::null_score(), |acc, &node_id| {
            if let Some(NodeData::Scoring(scoring_node)) = nodes.get_node(node_id) {
                acc + scoring_node.get_total_score()
            } else {
                acc
            }
        });

        Ok(total_score)
    }

    pub fn update_constraint_weight(&self, constraint_id: &str, new_weight: f64) {
        self.weights
            .borrow_mut()
            .set_weight(constraint_id.to_string(), new_weight);

        let mut nodes = self.nodes.borrow_mut();
        let tuples = self.tuples.borrow();

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(node)) = nodes.get_node_mut(node_id) {
                if node.constraint_id == constraint_id {
                    node.recalculate_scores(&tuples);
                }
            }
        }
    }

    pub fn get_constraint_matches(&mut self) -> Result<HashMap<String, Vec<AnyTuple>>, String> {
        self.flush()?;

        let nodes = self.nodes.borrow();
        let tuples = self.tuples.borrow();
        let mut all_matches = HashMap::new();

        for &node_id in &self.scoring_nodes {
            if let Some(NodeData::Scoring(scoring_node)) = nodes.get_node(node_id) {
                let constraint_id = scoring_node.constraint_id.clone();
                let matches_for_constraint: Vec<AnyTuple> = scoring_node
                    .match_indices()
                    .filter_map(|tuple_idx| tuples.get_tuple(*tuple_idx).cloned())
                    .collect();

                if !matches_for_constraint.is_empty() {
                    all_matches.insert(constraint_id, matches_for_constraint);
                }
            }
        }

        Ok(all_matches)
    }

    pub fn insert_batch<T: GreynetFact + 'static>(
        &mut self,
        facts: impl IntoIterator<Item = T>,
    ) -> Result<(), String> {
        for fact in facts {
            self.insert(fact)?;
        }
        Ok(())
    }

    pub fn retract_batch<'a, T: GreynetFact + 'a>(
        &mut self,
        facts: impl IntoIterator<Item = &'a T>,
    ) -> Result<(), String> {
        for fact in facts {
            self.retract(fact)?;
        }
        Ok(())
    }

    // FIXED: Add method to check for inconsistencies
    pub fn validate_consistency(&self) -> Result<(), String> {
        let map_count = self.fact_to_tuple_map.borrow().len();
        let mut arena_live_count = 0;

        // Count live tuples in arena
        let tuples = self.tuples.borrow();
        for (_, tuple) in tuples.arena.iter() {
            if !matches!(tuple.state(), TupleState::Dead) {
                arena_live_count += 1;
            }
        }

        if map_count != arena_live_count {
            return Err(format!(
                "Consistency violation: {} facts in map, {} live tuples in arena",
                map_count, arena_live_count
            ));
        }

        Ok(())
    }
}
