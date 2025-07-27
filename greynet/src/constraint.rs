// constraint.rs

use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

/// A type-safe, cheap-to-copy identifier for constraints, replacing String-based lookups.
/// It's a simple wrapper around a usize for performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConstraintId(pub usize);

/// Manages constraint weights and the mapping between string names and integer-based ConstraintIds.
#[derive(Debug, Clone)]
pub struct ConstraintWeights {
    /// Stores weights against the performant integer-based ConstraintId.
    weights: Rc<RefCell<HashMap<ConstraintId, f64>>>,
    /// Maps the original string names to their corresponding integer IDs.
    name_to_id: Rc<RefCell<HashMap<String, ConstraintId>>>,
    /// Provides a reverse mapping from an ID back to its string name, useful for reporting.
    id_to_name: Rc<RefCell<HashMap<ConstraintId, String>>>,
    /// A simple counter to generate new, unique IDs.
    next_id: Rc<RefCell<usize>>,
}

impl ConstraintWeights {
    pub fn new() -> Self {
        Self {
            weights: Rc::new(RefCell::new(HashMap::new())),
            name_to_id: Rc::new(RefCell::new(HashMap::new())),
            id_to_name: Rc::new(RefCell::new(HashMap::new())),
            next_id: Rc::new(RefCell::new(0)),
        }
    }

    /// Gets an existing ID for a name or creates a new one.
    /// This is the central point for converting a string name into a performant ID.
    pub fn get_or_create_id(&self, name: &str) -> ConstraintId {
        // First, check if the ID already exists to avoid mutable borrow if possible.
        if let Some(id) = self.name_to_id.borrow().get(name) {
            return *id;
        }

        // If it doesn't exist, we need to create it.
        let mut name_map = self.name_to_id.borrow_mut();
        let mut id_map = self.id_to_name.borrow_mut();
        let mut next_id_ref = self.next_id.borrow_mut();
        
        let id = ConstraintId(*next_id_ref);
        *next_id_ref += 1;

        name_map.insert(name.to_string(), id);
        id_map.insert(id, name.to_string());
        
        id
    }
    
    /// Gets the `ConstraintId` for a name that is expected to already exist.
    pub fn get_id(&self, name: &str) -> Option<ConstraintId> {
        self.name_to_id.borrow().get(name).copied()
    }
    
    /// Gets the original string name for a given `ConstraintId`.
    pub fn get_name(&self, id: ConstraintId) -> Option<String> {
        self.id_to_name.borrow().get(&id).cloned()
    }

    /// Sets a weight for a constraint using its string name.
    /// The name is resolved to an ID internally.
    pub fn set_weight(&self, constraint_name: &str, weight: f64) {
        let id = self.get_or_create_id(constraint_name);
        self.weights.borrow_mut().insert(id, weight);
    }

    /// Gets a weight for a constraint using its performant `ConstraintId`.
    /// This is called in the performance-critical scoring path.
    pub fn get_weight(&self, constraint_id: ConstraintId) -> f64 {
        self.weights.borrow().get(&constraint_id).copied().unwrap_or(1.0)
    }

    /// Returns a list of all registered constraint IDs and their corresponding names.
    /// This is useful for iterating over all constraints for reporting and analysis.
    pub fn get_all_constraints(&self) -> Vec<(ConstraintId, String)> {
        self.id_to_name
            .borrow()
            .iter()
            .map(|(id, name)| (*id, name.clone()))
            .collect()
    }
}

impl Default for ConstraintWeights {
    fn default() -> Self {
        Self::new()
    }
}
