//constraint.rs
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub struct ConstraintWeights {
    weights: Rc<RefCell<HashMap<String, f64>>>,
}

impl ConstraintWeights {
    pub fn new() -> Self {
        Self {
            weights: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn set_weight(&self, constraint_id: String, weight: f64) {
        self.weights.borrow_mut().insert(constraint_id, weight);
    }

    pub fn get_weight(&self, constraint_id: &str) -> f64 {
        self.weights.borrow().get(constraint_id).copied().unwrap_or(1.0)
    }
}

impl Default for ConstraintWeights {
    fn default() -> Self {
        Self::new()
    }
}