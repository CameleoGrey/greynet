// tuple.rs
use crate::utils::TupleUtils;
use crate::{GreynetFact, TupleState};
use crate::arena::NodeId;
use std::any::Any;
use std::rc::Rc;
use uuid::Uuid;


#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum TupleArity {
    One, Two, Three, Four, Five,
}

impl TupleArity {
    pub fn as_usize(&self) -> usize {
        match self {
            TupleArity::One => 1, TupleArity::Two => 2, TupleArity::Three => 3,
            TupleArity::Four => 4, TupleArity::Five => 5,
        }
    }

    pub fn from_usize(value: usize) -> Option<Self> {
        match value {
            1 => Some(TupleArity::One), 2 => Some(TupleArity::Two),
            3 => Some(TupleArity::Three), 4 => Some(TupleArity::Four),
            5 => Some(TupleArity::Five), _ => None,
        }
    }
}

/// A type-erased container for tuples of different arities.
#[derive(Clone, Debug)]
pub enum AnyTuple {
    Uni(UniTuple), Bi(BiTuple), Tri(TriTuple), Quad(QuadTuple), Penta(PentaTuple),
}

impl AnyTuple {
    pub fn combine(&self, other: &AnyTuple) -> Result<AnyTuple, String> {
        let mut combined_facts = self.facts();
        combined_facts.extend(other.facts());
        TupleUtils::create_tuple_from_facts(combined_facts)
    }

    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> {
        match self {
            AnyTuple::Uni(t) => t.facts(), AnyTuple::Bi(t) => t.facts(),
            AnyTuple::Tri(t) => t.facts(), AnyTuple::Quad(t) => t.facts(),
            AnyTuple::Penta(t) => t.facts(),
        }
    }

    pub fn arity(&self) -> usize {
        match self {
            AnyTuple::Uni(_) => 1, AnyTuple::Bi(_) => 2, AnyTuple::Tri(_) => 3,
            AnyTuple::Quad(_) => 4, AnyTuple::Penta(_) => 5,
        }
    }

    pub fn node(&self) -> Option<NodeId> {
        match self {
            AnyTuple::Uni(t) => t.node, AnyTuple::Bi(t) => t.node,
            AnyTuple::Tri(t) => t.node, AnyTuple::Quad(t) => t.node,
            AnyTuple::Penta(t) => t.node,
        }
    }

    pub fn set_node(&mut self, node_id: NodeId) {
        match self {
            AnyTuple::Uni(t) => t.node = Some(node_id), AnyTuple::Bi(t) => t.node = Some(node_id),
            AnyTuple::Tri(t) => t.node = Some(node_id), AnyTuple::Quad(t) => t.node = Some(node_id),
            AnyTuple::Penta(t) => t.node = Some(node_id),
        }
    }

    pub fn state(&self) -> TupleState {
        match self {
            AnyTuple::Uni(t) => t.state, AnyTuple::Bi(t) => t.state,
            AnyTuple::Tri(t) => t.state, AnyTuple::Quad(t) => t.state,
            AnyTuple::Penta(t) => t.state,
        }
    }

    pub fn set_state(&mut self, state: TupleState) {
        match self {
            AnyTuple::Uni(t) => t.state = state, AnyTuple::Bi(t) => t.state = state,
            AnyTuple::Tri(t) => t.state = state, AnyTuple::Quad(t) => t.state = state,
            AnyTuple::Penta(t) => t.state = state,
        }
    }

    pub fn reset(&mut self) {
        match self {
            AnyTuple::Uni(t) => t.reset(), AnyTuple::Bi(t) => t.reset(),
            AnyTuple::Tri(t) => t.reset(), AnyTuple::Quad(t) => t.reset(),
            AnyTuple::Penta(t) => t.reset(),
        }
    }

    pub fn tuple_arity(&self) -> TupleArity {
        match self {
            AnyTuple::Uni(_) => TupleArity::One, AnyTuple::Bi(_) => TupleArity::Two,
            AnyTuple::Tri(_) => TupleArity::Three, AnyTuple::Quad(_) => TupleArity::Four,
            AnyTuple::Penta(_) => TupleArity::Five,
        }
    }
}

#[derive(Clone, Debug)]
pub struct UniTuple {
    pub fact_a: Rc<dyn GreynetFact>,
    pub node: Option<NodeId>,
    pub state: TupleState,
}

impl UniTuple {
    pub fn new(fact_a: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, node: None, state: TupleState::default() }
    }
    pub fn arity(&self) -> usize { 1 }
    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> { vec![self.fact_a.clone()] }
    pub fn reset(&mut self) { self.node = None; self.state = TupleState::Dead; }
}

#[derive(Clone, Debug)]
pub struct BiTuple {
    pub fact_a: Rc<dyn GreynetFact>,
    pub fact_b: Rc<dyn GreynetFact>,
    pub node: Option<NodeId>,
    pub state: TupleState,
}

impl BiTuple {
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, node: None, state: TupleState::default() }
    }
    pub fn arity(&self) -> usize { 2 }
    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> { vec![self.fact_a.clone(), self.fact_b.clone()] }
    pub fn reset(&mut self) { self.node = None; self.state = TupleState::Dead; }
}

#[derive(Clone, Debug)]
pub struct TriTuple {
    pub fact_a: Rc<dyn GreynetFact>,
    pub fact_b: Rc<dyn GreynetFact>,
    pub fact_c: Rc<dyn GreynetFact>,
    pub node: Option<NodeId>,
    pub state: TupleState,
}

impl TriTuple {
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>, fact_c: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, fact_c, node: None, state: TupleState::default() }
    }
    pub fn arity(&self) -> usize { 3 }
    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> { vec![self.fact_a.clone(), self.fact_b.clone(), self.fact_c.clone()] }
    pub fn reset(&mut self) { self.node = None; self.state = TupleState::Dead; }
}

#[derive(Clone, Debug)]
pub struct QuadTuple {
    pub fact_a: Rc<dyn GreynetFact>,
    pub fact_b: Rc<dyn GreynetFact>,
    pub fact_c: Rc<dyn GreynetFact>,
    pub fact_d: Rc<dyn GreynetFact>,
    pub node: Option<NodeId>,
    pub state: TupleState,
}

impl QuadTuple {
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>, fact_c: Rc<dyn GreynetFact>, fact_d: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, fact_c, fact_d, node: None, state: TupleState::default() }
    }
    pub fn arity(&self) -> usize { 4 }
    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> { vec![self.fact_a.clone(), self.fact_b.clone(), self.fact_c.clone(), self.fact_d.clone()] }
    pub fn reset(&mut self) { self.node = None; self.state = TupleState::Dead; }
}

#[derive(Clone, Debug)]
pub struct PentaTuple {
    pub fact_a: Rc<dyn GreynetFact>,
    pub fact_b: Rc<dyn GreynetFact>,
    pub fact_c: Rc<dyn GreynetFact>,
    pub fact_d: Rc<dyn GreynetFact>,
    pub fact_e: Rc<dyn GreynetFact>,
    pub node: Option<NodeId>,
    pub state: TupleState,
}

impl PentaTuple {
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>, fact_c: Rc<dyn GreynetFact>, fact_d: Rc<dyn GreynetFact>, fact_e: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, fact_c, fact_d, fact_e, node: None, state: TupleState::default() }
    }
    pub fn arity(&self) -> usize { 5 }
    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> { vec![self.fact_a.clone(), self.fact_b.clone(), self.fact_c.clone(), self.fact_d.clone(), self.fact_e.clone()] }
    pub fn reset(&mut self) { self.node = None; self.state = TupleState::Dead; }
}


#[cfg(test)]
mod tuple_tests {
    use super::*;
    use crate::fact::GreynetFact;
    use crate::state::TupleState;
    use slotmap::SlotMap;
    use std::rc::Rc;
    use uuid::Uuid;

    // --- Test Setup: Mock Facts ---

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct TestFact {
        id: Uuid,
    }

    impl TestFact {
        fn new() -> Self {
            Self { id: Uuid::new_v4() }
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

    // Helper to create a vector of Rc<dyn GreynetFact>
    fn create_facts(count: usize) -> Vec<Rc<dyn GreynetFact>> {
        (0..count)
            .map(|_| Rc::new(TestFact::new()) as Rc<dyn GreynetFact>)
            .collect()
    }

    // --- TupleArity Tests ---

    #[test]
    fn test_tuple_arity_as_usize() {
        assert_eq!(TupleArity::One.as_usize(), 1);
        assert_eq!(TupleArity::Two.as_usize(), 2);
        assert_eq!(TupleArity::Three.as_usize(), 3);
        assert_eq!(TupleArity::Four.as_usize(), 4);
        assert_eq!(TupleArity::Five.as_usize(), 5);
    }

    #[test]
    fn test_tuple_arity_from_usize() {
        assert_eq!(TupleArity::from_usize(1), Some(TupleArity::One));
        assert_eq!(TupleArity::from_usize(2), Some(TupleArity::Two));
        assert_eq!(TupleArity::from_usize(3), Some(TupleArity::Three));
        assert_eq!(TupleArity::from_usize(4), Some(TupleArity::Four));
        assert_eq!(TupleArity::from_usize(5), Some(TupleArity::Five));
        assert_eq!(TupleArity::from_usize(0), None);
        assert_eq!(TupleArity::from_usize(6), None);
    }

    // --- Specific Arity Tuple Tests ---

    #[test]
    fn test_unituple_operations() {
        let facts = create_facts(1);
        let mut tuple = UniTuple::new(facts[0].clone());

        // Test creation
        assert_eq!(tuple.arity(), 1);
        assert_eq!(tuple.state, TupleState::Dead);
        assert!(tuple.node.is_none());
        assert!(Rc::ptr_eq(&tuple.fact_a, &facts[0]));

        // Test facts()
        let retrieved_facts = tuple.facts();
        assert_eq!(retrieved_facts.len(), 1);
        assert!(Rc::ptr_eq(&retrieved_facts[0], &facts[0]));

        // Test reset()
        let mut sm = SlotMap::new();
        let node_id = sm.insert(());
        tuple.node = Some(node_id);
        tuple.state = TupleState::Creating;
        tuple.reset();
        assert_eq!(tuple.state, TupleState::Dead);
        assert!(tuple.node.is_none());
        // Fact should be unchanged
        assert!(Rc::ptr_eq(&tuple.fact_a, &facts[0]));
    }

    #[test]
    fn test_bituple_operations() {
        let facts = create_facts(2);
        let mut tuple = BiTuple::new(facts[0].clone(), facts[1].clone());

        // Test creation
        assert_eq!(tuple.arity(), 2);
        assert_eq!(tuple.state, TupleState::Dead);
        assert!(tuple.node.is_none());
        assert!(Rc::ptr_eq(&tuple.fact_a, &facts[0]));
        assert!(Rc::ptr_eq(&tuple.fact_b, &facts[1]));

        // Test facts()
        let retrieved_facts = tuple.facts();
        assert_eq!(retrieved_facts.len(), 2);
        assert!(Rc::ptr_eq(&retrieved_facts[0], &facts[0]));
        assert!(Rc::ptr_eq(&retrieved_facts[1], &facts[1]));

        // Test reset()
        let mut sm = SlotMap::new();
        let node_id = sm.insert(());
        tuple.node = Some(node_id);
        tuple.state = TupleState::Ok;
        tuple.reset();
        assert_eq!(tuple.state, TupleState::Dead);
        assert!(tuple.node.is_none());
    }

    #[test]
    fn test_trituple_operations() {
        let facts = create_facts(3);
        let mut tuple = TriTuple::new(facts[0].clone(), facts[1].clone(), facts[2].clone());

        assert_eq!(tuple.arity(), 3);
        let retrieved_facts = tuple.facts();
        assert_eq!(retrieved_facts.len(), 3);
        assert!(Rc::ptr_eq(&retrieved_facts[2], &facts[2]));
        tuple.reset();
        assert_eq!(tuple.state, TupleState::Dead);
    }

    #[test]
    fn test_quadtuple_operations() {
        let facts = create_facts(4);
        let mut tuple = QuadTuple::new(
            facts[0].clone(),
            facts[1].clone(),
            facts[2].clone(),
            facts[3].clone(),
        );

        assert_eq!(tuple.arity(), 4);
        let retrieved_facts = tuple.facts();
        assert_eq!(retrieved_facts.len(), 4);
        assert!(Rc::ptr_eq(&retrieved_facts[3], &facts[3]));
        tuple.reset();
        assert_eq!(tuple.state, TupleState::Dead);
    }

    #[test]
    fn test_pentatuple_operations() {
        let facts = create_facts(5);
        let mut tuple = PentaTuple::new(
            facts[0].clone(),
            facts[1].clone(),
            facts[2].clone(),
            facts[3].clone(),
            facts[4].clone(),
        );

        assert_eq!(tuple.arity(), 5);
        let retrieved_facts = tuple.facts();
        assert_eq!(retrieved_facts.len(), 5);
        assert!(Rc::ptr_eq(&retrieved_facts[4], &facts[4]));
        tuple.reset();
        assert_eq!(tuple.state, TupleState::Dead);
    }

    // --- AnyTuple Tests ---

    #[test]
    fn test_any_tuple_delegation() {
        let facts = create_facts(5);
        let mut sm = SlotMap::new();
        let node_id = sm.insert(());

        // Test UniTuple through AnyTuple
        let mut any_uni = AnyTuple::Uni(UniTuple::new(facts[0].clone()));
        assert_eq!(any_uni.arity(), 1);
        assert_eq!(any_uni.tuple_arity(), TupleArity::One);
        assert_eq!(any_uni.facts().len(), 1);
        assert_eq!(any_uni.state(), TupleState::Dead);
        assert!(any_uni.node().is_none());

        any_uni.set_state(TupleState::Creating);
        any_uni.set_node(node_id);
        assert_eq!(any_uni.state(), TupleState::Creating);
        assert_eq!(any_uni.node(), Some(node_id));

        any_uni.reset();
        assert_eq!(any_uni.state(), TupleState::Dead);
        assert!(any_uni.node().is_none());

        // Test PentaTuple through AnyTuple
        let mut any_penta = AnyTuple::Penta(PentaTuple::new(
            facts[0].clone(),
            facts[1].clone(),
            facts[2].clone(),
            facts[3].clone(),
            facts[4].clone(),
        ));
        assert_eq!(any_penta.arity(), 5);
        assert_eq!(any_penta.tuple_arity(), TupleArity::Five);
        assert_eq!(any_penta.facts().len(), 5);

        any_penta.set_state(TupleState::Dying);
        any_penta.set_node(node_id);
        assert_eq!(any_penta.state(), TupleState::Dying);
        assert_eq!(any_penta.node(), Some(node_id));

        any_penta.reset();
        assert_eq!(any_penta.state(), TupleState::Dead);
        assert!(any_penta.node().is_none());
    }

    #[test]
    fn test_any_tuple_cloning() {
        let facts = create_facts(2);
        let original_tuple = AnyTuple::Bi(BiTuple::new(facts[0].clone(), facts[1].clone()));

        let cloned_tuple = original_tuple.clone();

        // Check that the facts within the cloned tuple are the same Rc instances
        let original_facts = original_tuple.facts();
        let cloned_facts = cloned_tuple.facts();

        assert_eq!(original_facts.len(), 2);
        assert_eq!(cloned_facts.len(), 2);

        // Use ptr_eq to confirm they point to the same allocation
        assert!(Rc::ptr_eq(&original_facts[0], &cloned_facts[0]));
        assert!(Rc::ptr_eq(&original_facts[1], &cloned_facts[1]));

        // Also check that they are the same as the original facts
        assert!(Rc::ptr_eq(&original_facts[0], &facts[0]));
        assert!(Rc::ptr_eq(&original_facts[1], &facts[1]));
    }

    #[test]
    fn test_any_tuple_pattern_matching() {
        let facts = create_facts(3);
        let any_tuple = AnyTuple::Tri(TriTuple::new(
            facts[0].clone(),
            facts[1].clone(),
            facts[2].clone(),
        ));

        match any_tuple {
            AnyTuple::Tri(tri_tuple) => {
                assert_eq!(tri_tuple.arity(), 3);
                assert!(Rc::ptr_eq(&tri_tuple.fact_c, &facts[2]));
            }
            _ => panic!("Pattern matching failed for AnyTuple::Tri"),
        }
    }
}
