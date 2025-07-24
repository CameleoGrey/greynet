// tuple.rs
use crate::utils::TupleUtils;
use crate::{GreynetFact, TupleState};
use crate::arena::NodeId;
use std::any::Any;
use std::rc::Rc;
use uuid::Uuid;
use smallvec::{SmallVec, smallvec};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum TupleArity {
    One, Two, Three, Four, Five,
}

impl TupleArity {
    #[inline]
    pub fn as_usize(&self) -> usize {
        match self {
            TupleArity::One => 1, TupleArity::Two => 2, TupleArity::Three => 3,
            TupleArity::Four => 4, TupleArity::Five => 5,
        }
    }

    #[inline]
    pub fn from_usize(value: usize) -> Option<Self> {
        match value {
            1 => Some(TupleArity::One), 2 => Some(TupleArity::Two),
            3 => Some(TupleArity::Three), 4 => Some(TupleArity::Four),
            5 => Some(TupleArity::Five), _ => None,
        }
    }
}

/// Zero-allocation iterator trait for facts
pub trait FactIterator {
    type Iter<'a>: Iterator<Item = &'a dyn GreynetFact> where Self: 'a;
    fn facts_iter(&self) -> Self::Iter<'_>;
}

#[derive(Clone, Debug)]
pub enum AnyTuple {
    Uni(UniTuple), Bi(BiTuple), Tri(TriTuple), Quad(QuadTuple), Penta(PentaTuple),
}

impl FactIterator for AnyTuple {
    type Iter<'a> = Box<dyn Iterator<Item = &'a dyn GreynetFact> + 'a>;
    
    #[inline]
    fn facts_iter(&self) -> Self::Iter<'_> {
        match self {
            AnyTuple::Uni(t) => Box::new(std::iter::once(t.fact_a.as_ref())),
            AnyTuple::Bi(t) => Box::new([t.fact_a.as_ref(), t.fact_b.as_ref()].into_iter()),
            AnyTuple::Tri(t) => Box::new([t.fact_a.as_ref(), t.fact_b.as_ref(), t.fact_c.as_ref()].into_iter()),
            AnyTuple::Quad(t) => Box::new([t.fact_a.as_ref(), t.fact_b.as_ref(), t.fact_c.as_ref(), t.fact_d.as_ref()].into_iter()),
            AnyTuple::Penta(t) => Box::new([t.fact_a.as_ref(), t.fact_b.as_ref(), t.fact_c.as_ref(), t.fact_d.as_ref(), t.fact_e.as_ref()].into_iter()),
        }
    }
}

impl AnyTuple {
    pub fn combine(&self, other: &AnyTuple) -> crate::Result<AnyTuple> {
        let mut combined_facts = self.facts_vec();
        combined_facts.extend(other.facts_vec());
        
        if combined_facts.len() > 5 {
            return Err(crate::GreynetError::invalid_arity(5, combined_facts.len()));
        }
        
        Ok(TupleUtils::create_tuple_from_facts(combined_facts.into_iter().collect())?)
    }

    /// High-performance SmallVec version optimized for each arity
    #[inline]
    pub fn facts_vec(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> {
        match self {
            AnyTuple::Uni(t) => t.facts(),
            AnyTuple::Bi(t) => t.facts(),
            AnyTuple::Tri(t) => t.facts(),
            AnyTuple::Quad(t) => t.facts(),
            AnyTuple::Penta(t) => t.facts(),
        }
    }

    /// Compatibility version - use facts_vec() for better performance
    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> {
        self.facts_vec().into_iter().collect()
    }

    #[inline]
    pub fn arity(&self) -> usize {
        match self {
            AnyTuple::Uni(_) => 1, AnyTuple::Bi(_) => 2, AnyTuple::Tri(_) => 3,
            AnyTuple::Quad(_) => 4, AnyTuple::Penta(_) => 5,
        }
    }

    #[inline]
    pub fn node(&self) -> Option<NodeId> {
        match self {
            AnyTuple::Uni(t) => t.node, AnyTuple::Bi(t) => t.node,
            AnyTuple::Tri(t) => t.node, AnyTuple::Quad(t) => t.node,
            AnyTuple::Penta(t) => t.node,
        }
    }

    #[inline]
    pub fn set_node(&mut self, node_id: NodeId) {
        match self {
            AnyTuple::Uni(t) => t.node = Some(node_id), AnyTuple::Bi(t) => t.node = Some(node_id),
            AnyTuple::Tri(t) => t.node = Some(node_id), AnyTuple::Quad(t) => t.node = Some(node_id),
            AnyTuple::Penta(t) => t.node = Some(node_id),
        }
    }

    #[inline]
    pub fn state(&self) -> TupleState {
        match self {
            AnyTuple::Uni(t) => t.state, AnyTuple::Bi(t) => t.state,
            AnyTuple::Tri(t) => t.state, AnyTuple::Quad(t) => t.state,
            AnyTuple::Penta(t) => t.state,
        }
    }

    #[inline]
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

    #[inline]
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
    #[inline]
    pub fn new(fact_a: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, node: None, state: TupleState::default() }
    }
    
    #[inline] 
    pub fn arity(&self) -> usize { 1 }
    
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> { 
        smallvec![self.fact_a.clone()] 
    }
    
    pub fn reset(&mut self) { 
        self.node = None; 
        self.state = TupleState::Dead; 
    }
}

#[derive(Clone, Debug)]
pub struct BiTuple {
    pub fact_a: Rc<dyn GreynetFact>,
    pub fact_b: Rc<dyn GreynetFact>,
    pub node: Option<NodeId>,
    pub state: TupleState,
}

impl BiTuple {
    #[inline]
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, node: None, state: TupleState::default() }
    }
    
    #[inline]
    pub fn arity(&self) -> usize { 2 }
    
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> { 
        smallvec![self.fact_a.clone(), self.fact_b.clone()] 
    }
    
    pub fn reset(&mut self) { 
        self.node = None; 
        self.state = TupleState::Dead; 
    }
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
    #[inline]
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>, fact_c: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, fact_c, node: None, state: TupleState::default() }
    }
    
    #[inline]
    pub fn arity(&self) -> usize { 3 }
    
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> { 
        smallvec![self.fact_a.clone(), self.fact_b.clone(), self.fact_c.clone()] 
    }
    
    pub fn reset(&mut self) { 
        self.node = None; 
        self.state = TupleState::Dead; 
    }
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
    #[inline]
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>, fact_c: Rc<dyn GreynetFact>, fact_d: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, fact_c, fact_d, node: None, state: TupleState::default() }
    }
    
    #[inline]
    pub fn arity(&self) -> usize { 4 }
    
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> { 
        smallvec![self.fact_a.clone(), self.fact_b.clone(), self.fact_c.clone(), self.fact_d.clone()] 
    }
    
    pub fn reset(&mut self) { 
        self.node = None; 
        self.state = TupleState::Dead; 
    }
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
    #[inline]
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>, fact_c: Rc<dyn GreynetFact>, fact_d: Rc<dyn GreynetFact>, fact_e: Rc<dyn GreynetFact>) -> Self {
        Self { fact_a, fact_b, fact_c, fact_d, fact_e, node: None, state: TupleState::default() }
    }
    
    #[inline]
    pub fn arity(&self) -> usize { 5 }
    
    // FIXED: Use correct SmallVec capacity for 5 elements
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> { 
        smallvec![self.fact_a.clone(), self.fact_b.clone(), self.fact_c.clone(), self.fact_d.clone(), self.fact_e.clone()] 
    }
    
    pub fn reset(&mut self) { 
        self.node = None; 
        self.state = TupleState::Dead; 
    }
}