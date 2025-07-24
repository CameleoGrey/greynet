//tuple.rs
use crate::arena::NodeId;
use crate::state::TupleState;
use crate::utils::TupleUtils;
use crate::GreynetFact;
use smallvec::{smallvec, SmallVec};
use std::rc::Rc;
use crate::impl_any_tuple_accessors;
use std::any::Any;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum TupleArity {
    One,
    Two,
    Three,
    Four,
    Five,
}

impl TupleArity {
    #[inline]
    pub fn as_usize(&self) -> usize {
        match self {
            TupleArity::One => 1,
            TupleArity::Two => 2,
            TupleArity::Three => 3,
            TupleArity::Four => 4,
            TupleArity::Five => 5,
        }
    }

    #[inline]
    pub fn from_usize(value: usize) -> Option<Self> {
        match value {
            1 => Some(TupleArity::One),
            2 => Some(TupleArity::Two),
            3 => Some(TupleArity::Three),
            4 => Some(TupleArity::Four),
            5 => Some(TupleArity::Five),
            _ => None,
        }
    }
}

/// OPTIMIZATION: A trait to provide a zero-allocation iterator over a tuple's facts.
/// This avoids creating an intermediate Vec/SmallVec just for iteration.
pub trait FactIterator {
    type Iter<'a>: Iterator<Item = &'a dyn GreynetFact>
    where
        Self: 'a;
    fn facts_iter(&self) -> Self::Iter<'_>;
}

#[derive(Clone, Debug)]
pub enum AnyTuple {
    Uni(UniTuple),
    Bi(BiTuple),
    Tri(TriTuple),
    Quad(QuadTuple),
    Penta(PentaTuple),
}

impl FactIterator for AnyTuple {
    type Iter<'a> = Box<dyn Iterator<Item = &'a dyn GreynetFact> + 'a>;

    #[inline]
    fn facts_iter(&self) -> Self::Iter<'_> {
        match self {
            AnyTuple::Uni(t) => Box::new(t.facts_iter()),
            AnyTuple::Bi(t) => Box::new(t.facts_iter()),
            AnyTuple::Tri(t) => Box::new(t.facts_iter()),
            AnyTuple::Quad(t) => Box::new(t.facts_iter()),
            AnyTuple::Penta(t) => Box::new(t.facts_iter()),
        }
    }
}

impl AnyTuple {
    /// OPTIMIZATION: Combine tuples without creating an intermediate Vec.
    /// This is more verbose but avoids a heap allocation on every join.
    pub fn combine(&self, other: &AnyTuple) -> crate::Result<AnyTuple> {
        match (self, other) {
            (AnyTuple::Uni(l), AnyTuple::Uni(r)) => Ok(AnyTuple::Bi(BiTuple::new(l.fact_a.clone(), r.fact_a.clone()))),
            (AnyTuple::Uni(l), AnyTuple::Bi(r)) => Ok(AnyTuple::Tri(TriTuple::new(l.fact_a.clone(), r.fact_a.clone(), r.fact_b.clone()))),
            (AnyTuple::Bi(l), AnyTuple::Uni(r)) => Ok(AnyTuple::Tri(TriTuple::new(l.fact_a.clone(), l.fact_b.clone(), r.fact_a.clone()))),
            (AnyTuple::Uni(l), AnyTuple::Tri(r)) => Ok(AnyTuple::Quad(QuadTuple::new(l.fact_a.clone(), r.fact_a.clone(), r.fact_b.clone(), r.fact_c.clone()))),
            (AnyTuple::Tri(l), AnyTuple::Uni(r)) => Ok(AnyTuple::Quad(QuadTuple::new(l.fact_a.clone(), l.fact_b.clone(), l.fact_c.clone(), r.fact_a.clone()))),
            (AnyTuple::Bi(l), AnyTuple::Bi(r)) => Ok(AnyTuple::Quad(QuadTuple::new(l.fact_a.clone(), l.fact_b.clone(), r.fact_a.clone(), r.fact_b.clone()))),
            (AnyTuple::Uni(l), AnyTuple::Quad(r)) => Ok(AnyTuple::Penta(PentaTuple::new(l.fact_a.clone(), r.fact_a.clone(), r.fact_b.clone(), r.fact_c.clone(), r.fact_d.clone()))),
            (AnyTuple::Quad(l), AnyTuple::Uni(r)) => Ok(AnyTuple::Penta(PentaTuple::new(l.fact_a.clone(), l.fact_b.clone(), l.fact_c.clone(), l.fact_d.clone(), r.fact_a.clone()))),
            (AnyTuple::Bi(l), AnyTuple::Tri(r)) => Ok(AnyTuple::Penta(PentaTuple::new(l.fact_a.clone(), l.fact_b.clone(), r.fact_a.clone(), r.fact_b.clone(), r.fact_c.clone()))),
            (AnyTuple::Tri(l), AnyTuple::Bi(r)) => Ok(AnyTuple::Penta(PentaTuple::new(l.fact_a.clone(), l.fact_b.clone(), l.fact_c.clone(), r.fact_a.clone(), r.fact_b.clone()))),
            _ => {
                let mut combined_facts = self.facts_vec();
                combined_facts.extend(other.facts_vec());
                if combined_facts.len() > 5 {
                    Err(crate::GreynetError::invalid_arity(5, combined_facts.len()))
                } else {
                    Ok(TupleUtils::create_tuple_from_facts(combined_facts.into_vec())?)
                }
            }
        }
    }

    /// High-performance version that returns a stack-allocated SmallVec.
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

    /// Compatibility version - use facts_iter() or facts_vec() for better performance.
    pub fn facts(&self) -> Vec<Rc<dyn GreynetFact>> {
        self.facts_vec().into_vec()
    }

    #[inline]
    pub fn arity(&self) -> usize {
        match self {
            AnyTuple::Uni(_) => 1,
            AnyTuple::Bi(_) => 2,
            AnyTuple::Tri(_) => 3,
            AnyTuple::Quad(_) => 4,
            AnyTuple::Penta(_) => 5,
        }
    }

    impl_any_tuple_accessors!(Uni, Bi, Tri, Quad, Penta);

    #[inline]
    pub fn tuple_arity(&self) -> TupleArity {
        match self {
            AnyTuple::Uni(_) => TupleArity::One,
            AnyTuple::Bi(_) => TupleArity::Two,
            AnyTuple::Tri(_) => TupleArity::Three,
            AnyTuple::Quad(_) => TupleArity::Four,
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

impl FactIterator for UniTuple {
    type Iter<'a> = std::iter::Once<&'a dyn GreynetFact>;
    #[inline]
    fn facts_iter(&self) -> Self::Iter<'_> {
        std::iter::once(self.fact_a.as_ref())
    }
}

impl UniTuple {
    #[inline]
    pub fn new(fact_a: Rc<dyn GreynetFact>) -> Self {
        Self {
            fact_a,
            node: None,
            state: TupleState::default(),
        }
    }
    #[inline]
    pub fn arity(&self) -> usize {
        1
    }
    #[inline]
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> {
        smallvec![self.fact_a.clone()]
    }
    #[inline]
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

impl FactIterator for BiTuple {
    type Iter<'a> = std::array::IntoIter<&'a dyn GreynetFact, 2>;
    #[inline]
    fn facts_iter(&self) -> Self::Iter<'_> {
        [self.fact_a.as_ref(), self.fact_b.as_ref()].into_iter()
    }
}

impl BiTuple {
    #[inline]
    pub fn new(fact_a: Rc<dyn GreynetFact>, fact_b: Rc<dyn GreynetFact>) -> Self {
        Self {
            fact_a,
            fact_b,
            node: None,
            state: TupleState::default(),
        }
    }
    #[inline]
    pub fn arity(&self) -> usize {
        2
    }
    #[inline]
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> {
        smallvec![self.fact_a.clone(), self.fact_b.clone()]
    }
    #[inline]
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

impl FactIterator for TriTuple {
    type Iter<'a> = std::array::IntoIter<&'a dyn GreynetFact, 3>;
    #[inline]
    fn facts_iter(&self) -> Self::Iter<'_> {
        [
            self.fact_a.as_ref(),
            self.fact_b.as_ref(),
            self.fact_c.as_ref(),
        ]
        .into_iter()
    }
}

impl TriTuple {
    #[inline]
    pub fn new(
        fact_a: Rc<dyn GreynetFact>,
        fact_b: Rc<dyn GreynetFact>,
        fact_c: Rc<dyn GreynetFact>,
    ) -> Self {
        Self {
            fact_a,
            fact_b,
            fact_c,
            node: None,
            state: TupleState::default(),
        }
    }
    #[inline]
    pub fn arity(&self) -> usize {
        3
    }
    #[inline]
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> {
        smallvec![self.fact_a.clone(), self.fact_b.clone(), self.fact_c.clone()]
    }
    #[inline]
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

impl FactIterator for QuadTuple {
    type Iter<'a> = std::array::IntoIter<&'a dyn GreynetFact, 4>;
    #[inline]
    fn facts_iter(&self) -> Self::Iter<'_> {
        [
            self.fact_a.as_ref(),
            self.fact_b.as_ref(),
            self.fact_c.as_ref(),
            self.fact_d.as_ref(),
        ]
        .into_iter()
    }
}

impl QuadTuple {
    #[inline]
    pub fn new(
        fact_a: Rc<dyn GreynetFact>,
        fact_b: Rc<dyn GreynetFact>,
        fact_c: Rc<dyn GreynetFact>,
        fact_d: Rc<dyn GreynetFact>,
    ) -> Self {
        Self {
            fact_a,
            fact_b,
            fact_c,
            fact_d,
            node: None,
            state: TupleState::default(),
        }
    }
    #[inline]
    pub fn arity(&self) -> usize {
        4
    }
    #[inline]
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> {
        smallvec![
            self.fact_a.clone(),
            self.fact_b.clone(),
            self.fact_c.clone(),
            self.fact_d.clone()
        ]
    }
    #[inline]
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

impl FactIterator for PentaTuple {
    type Iter<'a> = std::array::IntoIter<&'a dyn GreynetFact, 5>;
    #[inline]
    fn facts_iter(&self) -> Self::Iter<'_> {
        [
            self.fact_a.as_ref(),
            self.fact_b.as_ref(),
            self.fact_c.as_ref(),
            self.fact_d.as_ref(),
            self.fact_e.as_ref(),
        ]
        .into_iter()
    }
}

impl PentaTuple {
    #[inline]
    pub fn new(
        fact_a: Rc<dyn GreynetFact>,
        fact_b: Rc<dyn GreynetFact>,
        fact_c: Rc<dyn GreynetFact>,
        fact_d: Rc<dyn GreynetFact>,
        fact_e: Rc<dyn GreynetFact>,
    ) -> Self {
        Self {
            fact_a,
            fact_b,
            fact_c,
            fact_d,
            fact_e,
            node: None,
            state: TupleState::default(),
        }
    }
    #[inline]
    pub fn arity(&self) -> usize {
        5
    }
    #[inline]
    pub fn facts(&self) -> SmallVec<[Rc<dyn GreynetFact>; 5]> {
        smallvec![
            self.fact_a.clone(),
            self.fact_b.clone(),
            self.fact_c.clone(),
            self.fact_d.clone(),
            self.fact_e.clone()
        ]
    }
    #[inline]
    pub fn reset(&mut self) {
        self.node = None;
        self.state = TupleState::Dead;
    }
}

/// Zero-copy trait for direct fact access without allocations
pub trait ZeroCopyFacts {
    /// Get a direct reference to a fact by index without any allocations
    #[inline]
    fn get_fact_ref(&self, index: usize) -> Option<&dyn GreynetFact>;
    
    /// Get the first fact (optimized common case)
    #[inline]
    fn first_fact(&self) -> Option<&dyn GreynetFact> {
        self.get_fact_ref(0)
    }
    
    /// Get the last fact (optimized common case)  
    #[inline]
    fn last_fact(&self) -> Option<&dyn GreynetFact> {
        if self.arity() > 0 {
            self.get_fact_ref(self.arity() - 1)
        } else {
            None
        }
    }
    
    /// Check if index is valid for this tuple
    #[inline]
    fn has_fact_at(&self, index: usize) -> bool {
        index < self.arity()
    }
    
    /// Required for bounds checking
    fn arity(&self) -> usize;

    fn as_any(&self) -> &dyn Any;
}

impl ZeroCopyFacts for AnyTuple {
    #[inline]
    fn get_fact_ref(&self, index: usize) -> Option<&dyn GreynetFact> {
        match (self, index) {
            // UniTuple cases
            (AnyTuple::Uni(t), 0) => Some(t.fact_a.as_ref()),
            
            // BiTuple cases  
            (AnyTuple::Bi(t), 0) => Some(t.fact_a.as_ref()),
            (AnyTuple::Bi(t), 1) => Some(t.fact_b.as_ref()),
            
            // TriTuple cases
            (AnyTuple::Tri(t), 0) => Some(t.fact_a.as_ref()),
            (AnyTuple::Tri(t), 1) => Some(t.fact_b.as_ref()),
            (AnyTuple::Tri(t), 2) => Some(t.fact_c.as_ref()),
            
            // QuadTuple cases
            (AnyTuple::Quad(t), 0) => Some(t.fact_a.as_ref()),
            (AnyTuple::Quad(t), 1) => Some(t.fact_b.as_ref()),
            (AnyTuple::Quad(t), 2) => Some(t.fact_c.as_ref()),
            (AnyTuple::Quad(t), 3) => Some(t.fact_d.as_ref()),
            
            // PentaTuple cases
            (AnyTuple::Penta(t), 0) => Some(t.fact_a.as_ref()),
            (AnyTuple::Penta(t), 1) => Some(t.fact_b.as_ref()),
            (AnyTuple::Penta(t), 2) => Some(t.fact_c.as_ref()),
            (AnyTuple::Penta(t), 3) => Some(t.fact_d.as_ref()),
            (AnyTuple::Penta(t), 4) => Some(t.fact_e.as_ref()),
            
            // Out of bounds
            _ => None,
        }
    }
    
    #[inline]
    fn arity(&self) -> usize {
        AnyTuple::arity(self)
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    // Specialized implementations for common cases
    #[inline]
    fn first_fact(&self) -> Option<&dyn GreynetFact> {
        match self {
            AnyTuple::Uni(t) => Some(t.fact_a.as_ref()),
            AnyTuple::Bi(t) => Some(t.fact_a.as_ref()),
            AnyTuple::Tri(t) => Some(t.fact_a.as_ref()),
            AnyTuple::Quad(t) => Some(t.fact_a.as_ref()),
            AnyTuple::Penta(t) => Some(t.fact_a.as_ref()),
        }
    }
    
    #[inline]
    fn last_fact(&self) -> Option<&dyn GreynetFact> {
        match self {
            AnyTuple::Uni(t) => Some(t.fact_a.as_ref()),
            AnyTuple::Bi(t) => Some(t.fact_b.as_ref()),
            AnyTuple::Tri(t) => Some(t.fact_c.as_ref()),
            AnyTuple::Quad(t) => Some(t.fact_d.as_ref()),
            AnyTuple::Penta(t) => Some(t.fact_e.as_ref()),
        }
    }
}

// Individual tuple implementations for even better performance when type is known
impl ZeroCopyFacts for UniTuple {
    #[inline]
    fn get_fact_ref(&self, index: usize) -> Option<&dyn GreynetFact> {
        match index {
            0 => Some(self.fact_a.as_ref()),
            _ => None,
        }
    }
    
    #[inline]
    fn arity(&self) -> usize { 1 }
    
    #[inline]
    fn first_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_a.as_ref())
    }
    
    #[inline]
    fn last_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_a.as_ref())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ZeroCopyFacts for BiTuple {
    #[inline]
    fn get_fact_ref(&self, index: usize) -> Option<&dyn GreynetFact> {
        match index {
            0 => Some(self.fact_a.as_ref()),
            1 => Some(self.fact_b.as_ref()),
            _ => None,
        }
    }
    
    #[inline]
    fn arity(&self) -> usize { 2 }
    
    #[inline]
    fn first_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_a.as_ref())
    }
    
    #[inline]
    fn last_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_b.as_ref())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ZeroCopyFacts for TriTuple {
    #[inline]
    fn get_fact_ref(&self, index: usize) -> Option<&dyn GreynetFact> {
        match index {
            0 => Some(self.fact_a.as_ref()),
            1 => Some(self.fact_b.as_ref()),
            2 => Some(self.fact_c.as_ref()),
            _ => None,
        }
    }
    
    #[inline]
    fn arity(&self) -> usize { 3 }
    
    #[inline]
    fn first_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_a.as_ref())
    }
    
    #[inline]
    fn last_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_c.as_ref())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ZeroCopyFacts for QuadTuple {
    #[inline]
    fn get_fact_ref(&self, index: usize) -> Option<&dyn GreynetFact> {
        match index {
            0 => Some(self.fact_a.as_ref()),
            1 => Some(self.fact_b.as_ref()),
            2 => Some(self.fact_c.as_ref()),
            3 => Some(self.fact_d.as_ref()),
            _ => None,
        }
    }
    
    #[inline]
    fn arity(&self) -> usize { 4 }
    
    #[inline]
    fn first_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_a.as_ref())
    }
    
    #[inline]
    fn last_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_d.as_ref())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ZeroCopyFacts for PentaTuple {
    #[inline]
    fn get_fact_ref(&self, index: usize) -> Option<&dyn GreynetFact> {
        match index {
            0 => Some(self.fact_a.as_ref()),
            1 => Some(self.fact_b.as_ref()),
            2 => Some(self.fact_c.as_ref()),
            3 => Some(self.fact_d.as_ref()),
            4 => Some(self.fact_e.as_ref()),
            _ => None,
        }
    }
    
    #[inline]
    fn arity(&self) -> usize { 5 }
    
    #[inline]
    fn first_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_a.as_ref())
    }
    
    #[inline]
    fn last_fact(&self) -> Option<&dyn GreynetFact> {
        Some(self.fact_e.as_ref())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Optimized key extraction functions using zero-copy access
pub mod key_extractors {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    /// Extract hash key from first fact (most common case)
    #[inline]
    pub fn first_fact_key(tuple: &dyn ZeroCopyFacts) -> u64 {
        tuple.first_fact()
            .map(|fact| fact.hash_fact())
            .unwrap_or(0)
    }
    
    /// Extract hash key from last fact
    #[inline]
    pub fn last_fact_key(tuple: &dyn ZeroCopyFacts) -> u64 {
        tuple.last_fact()
            .map(|fact| fact.hash_fact())
            .unwrap_or(0)
    }
    
    /// Extract hash key from specific index
    #[inline]
    pub fn indexed_fact_key(tuple: &dyn ZeroCopyFacts, index: usize) -> u64 {
        tuple.get_fact_ref(index)
            .map(|fact| fact.hash_fact())
            .unwrap_or(0)
    }
    
    /// Extract key from multiple facts (for composite keys)
    #[inline]
    pub fn composite_key(tuple: &dyn ZeroCopyFacts, indices: &[usize]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for &index in indices {
            if let Some(fact) = tuple.get_fact_ref(index) {
                fact.hash_fact().hash(&mut hasher);
            }
        }
        hasher.finish()
    }
}