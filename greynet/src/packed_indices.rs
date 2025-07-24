// packed_indices.rs
use crate::arena::SafeTupleIndex;

#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub struct PackedIndices(u128);

impl PackedIndices {
    #[inline(always)]
    pub fn new(left: SafeTupleIndex, right: SafeTupleIndex) -> Self {
        let left_raw = left.index.into_raw_parts().0;
        let right_raw = right.index.into_raw_parts().0;
        
        let left_bits = ((left_raw as u64) | ((left.generation << 32) as u64)) as u128;
        let right_bits = ((right_raw as u64) | ((right.generation << 32) as u64)) as u128;
        
        Self((left_bits << 64) | right_bits)
    }
    
    #[inline(always)]
    pub fn left_index(&self) -> usize {
        (self.0 >> 64) as u32 as usize
    }
    
    #[inline(always)]
    pub fn left_generation(&self) -> u64 {
        (self.0 >> 96) as u64
    }
    
    #[inline(always)]
    pub fn right_index(&self) -> usize {
        self.0 as u32 as usize
    }
    
    #[inline(always)]
    pub fn right_generation(&self) -> u64 {
        (self.0 >> 32) as u32 as u64
    }
}
