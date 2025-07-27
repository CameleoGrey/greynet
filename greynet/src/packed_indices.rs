// packed_indices.rs
use crate::arena::SafeTupleIndex;
use slotmap::{DefaultKey, Key, KeyData};

/// Packs two `SafeTupleIndex` keys into a single `u128` for efficient HashMap storage.
///
/// This is a performance optimization used in `JoinNode`'s beta memory. It allows
/// us to use a pair of tuple indices as a single key in a hash map, which is more
/// efficient than using a `(SafeTupleIndex, SafeTupleIndex)` tuple as the key.
/// This implementation uses the FFI (Foreign Function Interface) representation of
/// `slotmap::KeyData` to ensure a stable `u64` value for packing.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct PackedIndices(u128);

impl PackedIndices {
    /// Creates a new `PackedIndices` from the `SafeTupleIndex` of a left and right tuple.
    ///
    /// # Arguments
    /// * `left` - The `SafeTupleIndex` of the tuple from the left parent node.
    /// * `right` - The `SafeTupleIndex` of the tuple from the right parent node.
    #[inline(always)]
    pub fn new(left: SafeTupleIndex, right: SafeTupleIndex) -> Self {
        // Get the stable u64 representation of each slotmap key's data.
        let left_ffi = left.key().data().as_ffi();
        let right_ffi = right.key().data().as_ffi();

        // Pack the two u64 values into a single u128.
        // The left key occupies the upper 64 bits, and the right key occupies the lower 64 bits.
        Self(((left_ffi as u128) << 64) | (right_ffi as u128))
    }

    /// Unpacks the `DefaultKey` for the left tuple from the packed `u128`.
    #[inline(always)]
    pub fn left_key(&self) -> DefaultKey {
        // Shift right by 64 bits to get the upper 64 bits, cast to u64,
        // and reconstruct the KeyData, then convert it back into a DefaultKey.
        KeyData::from_ffi((self.0 >> 64) as u64).into()
    }

    /// Unpacks the `SafeTupleIndex` for the left tuple.
    #[inline(always)]
    pub fn left_safe_index(&self) -> SafeTupleIndex {
        SafeTupleIndex(self.left_key())
    }

    /// Unpacks the `DefaultKey` for the right tuple from the packed `u128`.
    #[inline(always)]
    pub fn right_key(&self) -> DefaultKey {
        // Cast the lower 64 bits to u64 and reconstruct the KeyData,
        // then convert it back into a DefaultKey.
        KeyData::from_ffi(self.0 as u64).into()
    }

    /// Unpacks the `SafeTupleIndex` for the right tuple.
    #[inline(always)]
    pub fn right_safe_index(&self) -> SafeTupleIndex {
        SafeTupleIndex(self.right_key())
    }
}
