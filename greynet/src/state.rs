// state.rs - Tuple state management

/// This enum tracks whether a tuple is being created, is stable,
/// is being updated, or is being removed from the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TupleState {
    /// Tuple is being created and hasn't been processed yet
    Creating,
    /// Tuple is stable and active in the network
    Ok,
    /// Tuple is being updated (retract + reinsert)
    Updating,
    /// Tuple is being removed from the network
    Dying,
    /// Tuple creation was aborted before processing
    Aborting,
    /// Tuple is dead and can be recycled
    Dead,
}

impl TupleState {
    /// Returns true if the tuple is in a transitional state that requires processing.
    pub fn is_dirty(&self) -> bool {
        matches!(
            self,
            TupleState::Creating | TupleState::Updating | TupleState::Dying
        )
    }
}

impl Default for TupleState {
    fn default() -> Self {
        TupleState::Dead
    }
}