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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_state_is_dirty() {
        // States that are considered "dirty" and require processing.
        assert!(
            TupleState::Creating.is_dirty(),
            "Creating state should be dirty"
        );
        assert!(
            TupleState::Updating.is_dirty(),
            "Updating state should be dirty"
        );
        assert!(TupleState::Dying.is_dirty(), "Dying state should be dirty");

        // States that are considered "stable" and do not require processing.
        assert!(!TupleState::Ok.is_dirty(), "Ok state should not be dirty");
        assert!(
            !TupleState::Aborting.is_dirty(),
            "Aborting state should not be dirty"
        );
        assert!(
            !TupleState::Dead.is_dirty(),
            "Dead state should not be dirty"
        );
    }

    #[test]
    fn test_tuple_state_default() {
        // The default state for a tuple should be 'Dead', indicating it's inactive
        // and ready for potential recycling.
        assert_eq!(TupleState::default(), TupleState::Dead);
    }

    #[test]
    fn test_all_states_are_accounted_for() {
        // This test ensures that if a new state is added to TupleState,
        // the is_dirty method is updated accordingly. It's a bit verbose
        // but prevents accidental omissions.
        let all_states = [
            TupleState::Creating,
            TupleState::Ok,
            TupleState::Updating,
            TupleState::Dying,
            TupleState::Aborting,
            TupleState::Dead,
        ];

        for state in &all_states {
            match state {
                TupleState::Creating | TupleState::Updating | TupleState::Dying => {
                    assert!(state.is_dirty(), "State {:?} should be dirty", state);
                }
                TupleState::Ok | TupleState::Aborting | TupleState::Dead => {
                    assert!(!state.is_dirty(), "State {:?} should not be dirty", state);
                }
            }
        }
    }
}
