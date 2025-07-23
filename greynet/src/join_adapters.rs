//join_adapters.rs
use super::arena::NodeId;

#[derive(Debug)]
pub struct JoinLeftAdapter {
    pub parent_join_node: NodeId,
}

impl JoinLeftAdapter {
    pub fn new(parent_join_node: NodeId) -> Self {
        Self { parent_join_node }
    }
}

#[derive(Debug)]
pub struct JoinRightAdapter {
    pub parent_join_node: NodeId,
}

impl JoinRightAdapter {
    pub fn new(parent_join_node: NodeId) -> Self {
        Self { parent_join_node }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::NodeId;
    use slotmap::SlotMap;

    // Helper function to generate a unique NodeId for testing purposes.
    // NodeId is a key from a SlotMap, so we create one to get a valid key.
    fn create_test_node_id() -> NodeId {
        let mut sm = SlotMap::new();
        sm.insert(())
    }

    #[test]
    fn test_join_left_adapter_creation() {
        // Create a dummy parent node ID.
        let parent_node_id = create_test_node_id();

        // Instantiate a new JoinLeftAdapter.
        let left_adapter = JoinLeftAdapter::new(parent_node_id);

        // Verify that the parent_join_node field is set correctly.
        assert_eq!(left_adapter.parent_join_node, parent_node_id);
    }

    #[test]
    fn test_join_right_adapter_creation() {
        // Create a dummy parent node ID.
        let parent_node_id = create_test_node_id();

        // Instantiate a new JoinRightAdapter.
        let right_adapter = JoinRightAdapter::new(parent_node_id);

        // Verify that the parent_join_node field is set correctly.
        assert_eq!(right_adapter.parent_join_node, parent_node_id);
    }
}
