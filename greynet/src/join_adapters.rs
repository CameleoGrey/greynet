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
