use crate::arena::NodeId;
use crate::stream_def::RetrievalId;
use crate::score::Score;
use std::collections::HashMap;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct NodeSharingManager<S: Score> {
    nodes: HashMap<RetrievalId<S>, NodeId>,
    _phantom: PhantomData<S>,
}

impl<S: Score> NodeSharingManager<S> {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    pub fn get_node(&self, retrieval_id: &RetrievalId<S>) -> Option<NodeId> {
        self.nodes.get(retrieval_id).copied()
    }

    pub fn register_node(&mut self, retrieval_id: RetrievalId<S>, node_id: NodeId) -> Result<(), String> {
        if self.nodes.contains_key(&retrieval_id) {
            Err("Node already registered for this retrieval ID".to_string())
        } else {
            self.nodes.insert(retrieval_id, node_id);
            Ok(())
        }
    }
}

impl<S: Score> Default for NodeSharingManager<S> {
    fn default() -> Self {
        Self::new()
    }
}
