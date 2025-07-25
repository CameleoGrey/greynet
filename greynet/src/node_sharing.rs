//! Fixed node_sharing.rs with proper trait bounds

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
    
    /// Check if a retrieval ID is already registered
    pub fn contains(&self, retrieval_id: &RetrievalId<S>) -> bool {
        self.nodes.contains_key(retrieval_id)
    }
    
    /// Get the number of registered nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    
    /// Check if the manager is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    
    /// Clear all registered nodes
    pub fn clear(&mut self) {
        self.nodes.clear();
    }
    
    /// Get all registered retrieval IDs
    pub fn retrieval_ids(&self) -> impl Iterator<Item = &RetrievalId<S>> {
        self.nodes.keys()
    }
    
    /// Get all registered node IDs
    pub fn node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.values()
    }
}

impl<S: Score> Default for NodeSharingManager<S> {
    fn default() -> Self {
        Self::new()
    }
}