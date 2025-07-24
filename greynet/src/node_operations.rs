//node_operations.rs
use super::arena::{NodeArena, NodeData, NodeId, NodeOperation, SafeTupleIndex, TupleArena};
use crate::AnyTuple;
use rustc_hash::FxHashMap as HashMap;

pub struct NodeOperations;

impl NodeOperations {
    pub fn get_operation_stats<S: crate::Score>(nodes: &NodeArena<S>) -> HashMap<String, usize> {
        let mut stats = HashMap::default();
        let mut join_count = 0;
        let mut conditional_count = 0;
        let mut filter_count = 0;
        let mut from_count = 0;
        let mut group_count = 0;
        let mut flatmap_count = 0;
        let mut adapter_count = 0;
        let mut scoring_count = 0;

        for (_, node) in nodes.nodes.iter() {
            match node {
                NodeData::Join(_) => join_count += 1,
                NodeData::Conditional(_) => conditional_count += 1,
                NodeData::Filter(_) => filter_count += 1,
                NodeData::From(_) => from_count += 1,
                NodeData::Group(_) => group_count += 1,
                NodeData::FlatMap(_) => flatmap_count += 1,
                NodeData::JoinLeftAdapter(_) | NodeData::JoinRightAdapter(_) => adapter_count += 1,
                NodeData::Scoring(_) => scoring_count += 1,
            }
        }

        stats.insert("total_nodes".to_string(), nodes.len());
        stats.insert("join_nodes".to_string(), join_count);
        stats.insert("conditional_nodes".to_string(), conditional_count);
        stats.insert("filter_nodes".to_string(), filter_count);
        stats.insert("from_nodes".to_string(), from_count);
        stats.insert("group_nodes".to_string(), group_count);
        stats.insert("flatmap_nodes".to_string(), flatmap_count);
        stats.insert("adapter_nodes".to_string(), adapter_count);
        stats.insert("scoring_nodes".to_string(), scoring_count);
        stats
    }
}
