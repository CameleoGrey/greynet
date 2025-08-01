//analysis.rs
use crate::session::Session;
use crate::arena::NodeData;
use crate::{Score, AnyTuple, GreynetError};
use crate::error::Result;
use rustc_hash::FxHashMap as HashMap;

/// Analysis tools for constraint sessions
pub struct ConstraintAnalysis;

impl ConstraintAnalysis {
    /// Get detailed constraint violation analysis
    pub fn analyze_violations<S: Score>(session: &mut Session<S>) -> Result<ConstraintViolationReport<S>> {
        let constraint_matches = session.get_constraint_matches()?;
        let total_score = session.get_score()?;
        let feasible = Self::is_feasible(&total_score);

        let mut violations = Vec::new();
        for (constraint_id, matches) in constraint_matches {
            if !matches.is_empty() {
                violations.push(ConstraintViolation {
                    constraint_id: constraint_id.clone(),
                    violation_count: matches.len(),
                    violating_tuples: matches,
                });
            }
        }

        Ok(ConstraintViolationReport {
            total_score,
            violations,
            feasible: feasible,
        })
    }

    /// Check if the current solution is feasible (no hard constraint violations)
    fn is_feasible<S: Score>(score: &S) -> bool {
        // For HardSoftScore, feasible means hard_score >= 0
        // For SimpleScore, we assume feasible if score >= 0
        // This is a simplified heuristic
        score.get_priority_score() >= 0.0
    }

    /// Get network statistics
    pub fn get_network_stats<S: Score>(session: &Session<S>) -> NetworkStatistics {
        let mut stats = NetworkStatistics {
            total_nodes: session.nodes.len(),
            total_tuples: session.tuples.arena.len(),
            active_tuples: 0,
            node_type_counts: HashMap::default(),
            memory_usage_estimate: 0,
        };

        // Count node types
        for (_, node_data) in session.nodes.nodes.iter() {
            let node_type = match node_data {
                NodeData::From(_) => "From",
                NodeData::Filter(_) => "Filter",
                NodeData::Join(_) => "Join",
                NodeData::Conditional(_) => "Conditional",
                NodeData::Group(_) => "Group",
                NodeData::FlatMap(_) => "FlatMap",
                NodeData::Scoring(_) => "Scoring",
                NodeData::JoinLeftAdapter(_) => "JoinLeftAdapter",
                NodeData::JoinRightAdapter(_) => "JoinRightAdapter",
                NodeData::Distinct(_) => "Distinct",
                NodeData::Union(_) => "Union",
                NodeData::UnionAdapter(_) => "UnionAdapter",
                NodeData::Map(_) => "Map",
                NodeData::GlobalAggregate(_) => "GlobalAggregate",
            };
            *stats.node_type_counts.entry(node_type.to_string()).or_insert(0) += 1;
        }

        // Estimate memory usage (rough approximation)
        stats.memory_usage_estimate = session.tuples.memory_usage_estimate();

        stats
    }
}

#[derive(Debug)]
pub struct ConstraintViolationReport<S: Score> {
    pub total_score: S,
    pub violations: Vec<ConstraintViolation>,
    pub feasible: bool,
}

#[derive(Debug)]
pub struct ConstraintViolation {
    pub constraint_id: String,
    pub violation_count: usize,
    pub violating_tuples: Vec<AnyTuple>,
}

#[derive(Debug)]
pub struct NetworkStatistics {
    pub total_nodes: usize,
    pub total_tuples: usize,
    pub active_tuples: usize,
    pub node_type_counts: HashMap<String, usize>,
    pub memory_usage_estimate: usize, // bytes
}