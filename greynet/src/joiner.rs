// joiner.rs - Joiner types and comparators
use std::fmt::Debug;

/// Types of joins supported by the constraint system
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum JoinerType {
    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    NotEqual,
    // Range operations are planned for a future phase but included here
    // for completeness as per the original Python version.
    RangeOverlaps,
    RangeContains,
    RangeWithin,
}

impl JoinerType {
    /// Returns the inverse joiner type. This is essential for creating the
    /// correct index for the right-hand side of a join. For example, if the
    /// join is `left.key < right.key`, the right index must find entries
    /// where `right.key > left.key`.
    pub fn inverse(&self) -> JoinerType {
        match self {
            JoinerType::Equal => JoinerType::Equal,
            JoinerType::NotEqual => JoinerType::NotEqual,
            JoinerType::LessThan => JoinerType::GreaterThan,
            JoinerType::LessThanOrEqual => JoinerType::GreaterThanOrEqual,
            JoinerType::GreaterThan => JoinerType::LessThan,
            JoinerType::GreaterThanOrEqual => JoinerType::LessThanOrEqual,
            JoinerType::RangeOverlaps => JoinerType::RangeOverlaps,
            JoinerType::RangeContains => JoinerType::RangeWithin,
            JoinerType::RangeWithin => JoinerType::RangeContains,
        }
    }

    /// Creates a comparator function for this joiner type
    pub fn create_comparator<T>(&self) -> Box<dyn Fn(&T, &T) -> bool>
    where
        T: PartialOrd + 'static,
    {
        match self {
            JoinerType::Equal => Box::new(|a, b| a == b),
            JoinerType::LessThan => Box::new(|a, b| a < b),
            JoinerType::LessThanOrEqual => Box::new(|a, b| a <= b),
            JoinerType::GreaterThan => Box::new(|a, b| a > b),
            JoinerType::GreaterThanOrEqual => Box::new(|a, b| a >= b),
            JoinerType::NotEqual => Box::new(|a, b| a != b),
            JoinerType::RangeOverlaps => {
                Box::new(|_a, _b| panic!("Range operations require special handling"))
            }
            JoinerType::RangeContains => {
                Box::new(|_a, _b| panic!("Range operations require special handling"))
            }
            JoinerType::RangeWithin => {
                Box::new(|_a, _b| panic!("Range operations require special handling"))
            }
        }
    }
}

/// A trait for comparison operations.
pub trait Comparator<T> {
    fn compare(&self, left: &T, right: &T) -> bool;
}

/// Range utilities for range-based joins
pub struct RangeUtils;

impl RangeUtils {
    /// Checks if two ranges overlap
    pub fn ranges_overlap<T: PartialOrd>(range_a: (T, T), range_b: (T, T)) -> bool {
        !(range_a.1 < range_b.0 || range_b.1 < range_a.0)
    }

    /// Checks if container_range contains content_range
    pub fn range_contains<T: PartialOrd>(container: (T, T), content: (T, T)) -> bool {
        container.0 <= content.0 && content.1 <= container.1
    }

    /// Alias for range_contains with swapped parameters
    pub fn range_within<T: PartialOrd>(content: (T, T), container: (T, T)) -> bool {
        Self::range_contains(container, content)
    }
}