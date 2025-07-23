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

#[cfg(test)]
mod joiner_tests {
    use super::*;

    #[test]
    fn test_joiner_type_inverse() {
        assert_eq!(JoinerType::Equal.inverse(), JoinerType::Equal);
        assert_eq!(JoinerType::NotEqual.inverse(), JoinerType::NotEqual);
        assert_eq!(JoinerType::LessThan.inverse(), JoinerType::GreaterThan);
        assert_eq!(
            JoinerType::LessThanOrEqual.inverse(),
            JoinerType::GreaterThanOrEqual
        );
        assert_eq!(JoinerType::GreaterThan.inverse(), JoinerType::LessThan);
        assert_eq!(
            JoinerType::GreaterThanOrEqual.inverse(),
            JoinerType::LessThanOrEqual
        );
        assert_eq!(
            JoinerType::RangeOverlaps.inverse(),
            JoinerType::RangeOverlaps
        );
        assert_eq!(JoinerType::RangeContains.inverse(), JoinerType::RangeWithin);
        assert_eq!(JoinerType::RangeWithin.inverse(), JoinerType::RangeContains);
    }

    #[test]
    fn test_joiner_type_create_comparator_equal() {
        let comparator = JoinerType::Equal.create_comparator::<i32>();
        assert!(comparator(&5, &5));
        assert!(!comparator(&5, &3));
    }

    #[test]
    fn test_joiner_type_create_comparator_not_equal() {
        let comparator = JoinerType::NotEqual.create_comparator::<i32>();
        assert!(!comparator(&5, &5));
        assert!(comparator(&5, &3));
    }

    #[test]
    fn test_joiner_type_create_comparator_less_than() {
        let comparator = JoinerType::LessThan.create_comparator::<i32>();
        assert!(comparator(&3, &5));
        assert!(!comparator(&5, &3));
        assert!(!comparator(&5, &5));
    }

    #[test]
    fn test_joiner_type_create_comparator_less_than_or_equal() {
        let comparator = JoinerType::LessThanOrEqual.create_comparator::<i32>();
        assert!(comparator(&3, &5));
        assert!(!comparator(&5, &3));
        assert!(comparator(&5, &5));
    }

    #[test]
    fn test_joiner_type_create_comparator_greater_than() {
        let comparator = JoinerType::GreaterThan.create_comparator::<i32>();
        assert!(!comparator(&3, &5));
        assert!(comparator(&5, &3));
        assert!(!comparator(&5, &5));
    }

    #[test]
    fn test_joiner_type_create_comparator_greater_than_or_equal() {
        let comparator = JoinerType::GreaterThanOrEqual.create_comparator::<i32>();
        assert!(!comparator(&3, &5));
        assert!(comparator(&5, &3));
        assert!(comparator(&5, &5));
    }

    #[test]
    #[should_panic(expected = "Range operations require special handling")]
    fn test_joiner_type_range_operations_panic() {
        let comparator = JoinerType::RangeOverlaps.create_comparator::<i32>();
        comparator(&1, &2);
    }

    #[test]
    fn test_range_utils_ranges_overlap() {
        // Overlapping ranges
        assert!(RangeUtils::ranges_overlap((1, 5), (3, 7)));
        assert!(RangeUtils::ranges_overlap((3, 7), (1, 5)));
        assert!(RangeUtils::ranges_overlap((1, 5), (5, 8))); // Edge case: touching at boundary

        // Non-overlapping ranges
        assert!(!RangeUtils::ranges_overlap((1, 3), (5, 7)));
        assert!(!RangeUtils::ranges_overlap((5, 7), (1, 3)));
        assert!(!RangeUtils::ranges_overlap((1, 4), (5, 8))); // Gap between ranges
    }

    #[test]
    fn test_range_utils_range_contains() {
        // Container contains content
        assert!(RangeUtils::range_contains((1, 10), (3, 7)));
        assert!(RangeUtils::range_contains((1, 10), (1, 10))); // Same range
        assert!(RangeUtils::range_contains((1, 10), (5, 5))); // Point in range

        // Container does not contain content
        assert!(!RangeUtils::range_contains((3, 7), (1, 10)));
        assert!(!RangeUtils::range_contains((1, 5), (3, 7)));
        assert!(!RangeUtils::range_contains((1, 3), (5, 7)));
    }

    #[test]
    fn test_range_utils_range_within() {
        // Content within container (same as range_contains with swapped parameters)
        assert!(RangeUtils::range_within((3, 7), (1, 10)));
        assert!(RangeUtils::range_within((1, 10), (1, 10)));
        assert!(RangeUtils::range_within((5, 5), (1, 10)));

        // Content not within container
        assert!(!RangeUtils::range_within((1, 10), (3, 7)));
        assert!(!RangeUtils::range_within((3, 7), (1, 5)));
        assert!(!RangeUtils::range_within((5, 7), (1, 3)));
    }

    #[test]
    fn test_range_utils_with_floating_point() {
        assert!(RangeUtils::ranges_overlap((1.5, 5.5), (3.2, 7.8)));
        assert!(RangeUtils::range_contains((1.0, 10.0), (3.5, 7.2)));
        assert!(RangeUtils::range_within((3.5, 7.2), (1.0, 10.0)));
    }
}
