use std::fmt::Debug;
use std::ops::Add;

/// Core trait for all score types with mandatory accumulation support
pub trait Score: Clone + Add<Output = Self> + PartialOrd + Debug + 'static {
    /// Accumulator type for efficient score building
    type Accumulator: Default + Clone + Debug;
    
    /// Returns a "null" or zero score (the additive identity)
    fn null_score() -> Self;

    /// Returns the names of score fields for this score type
    fn get_fields() -> &'static [&'static str];

    /// Converts the score to a list of numeric values
    fn as_list(&self) -> Vec<f64>;

    /// Creates a score from a list of numeric values
    fn from_list(values: Vec<f64>) -> Self;

    /// Returns the sum of absolute values of all score components
    fn get_sum_abs(&self) -> f64;

    /// Returns the priority score (typically the most important component)
    fn get_priority_score(&self) -> f64;

    /// Multiplies the score by a scalar value
    fn mul(&self, scalar: f64) -> Self;
    
    /// Calculates a fitness value, often used in genetic algorithms
    fn get_fitness_value(&self) -> f64;

    /// Returns a "stub" or placeholder score, usually representing a very high penalty
    fn get_stub_score() -> Self;

    /// Rounds the score's components to a given number of decimal places
    fn round(&mut self, precision: &[i32]);
    
    // === NEW ACCUMULATION METHODS ===
    
    /// Add a score to the accumulator
    fn accumulate_into(accumulator: &mut Self::Accumulator, score: &Self);
    
    /// Build final score from accumulator
    fn from_accumulator(accumulator: &Self::Accumulator) -> Self;
    
    /// Reset the accumulator to zero state
    fn reset_accumulator(accumulator: &mut Self::Accumulator);
    
    /// Create a new accumulator with this score as initial value
    fn into_accumulator(&self) -> Self::Accumulator {
        let mut acc = Self::Accumulator::default();
        Self::accumulate_into(&mut acc, self);
        acc
    }
}

/// SimpleScore with primitive accumulator
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct SimpleScore {
    pub simple_value: f64,
}

impl SimpleScore {
    pub fn new(simple_value: f64) -> Self {
        Self { simple_value }
    }
}

impl Add for SimpleScore {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        SimpleScore {
            simple_value: self.simple_value + other.simple_value,
        }
    }
}

impl Score for SimpleScore {
    type Accumulator = f64;

    fn null_score() -> Self {
        SimpleScore { simple_value: 0.0 }
    }

    fn get_fields() -> &'static [&'static str] {
        &["simple_value"]
    }

    fn as_list(&self) -> Vec<f64> {
        vec![self.simple_value]
    }

    fn from_list(mut values: Vec<f64>) -> Self {
        SimpleScore {
            simple_value: values.pop().unwrap_or(0.0),
        }
    }

    fn get_sum_abs(&self) -> f64 {
        self.simple_value.abs()
    }

    fn get_priority_score(&self) -> f64 {
        self.simple_value
    }

    fn mul(&self, scalar: f64) -> Self {
        SimpleScore {
            simple_value: self.simple_value * scalar,
        }
    }

    fn get_fitness_value(&self) -> f64 {
        1.0 - (1.0 / (self.simple_value + 1.0))
    }

    fn get_stub_score() -> Self {
        SimpleScore { simple_value: f64::INFINITY }
    }

    fn round(&mut self, precision: &[i32]) {
        if let Some(&p_val) = precision.get(0) {
            let factor = 10f64.powi(p_val);
            self.simple_value = (self.simple_value * factor).round() / factor;
        }
    }

    // === ACCUMULATION METHODS ===
    
    #[inline]
    fn accumulate_into(accumulator: &mut Self::Accumulator, score: &Self) {
        *accumulator += score.simple_value;
    }
    
    #[inline]
    fn from_accumulator(accumulator: &Self::Accumulator) -> Self {
        SimpleScore { simple_value: *accumulator }
    }
    
    #[inline]
    fn reset_accumulator(accumulator: &mut Self::Accumulator) {
        *accumulator = 0.0;
    }
}

/// HardSoftScore with structured accumulator
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct HardSoftScore {
    pub hard_score: f64,
    pub soft_score: f64,
}

#[derive(Default, Clone, Debug)]
pub struct HardSoftAccumulator {
    pub hard_total: f64,
    pub soft_total: f64,
}

impl HardSoftScore {
    pub fn new(hard_score: f64, soft_score: f64) -> Self {
        Self { hard_score, soft_score }
    }

    pub fn hard(hard_score: f64) -> Self {
        Self { hard_score, soft_score: 0.0 }
    }

    pub fn soft(soft_score: f64) -> Self {
        Self { hard_score: 0.0, soft_score }
    }
}

impl Add for HardSoftScore {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        HardSoftScore {
            hard_score: self.hard_score + other.hard_score,
            soft_score: self.soft_score + other.soft_score,
        }
    }
}

impl Score for HardSoftScore {
    type Accumulator = HardSoftAccumulator;

    fn null_score() -> Self {
        HardSoftScore { hard_score: 0.0, soft_score: 0.0 }
    }

    fn get_fields() -> &'static [&'static str] {
        &["hard_score", "soft_score"]
    }

    fn as_list(&self) -> Vec<f64> {
        vec![self.hard_score, self.soft_score]
    }

    fn from_list(values: Vec<f64>) -> Self {
        HardSoftScore {
            hard_score: values.get(0).copied().unwrap_or(0.0),
            soft_score: values.get(1).copied().unwrap_or(0.0),
        }
    }

    fn get_sum_abs(&self) -> f64 {
        self.hard_score.abs() + self.soft_score.abs()
    }

    fn get_priority_score(&self) -> f64 {
        if self.hard_score != 0.0 {
            self.hard_score
        } else {
            self.soft_score
        }
    }

    fn mul(&self, scalar: f64) -> Self {
        HardSoftScore {
            hard_score: self.hard_score * scalar,
            soft_score: self.soft_score * scalar,
        }
    }

    fn get_fitness_value(&self) -> f64 {
        let hard_fitness = 1.0 - (1.0 / (self.hard_score + 1.0));
        let soft_fitness = 1.0 - (1.0 / (self.soft_score + 1.0));
        0.5 * hard_fitness + 0.5 * soft_fitness
    }

    fn get_stub_score() -> Self {
        HardSoftScore {
            hard_score: f64::INFINITY,
            soft_score: f64::INFINITY,
        }
    }

    fn round(&mut self, precision: &[i32]) {
        if precision.len() >= 2 {
            let p_hard = 10f64.powi(precision[0]);
            self.hard_score = (self.hard_score * p_hard).round() / p_hard;
            
            let p_soft = 10f64.powi(precision[1]);
            self.soft_score = (self.soft_score * p_soft).round() / p_soft;
        }
    }

    // === ACCUMULATION METHODS ===
    
    #[inline]
    fn accumulate_into(accumulator: &mut Self::Accumulator, score: &Self) {
        accumulator.hard_total += score.hard_score;
        accumulator.soft_total += score.soft_score;
    }
    
    #[inline]
    fn from_accumulator(accumulator: &Self::Accumulator) -> Self {
        HardSoftScore {
            hard_score: accumulator.hard_total,
            soft_score: accumulator.soft_total,
        }
    }
    
    #[inline]
    fn reset_accumulator(accumulator: &mut Self::Accumulator) {
        accumulator.hard_total = 0.0;
        accumulator.soft_total = 0.0;
    }
}

/// HardMediumSoftScore with structured accumulator
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct HardMediumSoftScore {
    pub hard_score: f64,
    pub medium_score: f64,
    pub soft_score: f64,
}

#[derive(Default, Clone, Debug)]
pub struct HardMediumSoftAccumulator {
    pub hard_total: f64,
    pub medium_total: f64,
    pub soft_total: f64,
}

impl HardMediumSoftScore {
    pub fn new(hard_score: f64, medium_score: f64, soft_score: f64) -> Self {
        Self { hard_score, medium_score, soft_score }
    }

    pub fn hard(hard_score: f64) -> Self {
        Self { hard_score, medium_score: 0.0, soft_score: 0.0 }
    }

    pub fn medium(medium_score: f64) -> Self {
        Self { hard_score: 0.0, medium_score, soft_score: 0.0 }
    }

    pub fn soft(soft_score: f64) -> Self {
        Self { hard_score: 0.0, medium_score: 0.0, soft_score }
    }
}

impl Add for HardMediumSoftScore {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        HardMediumSoftScore {
            hard_score: self.hard_score + other.hard_score,
            medium_score: self.medium_score + other.medium_score,
            soft_score: self.soft_score + other.soft_score,
        }
    }
}

impl Score for HardMediumSoftScore {
    type Accumulator = HardMediumSoftAccumulator;

    fn null_score() -> Self {
        HardMediumSoftScore {
            hard_score: 0.0,
            medium_score: 0.0,
            soft_score: 0.0,
        }
    }

    fn get_fields() -> &'static [&'static str] {
        &["hard_score", "medium_score", "soft_score"]
    }

    fn as_list(&self) -> Vec<f64> {
        vec![self.hard_score, self.medium_score, self.soft_score]
    }

    fn from_list(values: Vec<f64>) -> Self {
        HardMediumSoftScore {
            hard_score: values.get(0).copied().unwrap_or(0.0),
            medium_score: values.get(1).copied().unwrap_or(0.0),
            soft_score: values.get(2).copied().unwrap_or(0.0),
        }
    }

    fn get_sum_abs(&self) -> f64 {
        self.hard_score.abs() + self.medium_score.abs() + self.soft_score.abs()
    }

    fn get_priority_score(&self) -> f64 {
        if self.hard_score != 0.0 {
            self.hard_score
        } else if self.medium_score != 0.0 {
            self.medium_score
        } else {
            self.soft_score
        }
    }

    fn mul(&self, scalar: f64) -> Self {
        HardMediumSoftScore {
            hard_score: self.hard_score * scalar,
            medium_score: self.medium_score * scalar,
            soft_score: self.soft_score * scalar,
        }
    }

    fn get_fitness_value(&self) -> f64 {
        let hard_fitness = 1.0 - (1.0 / (self.hard_score + 1.0));
        let medium_fitness = 1.0 - (1.0 / (self.medium_score + 1.0));
        let soft_fitness = 1.0 - (1.0 / (self.soft_score + 1.0));
        const WEIGHT: f64 = 1.0 / 3.0;
        WEIGHT * hard_fitness + WEIGHT * medium_fitness + WEIGHT * soft_fitness
    }

    fn get_stub_score() -> Self {
        HardMediumSoftScore {
            hard_score: f64::INFINITY,
            medium_score: f64::INFINITY,
            soft_score: f64::INFINITY,
        }
    }

    fn round(&mut self, precision: &[i32]) {
        if precision.len() >= 3 {
            let p_hard = 10f64.powi(precision[0]);
            self.hard_score = (self.hard_score * p_hard).round() / p_hard;

            let p_medium = 10f64.powi(precision[1]);
            self.medium_score = (self.medium_score * p_medium).round() / p_medium;

            let p_soft = 10f64.powi(precision[2]);
            self.soft_score = (self.soft_score * p_soft).round() / p_soft;
        }
    }

    // === ACCUMULATION METHODS ===
    
    #[inline]
    fn accumulate_into(accumulator: &mut Self::Accumulator, score: &Self) {
        accumulator.hard_total += score.hard_score;
        accumulator.medium_total += score.medium_score;
        accumulator.soft_total += score.soft_score;
    }
    
    #[inline]
    fn from_accumulator(accumulator: &Self::Accumulator) -> Self {
        HardMediumSoftScore {
            hard_score: accumulator.hard_total,
            medium_score: accumulator.medium_total,
            soft_score: accumulator.soft_total,
        }
    }
    
    #[inline]
    fn reset_accumulator(accumulator: &mut Self::Accumulator) {
        accumulator.hard_total = 0.0;
        accumulator.medium_total = 0.0;
        accumulator.soft_total = 0.0;
    }
}

// Keep the trait implementations for ergonomic construction
pub trait FromSimple: Sized {
    fn simple(value: f64) -> Self;
}

pub trait FromHard: Sized {
    fn hard(value: f64) -> Self;
}

pub trait FromMedium: Sized {
    fn medium(value: f64) -> Self;
}

pub trait FromSoft: Sized {
    fn soft(value: f64) -> Self;
}

impl FromSimple for SimpleScore {
    fn simple(value: f64) -> Self {
        Self::new(value)
    }
}

impl FromHard for HardSoftScore {
    fn hard(value: f64) -> Self {
        Self::hard(value)
    }
}

impl FromSoft for HardSoftScore {
    fn soft(value: f64) -> Self {
        Self::soft(value)
    }
}

impl FromHard for HardMediumSoftScore {
    fn hard(value: f64) -> Self {
        Self::hard(value)
    }
}

impl FromMedium for HardMediumSoftScore {
    fn medium(value: f64) -> Self {
        Self::medium(value)
    }
}

impl FromSoft for HardMediumSoftScore {
    fn soft(value: f64) -> Self {
        Self::soft(value)
    }
}

impl From<f64> for SimpleScore {
    fn from(value: f64) -> Self {
        SimpleScore::new(value)
    }
}