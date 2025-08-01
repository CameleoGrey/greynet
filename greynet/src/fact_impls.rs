// fact_impls.rs

use crate::fact::GreynetFact;
use crate::tuple::AnyTuple;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};


#[macro_export]
macro_rules! greynet_fact_for_struct {
    ($T:ty) => {
        impl GreynetFact for $T {
            /// Returns a unique ID for the fact.
            /// This is now a hash of the value, returned as i64.
            fn fact_id(&self) -> i64 {
                self.hash_fact() as i64
            }

            fn clone_fact(&self) -> Box<dyn GreynetFact> {
                Box::new(self.clone())
            }

            fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
                other.as_any().downcast_ref::<$T>().map_or(false, |a| a.fact_id() == self.fact_id())
            }

            fn hash_fact(&self) -> u64 {
                let mut hasher = DefaultHasher::new();
                self.hash(&mut hasher);
                hasher.finish()
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    };
}

macro_rules! impl_greynet_fact_for_primitive {
    ($T:ty) => {
        impl GreynetFact for $T {
            /// Returns a unique ID for the fact.
            /// This is now a hash of the value, returned as i64.
            fn fact_id(&self) -> i64 {
                self.hash_fact() as i64
            }

            fn clone_fact(&self) -> Box<dyn GreynetFact> {
                Box::new(self.clone())
            }

            fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
                other.as_any().downcast_ref::<$T>().map_or(false, |a| a == self)
            }

            fn hash_fact(&self) -> u64 {
                let mut hasher = DefaultHasher::new();
                self.hash(&mut hasher);
                hasher.finish()
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    };
}

impl_greynet_fact_for_primitive!(u64);
impl_greynet_fact_for_primitive!(usize);
impl_greynet_fact_for_primitive!(i64);
impl_greynet_fact_for_primitive!(String);
impl_greynet_fact_for_primitive!(bool);

impl GreynetFact for f64 {
    fn fact_id(&self) -> i64 {
        self.hash_fact() as i64
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(*self)
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        other.as_any().downcast_ref::<f64>().map_or(false, |a| a == self)
    }

    fn hash_fact(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.to_bits().hash(&mut hasher);
        hasher.finish()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GreynetFact for Vec<AnyTuple> {
    fn fact_id(&self) -> i64 {
        self.hash_fact() as i64
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        if let Some(other_vec) = other.as_any().downcast_ref::<Vec<AnyTuple>>() {
            if self.len() != other_vec.len() {
                return false;
            }
            self.iter().zip(other_vec.iter()).all(|(t1, t2)| {
                if t1.arity() != t2.arity() {
                    return false;
                }
                t1.facts().iter().zip(t2.facts().iter()).all(|(f1, f2)| f1.eq_fact(&**f2))
            })
        } else {
            false
        }
    }

    fn hash_fact(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for tuple in self {
            for fact in tuple.facts() {
                fact.hash_fact().hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
