use crate::fact::GreynetFact;
use crate::tuple::AnyTuple;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

macro_rules! impl_greynet_fact_for_primitive {
    ($T:ty, $namespace_uuid:expr) => {
        impl GreynetFact for $T {
            fn fact_id(&self) -> Uuid {
                let mut hasher = DefaultHasher::new();
                self.hash(&mut hasher);
                let hash_val = hasher.finish();
                let namespace = Uuid::parse_str($namespace_uuid).unwrap();
                Uuid::new_v5(&namespace, &hash_val.to_be_bytes())
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

impl_greynet_fact_for_primitive!(u64, "a4b67a5b-324f-4227-8433-25a2a22c97f4");
impl_greynet_fact_for_primitive!(usize, "b5c78b6c-4350-5338-9544-36b3b33d08a5");
impl_greynet_fact_for_primitive!(i64, "c6d89c7d-5461-6449-a655-47c4c44e19b6");
impl_greynet_fact_for_primitive!(String, "d7e9ad8e-6572-755a-b766-58d5d55f2ac7");
impl_greynet_fact_for_primitive!(bool, "e8f0be9f-7683-866b-c877-69e6e6603bd8");

impl GreynetFact for f64 {
    fn fact_id(&self) -> Uuid {
        Uuid::new_v5(
            &Uuid::parse_str("f9a1cfaf-8794-977c-d988-7af7f7714ce9").unwrap(),
            &self.to_le_bytes(),
        )
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
    fn fact_id(&self) -> Uuid {
        let namespace = Uuid::parse_str("c8b1d2e3-f4a5-b6c7-d8e9-f0a1b2c3d4e5").unwrap();
        let mut hasher = DefaultHasher::new();
        for tuple in self {
            for fact in tuple.facts() {
                fact.hash_fact().hash(&mut hasher);
            }
        }
        Uuid::new_v5(&namespace, &hasher.finish().to_be_bytes())
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