use chrono::NaiveDate;
use greynet::{
    analysis::ConstraintAnalysis,
    constraint_builder::ConstraintBuilder,
    fact::GreynetFact,
    joiner::JoinerType,
    score::{FromHard, FromMedium, FromSoft, HardMediumSoftScore, Score},
    stream_def::Stream,
    AnyTuple, BiTuple, UniTuple,
};
use std::any::Any;
use std::collections::{hash_map::DefaultHasher, HashSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use uuid::Uuid;

// --- 1. Data Models ---
// Define the "facts" that will drive the rule engine.
// We derive Clone and Debug for convenience.

#[derive(Clone, Debug, Eq)]
struct Employee {
    id: Uuid,
    name: String,
    skills: HashSet<String>,
    unavailable_dates: HashSet<NaiveDate>,
}

// Manual implementation of Hash and PartialEq for Employee.
// Equality and hashing are based on the unique 'id' of the fact.
impl Hash for Employee {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Employee {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}


impl Employee {
    fn new(name: &str, skills: &[&str], unavailable_dates: &[NaiveDate]) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            skills: skills.iter().map(|s| s.to_string()).collect(),
            unavailable_dates: unavailable_dates.iter().cloned().collect(),
        }
    }
}

// Implement the core GreynetFact trait for Employee.
impl GreynetFact for Employee {
    fn fact_id(&self) -> Uuid {
        self.id
    }
    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Shift {
    id: Uuid,
    shift_id: String,
    employee_name: String,
    shift_date: NaiveDate,
    start_time: i32,
    end_time: i32,
    required_skill: String,
}

impl Shift {
    fn new(
        shift_id: &str,
        employee_name: &str,
        shift_date: NaiveDate,
        start_time: i32,
        end_time: i32,
        required_skill: &str,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            shift_id: shift_id.to_string(),
            employee_name: employee_name.to_string(),
            shift_date,
            start_time,
            end_time,
            required_skill: required_skill.to_string(),
        }
    }
}

impl GreynetFact for Shift {
    fn fact_id(&self) -> Uuid {
        self.id
    }
    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// A helper function to hash a string key for joins.
fn hash_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn main() {
    // --- 2. Constraint Definitions ---
    // Initialize the builder with the chosen score class.
    let builder = ConstraintBuilder::<HardMediumSoftScore>::new();

    // --- Rule 1: Skill Match (Medium Priority) ---
    builder.constraint("Required skill missing", 1.0, || {
        builder
            .for_each::<Shift>()
            .join(
                builder.for_each::<Employee>(),
                JoinerType::Equal,
                // Left key: employee_name from Shift
                Rc::new(|tuple| {
                    let facts = tuple.facts();
                    let shift = facts[0].as_any().downcast_ref::<Shift>().unwrap();
                    hash_key(&shift.employee_name)
                }),
                // Right key: name from Employee
                Rc::new(|tuple| {
                    let facts = tuple.facts();
                    let employee = facts[0].as_any().downcast_ref::<Employee>().unwrap();
                    hash_key(&employee.name)
                }),
            )
            .filter(Rc::new(|tuple| {
                let facts = tuple.facts();
                let shift = facts[0].as_any().downcast_ref::<Shift>().unwrap();
                let employee = facts[1].as_any().downcast_ref::<Employee>().unwrap();
                !employee.skills.contains(&shift.required_skill)
            }))
            .penalize("Required skill missing", |tuple| {
                HardMediumSoftScore::medium(1.0)
            })
    });

    // --- Rule 2: Employee Existence (Hard Priority) ---
    builder.constraint("Shift for non-existent employee", 1.0, || {
        builder
            .for_each::<Shift>()
            .if_not_exists(
                builder.for_each::<Employee>(),
                Rc::new(|tuple| {
                    let facts = tuple.facts();
                    let shift = facts[0].as_any().downcast_ref::<Shift>().unwrap();
                    hash_key(&shift.employee_name)
                }),
                Rc::new(|tuple| {
                    let facts = tuple.facts();
                    let employee = facts[0].as_any().downcast_ref::<Employee>().unwrap();
                    hash_key(&employee.name)
                }),
            )
            .penalize("Shift for non-existent employee", |tuple| {
                HardMediumSoftScore::hard(1.0)
            })
    });

    // --- Rule 3: Availability (Medium Priority) ---
    builder.constraint("Scheduled on unavailable day", 1.0, || {
        builder
            .for_each::<Shift>()
            .join(
                builder.for_each::<Employee>(),
                JoinerType::Equal,
                Rc::new(|tuple| {
                    let facts = tuple.facts();
                    let shift = facts[0].as_any().downcast_ref::<Shift>().unwrap();
                    hash_key(&shift.employee_name)
                }),
                Rc::new(|tuple| {
                    let facts = tuple.facts();
                    let employee = facts[0].as_any().downcast_ref::<Employee>().unwrap();
                    hash_key(&employee.name)
                }),
            )
            .filter(Rc::new(|tuple| {
                let facts = tuple.facts();
                let shift = facts[0].as_any().downcast_ref::<Shift>().unwrap();
                let employee = facts[1].as_any().downcast_ref::<Employee>().unwrap();
                employee.unavailable_dates.contains(&shift.shift_date)
            }))
            .penalize("Scheduled on unavailable day", |tuple| {
                HardMediumSoftScore::medium(1.0)
            })
    });

    // --- Rule 4: Overlapping Shifts (Hard Priority) ---
    builder.constraint("Overlapping shifts", 1.0, || {
        builder
            .for_each_unique_pair::<Shift>()
            .filter(Rc::new(|tuple| {
                // Ensure we are only comparing shifts for the same employee on the same day
                let facts = tuple.facts();
                let s1 = facts[0].as_any().downcast_ref::<Shift>().unwrap();
                let s2 = facts[1].as_any().downcast_ref::<Shift>().unwrap();
                s1.employee_name == s2.employee_name && s1.shift_date == s2.shift_date
            }))
            .filter(Rc::new(|tuple| {
                // Check for time overlap
                let facts = tuple.facts();
                let s1 = facts[0].as_any().downcast_ref::<Shift>().unwrap();
                let s2 = facts[1].as_any().downcast_ref::<Shift>().unwrap();
                s1.start_time.max(s2.start_time) < s1.end_time.min(s2.end_time)
            }))
            .penalize("Overlapping shifts", |tuple| {
                HardMediumSoftScore::hard(1.0)
            })
    });

    // --- 3. Execution and Verification ---
    println!("--- Building Greynet Session ---");
    let mut session = builder.build();

    let employee_ana = Employee::new(
        "Ana",
        &["Cashier", "Manager"],
        &[NaiveDate::from_ymd_opt(2025, 7, 18).unwrap()],
    );
    let employee_ben = Employee::new("Ben", &["Chef"], &[]);

    let shifts = vec![
        Shift::new("S01", "Ben", NaiveDate::from_ymd_opt(2025, 7, 14).unwrap(), 9, 17, "Chef"),
        Shift::new("S02", "Ben", NaiveDate::from_ymd_opt(2025, 7, 15).unwrap(), 9, 17, "Chef"),
        Shift::new("S03", "Ben", NaiveDate::from_ymd_opt(2025, 7, 16).unwrap(), 9, 17, "Chef"),
        Shift::new("S04", "Ben", NaiveDate::from_ymd_opt(2025, 7, 17).unwrap(), 9, 17, "Chef"),
        Shift::new("S05", "Ben", NaiveDate::from_ymd_opt(2025, 7, 18).unwrap(), 9, 17, "Chef"),
        Shift::new("S06", "Ben", NaiveDate::from_ymd_opt(2025, 7, 19).unwrap(), 9, 17, "Chef"),
        Shift::new("S07", "Ana", NaiveDate::from_ymd_opt(2025, 7, 15).unwrap(), 9, 18, "Cashier"),
        Shift::new("S08", "Ana", NaiveDate::from_ymd_opt(2025, 7, 15).unwrap(), 17, 20, "Manager"),
        Shift::new("S09", "Ana", NaiveDate::from_ymd_opt(2025, 7, 16).unwrap(), 9, 17, "Chef"),
        Shift::new("S10", "Ana", NaiveDate::from_ymd_opt(2025, 7, 18).unwrap(), 10, 16, "Cashier"),
        Shift::new("S11", "Charlie", NaiveDate::from_ymd_opt(2025, 7, 14).unwrap(), 9, 17, "Cashier"),
    ];

    println!("\nInitial Score: {:?} (Hard|Medium|Soft)", session.get_score().unwrap());

    println!("\n--- Inserting all facts into the session ---");
    session.insert(employee_ana.clone()).unwrap();
    session.insert(employee_ben.clone()).unwrap();
    session.insert_batch(shifts.clone()).unwrap();
    session.flush().unwrap();

    println!("\nScore after insert: {:?} (Hard|Medium|Soft)", session.get_score().unwrap());

    println!("\nConstraint Violations:");
    if let Ok(matches) = session.get_constraint_matches() {
        for (constraint_id, violations) in matches {
            println!("  - {} ({} violations)", constraint_id, violations.len());
            for tuple in violations {
                println!("    - Violation with facts: {:?}", tuple.facts());
            }
        }
    }

    session.update_constraint_weight("Scheduled on unavailable day", 10.0);
    session.update_constraint_weight("Overlapping shifts", 20.0);

    println!("\nScore after updating constraint weights: {:?} (Hard|Medium|Soft)", session.get_score().unwrap());

    println!("\n--- Retracting Ana's overlapping shift (S08) ---");
    let shift_to_retract = shifts.iter().find(|s| s.shift_id == "S08").unwrap();
    session.retract(shift_to_retract).unwrap();
    session.flush().unwrap();
    println!("Score after retracting S08: {:?} (Hard|Medium|Soft)", session.get_score().unwrap());

    println!("\n--- Correcting Ana's skill-mismatch shift (S09) ---");
    let old_shift = shifts.iter().find(|s| s.shift_id == "S09").unwrap();
    session.retract(old_shift).unwrap();
    let corrected_shift = Shift::new("S09-FIXED", "Ana", NaiveDate::from_ymd_opt(2025, 7, 16).unwrap(), 9, 17, "Manager");
    session.insert(corrected_shift).unwrap();
    session.flush().unwrap();
    println!("Score after correcting S09: {:?} (Hard|Medium|Soft)", session.get_score().unwrap());

    println!("\n--- Final Violations ---");
    if let Ok(matches) = session.get_constraint_matches() {
        for (constraint_id, violations) in matches {
            println!("  - {}", constraint_id);
            for tuple in violations {
                 println!("    - Remaining violation with facts: {:?}", tuple.facts());
            }
        }
    }
    
    println!("\n--- Example Complete ---");
}

