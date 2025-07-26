// examples/fact_constraint_matching.rs
// Demonstrates constraint matching with corrected joins
//
// Key fixes:
// 1. Used wrapper type PersonId instead of implementing GreynetFact for external Uuid type
//    (This avoids Rust's orphan rule - can't implement external trait for external type)
//    
//    Note: For production use, consider adding Uuid implementation to greynet/src/fact_impls.rs:
//    
//    impl GreynetFact for Uuid {
//        fn fact_id(&self) -> Uuid { *self }
//        fn clone_fact(&self) -> Box<dyn GreynetFact> { Box::new(*self) }
//        fn as_any(&self) -> &dyn std::any::Any { self }
//        // ... hash and equality implementations
//    }
//
// 2. Corrected join directions (Arity1 + Arity2 uses join_with_bi_first)
// 3. Fixed fact index extractions in filter_tuple operations  
// 4. Used only existing API methods from the session
// 5. Added comprehensive tests for the new functionality

use greynet::prelude::*;
use std::rc::Rc;
use uuid::Uuid;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// Wrapper type for Uuid to implement GreynetFact
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PersonId(Uuid);

impl PersonId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
    
    fn nil() -> Self {
        Self(Uuid::nil())
    }
    
    fn inner(&self) -> Uuid {
        self.0
    }
}

impl GreynetFact for PersonId {
    fn fact_id(&self) -> Uuid { self.0 }
    fn clone_fact(&self) -> Box<dyn GreynetFact> { Box::new(*self) }
    fn as_any(&self) -> &dyn std::any::Any { self }
    
    fn hash_fact(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
    
    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        other.as_any().downcast_ref::<PersonId>().map_or(false, |other_id| self.0 == other_id.0)
    }
}

// Example domain objects
#[derive(Debug, Clone, PartialEq)]
struct Person {
    id: PersonId,
    name: String,
    age: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct Assignment {
    id: Uuid,
    person_id: PersonId,
    task: String,
    hours: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct Skill {
    id: Uuid,
    person_id: PersonId,
    skill_name: String,
    level: u32,
}

// Implement GreynetFact for our domain objects
impl GreynetFact for Person {
    fn fact_id(&self) -> Uuid { self.id.inner() }
    fn clone_fact(&self) -> Box<dyn GreynetFact> { Box::new(self.clone()) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

impl GreynetFact for Assignment {
    fn fact_id(&self) -> Uuid { self.id }
    fn clone_fact(&self) -> Box<dyn GreynetFact> { Box::new(self.clone()) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

impl GreynetFact for Skill {
    fn fact_id(&self) -> Uuid { self.id }
    fn clone_fact(&self) -> Box<dyn GreynetFact> { Box::new(self.clone()) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

fn main() -> Result<()> {
    // Create a constraint builder
    let mut builder = builder::<HardSoftScore>();

    // Define constraints

    // Constraint 1: No person should work more than 40 hours total
    builder.for_each::<Assignment>()
        .group_by(
            |assignment: &Assignment| assignment.person_id,
            Collectors::sum(|tuple| {
                if let Some(assignment) = extract_fact::<Assignment>(tuple, 0) {
                    assignment.hours as f64
                } else {
                    0.0
                }
            })
        )
        .filter_tuple(|tuple| {
            // In grouped tuple: fact 0 = person_id (Uuid), fact 1 = total_hours (f64)
            if let Some(total_hours) = extract_fact::<f64>(tuple, 1) {
                *total_hours > 40.0
            } else {
                false
            }
        })
        .penalize("max_hours_per_person", |_| HardSoftScore::hard(1.0));

    // Constraint 2: Senior tasks should only be assigned to people with skill level >= 4
    builder.for_each::<Assignment>()
        .filter(|assignment: &Assignment| assignment.task.contains("Senior"))
        .join_on::<Assignment, Skill, _, _, PersonId>(
            builder.for_each::<Skill>(),
            |assignment| assignment.person_id,
            |skill| skill.person_id,
        )
        .filter_tuple(|tuple| {
            // After join: fact 0 = Assignment, fact 1 = Skill
            if let (Some(assignment), Some(skill)) = (
                extract_fact::<Assignment>(tuple, 0),
                extract_fact::<Skill>(tuple, 1)
            ) {
                // Check if the skill is relevant to the task and level is insufficient
                assignment.task.to_lowercase().contains(&skill.skill_name.to_lowercase()) 
                    && skill.level < 4
            } else {
                false
            }
        })
        .penalize("senior_task_skill_requirement", |_| HardSoftScore::hard(2.0));

    // Constraint 3: People under 25 should not work more than 30 hours (soft constraint)
    // Simplified approach: Join young people directly with assignments and check hours
    builder.for_each::<Person>()
        .filter(|person: &Person| person.age < 25)
        .join_on::<Person, Assignment, _, _, PersonId>(
            builder.for_each::<Assignment>(),
            |person| person.id,
            |assignment| assignment.person_id,
        )
        .filter_tuple(|tuple| {
            // After join: fact 0 = Person, fact 1 = Assignment
            if let Some(assignment) = extract_fact::<Assignment>(tuple, 1) {
                assignment.hours > 20  // Individual assignment over 20 hours is flagged for young workers
            } else {
                false
            }
        })
        .penalize("young_worker_heavy_assignment", |_| HardSoftScore::soft(1.0));

    // Constraint 4: Demonstrate complex grouping with total hours check
    // This shows how to use the PersonId GreynetFact implementation
    builder.for_each::<Person>()
        .filter(|person: &Person| person.age < 25)
        .join_with_bi_first::<Person, PersonId, _, _, PersonId>(
            builder.for_each::<Assignment>()
                .group_by(
                    |assignment: &Assignment| assignment.person_id,
                    Collectors::sum(|tuple| {
                        if let Some(assignment) = extract_fact::<Assignment>(tuple, 0) {
                            assignment.hours as f64
                        } else {
                            0.0
                        }
                    })
                ),
            |person| person.id,
            |person_id| *person_id,  // Extract person_id from grouped tuple (first fact)
        )
        .filter_tuple(|tuple| {
            // After join: fact 0 = Person, fact 1 = person_id (PersonId), fact 2 = total_hours (f64)
            if let Some(total_hours) = extract_fact::<f64>(tuple, 2) {
                *total_hours > 30.0
            } else {
                false
            }
        })
        .penalize("young_worker_total_hours", |_| HardSoftScore::soft(2.0));

    // Build the session
    let mut session = builder.build()?;

    // Create some test data
    let john = Person {
        id: PersonId::new(),
        name: "John Smith".to_string(),
        age: 23,
    };

    let jane = Person {
        id: PersonId::new(),
        name: "Jane Doe".to_string(),
        age: 35,
    };

    let mike = Person {
        id: PersonId::new(),
        name: "Mike Johnson".to_string(),
        age: 42,
    };

    // Insert people
    session.insert(john.clone())?;
    session.insert(jane.clone())?;
    session.insert(mike.clone())?;

    // Add skills
    let john_programming = Skill {
        id: Uuid::new_v4(),
        person_id: john.id,
        skill_name: "Programming".to_string(),
        level: 2, // Junior level
    };

    let jane_programming = Skill {
        id: Uuid::new_v4(),
        person_id: jane.id,
        skill_name: "Programming".to_string(),
        level: 5, // Senior level
    };

    let mike_management = Skill {
        id: Uuid::new_v4(),
        person_id: mike.id,
        skill_name: "Management".to_string(),
        level: 4,
    };

    session.insert(john_programming.clone())?;
    session.insert(jane_programming.clone())?;
    session.insert(mike_management.clone())?;

    // Add assignments that will cause violations
    let john_assignment1 = Assignment {
        id: Uuid::new_v4(),
        person_id: john.id,
        task: "Senior Programming Project".to_string(), // This will violate skill requirement
        hours: 25,
    };

    let john_assignment2 = Assignment {
        id: Uuid::new_v4(),
        person_id: john.id,
        task: "Junior Programming Task".to_string(),
        hours: 20, // Total with above: 45 hours, violates max hours and young worker hours
    };

    let jane_assignment = Assignment {
        id: Uuid::new_v4(),
        person_id: jane.id,
        task: "Senior Programming Project".to_string(),
        hours: 35, // No violations for Jane
    };

    session.insert(john_assignment1.clone())?;
    session.insert(john_assignment2.clone())?;
    session.insert(jane_assignment.clone())?;

    // Flush to ensure all constraints are evaluated
    session.flush()?;

    println!("=== Constraint Violation Analysis ===\n");

    // Get constraint matches (using existing API)
    let constraint_matches = session.get_constraint_matches()?;
    
    println!("1. All constraint violations:");
    for (constraint_id, violating_tuples) in &constraint_matches {
        println!("   Constraint: {}", constraint_id);
        println!("   Violations: {}", violating_tuples.len());
        for (i, tuple) in violating_tuples.iter().enumerate() {
            println!("     {}. Tuple arity: {} facts", i + 1, tuple.arity());
            // Try to identify the facts in this tuple
            for fact_idx in 0..tuple.arity() {
                if let Some(person) = extract_fact::<Person>(tuple, fact_idx) {
                    println!("        Fact {}: Person - {}", fact_idx, person.name);
                } else if let Some(assignment) = extract_fact::<Assignment>(tuple, fact_idx) {
                    println!("        Fact {}: Assignment - {} ({} hours)", fact_idx, assignment.task, assignment.hours);
                } else if let Some(skill) = extract_fact::<Skill>(tuple, fact_idx) {
                    println!("        Fact {}: Skill - {} (level {})", fact_idx, skill.skill_name, skill.level);
                } else if let Some(hours) = extract_fact::<f64>(tuple, fact_idx) {
                    println!("        Fact {}: Total Hours - {}", fact_idx, hours);
                } else if let Some(id) = extract_fact::<PersonId>(tuple, fact_idx) {
                    // Check if this PersonId matches any of our known entities
                    if *id == john.id {
                        println!("        Fact {}: Person ID - John", fact_idx);
                    } else if *id == jane.id {
                        println!("        Fact {}: Person ID - Jane", fact_idx);
                    } else if *id == mike.id {
                        println!("        Fact {}: Person ID - Mike", fact_idx);
                    } else {
                        println!("        Fact {}: Person ID - {:?}", fact_idx, id);
                    }
                } else if let Some(id) = extract_fact::<Uuid>(tuple, fact_idx) {
                    println!("        Fact {}: UUID - {}", fact_idx, id);
                }
            }
        }
        println!();
    }

    // Show the overall score
    println!("2. Overall constraint score:");
    let score = session.get_score()?;
    println!("   Score: {:?}", score);
    println!("   Hard score: {}", score.hard_score);
    println!("   Soft score: {}", score.soft_score);

    // Demonstrate some analysis
    println!("\n3. Analysis:");
    if score.hard_score > 0.0 {
        println!("   ❌ Hard constraints violated - solution is infeasible");
        
        // Look for specific constraint violations
        if constraint_matches.contains_key("max_hours_per_person") {
            println!("      - Someone is working more than 40 hours");
        }
        if constraint_matches.contains_key("senior_task_skill_requirement") {
            println!("      - Someone unqualified is assigned to a senior task");
        }
    } else {
        println!("   ✅ All hard constraints satisfied");
    }
    
    if score.soft_score > 0.0 {
        println!("   ⚠️  Soft constraints violated - solution could be improved");
        
        if constraint_matches.contains_key("young_worker_heavy_assignment") {
            println!("      - Young workers have heavy individual assignments");
        }
        if constraint_matches.contains_key("young_worker_total_hours") {
            println!("      - Young workers have too many total hours");
        }
    } else {
        println!("   ✅ All soft constraints satisfied");
    }

    // Test removing a problematic assignment
    println!("\n4. Testing constraint resolution:");
    println!("   Removing John's senior assignment...");
    session.retract(&john_assignment1)?;
    session.flush()?;
    
    let new_score = session.get_score()?;
    println!("   New score: {:?}", new_score);
    
    if new_score.hard_score < score.hard_score {
        println!("   ✅ Hard constraint violations reduced!");
    }
    if new_score.soft_score < score.soft_score {
        println!("   ✅ Soft constraint violations reduced!");
    }
    
    // Show final constraint status
    let final_matches = session.get_constraint_matches()?;
    println!("   Remaining violations: {}", final_matches.len());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fact_constraint_matching() {
        let result = main();
        assert!(result.is_ok(), "Example should run without errors: {:?}", result);
    }

    #[test]
    fn test_simple_constraint_violations() -> Result<()> {
        let mut builder = builder::<SimpleScore>();
        
        // Simple constraint: numbers greater than 10 are penalized
        builder.for_each::<i64>()
            .filter(|n: &i64| *n > 10)
            .penalize("large_numbers", |_| SimpleScore::new(1.0));

        let mut session = builder.build()?;

        let small_num = 5i64;
        let large_num = 15i64;

        session.insert(small_num)?;
        session.insert(large_num)?;
        session.flush()?;

        let score = session.get_score()?;
        assert_eq!(score.simple_value, 1.0); // Only the large number should be penalized

        let constraint_matches = session.get_constraint_matches()?;
        assert_eq!(constraint_matches.len(), 1);
        assert!(constraint_matches.contains_key("large_numbers"));

        Ok(())
    }

    #[test]
    fn test_join_constraint() -> Result<()> {
        let mut builder = builder::<SimpleScore>();
        
        // Constraint: Person-Assignment pairs where person is too young for the task
        builder.for_each::<Person>()
            .filter(|person: &Person| person.age < 25)
            .join_on::<Person, Assignment, _, _, PersonId>(
                builder.for_each::<Assignment>()
                    .filter(|assignment: &Assignment| assignment.hours > 30),
                |person| person.id,
                |assignment| assignment.person_id,
            )
            .penalize("young_person_heavy_task", |_| SimpleScore::new(2.0));

        let mut session = builder.build()?;

        let young_person = Person {
            id: PersonId(Uuid::new_v4()),
            name: "Young Worker".to_string(),
            age: 22,
        };

        let heavy_assignment = Assignment {
            id: Uuid::new_v4(),
            person_id: young_person.id,
            task: "Heavy Task".to_string(),
            hours: 35,
        };

        session.insert(young_person)?;
        session.insert(heavy_assignment)?;
        session.flush()?;

        let score = session.get_score()?;
        assert_eq!(score.simple_value, 2.0);

        Ok(())
    }
}