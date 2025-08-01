use greynet::prelude::*;
use greynet::collectors;
use greynet::FactIterator; // Explicit import for clarity
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, Ordering};
use std::any::Any;

// --- ID Generation ---
// A simple atomic counter to generate unique i64 IDs for our facts.
static NEXT_ID: AtomicI64 = AtomicI64::new(1);
fn get_next_id() -> i64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

// Wrapper type for i64 to represent a Person's ID, implementing GreynetFact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PersonId(i64);

impl PersonId {
    fn new() -> Self {
        Self(get_next_id())
    }
    
    fn inner(&self) -> i64 {
        self.0
    }
}

// --- Domain Objects ---
#[derive(Debug, Clone, PartialEq, Hash)]
struct Person {
    id: PersonId,
    name: String,
    age: u32,
}

#[derive(Debug, Clone, PartialEq, Hash)]
struct Assignment {
    id: i64,
    person_id: PersonId,
    task: String,
    hours: u32,
}

#[derive(Debug, Clone, PartialEq, Hash)]
struct Skill {
    id: i64,
    person_id: PersonId,
    skill_name: String,
    level: u32,
}

greynet::greynet_fact_for_struct!(PersonId);
greynet::greynet_fact_for_struct!(Person);
greynet::greynet_fact_for_struct!(Assignment);
greynet::greynet_fact_for_struct!(Skill);

fn main() -> Result<()> {
    // 1. Create a constraint builder
    let mut builder = builder::<HardSoftScore>();

    // --- Constraint Definitions using the new Fluent API ---

    // Constraint 1: No person should work more than 40 hours total (Hard Constraint)
    builder.add_constraint("max_hours_per_person", 1.0)
        .for_each::<Assignment>()
        .group_by(
            |assignment: &Assignment| assignment.person_id,
            collectors::sum(|tuple| {
                extract_fact::<Assignment>(tuple, 0)
                    .map_or(0.0, |a| a.hours as f64)
            })
        )
        .filter_tuple(|tuple| {
            // Grouped tuple: fact 0 = person_id (PersonId), fact 1 = total_hours (f64)
            extract_fact::<f64>(tuple, 1).map_or(false, |total_hours| *total_hours > 40.0)
        })
        .penalize(|_| HardSoftScore::hard(1.0));

    // Constraint 2: Senior tasks require skill level >= 4 (Hard Constraint)
    builder.add_constraint("senior_task_skill_requirement", 2.0)
        .for_each::<Assignment>()
        .filter(|assignment: &Assignment| assignment.task.contains("Senior"))
        .join_on::<Assignment, Skill, _, _, PersonId>(
            builder.for_each::<Skill>(),
            |assignment| assignment.person_id,
            |skill| skill.person_id,
        )
        .filter_tuple(|tuple| {
            // Joined tuple: fact 0 = Assignment, fact 1 = Skill
            if let (Some(assignment), Some(skill)) = (
                extract_fact::<Assignment>(tuple, 0),
                extract_fact::<Skill>(tuple, 1)
            ) {
                // Penalize if the skill is relevant but the level is too low
                assignment.task.to_lowercase().contains(&skill.skill_name.to_lowercase()) 
                    && skill.level < 4
            } else {
                false
            }
        })
        .penalize(|_| HardSoftScore::hard(2.0));

    // Constraint 3: People under 25 should not have single assignments over 20 hours (Soft Constraint)
    builder.add_constraint("young_worker_heavy_assignment", 1.0)
        .for_each::<Person>()
        .filter(|person: &Person| person.age < 25)
        .join_on::<Person, Assignment, _, _, PersonId>(
            builder.for_each::<Assignment>(),
            |person| person.id,
            |assignment| assignment.person_id,
        )
        .filter_tuple(|tuple| {
            // Joined tuple: fact 0 = Person, fact 1 = Assignment
            extract_fact::<Assignment>(tuple, 1).map_or(false, |a| a.hours > 20)
        })
        .penalize(|_| HardSoftScore::soft(1.0));

    // Constraint 4: People under 25 should not work more than 30 hours in total (Soft Constraint)
    // This demonstrates joining a simple stream (Arity 1) with a grouped stream (Arity 2).
    let young_people_stream = builder.for_each::<Person>()
        .filter(|person: &Person| person.age < 25);
        
    builder.add_constraint("young_worker_total_hours", 2.0)
        .for_each::<Assignment>()
        .group_by(
            |assignment: &Assignment| assignment.person_id,
            collectors::sum(|tuple| {
                extract_fact::<Assignment>(tuple, 0).map_or(0.0, |a| a.hours as f64)
            })
        )
        // Join the grouped stream (Arity 2) with the young people stream (Arity 1)
        .join_on_indexed::<PersonId, Person, _, _, PersonId>(
            young_people_stream,
            0, // The key in the grouped stream is the PersonId at index 0
            |person_id| *person_id,
            |person| person.id,
        )
        .filter_tuple(|tuple| {
            // Resulting tuple: fact 0 = PersonId, fact 1 = total_hours (f64), fact 2 = Person
            extract_fact::<f64>(tuple, 1).map_or(false, |total_hours| *total_hours > 30.0)
        })
        .penalize(|_| HardSoftScore::soft(2.0));

    // 2. Build the session
    let mut session = builder.build()?;

    // 3. Create and insert test data
    let john = Person { id: PersonId::new(), name: "John Smith".to_string(), age: 23 };
    let jane = Person { id: PersonId::new(), name: "Jane Doe".to_string(), age: 35 };
    let mike = Person { id: PersonId::new(), name: "Mike Johnson".to_string(), age: 42 };

    session.insert_batch([john.clone(), jane.clone(), mike.clone()])?;

    let john_programming = Skill { id: get_next_id(), person_id: john.id, skill_name: "Programming".to_string(), level: 2 };
    let jane_programming = Skill { id: get_next_id(), person_id: jane.id, skill_name: "Programming".to_string(), level: 5 };
    session.insert_batch([john_programming.clone(), jane_programming.clone()])?;

    let john_assignment1 = Assignment { id: get_next_id(), person_id: john.id, task: "Senior Programming Project".to_string(), hours: 25 };
    let john_assignment2 = Assignment { id: get_next_id(), person_id: john.id, task: "Junior Task".to_string(), hours: 20 };
    session.insert_batch([john_assignment1.clone(), john_assignment2.clone()])?;

    // 4. Flush to evaluate constraints and get results
    session.flush()?;
    println!("=== Constraint Violation Analysis ===\n");

    let score = session.get_score()?;
    println!("1. Overall constraint score:");
    println!("   Score: {:?}", score);
    println!("\n2. Analysis:");
    if score.hard_score > 0.0 {
        println!("   ❌ Hard constraints violated - solution is infeasible.");
    } else {
        println!("   ✅ All hard constraints satisfied.");
    }
    if score.soft_score > 0.0 {
        println!("   ⚠️  Soft constraints violated - solution could be improved.");
    } else {
        println!("   ✅ All soft constraints satisfied.");
    }

    // 3. NEW: Show a detailed report of all violations grouped by the facts involved.
    println!("\n=== Detailed Fact-by-Fact Violation Report ===\n");
    let all_violations_report = session.get_all_fact_constraint_matches()?;
    
    // Create a lookup map for more readable names in the output
    let fact_lookup: HashMap<i64, String> = [
        (john.fact_id(), john.name.clone()),
        (jane.fact_id(), jane.name.clone()),
        (mike.fact_id(), mike.name.clone()),
    ].iter().cloned().collect();

    for (fact_id, violations) in all_violations_report.matches_by_fact {
        let fact_name = fact_lookup.get(&fact_id).cloned().unwrap_or_else(|| fact_id.to_string());
        println!("Fact: '{}' ({} violations)", fact_name, violations.len());
        for violation in violations {
            println!("   - Violated Constraint: '{}'", violation.constraint_id);
            println!("     Score Impact: {:?}", violation.violation_score);
            print!("     Involved Tuple: [");
            for (fact_idx, fact) in violation.violating_tuple.facts_iter().enumerate() {
                 if let Some(p) = fact.as_any().downcast_ref::<Person>() { print!("Person({})", p.name) }
                 else if let Some(a) = fact.as_any().downcast_ref::<Assignment>() { print!("Assignment({}h)", a.hours) }
                 else if let Some(s) = fact.as_any().downcast_ref::<Skill>() { print!("Skill(Lvl {})", s.level) }
                 else if let Some(h) = fact.as_any().downcast_ref::<f64>() { print!("TotalHours({})", h) }
                 else if let Some(pid) = fact.as_any().downcast_ref::<PersonId>() {
                     if *pid == john.id { print!("ID(John)") } else if *pid == jane.id { print!("ID(Jane)") } else { print!("ID") }
                 }
                 if fact_idx < violation.violating_tuple.arity() - 1 { print!(", ") }
            }
            println!("]");
        }
        println!();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use greynet::{SimpleScore, Result};

    #[test]
    fn test_fact_constraint_matching() {
        let result = main();
        assert!(result.is_ok(), "Example should run without errors: {:?}", result);
    }

    #[test]
    fn test_simple_constraint_violations() -> Result<()> {
        let mut builder = builder::<SimpleScore>();
        
        builder.add_constraint("large_numbers", 1.0)
            .for_each::<i64>()
            .filter(|n: &i64| *n > 10)
            .penalize(|_| SimpleScore::new(1.0));

        let mut session = builder.build()?;
        session.insert_batch([5i64, 15i64])?;
        session.flush()?;

        let score = session.get_score()?;
        assert_eq!(score.simple_value, 1.0);

        let constraint_matches = session.get_constraint_matches()?;
        assert_eq!(constraint_matches.len(), 1);
        assert!(constraint_matches.contains_key("large_numbers"));

        Ok(())
    }
}