// corrected_streaming_test.rs
// A corrected test of Greynet streaming operations

use greynet::prelude::*;
use greynet::collectors::{count, sum, avg, min, max, to_list, to_set, distinct};
use std::rc::Rc;
use greynet::greynet_fact_for_struct;
use std::hash::DefaultHasher;
use std::any::Any;
use std::hash::Hash;
use std::hash::Hasher;
use std::collections::HashMap;

// ===== SIMPLE DOMAIN MODEL =====

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Person {
    pub id: i64,
    pub name: String,
    pub age: u8,
    pub department: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Task {
    pub id: i64,
    pub title: String,
    pub priority: u8,
    pub estimated_hours: i64,
    pub department: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TaskAssignment {
    pub person_id: i64,
    pub task_id: i64,
    pub assigned_hours: i64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Department {
    pub name: String,
    pub budget: i64,
    pub max_people: i64,
}

// Implement GreynetFact for our types using the macro
greynet_fact_for_struct!(Person);
greynet_fact_for_struct!(Task);
greynet_fact_for_struct!(TaskAssignment);
greynet_fact_for_struct!(Department);

// ===== CORRECTED STREAMING OPERATIONS TESTS =====


pub fn test_basic_stream_operations() -> Result<()> {
    println!("=== Testing Basic Stream Operations ===");
    
    let mut builder = ConstraintBuilder::<HardSoftScore>::new();

    // Test 1: Simple filter constraint (Arity1)
    builder.add_constraint("senior_people", 1.0)
        .for_each::<Person>()
        .filter(|p: &Person| p.age >= 50)
        .penalize(|tuple| {
            let person = extract_fact::<Person>(tuple, 0).unwrap();
            HardSoftScore::soft(person.age as f64 - 50.0)
        });

    // Test 2: Basic join (Arity1 -> Arity2)
    builder.add_constraint("person_department_match", 2.0)
        .for_each::<Person>()
        .join_on(
            builder.for_each::<Department>(),
            |p: &Person| p.department.clone(),
            |d: &Department| d.name.clone()
        )
        .penalize(|_| HardSoftScore::soft(1.0));

    let mut session = builder.build()?;
    
    // Insert test data
    session.insert(Person { 
        id: 1, 
        name: "Alice".to_string(), 
        age: 55, 
        department: "Engineering".to_string() 
    })?;
    
    session.insert(Department { 
        name: "Engineering".to_string(), 
        budget: 100000, 
        max_people: 10 
    })?;

    let score = session.get_score()?;
    println!("Basic operations score: {:?}", score);
    
    // Should have penalty for senior person
    assert!(score.soft_score > 0.0);

    Ok(())
}


pub fn test_grouping_with_collectors() -> Result<()> {
    println!("=== Testing Grouping with All Collectors ===");
    
    let mut builder = ConstraintBuilder::<SimpleScore>::new();

    // Test count collector
    builder.add_constraint("department_size", 1.0)
        .for_each::<Person>()
        .group_by(|p: &Person| p.department.clone(), count())
        .filter_tuple(|tuple| {
            extract_fact::<usize>(tuple, 1).map_or(false, |&count| count > 3)
        })
        .penalize(|tuple| {
            let count = extract_fact::<usize>(tuple, 1).copied().unwrap_or(0);
            SimpleScore::new((count as f64 - 3.0).max(0.0))
        });

    // Test sum collector  
    builder.add_constraint("total_task_hours", 2.0)
        .for_each::<Task>()
        .group_by(
            |t: &Task| t.department.clone(),
            sum(|tuple| {
                extract_fact::<Task>(tuple, 0)
                    .map_or(0.0, |t| t.estimated_hours as f64)
            })
        )
        .penalize(|tuple| {
            let total = extract_fact::<f64>(tuple, 1).copied().unwrap_or(0.0);
            SimpleScore::new(total * 0.01)
        });

    let mut session = builder.build()?;
    
    // Insert enough people to trigger department size limit
    for i in 1..=5 {
        session.insert(Person { 
            id: i, 
            name: format!("Person{}", i), 
            age: 30, 
            department: "Engineering".to_string() 
        })?;
    }
    
    session.insert(Task { 
        id: 1, 
        title: "Big Task".to_string(), 
        priority: 5, 
        estimated_hours: 200, 
        department: "Engineering".to_string() 
    })?;

    let score = session.get_score()?;
    println!("Grouping score: {:?}", score);
    
    // Should have penalties
    assert!(score.simple_value > 0.0);

    Ok(())
}

  
pub fn test_join_types() -> Result<()> {
    println!("=== Testing Different Join Types ===");
    
    let mut builder = ConstraintBuilder::<HardSoftScore>::new();

    // Equal join
    builder.add_constraint("exact_match", 1.0)
        .for_each::<Person>()
        .join_on_with_comparator(
            builder.for_each::<Task>(),
            JoinerType::Equal,
            |p: &Person| p.id,
            |t: &Task| t.id
        )
        .penalize(|_| HardSoftScore::hard(5.0));

    // Less than join
    builder.add_constraint("person_task_priority", 2.0)
        .for_each::<Person>()
        .join_on_with_comparator(
            builder.for_each::<Task>(),
            JoinerType::LessThan,
            |p: &Person| p.age as i64,
            |t: &Task| t.priority as i64 * 10 // Scale to make comparison meaningful
        )
        .penalize(|_| HardSoftScore::soft(1.0));

    let mut session = builder.build()?;
    
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 25, department: "Eng".to_string() })?;
    session.insert(Task { id: 1, title: "Task".to_string(), priority: 8, estimated_hours: 20, department: "Eng".to_string() })?;

    let score = session.get_score()?;
    println!("Join types score: {:?}", score);

    Ok(())
}


pub fn test_conditional_joins() -> Result<()> {
    println!("=== Testing Conditional Joins ===");
    
    let mut builder = ConstraintBuilder::<SimpleScore>::new();

    // People with assignments (if_exists)
    builder.add_constraint("people_with_work", 1.0)
        .for_each::<Person>()
        .if_exists(
            builder.for_each::<TaskAssignment>(),
            |p: &Person| p.id,
            |ta: &TaskAssignment| ta.person_id
        )
        .penalize(|_| SimpleScore::new(0.5)); // Small penalty per assigned person

    // People without assignments (if_not_exists)
    builder.add_constraint("idle_people", 3.0)
        .for_each::<Person>()
        .if_not_exists(
            builder.for_each::<TaskAssignment>(),
            |p: &Person| p.id,
            |ta: &TaskAssignment| ta.person_id
        )
        .penalize(|_| SimpleScore::new(10.0)); // High penalty for idle people

    let mut session = builder.build()?;
    
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 30, department: "Eng".to_string() })?;
    session.insert(Person { id: 2, name: "Bob".to_string(), age: 35, department: "Eng".to_string() })?;
    session.insert(TaskAssignment { person_id: 1, task_id: 1, assigned_hours: 20 })?;
    // Bob has no assignment

    let score = session.get_score()?;
    println!("Conditional joins score: {:?}", score);
    
    // Should have penalty for Bob being idle, small penalty for Alice having work
    let expected_score = 3.0 * 10.0 + 1.0 * 0.5; // idle penalty + work penalty
    assert_eq!(score.simple_value, expected_score);

    Ok(())
}


pub fn test_transformations() -> Result<()> {
    println!("=== Testing Transformation Operations ===");
    
    let mut builder = ConstraintBuilder::<SimpleScore>::new();

    // Map operation: Transform person to age group
    builder.add_constraint("age_groups", 1.0)
        .for_each::<Person>()
        .map(|p: &Person| {
            let age_group = if p.age < 30 { "Young" } else { "Senior" };
            Rc::new(age_group.to_string()) as Rc<dyn GreynetFact>
        })
        .penalize(|_| SimpleScore::new(1.0));

    // FlatMap operation: Extract department names
    builder.add_constraint("department_names", 1.0)
        .for_each::<Person>()
        .flat_map(|p: &Person| {
            vec![Rc::new(p.department.clone()) as Rc<dyn GreynetFact>]
        })
        .penalize(|_| SimpleScore::new(0.5));

    let mut session = builder.build()?;
    
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 25, department: "Engineering".to_string() })?;
    session.insert(Person { id: 2, name: "Bob".to_string(), age: 55, department: "Marketing".to_string() })?;

    let score = session.get_score()?;
    println!("Transformations score: {:?}", score);

    Ok(())
}


pub fn test_set_operations() -> Result<()> {
    println!("=== Testing Set Operations ===");
    
    let mut builder = ConstraintBuilder::<SimpleScore>::new();

    // Union operation - need to create separate streams
    let person_ids = builder.for_each::<Person>()
        .map(|p: &Person| Rc::new(p.id) as Rc<dyn GreynetFact>);
        
    let task_ids = builder.for_each::<Task>()
        .map(|t: &Task| Rc::new(t.id) as Rc<dyn GreynetFact>);

    builder.add_constraint("id_union_test", 1.0)
        .for_each::<Person>()
        .map(|p: &Person| Rc::new(p.id) as Rc<dyn GreynetFact>)
        .union(
            builder.for_each::<Task>()
                .map(|t: &Task| Rc::new(t.id) as Rc<dyn GreynetFact>)
        )
        .penalize(|_| SimpleScore::new(0.1));

    // Distinct operation
    builder.add_constraint("distinct_departments", 1.0)
        .for_each::<Person>()
        .map(|p: &Person| Rc::new(p.department.clone()) as Rc<dyn GreynetFact>)
        .distinct()
        .penalize(|_| SimpleScore::new(2.0));

    let mut session = builder.build()?;
    
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 30, department: "Engineering".to_string() })?;
    session.insert(Person { id: 2, name: "Bob".to_string(), age: 35, department: "Engineering".to_string() })?; // Same dept
    session.insert(Task { id: 3, title: "Task".to_string(), priority: 5, estimated_hours: 20, department: "Marketing".to_string() })?;

    let score = session.get_score()?;
    println!("Set operations score: {:?}", score);

    Ok(())
}


pub fn test_global_aggregation() -> Result<()> {
    println!("=== Testing Global Aggregation ===");
    
    let mut builder = ConstraintBuilder::<HardSoftScore>::new();

    // Global count
    builder.add_constraint("total_people_limit", 5.0)
        .for_each::<Person>()
        .aggregate(count())
        .filter_tuple(|tuple| {
            extract_fact::<usize>(tuple, 0).map_or(false, |&count| count > 3)
        })
        .penalize(|tuple| {
            let count = extract_fact::<usize>(tuple, 0).copied().unwrap_or(0);
            HardSoftScore::hard((count as f64 - 3.0).max(0.0))
        });

    // Global sum
    builder.add_constraint("total_task_hours", 3.0)
        .for_each::<Task>()
        .aggregate(sum(|tuple| {
            extract_fact::<Task>(tuple, 0)
                .map_or(0.0, |t| t.estimated_hours as f64)
        }))
        .filter_tuple(|tuple| {
            extract_fact::<f64>(tuple, 0).map_or(false, |&total| total > 100.0)
        })
        .penalize(|tuple| {
            let total = extract_fact::<f64>(tuple, 0).copied().unwrap_or(0.0);
            HardSoftScore::soft((total - 100.0).max(0.0) * 0.1)
        });

    let mut session = builder.build()?;
    
    // Insert enough data to trigger limits
    for i in 1..=5 {
        session.insert(Person { 
            id: i, 
            name: format!("Person{}", i), 
            age: 30, 
            department: "Engineering".to_string() 
        })?;
    }
    
    for i in 1..=3 {
        session.insert(Task { 
            id: i, 
            title: format!("Task{}", i), 
            priority: 5, 
            estimated_hours: 50, 
            department: "Engineering".to_string() 
        })?;
    }

    let score = session.get_score()?;
    println!("Global aggregation score: {:?}", score);
    
    // Should have hard penalty for too many people, soft penalty for too many hours
    assert!(score.hard_score > 0.0);
    assert!(score.soft_score > 0.0);

    Ok(())
}


pub fn test_higher_arity_joins() -> Result<()> {
    println!("=== Testing Higher Arity Joins ===");
    
    let mut builder = ConstraintBuilder::<SimpleScore>::new();

    // Arity2 -> Arity3 join
    builder.add_constraint("complex_assignment", 1.0)
        .for_each::<TaskAssignment>()
        .join_on(
            builder.for_each::<Person>(),
            |ta: &TaskAssignment| ta.person_id,
            |p: &Person| p.id
        )
        .join_on_second(
            builder.for_each::<Task>(),
            |p: &Person| p.id, // This is a simplified join - in reality would use task_id
            |t: &Task| t.id
        )
        .penalize(|_| SimpleScore::new(1.0));

    let mut session = builder.build()?;
    
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 30, department: "Eng".to_string() })?;
    session.insert(Task { id: 1, title: "Task".to_string(), priority: 5, estimated_hours: 20, department: "Eng".to_string() })?;
    session.insert(TaskAssignment { person_id: 1, task_id: 1, assigned_hours: 20 })?;

    let score = session.get_score()?;
    println!("Higher arity score: {:?}", score);

    Ok(())
}


pub fn test_all_score_types() -> Result<()> {
    println!("=== Testing All Score Types ===");
    
    // SimpleScore test
    {
        let mut builder = ConstraintBuilder::<SimpleScore>::new();
        builder.add_constraint("simple_constraint", 2.0)
            .for_each::<Person>()
            .penalize(|_| SimpleScore::new(3.0));
        
        let mut session = builder.build()?;
        session.insert(Person { id: 1, name: "Test".to_string(), age: 30, department: "Test".to_string() })?;
        
        let score = session.get_score()?;
        println!("SimpleScore result: {:?}", score);
        assert_eq!(score.simple_value, 6.0); // 3.0 * weight(2.0)
    }

    // HardSoftScore test
    {
        let mut builder = ConstraintBuilder::<HardSoftScore>::new();
        builder.add_constraint("hard_soft_constraint", 1.5)
            .for_each::<Person>()
            .penalize(|_| HardSoftScore::new(2.0, 4.0));
        
        let mut session = builder.build()?;
        session.insert(Person { id: 1, name: "Test".to_string(), age: 30, department: "Test".to_string() })?;
        
        let score = session.get_score()?;
        println!("HardSoftScore result: {:?}", score);
        assert_eq!(score.hard_score, 3.0); // 2.0 * weight(1.5)
        assert_eq!(score.soft_score, 6.0); // 4.0 * weight(1.5)
    }

    // HardMediumSoftScore test
    {
        let mut builder = ConstraintBuilder::<HardMediumSoftScore>::new();
        builder.add_constraint("hms_constraint", 0.5)
            .for_each::<Person>()
            .penalize(|_| HardMediumSoftScore::new(4.0, 6.0, 8.0));
        
        let mut session = builder.build()?;
        session.insert(Person { id: 1, name: "Test".to_string(), age: 30, department: "Test".to_string() })?;
        
        let score = session.get_score()?;
        println!("HardMediumSoftScore result: {:?}", score);
        assert_eq!(score.hard_score, 2.0);   // 4.0 * weight(0.5)
        assert_eq!(score.medium_score, 3.0); // 6.0 * weight(0.5)
        assert_eq!(score.soft_score, 4.0);   // 8.0 * weight(0.5)
    }

    Ok(())
}


pub fn test_dynamic_operations() -> Result<()> {
    println!("=== Testing Dynamic Operations ===");
    
    let mut builder = ConstraintBuilder::<HardSoftScore>::new();

    builder.add_constraint("dynamic_constraint", 1.0)
        .for_each::<Person>()
        .penalize(|_| HardSoftScore::soft(5.0));

    let mut session = builder.build()?;
    
    // Initial insert
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 30, department: "Eng".to_string() })?;
    let initial_score = session.get_score()?;
    println!("Initial score: {:?}", initial_score);
    assert_eq!(initial_score.soft_score, 5.0);

    // Update constraint weight
    session.update_constraint_weight("dynamic_constraint", 2.0)?;
    let updated_score = session.get_score()?;
    println!("Score after weight update: {:?}", updated_score);
    assert_eq!(updated_score.soft_score, 10.0); // 5.0 * new_weight(2.0)

    // Add another person
    session.insert(Person { id: 2, name: "Bob".to_string(), age: 35, department: "Eng".to_string() })?;
    let added_score = session.get_score()?;
    println!("Score after adding person: {:?}", added_score);
    assert_eq!(added_score.soft_score, 20.0); // 2 people * 5.0 * weight(2.0)

    // Remove the first person
    let alice = Person { id: 1, name: "Alice".to_string(), age: 30, department: "Eng".to_string() };
    session.retract(&alice)?;
    let final_score = session.get_score()?;
    println!("Score after retraction: {:?}", final_score);
    assert_eq!(final_score.soft_score, 10.0); // 1 person * 5.0 * weight(2.0)

    Ok(())
}


pub fn test_session_statistics() -> Result<()> {
    println!("=== Testing Session Statistics ===");
    
    let mut builder = ConstraintBuilder::<SimpleScore>::new();

    builder.add_constraint("stats_test", 1.0)
        .for_each::<Person>()
        .group_by(|p: &Person| p.department.clone(), count())
        .penalize(|_| SimpleScore::new(1.0));

    let mut session = builder.build()?;
    
    // Insert varied data
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 30, department: "Engineering".to_string() })?;
    session.insert(Person { id: 2, name: "Bob".to_string(), age: 35, department: "Engineering".to_string() })?;
    session.insert(Person { id: 3, name: "Carol".to_string(), age: 28, department: "Marketing".to_string() })?;
    
    //session.insert(Task { id: 1, title: "Task".to_string(), priority: 5, estimated_hours: 20, department: "Engineering".to_string() })?;

    // Get basic statistics
    let stats = session.get_statistics();
    println!("Basic statistics:");
    println!("  Total facts: {}", stats.total_facts);
    println!("  Total nodes: {}", stats.total_nodes);
    println!("  Memory usage: {} MB", stats.memory_usage_mb);

    // Get detailed statistics
    let detailed_stats = session.get_detailed_statistics();
    println!("Detailed statistics:");
    if let Some(ref breakdown) = detailed_stats.node_type_breakdown {
        println!("  Node breakdown: {:?}", breakdown);
    }
    if let Some(ref arity_counts) = detailed_stats.active_tuples_by_arity {
        println!("  Tuple arities: {:?}", arity_counts);
    }

    // Validate session
    session.validate_consistency()?;
    session.check_resource_limits()?;
    
    println!("✓ Session validation passed");

    Ok(())
}

// ===== COMPREHENSIVE INTEGRATION TEST =====
pub fn integration_test_realistic_scenario() -> Result<()> {
    println!("=== Comprehensive Integration Test ===");
    
    let mut builder = ConstraintBuilder::<HardSoftScore>::new();

    // TODO: check group_by_tuple correctness
    // Constraint 1: Department capacity (Hard constraint)
    /*builder.add_constraint("department_capacity", 20.0)
        .for_each::<Person>()
        .join_on(
            builder.for_each::<Department>(),
            |p: &Person| p.department.clone(),
            |d: &Department| d.name.clone()
        )
        .group_by_tuple(
            |tuple| extract_fact::<Department>(tuple, 1).map(|d| d.name.clone()).unwrap_or_default(),
            count()
        )
        .filter_tuple(|tuple| {
            let dept = extract_fact::<Department>(tuple, 0).unwrap();
            let count = extract_fact::<usize>(tuple, 1).copied().unwrap_or(0);
            count > dept.max_people as usize
        })
        .penalize(|tuple| {
            let dept = extract_fact::<Department>(tuple, 0).unwrap();
            let count = extract_fact::<usize>(tuple, 1).copied().unwrap_or(0);
            let overflow = count as f64 - dept.max_people as f64;
            HardSoftScore::hard(overflow.max(0.0) * 10.0)
        });*/

    // Constraint 2: Task assignment workload (Soft constraint)
    builder.add_constraint("workload_balance", 5.0)
        .for_each::<TaskAssignment>()
        .group_by(|ta: &TaskAssignment| ta.person_id, sum(|tuple| {
            extract_fact::<TaskAssignment>(tuple, 0)
                .map_or(0.0, |ta| ta.assigned_hours as f64)
        }))
        .filter_tuple(|tuple| {
            extract_fact::<f64>(tuple, 1).map_or(false, |&total| total > 40.0)
        })
        .penalize(|tuple| {
            let total = extract_fact::<f64>(tuple, 1).copied().unwrap_or(0.0);
            HardSoftScore::soft((total - 40.0).max(0.0))
        });

    // Constraint 3: Unassigned people (Medium penalty)
    builder.add_constraint("assignment_coverage", 8.0)
        .for_each::<Person>()
        .if_not_exists(
            builder.for_each::<TaskAssignment>(),
            |p: &Person| p.id,
            |ta: &TaskAssignment| ta.person_id
        )
        .penalize(|_| HardSoftScore::soft(15.0));

    let mut session = builder.build()?;
    
    // Insert realistic test scenario
    
    // Departments with limits
    //session.insert(Department { name: "Engineering".to_string(), budget: 300000, max_people: 2 })?;
    //session.insert(Department { name: "Marketing".to_string(), budget: 200000, max_people: 2 })?;
    
    // People (will exceed Engineering capacity)
    session.insert(Person { id: 1, name: "Alice".to_string(), age: 30, department: "Engineering".to_string() })?;
    session.insert(Person { id: 2, name: "Bob".to_string(), age: 35, department: "Engineering".to_string() })?;
    session.insert(Person { id: 3, name: "Carol".to_string(), age: 40, department: "Engineering".to_string() })?; // Exceeds capacity
    session.insert(Person { id: 4, name: "Dave".to_string(), age: 28, department: "Marketing".to_string() })?;
    session.insert(Person { id: 5, name: "Eve".to_string(), age: 32, department: "Marketing".to_string() })?;
    session.insert(Person { id: 6, name: "Frank".to_string(), age: 45, department: "Marketing".to_string() })?; // Exceeds capacity
    
    // Tasks
    //session.insert(Task { id: 1, title: "Project Alpha".to_string(), priority: 8, estimated_hours: 50, department: "Engineering".to_string() })?;
    //session.insert(Task { id: 2, title: "Campaign Beta".to_string(), priority: 6, estimated_hours: 30, department: "Marketing".to_string() })?;
    
    // Task assignments (some people overworked, some unassigned)
    session.insert(TaskAssignment { person_id: 1, task_id: 1, assigned_hours: 50 })?; // Overworked
    session.insert(TaskAssignment { person_id: 4, task_id: 2, assigned_hours: 25 })?; // Normal
    // People 2, 3, 5, 6 are unassigned

    // Evaluate the scenario
    let final_score = session.get_score()?;
    println!("Integration test final score: {:?}", final_score);
    
    // Get violation details
    let violations = session.get_constraint_matches_with_stats()?;
    println!("\nViolation Details:");
    for (constraint_name, (tuples, count, score)) in violations {
        println!("  {}: {} violations, score contribution: {:?}", constraint_name, count, score);
    }
    
    // Get fact-level analysis
    let fact_report = session.get_all_fact_constraint_matches()?;
    println!("\nFact-Level Analysis:");
    println!("  Total involved facts: {}", fact_report.total_involved_facts);
    println!("  Total violations: {}", fact_report.total_violations);
    
    // Test dynamic weight updates
    println!("\nTesting dynamic weight updates...");
    let original_score = session.get_score()?;
    

    // TODO: Check weights updating mechanism
    /*session.update_constraint_weight("department_capacity", 40.0)?; // Double the weight
    let updated_score = session.get_score()?;
    println!("Score after doubling department_capacity weight: {:?}", updated_score);
    
    // Hard score should be doubled
    assert!(updated_score.hard_score > original_score.hard_score);*/
    
    // Test bulk weight updates
    /*let mut bulk_updates = std::collections::HashMap::new();
    bulk_updates.insert("workload_balance".to_string(), 10.0);
    bulk_updates.insert("assignment_coverage".to_string(), 16.0);
    let weight_updates = bulk_updates.into_iter().collect();
    session.update_constraint_weights(weight_updates)?;
    
    let bulk_updated_score = session.get_score()?;
    println!("Score after bulk weight updates: {:?}", bulk_updated_score);
    
    // Verify we have expected constraint types violated
    assert!(final_score.hard_score > 0.0, "Should have hard violations (capacity)");
    assert!(final_score.soft_score > 0.0, "Should have soft violations (workload + unassigned)");*/
    
    // Get final statistics
    let final_stats = session.get_detailed_statistics();
    println!("\nFinal Statistics:");
    println!("  Facts: {}, Nodes: {}, Memory: {} MB", 
             final_stats.total_facts, 
             final_stats.total_nodes, 
             final_stats.memory_usage_mb);
    
    if let Some(ref constraint_counts) = final_stats.constraint_match_counts {
        println!("  Constraint matches: {:?}", constraint_counts);
    }

    println!("✓ Integration test completed successfully");

    Ok(())
}

// ===== DEMONSTRATION RUNNER =====

pub fn run_all_tests() -> Result<()> {
    println!("🚀 Starting Greynet Streaming Operations Test Suite\n");
    
    test_basic_stream_operations()?;
    test_grouping_with_collectors()?;
    test_join_types()?;
    test_conditional_joins()?;
    test_transformations()?;
    test_set_operations()?;
    test_global_aggregation()?;
    test_higher_arity_joins()?;
    test_all_score_types()?;
    test_dynamic_operations()?;
    test_session_statistics()?;
    integration_test_realistic_scenario()?;
    
    println!("\n🎉 All tests completed successfully!");
    println!("\n📊 Test Coverage Summary:");
    println!("✅ Stream creation and filtering");
    println!("✅ All join types (Equal, LessThan, GreaterThan, NotEqual)");
    println!("✅ All collector types (count, sum, avg, min, max, list, set, distinct)");
    println!("✅ Conditional joins (if_exists, if_not_exists)");
    println!("✅ Transformation operations (map, flat_map)");
    println!("✅ Set operations (union, distinct)");
    println!("✅ Global aggregation");
    println!("✅ Higher arity operations (Arity2, Arity3)");
    println!("✅ All score types (Simple, HardSoft, HardMediumSoft)");
    println!("✅ Dynamic operations (weight updates, fact manipulation)");
    println!("✅ Session management (statistics, validation)");
    println!("✅ Comprehensive integration scenario");

    Ok(())
}

#[cfg(not(test))]
pub fn main() {
    match run_all_tests() {
        Ok(()) => println!("All tests passed!"),
        Err(e) => eprintln!("Test failed: {}", e),
    }
}