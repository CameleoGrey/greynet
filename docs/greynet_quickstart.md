# Greynet Quick Start Guide

Greynet is a high-performance constraint satisfaction engine for Rust. This guide will get you up and running quickly.

## Core Concepts

- **Facts**: Data objects that implement the `GreynetFact` trait
- **Tuples**: Collections of facts (1-5 facts per tuple)
- **Streams**: Declarative processing pipelines for transforming and joining data
- **Constraints**: Rules that define violations and assign penalty scores
- **Session**: The runtime environment where facts are inserted and constraints evaluated

## Basic Setup

### 1. Define Your Facts

First, create structs representing your domain data and implement `GreynetFact`:

```rust
use greynet::prelude::*;
use greynet::greynet_fact_for_struct;

#[derive(Debug, Clone, Hash, PartialEq)]
struct Person {
    id: u64,
    name: String,
    department: String,
}

#[derive(Debug, Clone, Hash, PartialEq)]
struct Task {
    id: u64,
    assigned_to: u64,
    priority: i32,
    completed: bool,
}

// Implement GreynetFact for your structs
greynet_fact_for_struct!(Person);
greynet_fact_for_struct!(Task);
```

### 2. Create a Constraint Builder

```rust
use greynet::prelude::*;

// Choose a score type - SimpleScore for basic use cases
let mut builder = builder::<SimpleScore>();
```

### 3. Define Constraints

Use the fluent API to define constraints:

```rust
// Constraint: Each person should have at most 3 tasks
builder
    .add_constraint("max_tasks_per_person", 1.0)
    .for_each::<Task>()
    .group_by(|task: &Task| task.assigned_to, Collectors::count())
    .filter(|tuple| {
        // Extract the count from the grouped result
        extract_fact::<usize>(tuple, 1).map_or(false, |count| *count > 3)
    })
    .penalize(|_| SimpleScore::simple(1.0));

// Constraint: High priority tasks should be completed
builder
    .add_constraint("high_priority_completion", 2.0)
    .for_each::<Task>()
    .filter(|task: &Task| task.priority >= 8 && !task.completed)
    .penalize(|_| SimpleScore::simple(5.0));
```

### 4. Build Session and Insert Facts

```rust
let mut session = builder.build()?;

// Insert some facts
session.insert(Person {
    id: 1,
    name: "Alice".to_string(),
    department: "Engineering".to_string(),
})?;

session.insert(Task {
    id: 1,
    assigned_to: 1,
    priority: 9,
    completed: false,
})?;

session.insert(Task {
    id: 2,
    assigned_to: 1,
    priority: 7,
    completed: true,
})?;

// Process all insertions
session.flush()?;
```

### 5. Evaluate Constraints

```rust
// Get the total score
let score = session.get_score()?;
println!("Total violation score: {:?}", score);

// Get detailed constraint violations
let violations = session.get_constraint_matches()?;
for (constraint_name, violating_tuples) in violations {
    println!("Constraint '{}' violated by {} tuples", 
             constraint_name, violating_tuples.len());
}
```

## Advanced Patterns

### Joins Between Fact Types

```rust
// Join people with their tasks
builder
    .add_constraint("department_task_distribution", 1.0)
    .for_each::<Person>()
    .join_on::<Person, Task, _, _, u64>(
        builder.for_each::<Task>(),
        |person| person.id,           // Left key function
        |task| task.assigned_to       // Right key function
    )
    .filter(|tuple| {
        // Access joined facts by index
        let person = extract_fact::<Person>(tuple, 0).unwrap();
        let task = extract_fact::<Task>(tuple, 1).unwrap();
        person.department == "Sales" && task.priority < 5
    })
    .penalize(|_| SimpleScore::simple(2.0));
```

### Conditional Existence Checks

```rust
// Ensure every person has at least one task
builder
    .add_constraint("everyone_has_tasks", 3.0)
    .for_each::<Person>()
    .if_not_exists::<Person, Task, _, _, u64>(
        builder.for_each::<Task>(),
        |person| person.id,
        |task| task.assigned_to
    )
    .penalize(|_| SimpleScore::simple(10.0));
```

### Aggregations and Grouping

```rust
use greynet::collectors;

// Group tasks by person and calculate average priority
builder
    .add_constraint("balanced_workload", 1.5)
    .for_each::<Task>()
    .group_by(
        |task: &Task| task.assigned_to,
        collectors::avg(|tuple| {
            extract_fact::<Task>(tuple, 0)
                .map(|task| task.priority as f64)
                .unwrap_or(0.0)
        })
    )
    .filter(|tuple| {
        // Check if average priority is too high
        extract_fact::<f64>(tuple, 1).map_or(false, |avg| *avg > 8.0)
    })
    .penalize(|_| SimpleScore::simple(3.0));
```

## Score Types

Choose the appropriate score type for your use case:

### SimpleScore
```rust
let builder = builder::<SimpleScore>();
// Penalties are simple numeric values
.penalize(|_| SimpleScore::simple(1.0))
```

### HardSoftScore
```rust
let builder = builder::<HardSoftScore>();
// Hard constraints (must be satisfied)
.penalize(|_| HardSoftScore::hard(1.0))
// Soft constraints (preferences)
.penalize(|_| HardSoftScore::soft(2.0))
```

### HardMediumSoftScore
```rust
let builder = builder::<HardMediumSoftScore>();
.penalize(|_| HardMediumSoftScore::hard(1.0))
.penalize(|_| HardMediumSoftScore::medium(1.0))
.penalize(|_| HardMediumSoftScore::soft(1.0))
```

## Error Handling

Greynet uses a comprehensive error system:

```rust
match session.insert(duplicate_person) {
    Ok(()) => println!("Inserted successfully"),
    Err(GreynetError::DuplicateFact(id)) => {
        println!("Fact {} already exists", id);
    }
    Err(e) => println!("Error: {}", e),
}
```

## Performance Tips

### Resource Limits
```rust
let limits = ResourceLimits::conservative(); // or aggressive()
let builder = builder_with_limits::<SimpleScore>(limits);
```

### Bulk Operations
```rust
// Insert many facts at once
let people = vec![person1, person2, person3];
session.insert_bulk(people)?;

// Batch operations are more efficient
session.insert_batch([task1, task2, task3])?;
```

### Memory Management
```rust
// Clean up periodically in long-running applications
session.cleanup_dying_tuples()?;

// Check resource usage
let stats = session.get_statistics();
println!("Memory usage: {}MB", stats.memory_usage_mb);
```

## Complete Example

Here's a complete working example:

```rust
use greynet::prelude::*;
use greynet::greynet_fact_for_struct;

#[derive(Debug, Clone, Hash, PartialEq)]
struct Employee {
    id: u64,
    name: String,
    skill_level: i32,
}

#[derive(Debug, Clone, Hash, PartialEq)]
struct Project {
    id: u64,
    assigned_employee: u64,
    difficulty: i32,
    deadline_days: i32,
}

greynet_fact_for_struct!(Employee);
greynet_fact_for_struct!(Project);

fn main() -> Result<()> {
    // Build the constraint system
    let mut builder = builder::<SimpleScore>();
    
    // Constraint: Don't assign difficult projects to junior employees
    builder
        .add_constraint("skill_match", 2.0)
        .for_each::<Employee>()
        .join_on::<Employee, Project, _, _, u64>(
            builder.for_each::<Project>(),
            |emp| emp.id,
            |proj| proj.assigned_employee
        )
        .filter(|tuple| {
            let employee = extract_fact::<Employee>(tuple, 0).unwrap();
            let project = extract_fact::<Project>(tuple, 1).unwrap();
            employee.skill_level < 5 && project.difficulty > 7
        })
        .penalize(|tuple| {
            let employee = extract_fact::<Employee>(tuple, 0).unwrap();
            let project = extract_fact::<Project>(tuple, 1).unwrap();
            let gap = project.difficulty - employee.skill_level;
            SimpleScore::simple(gap as f64)
        });

    // Build the session
    let mut session = builder.build()?;
    
    // Insert facts
    session.insert(Employee {
        id: 1,
        name: "Junior Dev".to_string(),
        skill_level: 3,
    })?;
    
    session.insert(Project {
        id: 1,
        assigned_employee: 1,
        difficulty: 9,
        deadline_days: 7,
    })?;
    
    // Evaluate
    session.flush()?;
    let score = session.get_score()?;
    
    println!("Constraint violation score: {:?}", score);
    
    // Get specific violations
    let violations = session.get_constraint_matches()?;
    for (constraint, tuples) in violations {
        println!("Violated constraint: {}, {} violations", 
                 constraint, tuples.len());
    }
    
    Ok(())
}
```

## Next Steps

- Explore complex join patterns with multiple fact types
- Use different collector types for various aggregations
- Implement custom scoring functions for domain-specific penalties
- Tune performance with resource limits and bulk operations
- Add constraint analysis for debugging violated rules

For more advanced features, check the full API documentation and examples in the crate.
