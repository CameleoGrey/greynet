# Greynet Constraint Satisfaction Engine
## Comprehensive User Manual

### Table of Contents

1. [Overview & Architecture](#overview--architecture)
2. [Core Concepts](#core-concepts)
3. [Getting Started](#getting-started)
4. [Score Types & Constraint Definition](#score-types--constraint-definition)
5. [Stream Processing & Operations](#stream-processing--operations)
6. [Advanced Features](#advanced-features)
7. [Performance Optimization](#performance-optimization)
8. [Resource Management](#resource-management)
9. [API Reference](#api-reference)
10. [Examples & Use Cases](#examples--use-cases)

---

## Overview & Architecture

Greynet is a high-performance constraint satisfaction engine built in Rust. It's designed for applications requiring real-time constraint checking, optimization problems, and rule-based systems.

### Key Design Principles

- **Zero-Copy Performance**: Optimized for minimal allocations in hot paths
- **Type Safety**: Leverages Rust's type system for compile-time guarantees
- **Memory Safety**: Uses SlotMap-based arenas to prevent use-after-free errors
- **Fluent API**: Intuitive stream-based constraint definition
- **Resource Management**: Built-in limits and monitoring for production use

### Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Facts         │───▶│   Tuples        │───▶│   Streams       │
│ (Data Elements) │    │ (Fact Groups)   │    │ (Processing)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Session       │◀───│   Constraints   │◀───│   Nodes         │
│ (Execution)     │    │ (Rules/Scores)  │    │ (Network)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## Core Concepts

### Facts

Facts are the basic data elements in Greynet. Any type that implements the `GreynetFact` trait can be used as a fact.

```rust
use greynet::prelude::*;

#[derive(Debug, Clone, Hash, PartialEq)]
struct Person {
    id: i64,
    name: String,
    age: u32,
    department: String,
}

// Auto-implement GreynetFact for structs
greynet_fact_for_struct!(Person);

// Primitives already implement GreynetFact
let name: String = "Alice".to_string();
let age: u32 = 30;
```

### Tuples

Tuples are collections of 1-5 facts that flow through the constraint network. They come in different arities:

- `UniTuple` - 1 fact
- `BiTuple` - 2 facts  
- `TriTuple` - 3 facts
- `QuadTuple` - 4 facts
- `PentaTuple` - 5 facts

### Streams

Streams represent processing pipelines that transform, filter, join, and evaluate tuples. They use a fluent API for building complex constraint logic.

### Constraints

Constraints are rules that evaluate tuples and assign penalty scores. They define what constitutes a violation in your domain.

### Sessions

Sessions are execution contexts that manage facts, process constraints, and calculate total scores.

---

## Getting Started

### Basic Setup

```rust
use greynet::prelude::*;

// Define your fact types
#[derive(Debug, Clone, Hash, PartialEq)]
struct Employee {
    id: i64,
    name: String,
    department: String,
    salary: f64,
}

greynet_fact_for_struct!(Employee);

#[derive(Debug, Clone, Hash, PartialEq)]  
struct Department {
    name: String,
    budget: f64,
    max_employees: usize,
}

greynet_fact_for_struct!(Department);
```

### Creating a Session

```rust
// Create a constraint builder
let mut builder = builder::<SimpleScore>();

// Add constraints (we'll define these next)
builder.add_constraint("salary_limit", 1.0)
    .for_each::<Employee>()
    .filter(|emp: &Employee| emp.salary > 100000.0)
    .penalize(|_| SimpleScore::simple(1.0));

// Build the session
let mut session = builder.build()?;
```

### Adding Facts and Evaluating

```rust
// Add some facts
let employee = Employee {
    id: 1,
    name: "Alice".to_string(),
    department: "Engineering".to_string(), 
    salary: 120000.0,
};

session.insert(employee)?;
session.flush()?; // Process all pending operations

// Get the current score
let score = session.get_score()?;
println!("Total penalty score: {:?}", score);
```

---

## Score Types & Constraint Definition

Greynet supports three built-in score types, each suitable for different optimization scenarios:

### SimpleScore

A single numeric penalty value, suitable for simple optimization problems.

```rust
use greynet::prelude::*;

let mut builder = builder::<SimpleScore>();

builder.add_constraint("high_salary", 1.0)
    .for_each::<Employee>()
    .filter(|emp: &Employee| emp.salary > 100000.0)
    .penalize(|tuple| {
        let emp = extract_fact::<Employee>(tuple, 0).unwrap();
        SimpleScore::simple(emp.salary - 100000.0)
    });
```

### HardSoftScore

Separates violations into hard constraints (must be satisfied) and soft constraints (preferences).

```rust
let mut builder = builder::<HardSoftScore>();

// Hard constraint: No employee can earn more than budget allows
builder.add_constraint("budget_limit", 1.0)
    .for_each::<Employee>()
    .join_on::<Employee, Department, _, _, String>(
        dept_stream,
        |emp| emp.department.clone(),
        |dept| dept.name.clone()
    )
    .filter_tuple(|tuple| {
        let emp = extract_fact::<Employee>(tuple, 0).unwrap();
        let dept = extract_fact::<Department>(tuple, 1).unwrap();
        emp.salary > dept.budget / dept.max_employees as f64
    })
    .penalize(|_| HardSoftScore::hard(1.0));

// Soft constraint: Prefer lower salaries
builder.add_constraint("minimize_salaries", 0.1)
    .for_each::<Employee>()
    .penalize(|tuple| {
        let emp = extract_fact::<Employee>(tuple, 0).unwrap();
        HardSoftScore::soft(emp.salary / 1000.0)
    });
```

### HardMediumSoftScore

Three-tier scoring for complex optimization with multiple priority levels.

```rust
let mut builder = builder::<HardMediumSoftScore>();

// Hard: Legal requirements
builder.add_constraint("legal_compliance", 1.0)
    .for_each::<Employee>()
    .filter(|emp: &Employee| emp.salary < 15000.0) // Below minimum wage
    .penalize(|_| HardMediumSoftScore::hard(1.0));

// Medium: Business rules  
builder.add_constraint("department_budget", 1.0)
    .for_each::<Employee>()
    // ... budget logic
    .penalize(|_| HardMediumSoftScore::medium(1.0));

// Soft: Preferences
builder.add_constraint("salary_fairness", 1.0)
    .for_each::<Employee>()
    // ... fairness logic
    .penalize(|_| HardMediumSoftScore::soft(1.0));
```

---

## Stream Processing & Operations

### Filtering

Filter tuples based on predicates:

```rust
// Filter by single fact type
stream.filter(|emp: &Employee| emp.age > 30)

// Filter by tuple (when you need access to multiple facts)
stream.filter_tuple(|tuple| {
    let emp = extract_fact::<Employee>(tuple, 0).unwrap();
    let dept = extract_fact::<Department>(tuple, 1).unwrap();
    emp.department == dept.name
})
```

### Joining

Combine streams based on key relationships:

```rust
// Basic equality join
let employee_stream = builder.for_each::<Employee>();
let department_stream = builder.for_each::<Department>();

let joined = employee_stream.join_on::<Employee, Department, _, _, String>(
    department_stream,
    |emp| emp.department.clone(),    // Left key function
    |dept| dept.name.clone()         // Right key function
);

// Advanced joins with custom comparators
let salary_comparison = employee_stream.join_on_with_comparator::<Employee, Employee, _, _, f64>(
    other_employee_stream,
    JoinerType::GreaterThan,
    |emp| emp.salary,
    |emp| emp.salary
);
```

### Conditional Operations

Check for existence or non-existence of related facts:

```rust
// Only process employees if their department exists
let valid_employees = employee_stream.if_exists::<Employee, Department, _, _, String>(
    department_stream,
    |emp| emp.department.clone(),
    |dept| dept.name.clone()
);

// Process employees whose departments are missing
let orphaned_employees = employee_stream.if_not_exists::<Employee, Department, _, _, String>(
    department_stream,
    |emp| emp.department.clone(),
    |dept| dept.name.clone()
);
```

### Grouping & Aggregation

Group tuples and apply aggregation functions:

```rust
use greynet::collectors;

// Group employees by department and count them
let dept_counts = employee_stream.group_by::<Employee, _, String>(
    |emp| emp.department.clone(),
    collectors::count()
);

// Group and calculate average salary
let avg_salaries = employee_stream.group_by::<Employee, _, String>(
    |emp| emp.department.clone(),
    collectors::avg(|tuple| {
        let emp = extract_fact::<Employee>(tuple, 0).unwrap();
        emp.salary
    })
);

// Global aggregation
let total_salary = employee_stream.aggregate(
    collectors::sum(|tuple| {
        let emp = extract_fact::<Employee>(tuple, 0).unwrap();
        emp.salary
    })
);
```

### Transformation Operations

Transform tuples into new forms:

```rust
// Map: Transform each tuple
let salary_stream = employee_stream.map(|emp: &Employee| {
    Rc::new(emp.salary) as Rc<dyn GreynetFact>
});

// FlatMap: Transform each tuple into multiple facts
let skills_stream = employee_stream.flat_map(|emp: &Employee| {
    emp.skills.iter()
        .map(|skill| Rc::new(skill.clone()) as Rc<dyn GreynetFact>)
        .collect()
});

// Map to pair: Create BiTuples
let emp_dept_pairs = employee_stream.map_to_pair(|emp: &Employee| {
    (
        Rc::new(emp.name.clone()) as Rc<dyn GreynetFact>,
        Rc::new(emp.department.clone()) as Rc<dyn GreynetFact>
    )
});
```

### Set Operations

```rust
// Union: Combine multiple streams
let all_people = employee_stream.union(contractor_stream);

// Distinct: Remove duplicates
let unique_departments = department_stream.distinct();
```

---

## Advanced Features

### Complex Multi-Stream Constraints

```rust
// Complex constraint involving multiple joins and conditions
builder.add_constraint("department_salary_fairness", 2.0)
    .for_each::<Employee>()
    .join_on::<Employee, Department, _, _, String>(
        department_stream.clone(),
        |emp| emp.department.clone(),
        |dept| dept.name.clone()
    )
    .join_on_second::<Department, Employee, _, _, String>(
        other_employee_stream,
        |dept| dept.name.clone(),
        |other_emp| other_emp.department.clone()
    )
    .filter_tuple(|tuple| {
        let emp1 = extract_fact::<Employee>(tuple, 0).unwrap();
        let dept = extract_fact::<Department>(tuple, 1).unwrap();
        let emp2 = extract_fact::<Employee>(tuple, 2).unwrap();
        
        // Check if salary difference is too high within same department
        (emp1.salary - emp2.salary).abs() > dept.budget * 0.1
    })
    .penalize(|tuple| {
        let emp1 = extract_fact::<Employee>(tuple, 0).unwrap();
        let emp2 = extract_fact::<Employee>(tuple, 2).unwrap();
        HardSoftScore::soft((emp1.salary - emp2.salary).abs() / 1000.0)
    });
```

### Custom Collectors

```rust
// Create custom aggregation logic
let custom_collector = || -> Box<dyn BaseCollector> {
    Box::new(CustomSalaryAnalyzer::new())
};

let salary_analysis = employee_stream.group_by::<Employee, _, String>(
    |emp| emp.department.clone(),
    Box::new(custom_collector)
);
```

### Resource Limits

```rust
// Configure resource limits for production use
let limits = ResourceLimits {
    max_tuples: 1_000_000,
    max_operations_per_batch: 100_000,
    max_memory_mb: 512,
    max_cascade_depth: 1000,
    max_facts_per_type: 500_000,
};

let mut builder = builder_with_limits::<HardSoftScore>(limits);
```

---

## Performance Optimization

### Zero-Copy Operations

Greynet is optimized for zero-copy operations in critical paths:

```rust
// Efficient fact extraction without allocations
let fast_filter = stream.filter_tuple(|tuple| {
    // Direct access without cloning
    if let Some(emp) = extract_fact::<Employee>(tuple, 0) {
        emp.salary > 50000.0
    } else {
        false
    }
});
```

### Batch Operations

```rust
// Bulk insert for better performance
let employees = vec![emp1, emp2, emp3, /* ... */];
session.insert_bulk(employees)?;

// Bulk constraint weight updates
let weight_updates = hashmap! {
    "salary_limit".to_string() => 2.0,
    "department_balance".to_string() => 1.5,
};
session.update_constraint_weights(weight_updates)?;
```

### Memory Management

```rust
// Explicit cleanup during long-running sessions
session.cleanup_dying_tuples()?;

// Check memory pressure
session.check_resource_limits()?;

// Pre-allocate for known workloads
session.tuples.reserve_capacity(expected_tuple_count);
```

---

## Resource Management

### Memory Arenas

Greynet uses SlotMap-based arenas for safe, efficient memory management:

```rust
// Arena automatically handles:
// - Memory recycling
// - Generational indices (prevent use-after-free)
// - Compact storage
// - Fast allocation/deallocation

// Get arena statistics
let stats = session.tuples.stats();
println!("Live tuples: {}, Dead tuples: {}", 
         stats.live_tuples, stats.dead_tuples);
```

### Scheduler & Batch Processing

The scheduler processes operations in batches to ensure consistency:

```rust
// Operations are automatically batched and executed
session.insert(fact1)?;
session.insert(fact2)?;  
session.insert(fact3)?;
session.flush()?; // Process all at once

// Check scheduler state
let scheduler_stats = session.scheduler.stats();
println!("Pending operations: {}", scheduler_stats.pending_operations);
```

---

## API Reference

### ConstraintBuilder

```rust
impl<S: Score> ConstraintBuilder<S> {
    // Create builder with default limits
    pub fn new() -> Self;
    
    // Create builder with custom limits
    pub fn with_limits(limits: ResourceLimits) -> Self;
    
    // Start defining a constraint (preferred method)
    pub fn add_constraint(&mut self, id: &str, weight: f64) -> ConstraintStreamBuilder<S>;
    
    // Create a fact stream
    pub fn for_each<T: GreynetFact + 'static>(&self) -> Stream<Arity1, S>;
    
    // Build the final session
    pub fn build(self) -> Result<Session<S>>;
}
```

### Stream Operations

```rust
impl<S: Score> Stream<Arity1, S> {
    // Filtering
    pub fn filter<T, F>(self, predicate: F) -> Self;
    pub fn filter_tuple<F>(self, predicate: F) -> Self;
    
    // Joining
    pub fn join_on<T1, T2, F1, F2, K>(self, other: Stream<Arity1, S>, 
                                       left_key: F1, right_key: F2) -> Stream<Arity2, S>;
    
    // Conditional operations
    pub fn if_exists<T1, T2, F1, F2, K>(self, other: Stream<Arity1, S>, 
                                         left_key: F1, right_key: F2) -> Self;
    pub fn if_not_exists<T1, T2, F1, F2, K>(self, other: Stream<Arity1, S>, 
                                             left_key: F1, right_key: F2) -> Self;
    
    // Grouping
    pub fn group_by<T, F, K>(self, key_fn: F, 
                             collector: Box<dyn Fn() -> Box<dyn BaseCollector>>) -> Stream<Arity2, S>;
    
    // Transformation
    pub fn map<T, F>(self, mapper: F) -> Stream<Arity1, S>;
    pub fn flat_map<T, F>(self, mapper: F) -> Stream<Arity1, S>;
    
    // Set operations
    pub fn union(self, other: Stream<A, S>) -> Self;
    pub fn distinct(self) -> Self;
    
    // Terminal operation
    pub fn penalize(self, penalty_fn: impl Fn(&dyn ZeroCopyFacts) -> S + 'static) -> ConstraintRecipe<S>;
}
```

### Session Management

```rust
impl<S: Score> Session<S> {
    // Fact management
    pub fn insert<T: GreynetFact + 'static>(&mut self, fact: T) -> Result<()>;
    pub fn insert_batch<T: GreynetFact + 'static>(&mut self, facts: impl IntoIterator<Item = T>) -> Result<()>;
    pub fn retract<T: GreynetFact>(&mut self, fact: &T) -> Result<()>;
    pub fn clear(&mut self) -> Result<()>;
    
    // Processing
    pub fn flush(&mut self) -> Result<()>;
    pub fn get_score(&mut self) -> Result<S>;
    
    // Analysis
    pub fn get_constraint_matches(&mut self) -> Result<HashMap<String, Vec<AnyTuple>>>;
    pub fn get_statistics(&self) -> SessionStatistics;
    pub fn get_detailed_statistics(&self) -> SessionStatistics;
    
    // Constraint management
    pub fn update_constraint_weight(&mut self, id: &str, weight: f64) -> Result<()>;
    
    // Maintenance
    pub fn validate_consistency(&self) -> Result<()>;
    pub fn cleanup_dying_tuples(&mut self) -> Result<usize>;
}
```

---

## Examples & Use Cases

### Employee Scheduling

```rust
use greynet::prelude::*;

#[derive(Debug, Clone, Hash, PartialEq)]
struct Shift {
    employee_id: i64,
    day: String,
    start_hour: u8,
    duration: u8,
}

#[derive(Debug, Clone, Hash, PartialEq)]
struct Employee {
    id: i64,
    name: String,
    max_hours_per_week: u8,
}

greynet_fact_for_struct!(Shift);
greynet_fact_for_struct!(Employee);

let mut builder = builder::<HardSoftScore>();

// Hard constraint: No employee works more than their limit
builder.add_constraint("max_hours", 1.0)
    .for_each::<Shift>()
    .group_by::<Shift, _, i64>(
        |shift| shift.employee_id,
        collectors::sum(|tuple| {
            let shift = extract_fact::<Shift>(tuple, 0).unwrap();
            shift.duration as f64
        })
    )
    .join_on_first::<u64, Employee, _, _, i64>(
        builder.for_each::<Employee>(),
        |total_hours| *total_hours as i64,
        |emp| emp.id
    )
    .filter_tuple(|tuple| {
        let total_hours = extract_fact::<f64>(tuple, 1).unwrap();
        let employee = extract_fact::<Employee>(tuple, 2).unwrap();
        *total_hours > employee.max_hours_per_week as f64
    })
    .penalize(|tuple| {
        let total_hours = extract_fact::<f64>(tuple, 1).unwrap();
        let employee = extract_fact::<Employee>(tuple, 2).unwrap();
        let overflow = total_hours - employee.max_hours_per_week as f64;
        HardSoftScore::hard(overflow)
    });

// Soft constraint: Prefer even distribution of hours
builder.add_constraint("even_distribution", 0.5)
    .for_each::<Employee>()
    .join_on::<Employee, Shift, _, _, i64>(
        builder.for_each::<Shift>(),
        |emp| emp.id,
        |shift| shift.employee_id
    )
    .group_by_tuple(
        |tuple| {
            let shift = extract_fact::<Shift>(tuple, 1).unwrap();
            shift.day.clone()
        },
        collectors::count()
    )
    .penalize(|tuple| {
        let count = extract_fact::<usize>(tuple, 1).unwrap();
        let deviation = (*count as f64 - 5.0).abs(); // Prefer 5 people per day
        HardSoftScore::soft(deviation)
    });

let mut session = builder.build()?;
```

### Supply Chain Optimization

```rust
#[derive(Debug, Clone, Hash, PartialEq)]
struct Supplier {
    id: i64,
    location: String,
    capacity: u32,
    cost_per_unit: f64,
}

#[derive(Debug, Clone, Hash, PartialEq)]
struct Demand {
    location: String,
    quantity: u32,
    deadline: String,
}

#[derive(Debug, Clone, Hash, PartialEq)]
struct Assignment {
    supplier_id: i64,
    demand_location: String,
    quantity: u32,
}

greynet_fact_for_struct!(Supplier);
greynet_fact_for_struct!(Demand);
greynet_fact_for_struct!(Assignment);

let mut builder = builder::<HardMediumSoftScore>();

// Hard: Don't exceed supplier capacity
builder.add_constraint("supplier_capacity", 1.0)
    .for_each::<Assignment>()
    .group_by::<Assignment, _, i64>(
        |assignment| assignment.supplier_id,
        collectors::sum(|tuple| {
            let assignment = extract_fact::<Assignment>(tuple, 0).unwrap();
            assignment.quantity as f64
        })
    )
    .join_on_first::<f64, Supplier, _, _, i64>(
        builder.for_each::<Supplier>(),
        |total_assigned| *total_assigned as i64,
        |supplier| supplier.id
    )
    .filter_tuple(|tuple| {
        let total_assigned = extract_fact::<f64>(tuple, 1).unwrap();
        let supplier = extract_fact::<Supplier>(tuple, 2).unwrap();
        *total_assigned > supplier.capacity as f64
    })
    .penalize(|tuple| {
        let total_assigned = extract_fact::<f64>(tuple, 1).unwrap();
        let supplier = extract_fact::<Supplier>(tuple, 2).unwrap();
        let excess = total_assigned - supplier.capacity as f64;
        HardMediumSoftScore::hard(excess)
    });

// Medium: Meet all demand
builder.add_constraint("meet_demand", 1.0)
    .for_each::<Demand>()
    .if_not_exists::<Demand, Assignment, _, _, String>(
        builder.for_each::<Assignment>(),
        |demand| demand.location.clone(),
        |assignment| assignment.demand_location.clone()
    )
    .penalize(|tuple| {
        let demand = extract_fact::<Demand>(tuple, 0).unwrap();
        HardMediumSoftScore::medium(demand.quantity as f64)
    });

// Soft: Minimize cost
builder.add_constraint("minimize_cost", 1.0)
    .for_each::<Assignment>()
    .join_on::<Assignment, Supplier, _, _, i64>(
        builder.for_each::<Supplier>(),
        |assignment| assignment.supplier_id,
        |supplier| supplier.id
    )
    .penalize(|tuple| {
        let assignment = extract_fact::<Assignment>(tuple, 0).unwrap();
        let supplier = extract_fact::<Supplier>(tuple, 1).unwrap();
        let cost = assignment.quantity as f64 * supplier.cost_per_unit;
        HardMediumSoftScore::soft(cost / 1000.0)
    });
```

### Real-time Constraint Monitoring

```rust
// Set up session for real-time updates
let mut session = builder.build()?;

loop {
    // Add new facts as they arrive
    for new_fact in incoming_facts {
        session.insert(new_fact)?;
    }
    
    // Remove outdated facts
    for old_fact in expired_facts {
        session.retract(&old_fact)?;
    }
    
    // Process all changes
    session.flush()?;
    
    // Check current score
    let current_score = session.get_score()?;
    
    // Get violations if score indicates problems
    if current_score.get_priority_score() > 0.0 {
        let violations = session.get_constraint_matches()?;
        handle_violations(violations);
    }
    
    // Periodic maintenance
    if should_cleanup() {
        session.cleanup_dying_tuples()?;
    }
}
```

### Diagnostic and Analysis

```rust
// Get detailed analysis of constraint violations
let violation_report = session.get_all_fact_constraint_matches()?;

for (fact_id, matches) in violation_report.matches_by_fact {
    println!("Fact {} is involved in {} violations:", fact_id, matches.len());
    
    for constraint_match in matches {
        println!("  - Constraint: {}", constraint_match.constraint_id);
        println!("    Role: {:?}", constraint_match.fact_role);
        println!("    Score: {:?}", constraint_match.violation_score);
        println!("    Violating tuple: {:?}", constraint_match.violating_tuple);
    }
}

// Get system statistics
let stats = session.get_detailed_statistics();
println!("Network Statistics:");
println!("  Total nodes: {}", stats.total_nodes);
println!("  Total tuples: {}", stats.total_tuples);
println!("  Memory usage: {}MB", stats.memory_usage_mb);

if let Some(ref node_breakdown) = stats.node_type_breakdown {
    println!("  Node types:");
    for (node_type, count) in node_breakdown {
        println!("    {}: {}", node_type, count);
    }
}
```

---

## Error Handling

Greynet provides comprehensive error handling:

```rust
use greynet::{GreynetError, Result};

match session.insert(fact) {
    Ok(()) => println!("Fact inserted successfully"),
    Err(GreynetError::DuplicateFact(id)) => {
        println!("Fact with ID {} already exists", id);
    },
    Err(GreynetError::ResourceLimit { limit_type, details }) => {
        println!("Resource limit exceeded - {}: {}", limit_type, details);
    },
    Err(GreynetError::UnregisteredType { type_name }) => {
        println!("No stream registered for type: {}", type_name);
    },
    Err(e) => println!("Other error: {}", e),
}
```

---

## Best Practices

### 1. Constraint Design

- **Start simple**: Begin with basic constraints and add complexity gradually
- **Use appropriate score types**: SimpleScore for simple cases, HardSoftScore for feasibility problems
- **Weight constraints carefully**: Hard constraints should have much higher weights than soft ones

### 2. Performance

- **Batch operations**: Use `insert_batch` and `insert_bulk` for multiple facts
- **Resource limits**: Set appropriate limits for your use case
- **Regular cleanup**: Call `cleanup_dying_tuples()` in long-running applications
- **Monitor memory**: Use `get_statistics()` to track resource usage

### 3. Debugging

- **Use detailed statistics**: `get_detailed_statistics()` provides comprehensive analysis
- **Validate consistency**: Call `validate_consistency()` during development
- **Analyze violations**: Use `get_constraint_matches()` to understand why scores are high

### 4. Production Deployment

```rust
// Production-ready configuration
let limits = ResourceLimits::conservative(); // or aggressive() for high-performance systems

let mut builder = builder_with_limits::<HardSoftScore>(limits);

// Enable detailed monitoring in development
#[cfg(feature = "detailed-stats")]
{
    let stream_stats = session.get_stream_processing_stats();
    let perf_metrics = session.get_performance_metrics();
}

// Regular health checks
session.check_resource_limits()?;
session.validate_consistency()?;
```

---

## Conclusion

Greynet provides a powerful, type-safe, and high-performance platform for constraint satisfaction problems. Its fluent API makes complex constraint logic readable and maintainable.

The engine is particularly well-suited for:

- **Real-time optimization**: Resource allocation, scheduling, routing
- **Rule-based systems**: Business logic validation, compliance checking  
- **Planning problems**: Project management, supply chain optimization
- **Configuration management**: System tuning, parameter optimization

For more examples and advanced usage patterns, refer to the test suite and example applications in the Greynet repository.
