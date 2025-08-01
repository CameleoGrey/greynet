# Greynet Engine: The Comprehensive Manual

## 1. Introduction

Welcome to the Greynet Engine, a high-performance, declarative constraint satisfaction and reasoning framework for Rust. Greynet provides a powerful and expressive API to define complex rules, perform efficient data-driven computations, and solve intricate optimization problems.

At its core, Greynet builds a network of nodes from your declarative constraints. When you insert, update, or retract data "facts," they propagate through this network, which efficiently finds matches, performs aggregations, and calculates a final score representing the "health" or "quality" of the system according to your rules.

This manual provides a comprehensive guide to understanding and utilizing the Greynet engine, from its fundamental concepts to advanced usage patterns.

### Key Features:

-   **Declarative & Fluent API:** Define complex rules and constraints in a readable, chainable, and expressive way.
    
-   **High Performance:** Built with a focus on speed, featuring zero-copy operations, efficient indexing, and memory-safe arenas for managing data.
    
-   **Flexible Scoring:** Supports multiple scoring strategies (e.g., `HardSoftScore`, `HardMediumSoftScore`) to handle different levels of constraint priority.
    
-   **Powerful Data Operations:** Includes a rich set of operations like `join`, `filter`, `group_by`, `map`, `distinct`, and conditional existence checks (`if_exists`/`if_not_exists`).
    
-   **Extensible Aggregation:** Use built-in collectors (`count`, `sum`, `avg`, `to_list`, etc.) or create your own to perform complex data aggregations.
    
-   **Resource Management:** Configurable limits to prevent unbounded memory usage and ensure system stability.
    
-   **Introspection & Analysis:** Tools to analyze constraint violations, inspect the network, and gather performance statistics.
    

## 2. Core Concepts

Understanding these core concepts is key to using Greynet effectively.

**Concept**

**Description**

**Fact**

A single piece of data in the system. Any struct or type that implements the `GreynetFact` trait can be a fact.

**Tuple**

An ordered collection of one or more facts. Tuples are the primary data structure that flows through the network.

**Stream**

A fluent, chainable representation of a sequence of operations on tuples (e.g., filtering, joining).

**Constraint**

A rule that, when violated, applies a penalty to the system's score. You define constraints using streams.

**Node**

An operation in the compiled network (e.g., a `FilterNode`, `JoinNode`). Streams are compiled into a network of connected nodes.

**Session**

The main entry point for interacting with the Greynet engine. It holds the network, manages facts, and calculates the score.

**Score**

A value that represents the overall state of the system. The goal is typically to minimize the penalty score.

**Collector**

An aggregation mechanism used within `group_by` or `aggregate` operations to compute results like counts, sums, or lists from groups of tuples.

**ConstraintBuilder**

The primary tool for defining constraints and building the `Session`.

### The Data Flow

1.  **Fact Insertion:** You insert facts (e.g., `Person`, `Task` objects) into the `Session`.
    
2.  **Network Entry:** Each fact enters the network through a `FromNode` specific to its type, wrapped in a `UniTuple` (a tuple of arity 1).
    
3.  **Propagation & Transformation:** The tuple flows to connected nodes.
    
    -   A `FilterNode` will only pass the tuple if it meets a condition.
        
    -   A `JoinNode` will try to combine the tuple with tuples from another stream if their keys match. This creates a new, larger tuple (e.g., a `BiTuple`).
        
    -   A `GroupNode` will group tuples by a key and use a `Collector` to produce an aggregate result tuple.
        
    -   Other nodes like `MapNode` and `FlatMapNode` transform tuples.
        
4.  **Scoring:** If a tuple reaches a `ScoringNode` at the end of a constraint stream, it means a constraint has been violated. The node calculates a penalty score.
    
5.  **Final Score:** The `Session` sums the scores from all `ScoringNode`s to produce the final system score.
    

## 3. Getting Started: A Simple Example

Let's model a simple rule: "A person under 18 should not be assigned a task with a 'High' priority."

### Step 1: Define Your Facts

First, define the data structures that will act as your facts. They must derive `Clone`, `Debug`, and `Hash`, and you must implement the `GreynetFact` trait for them. The `greynet_fact_for_struct!` macro makes this trivial.

```
use greynet::prelude::*;
use greynet::greynet_fact_for_struct;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Person {
    pub id: i64,
    pub age: u32,
}
greynet_fact_for_struct!(Person);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Task {
    pub person_id: i64,
    pub priority: String,
}
greynet_fact_for_struct!(Task);

```

### Step 2: Build the Constraint

Use the `ConstraintBuilder` to define your rules. The API is designed to be fluent and readable.

```
use greynet::prelude::*;

fn main() -> Result<()> {
    // Use a score with "hard" and "soft" levels. Hard scores should always be zero for a valid solution.
    let mut builder = builder::<HardSoftScore>();

    builder.add_constraint("underage_high_priority_task", 1.0)
        // Start a stream for each `Person` fact.
        .for_each::<Person>()
        // Keep only people under 18.
        .filter(|p| p.age < 18)
        // Join with `Task` facts where Person.id matches Task.person_id.
        .join_on::<Person, Task, _, _, i64>(
            builder.for_each::<Task>(),
            |person| person.id,
            |task| task.person_id
        )
        // The result of the join is a BiTuple(Person, Task).
        // Now, filter these pairs to find those with high-priority tasks.
        .filter_tuple(|tuple| {
            // extract_fact is a safe way to get a typed fact from a tuple
            let task = extract_fact::<Task>(tuple, 1).unwrap();
            task.priority == "High"
        })
        // Any tuple reaching this point is a violation. Apply a hard penalty.
        .penalize(|_tuple| HardSoftScore::hard(1.0));

    // Build the session.
    let mut session = builder.build()?;

    // -- More code to insert facts will go here --

    Ok(())
}

```

### Step 3: Insert Data and Get the Score

Now, let's add some data to the session and see the result.

```
# use greynet::prelude::*;
# use greynet::greynet_fact_for_struct;
# #[derive(Clone, Debug, Hash, PartialEq, Eq)]
# pub struct Person { pub id: i64, pub age: u32, }
# greynet_fact_for_struct!(Person);
# #[derive(Clone, Debug, Hash, PartialEq, Eq)]
# pub struct Task { pub person_id: i64, pub priority: String, }
# greynet_fact_for_struct!(Task);
# fn main() -> Result<()> {
# let mut builder = builder::<HardSoftScore>();
# builder.add_constraint("underage_high_priority_task", 1.0)
#     .for_each::<Person>()
#     .filter(|p| p.age < 18)
#     .join_on::<Person, Task, _, _, i64>(
#         builder.for_each::<Task>(),
#         |person| person.id,
#         |task| task.person_id
#     )
#     .filter_tuple(|tuple| {
#         let task = extract_fact::<Task>(tuple, 1).unwrap();
#         task.priority == "High"
#     })
#     .penalize(|_tuple| HardSoftScore::hard(1.0));
# let mut session = builder.build()?;
// --- Data Insertion ---

// This person is 17.
let p1 = Person { id: 1, age: 17 };
// This person is 25.
let p2 = Person { id: 2, age: 25 };

// A high-priority task for the 17-year-old (VIOLATION)
let t1 = Task { person_id: 1, priority: "High".to_string() };
// A low-priority task for the 17-year-old (OK)
let t2 = Task { person_id: 1, priority: "Low".to_string() };
// A high-priority task for the 25-year-old (OK)
let t3 = Task { person_id: 2, priority: "High".to_string() };

session.insert(p1)?;
session.insert(p2)?;
session.insert(t1)?;
session.insert(t2)?;
session.insert(t3)?;

// Flush processes all pending operations and calculates the score.
let final_score = session.get_score()?;

// We expect one violation, so the hard score should be 1.0.
println!("Final Score: {:?}", final_score);
assert_eq!(final_score.hard_score, 1.0);
assert_eq!(final_score.soft_score, 0.0);

// You can also inspect the violations.
let matches = session.get_constraint_matches()?;
println!("Violations: {:#?}", matches);
# Ok(())
# }

```

## 4. The Stream API in Detail

The fluent `Stream` API is the heart of defining your logic.

### Creating a Stream

You always start a stream from a base set of facts.

```
// Start a stream of all `Person` facts.
builder.for_each::<Person>()

```

### Filtering

`filter()` and `filter_tuple()` are used to conditionally control the flow of tuples.

```
// Filter based on a single fact in a UniTuple
.filter(|person: &Person| person.age > 21)

// Filter based on the contents of a multi-fact tuple
.filter_tuple(|tuple: &dyn ZeroCopyFacts| {
    let person = extract_fact::<Person>(tuple, 0).unwrap();
    let location = extract_fact::<Location>(tuple, 1).unwrap();
    person.city == location.city
})

```

### Joining

Joins combine tuples from two different streams. The `join_on` method is the most common, performing an equality check on keys.

```
// stream_of_people: Stream<Arity1, S> containing Person
// stream_of_tasks: Stream<Arity1, S> containing Task

stream_of_people.join_on::<Person, Task, _, _, i64>(
    stream_of_tasks,
    |person| person.id,      // Left key extractor
    |task| task.person_id    // Right key extractor
)
// The output is a new stream of BiTuple(Person, Task)

```

For non-equality joins, use `join_on_with_comparator`:

```
// Join if the order's price is greater than the customer's credit limit.
stream_of_orders.join_on_with_comparator::<Order, Customer, _, _, f64>(
    stream_of_customers,
    JoinerType::GreaterThan,
    |order| order.price,
    |customer| customer.credit_limit
)

```

Available `JoinerType` options include: `Equal`, `NotEqual`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`.

### Conditional Existence (`if_exists` / `if_not_exists`)

These powerful operators allow a stream to proceed only if a matching fact exists (or doesn't exist) in another stream, without actually adding the other fact to the tuple.

```
// Penalize employees who have no assigned tasks.
builder.for_each::<Employee>()
    .if_not_exists::<Employee, Task, _, _, i64>(
        builder.for_each::<Task>(),
        |employee| employee.id,
        |task| task.employee_id
    )
    .penalize(...)

```

### Grouping and Aggregation (`group_by`)

`group_by` is one of Greynet's most powerful features. It groups incoming tuples by a key and then applies a `Collector` to each group to produce a single aggregate result.

The output of a `group_by` is always a `Stream<Arity2, S>` containing `BiTuple(GroupKey, CollectorResult)`.

```
use greynet::Collectors;

// For each department, count the number of employees.
builder.for_each::<Employee>()
    .group_by(
        // Key extractor: group by the department_id field.
        |employee: &Employee| employee.department_id,
        // Collector: use the built-in `count` collector.
        Collectors::count()
    )
    // The stream now contains BiTuple(department_id, count).
    .filter_tuple(|tuple| {
        let count = extract_fact::<usize>(tuple, 1).unwrap();
        // Find departments with fewer than 3 employees.
        *count < 3
    })
    .penalize(...)

```

#### Collectors

Greynet provides several built-in collectors via the `Collectors` factory.

-   `Collectors::count()`: Counts the number of items in the group.
    
-   `Collectors::sum(|t| ...)`: Sums a value extracted from each tuple.
    
-   `Collectors::avg(|t| ...)`: Averages a value extracted from each tuple.
    
-   `Collectors::to_list()`: Collects all tuples in the group into a `Vec<AnyTuple>`.
    
-   `Collectors::distinct()`: Collects unique tuples.
    
-   `Collectors::min(|t| ...)`: Finds the minimum value.
    
-   `Collectors::max(|t| ...)`: Finds the maximum value.
    

### Transforming Tuples (`map`, `flat_map`)

-   **`map`**: Transforms each incoming tuple into exactly one new tuple. This is useful for restructuring or extracting data.
    
-   **`flat_map`**: Transforms each incoming tuple into _zero or more_ new tuples. This is useful for "exploding" a tuple that contains a list.
    

```
// Example of `map`: Extract the name and age into a new BiTuple.
builder.for_each::<Person>()
    .map_to_pair(|p: &Person| {
        (Rc::new(p.name.clone()), Rc::new(p.age))
    })
    // Stream now contains BiTuple(String, u32)

// Example of `flat_map`: Create a separate `Item` fact for each item in an order.
builder.for_each::<Order>() // An Order contains a `Vec<Item>`
    .flat_map(|order: &Order| {
        order.items.iter()
            .map(|item| Rc::new(item.clone()) as Rc<dyn GreynetFact>)
            .collect()
    })
    // Stream now contains individual `Item` facts.

```

### Set Operations (`union`, `distinct`)

-   **`union`**: Merges two or more streams of the _same arity_ into a single stream.
    
-   **`distinct`**: Ensures that only unique tuples (based on the hash of their contents) are passed down the stream.
    

## 5. Advanced Topics

### Scores

Greynet supports different score structures. You choose one when creating the `ConstraintBuilder`.

-   **`SimpleScore`**: A single `f64` value.
    
-   **`HardSoftScore`**: Contains `hard_score` and `soft_score`. Hard scores have absolute priority; a solution is only considered feasible if the total hard score is 0.
    
-   **`HardMediumSoftScore`**: Adds a third level of priority.
    

When penalizing, you create an instance of your chosen score type:

```
.penalize(|_| HardSoftScore::hard(1.0))
.penalize(|_| HardSoftScore::soft(50.0))

```

### Resource Limits

To prevent runaway computations or memory usage, you can configure limits when creating the builder.

```
use greynet::ResourceLimits;

// Start with conservative limits.
let limits = ResourceLimits::conservative();
let mut builder = builder_with_limits::<SimpleScore>(limits);

```

### Session Management

-   **`session.flush()`**: It's crucial to call `flush()` after a batch of insertions or retractions. This tells the scheduler to process all pending operations and ensure the network state is consistent. `get_score()` and `get_constraint_matches()` call `flush()` internally.
    
-   **`session.retract(&fact)`**: To update a fact, you must first `retract()` the old version and then `insert()` the new version.
    
-   **`session.clear()`**: Removes all facts from the network.
    

### Analysis and Debugging

Greynet provides tools to understand what's happening inside the network.

```
// Get a map of constraint names to the list of tuples that violated them.
let violations = session.get_constraint_matches()?;
for (name, tuples) in violations {
    println!("Constraint '{}' was violated by {} tuples.", name, tuples.len());
}

// Use the analysis module for more detailed reports.
use greynet::analysis::ConstraintAnalysis;

let report = ConstraintAnalysis::analyze_violations(&mut session)?;
println!("Is solution feasible? {}", report.feasible);

let stats = ConstraintAnalysis::get_network_stats(&session);
println!("Network has {} total nodes.", stats.total_nodes);

```

## 6. Conclusion

The Greynet Engine is a versatile and powerful tool for solving complex rule-based problems in Rust. By mastering the core concepts of Facts, Streams, and Nodes, and by leveraging the expressive fluent API, you can build sophisticated reasoning systems that are both efficient and maintainable. This manual serves as a starting point; the best way to learn is to experiment with building your own constraints and observing how the network behaves.
