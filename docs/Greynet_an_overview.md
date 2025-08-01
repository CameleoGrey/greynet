
# Greynet: An Overview

**Greynet** is a powerful and highly optimized software framework for building complex rule-based systems, designed to efficiently solve constraint satisfaction problems and process complex events in real time.

Think of it as a system that can sift through large amounts of changing data (called "facts"), continuously match patterns and conditions you define (called "constraints" or "rules"), and calculate a "score" that represents the quality of a solution or the severity of detected issues. Its architecture is geared towards high-throughput, low-latency applications where performance is critical.

## Possibilities and Use Cases

Greynet's architecture makes it suitable for a wide range of sophisticated applications where complex decisions must be made based on a large volume of rapidly changing information.

-   **Financial Services:**
    
    -   **Real-time Fraud Detection:** Correlate transaction data, user behavior, location history, and known fraud patterns in milliseconds to block suspicious activities before they complete.
        
    -   **Algorithmic Trading:** Define complex trading strategies that react to market data, news sentiment, and technical indicators simultaneously. Greynet can identify fleeting opportunities based on multiple converging signals.
        
    -   **Live Risk Assessment:** Continuously update a customer's or portfolio's risk profile by ingesting new financial data, payment history, and market trends, providing a dynamic assessment instead of a static score.
        
-   **Cybersecurity:**
    
    -   **Complex Event Processing (CEP):** Go beyond simple signature matching. Greynet can define rules like, "Alert if a user from a new location fails a login, then immediately tries to access a sensitive database from a different IP." This correlates low-level events into high-level, actionable threat indicators.
        
    -   **Insider Threat Detection:** Model normal user behavior and flag deviations that, in isolation, may seem benign but form a threatening pattern when combined.
        
-   **Logistics and Supply Chain:**
    
    -   **Dynamic Routing and Scheduling:** Re-route delivery fleets in real-time based on live traffic, weather conditions, new pickup requests, and vehicle maintenance data to optimize for fuel efficiency and delivery times.
        
    -   **Automated Inventory Management:** Create rules to automatically trigger re-orders, identify slow-moving stock, or manage just-in-time (JIT) inventory by monitoring sales data, supplier lead times, and warehouse capacity.
        
-   **IoT and Smart Systems:**
    
    -   **Predictive Maintenance:** In a smart factory, correlate sensor data (vibration, temperature, pressure) from thousands of machines to predict failures before they happen, scheduling maintenance proactively to prevent downtime.
        
    -   **Smart Building Automation:** Optimize energy consumption by creating rules that consider occupancy, outdoor temperature, time of day, and energy pricing to dynamically control HVAC and lighting systems.
        
-   **Telecommunications:**
    
    -   **Network Monitoring & QoS:** Analyze billions of network events per day to predict equipment failure, detect service degradation, or identify security threats by correlating performance metrics across thousands of nodes.
        
    -   **Dynamic Call/Data Routing:** Implement sophisticated routing rules that consider network congestion, customer priority level, time of day, and cost, optimizing data paths in real-time.
        
-   **Gaming:**
    
    -   **Advanced AI for NPCs:** Develop non-player characters that exhibit emergent behavior. An NPC's actions could be governed by rules that consider the player's actions, the state of the environment, the time of day, and the NPC's own "needs" (e.g., hunger, fear), leading to more realistic and less predictable gameplay.
        
-   **Healthcare:**
    
    -   **Clinical Decision Support:** Develop systems that monitor patient data from EMRs and live monitors, alerting clinicians to potential drug interactions, sepsis indicators, or deviations from established care pathways.

## Advantages 

-   **Exceptional Performance:** The entire design is focused on speed. The use of a Rete-like network, zero-copy functions, efficient indexing (`FxHashMap`, `BTreeMap`), and a memory-safe `TupleArena` minimizes overhead in the critical data processing path.
    
-   **Expressive and Fluent API:** The `ConstraintBuilder` allows developers to define complex rules in a readable, chainable, and type-safe manner, making the logic easy to follow.
    
-   **Resource Management:** Greynet includes built-in `ResourceLimits` to prevent unbounded memory usage or infinite loops in the network, making it safe for production environments.
    
-   **Real-time & Incremental:** The engine is inherently incremental. It only computes the delta of changes, making it ideal for real-time systems where data is constantly arriving.
    
-   **Advanced Features:** It supports a wide range of join types, conditional existence checks (`if_exists`), powerful aggregations (`collectors`), and tuple transformations (`map`, `flat_map`).
    
-   **Rust Implementation Benefits:**
    
    -   **Memory Safety:** Rust's ownership model and borrow checker eliminate entire classes of bugs (e.g., null pointer dereferences, data races) at compile time, which is invaluable for a complex, long-running engine.
        
    -   **Predictable, Native Performance:** Because Rust has no garbage collector (GC) or virtual machine (VM), performance is highly predictable. There are no "stop-the-world" GC pauses that can introduce latency spikes, which is critical for real-time use cases. The code compiles directly to optimized machine code, running with the same bare-metal speed as C or C++.
        
    -   **Zero-Cost Abstractions:** Rust allows for writing high-level, expressive code (like the fluent builder) that compiles down to highly efficient machine code, achieving top-tier performance without sacrificing readability or safety.
        
    -   **Ultimate Portability (WASM):** Rust's compilation model allows Greynet to target a vast range of architectures. Crucially, it can be compiled to WebAssembly (WASM), enabling the entire engine to run in a web browser, on edge devices, or in serverless environments. This opens up possibilities for complex client-side rule processing or highly efficient, isolated cloud functions.

## How Greynet Works: The Data-Flow Network

Greynet works by building a data-flow network of nodes. Each node performs a specific task, and data (in the form of "tuples," which are collections of facts) flows from one node to the next. This architecture avoids re-evaluating rules against all facts every time something changes, which is the key to its efficiency.

Here’s a breakdown of the key components and the workflow:

### 1. **Facts and Tuples: The Data**

-   **Facts:** These are the basic units of data in the system. A fact is a simple data structure (like a `struct` in Rust) that represents a piece of information, such as a user action, a sensor reading, or a business object. The code shows that any type, from simple numbers (`u64`, `f64`) to complex structs, can be a fact as long as it implements the `GreynetFact` trait.
    
-   **Tuples:** As facts are processed, they are grouped into tuples (`UniTuple`, `BiTuple`, etc.). A tuple represents a partial match of a rule. For example, a `BiTuple` holds two facts that have been successfully joined together.
    

### 2. **The Node Network: The Logic**

You define rules using a fluent API (`ConstraintBuilder`), which translates your logic into a network of interconnected nodes. Data flows through this network:

-   **`FromNode`:** The entry point. Each type of fact has its own `FromNode`. When you insert a fact into the session, it starts its journey here.
    
-   **`FilterNode`:** Acts like a `WHERE` clause in SQL. It only allows tuples to pass if they satisfy a specific condition (a "predicate").
    
-   **`JoinNode`:** This is the most crucial node. It combines tuples from two different streams based on a join condition (e.g., `Equal`, `LessThan`, `NotEqual`). This is how the system finds relationships between different facts. The implementation uses sophisticated indexing (`UniIndex`, `AdvancedIndex`) to perform these joins very quickly.
    
-   **`GroupNode` & `GlobalAggregateNode`:** These nodes perform aggregations, similar to `GROUP BY` in SQL. They can `count`, `sum`, `average`, or collect items into a list or set.
    
-   **`MapNode` & `FlatMapNode`:** These nodes transform tuples, either by changing their contents or by creating multiple new tuples from a single incoming one.
    
-   **`ScoringNode`:** The terminal node for a constraint. When a tuple reaches this node, it signifies that a complete rule has been matched. The node then applies a penalty or reward function to calculate a `Score`.
    

### 3. **The Session and Scheduler: The Engine Core**

-   **`Session`:** This is the main object you interact with. It holds the node network, all the live data in a highly optimized `TupleArena`, and the current score.
    
-   **`BatchScheduler`:** When you insert or retract a fact, the change isn't processed immediately. Instead, it's queued in the scheduler. When you call `session.flush()`, the scheduler processes all pending changes in a batch, propagating them through the network in a controlled cascade. This is crucial for performance and for ensuring the network reaches a stable, consistent state.
    
-   **`TupleArena`:** A memory management workhorse. It uses a `slotmap` to allocate and deallocate memory for tuples efficiently, preventing memory leaks and fragmentation in long-running applications.
    

### Workflow Example:

1.  You define a constraint: "Penalize any employee (`Employee` fact) who has logged more than 3 failed access attempts (`AccessLog` fact) today."
    
2.  Greynet builds a network: `FromNode<Employee>` -> `JoinNode` <- `FromNode<AccessLog>` -> `FilterNode` (failed attempts) -> `GroupNode` (count by employee) -> `FilterNode` (count > 3) -> `ScoringNode`.
    
3.  You insert `Employee` and `AccessLog` facts into the `Session`.
    
4.  The facts flow into their respective `FromNode`s as `UniTuple`s.
    
5.  The `JoinNode` combines employees with their access logs.
    
6.  The `GroupNode` counts the failed logs for each employee.
    
7.  The `FilterNode` only passes the employee groups with a count greater than 3.
    
8.  These tuples reach the `ScoringNode`, which calculates a penalty score.
    
9.  You can query the session for the total score or for the specific employees who violated the constraint.
        
