use greynet::prelude::*;
use greynet::tuple::ZeroCopyFacts; // Import the necessary zero-copy trait
use greynet::Collectors;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use rand::prelude::*;

// --- Fact Definitions (Unchanged) ---

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum CustomerStatus {
    Active,
    Inactive,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Customer {
    pub id: i32,
    pub risk_level: RiskLevel,
    pub status: CustomerStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub id: i32,
    pub customer_id: i32,
    pub amount: f64, // Amount in dollars
    pub location: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SecurityAlert {
    pub location: String,
    pub severity: i32,
}

// --- Manual Implementations for Transaction due to f64 ---

impl Transaction {
    pub fn print_amount(&self) {
        println!("{}", self.amount);
    }
}

impl Hash for Transaction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.customer_id.hash(state);
        self.amount.to_bits().hash(state); // Hash the bit representation of f64
        self.location.hash(state);
    }
}

impl Eq for Transaction {} // Marker trait

greynet::greynet_fact_for_struct!(Customer);
greynet::greynet_fact_for_struct!(Transaction);
greynet::greynet_fact_for_struct!(SecurityAlert);


// --- Modern API Constraint Definitions (Unchanged) ---

fn build_constraints() -> Result<Session<SimpleScore>> {
    // Setup with optimized limits
    let limits = ResourceLimits {
        max_tuples: 50_000_000,
        max_operations_per_batch: 50_000_000,
        max_memory_mb: 40960,
        max_cascade_depth: 200_000,
        max_facts_per_type: 20_000_000,
    };
    
    // Use the modern builder from the prelude
    let mut builder = builder_with_limits::<SimpleScore>(limits);

    // Constraint 1: Penalize high-value transactions.
    builder.add_constraint("high_value_transaction", 1.0)
        .for_each::<Transaction>()
        .filter(|tx: &Transaction| tx.amount > 45000.0)
        .penalize(|tuple: &dyn ZeroCopyFacts| {
            let tx = extract_fact::<Transaction>(tuple, 0).unwrap();
            SimpleScore::new(tx.amount / 1000.0)
        });

    // Constraint 2: Penalize customers with an excessive number of transactions.
    builder.add_constraint("excessive_transactions_per_customer", 1.0)
        .for_each::<Transaction>()
        .group_by(
            |tx: &Transaction| tx.customer_id, // Group transactions by customer
            Collectors::count(),                // Count transactions in each group
        )
        .filter_tuple(|tuple: &dyn ZeroCopyFacts| {
            extract_fact::<usize>(tuple, 1)
                 .map_or(false, |count| *count > 25)
        })
        .penalize(|tuple: &dyn ZeroCopyFacts| {
            // The tuple is a BiTuple<(u64_key, usize_count)>.
            let count = extract_fact::<usize>(tuple, 1).unwrap();
            SimpleScore::new((*count as f64 - 25.0) * 10.0)
        });

    // Create reusable streams for joins. `builder.for_each()` is non-consuming.
    let tx_stream = builder.for_each::<Transaction>();
    let alerts_stream = builder.for_each::<SecurityAlert>();

    // Constraint 3: Penalize transactions occurring in locations with security alerts.
    builder.add_constraint("transaction_in_alerted_location", 1.0)
        .for_each::<Transaction>()
        .join_on(
            alerts_stream.clone(), // Clone the stream definition for reuse
            |tx: &Transaction| tx.location.clone(),
            |alert: &SecurityAlert| alert.location.clone(),
        )
        .penalize(|tuple: &dyn ZeroCopyFacts| {
            // The tuple is a BiTuple<(Transaction, SecurityAlert)>.
            let alert = extract_fact::<SecurityAlert>(tuple, 1).unwrap();
            SimpleScore::new(100.0 * alert.severity as f64)
        });

    // Constraint 4: Penalize transactions made by inactive customers.
    builder.add_constraint("inactive_customer_transaction", 1.0)
        .for_each::<Customer>()
        .filter(|c: &Customer| matches!(c.status, CustomerStatus::Inactive))
        .join_on(
            tx_stream.clone(),
            |c: &Customer| c.id,
            |tx: &Transaction| tx.customer_id,
        )
        .penalize(|_| SimpleScore::new(500.0));

    // Constraint 5: Penalize transactions from high-risk customers in locations *without* alerts.
    builder.add_constraint("high_risk_transaction_without_alert", 1.0)
        .for_each::<Customer>()
        .filter(|c: &Customer| matches!(c.risk_level, RiskLevel::High))
        .join_on(
            tx_stream.clone(), // Reuse the transaction stream
            |c: &Customer| c.id,
            |tx: &Transaction| tx.customer_id,
        )
        // After the join, the stream contains (Customer, Transaction) tuples.
        // We check for the non-existence of an alert based on the transaction's location.
        .if_not_exists_on_indexed(
            alerts_stream.clone(), // Reuse the alerts stream
            1,                     // Index of Transaction in the (Customer, Transaction) stream
            |tx: &Transaction| tx.location.clone(), // Key from the second fact (Transaction)
            |alert: &SecurityAlert| alert.location.clone(), // Key from the "other" stream
        )
        .penalize(|_| SimpleScore::new(1000.0));

    // Build the session from all the defined constraints.
    builder.build()
}

// --- Data Generation (Unchanged) ---

fn generate_data(
    num_customers: usize,
    num_transactions: usize,
    num_locations: usize,
) -> (Vec<Customer>, Vec<Transaction>, Vec<SecurityAlert>) {
    let mut rng = rand::rng();

    // Generate locations
    let locations: Vec<String> = (0..num_locations)
        .map(|i| format!("location_{}", i))
        .collect();

    // Generate customers
    let customers: Vec<Customer> = (0..num_customers)
        .map(|i| Customer {
            id: i as i32,
            risk_level: match rng.random_range(0..3) {
                0 => RiskLevel::Low,
                1 => RiskLevel::Medium,
                _ => RiskLevel::High,
            },
            status: if rng.random_bool(0.95) {
                CustomerStatus::Active
            } else {
                CustomerStatus::Inactive
            },
        })
        .collect();

    // Generate transactions
    let transactions: Vec<Transaction> = (0..num_transactions)
        .map(|i| Transaction {
            id: i as i32,
            customer_id: rng.random_range(0..num_customers) as i32,
            amount: rng.random_range(1.0..50000.0), // Amount in dollars
            location: locations[rng.random_range(0..locations.len())].clone(),
        })
        .collect();

    // Generate security alerts for a subset of locations
    let num_alerts = num_locations / 4;
    let alerted_locations: Vec<&String> = locations.choose_multiple(&mut rng, num_alerts).collect();
    let alerts: Vec<SecurityAlert> = alerted_locations
        .into_iter()
        .map(|loc| SecurityAlert {
            location: loc.clone(),
            severity: rng.random_range(1..=5),
        })
        .collect();

    (customers, transactions, alerts)
}

// --- Performance Testing (Unchanged) ---

fn run_performance_benchmark() -> Result<()> {
    println!("### Starting Modern Fluent API Performance Test ###");

    // Configuration
    const NUM_CUSTOMERS: usize = 10_000;
    const NUM_TRANSACTIONS: usize = 10_000_000;
    const NUM_LOCATIONS: usize = 1_000;

    // 1. Setup Phase
    let setup_start = Instant::now();
    let mut session = build_constraints()?; // <-- Use the rewritten function
    let setup_duration = setup_start.elapsed();

    // 2. Data Generation Phase
    let data_start = Instant::now();
    let (customers, transactions, alerts) = generate_data(NUM_CUSTOMERS, NUM_TRANSACTIONS, NUM_LOCATIONS);
    let total_facts = customers.len() + transactions.len() + alerts.len();
    let data_gen_duration = data_start.elapsed();

    // Debug information
    let high_value_count = transactions.iter().filter(|tx| tx.amount > 45000.0).count();
    let inactive_customers = customers.iter().filter(|c| matches!(c.status, CustomerStatus::Inactive)).count();
    let high_risk_customers = customers.iter().filter(|c| matches!(c.risk_level, RiskLevel::High)).count();
    
    println!("Debug: High-value transactions: {} ({:.2}%)", 
             high_value_count, high_value_count as f64 / transactions.len() as f64 * 100.0);
    println!("Debug: Inactive customers: {} ({:.2}%)", 
             inactive_customers, inactive_customers as f64 / customers.len() as f64 * 100.0);
    println!("Debug: High-risk customers: {} ({:.2}%)", 
             high_risk_customers, high_risk_customers as f64 / customers.len() as f64 * 100.0);
    println!("Debug: Security alerts: {} for {} locations ({:.2}%)", 
             alerts.len(), NUM_LOCATIONS, alerts.len() as f64 / NUM_LOCATIONS as f64 * 100.0);

    // 3. Processing Phase
    println!("Processing facts using modern fluent API...");
    let processing_start = Instant::now();

    session.insert_batch(customers)?;
    session.insert_batch(transactions)?;
    session.insert_batch(alerts)?;

    session.flush()?;
    let final_score = session.get_score()?;
    let constraint_matches = session.get_constraint_matches()?;
    
    let processing_duration = processing_start.elapsed();
    let total_duration = setup_start.elapsed();

    // 4. Calculate performance metrics
    let facts_per_second = if processing_duration.as_secs_f64() > 0.0 {
        total_facts as f64 / processing_duration.as_secs_f64()
    } else {
        f64::INFINITY
    };

    let memory_efficiency = {
        let stats = session.get_statistics();
        if stats.memory_usage_mb > 0 {
            total_facts as f64 / stats.memory_usage_mb as f64
        } else {
            f64::INFINITY
        }
    };

    // 5. Get detailed statistics
    let stats = session.get_statistics();

    // 6. Comprehensive reporting
    println!("\n=== MODERN FLUENT API PERFORMANCE RESULTS ===");
    
    println!("\n🚀 Performance Metrics:");
    println!("┌─────────────────────────────────┬─────────────────────┐");
    println!("│ Metric                          │ Value               │");
    println!("├─────────────────────────────────┼─────────────────────┤");
    println!("│ Total Facts Processed          │ {:>19} │", format!("{:}", total_facts));
    println!("│ Setup Time                      │ {:>15.4} s │", setup_duration.as_secs_f64());
    println!("│ Data Generation Time            │ {:>15.4} s │", data_gen_duration.as_secs_f64());
    println!("│ **Processing Time** │ **{:>11.4} s** │", processing_duration.as_secs_f64());
    println!("│ Total Time                      │ {:>15.4} s │", total_duration.as_secs_f64());
    println!("│ **Throughput** │ **{:>9.0} facts/s** │", facts_per_second);
    println!("│ Memory Efficiency               │ {:>11.0} facts/MB │", memory_efficiency);
    println!("└─────────────────────────────────┴─────────────────────┘");

    println!("\n💾 Memory Usage:");
    println!("┌─────────────────────────────────┬─────────────────────┐");
    println!("│ Metric                          │ Value               │");
    println!("├─────────────────────────────────┼─────────────────────┤");
    println!("│ Estimated Memory Usage          │ {:>15} MB │", stats.memory_usage_mb);
    println!("│ Total Arena Slots               │ {:>19} │", format!("{:}", stats.arena_stats.total_slots));
    println!("│ Live Tuples                     │ {:>19} │", format!("{:}", stats.arena_stats.live_tuples));
    println!("│ Dead/Pooled Tuples              │ {:>19} │", format!("{:}", stats.arena_stats.dead_tuples));
    println!("└─────────────────────────────────┴─────────────────────┘");

    println!("\n🎯 Constraint Results:");
    println!("• **Final Score:** {:?}", final_score);
    
    let total_matches: usize = constraint_matches.values().map(|v| v.len()).sum();
    println!("• **Total Constraint Violations:** {}", format!("{:}", total_matches));
    
    for (constraint_id, matches) in constraint_matches.iter() {
        let percentage = if total_facts > 0 {
            matches.len() as f64 / total_facts as f64 * 100.0
        } else {
            0.0
        };
        println!("  ├─ `{}`: {} violations ({:.3}%)", 
                 constraint_id, format!("{:}", matches.len()), percentage);
    }

    println!("\n🏗️ Network Statistics:");
    println!("• **Total Nodes:** {}", stats.total_nodes);
    println!("• **Scoring Nodes:** {}", stats.scoring_nodes);
    println!("• **Network Efficiency:** {:.2} facts per node", 
             if stats.total_nodes > 0 { total_facts as f64 / stats.total_nodes as f64 } else { 0.0 });

    // 8. Validate system consistency
    session.validate_consistency()?;
    println!("\n✅ **System consistency validation passed!**");

    Ok(())
}

fn main() -> Result<()> {
    // Run main performance benchmark
    run_performance_benchmark()?;
    
    Ok(())
}
