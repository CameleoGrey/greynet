use greynet::prelude::*;
// Import zero-copy API
use greynet::streams_zero_copy::{ZeroCopyJoinOps, ZeroCopyStreamOps, enhanced_zero_copy_ops};
use greynet::streams_zero_copy::zero_copy_ops;
use greynet::tuple::ZeroCopyFacts;
use greynet::nodes::{ZeroCopyImpactFn, ZeroCopyKeyFn, ZeroCopyPredicate};
use greynet::{Collectors, JoinerType};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use std::rc::Rc;
use std::any::Any;
use uuid::Uuid;
use rand::prelude::*;
use greynet::zero_copy_builder_with_limits;

// --- Fact Definitions ---

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Customer {
    pub id: i32,
    pub risk_level: RiskLevel,
    pub status: CustomerStatus,
}

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
pub struct Transaction {
    pub id: i32,
    pub customer_id: i32,
    pub amount: i64, // Amount in cents
    pub location: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SecurityAlert {
    pub location: String,
    pub severity: i32,
}

// --- GreynetFact Implementations ---

impl GreynetFact for Customer {
    fn fact_id(&self) -> Uuid {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash_val = hasher.finish();
        let namespace = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        Uuid::new_v5(&namespace, &hash_val.to_be_bytes())
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        other.as_any().downcast_ref::<Customer>().map_or(false, |c| c == self)
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

impl GreynetFact for Transaction {
    fn fact_id(&self) -> Uuid {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash_val = hasher.finish();
        let namespace = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        Uuid::new_v5(&namespace, &hash_val.to_be_bytes())
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        other.as_any().downcast_ref::<Transaction>().map_or(false, |t| t == self)
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

impl GreynetFact for SecurityAlert {
    fn fact_id(&self) -> Uuid {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash_val = hasher.finish();
        let namespace = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
        Uuid::new_v5(&namespace, &hash_val.to_be_bytes())
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        other.as_any().downcast_ref::<SecurityAlert>().map_or(false, |a| a == self)
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

// --- Pure Zero-Copy Operations ---

/// Zero-copy key extractor for Transaction customer_id
fn transaction_customer_key() -> ZeroCopyKeyFn {
    zero_copy_ops::field_key(|tx: &Transaction| tx.customer_id)
}

/// Zero-copy key extractor for Customer id
fn customer_id_key() -> ZeroCopyKeyFn {
    zero_copy_ops::field_key(|c: &Customer| c.id)
}

/// Zero-copy key extractor for location (Transaction)
fn transaction_location_key() -> ZeroCopyKeyFn {
    Rc::new(|tuple: &dyn ZeroCopyFacts| {
        tuple.first_fact()
            .and_then(|fact| fact.as_any().downcast_ref::<Transaction>())
            .map(|tx| {
                let mut hasher = DefaultHasher::new();
                tx.location.hash(&mut hasher);
                hasher.finish()
            })
            .unwrap_or(0)
    })
}

/// Zero-copy key extractor for location (SecurityAlert)
fn alert_location_key() -> ZeroCopyKeyFn {
    Rc::new(|tuple: &dyn ZeroCopyFacts| {
        tuple.first_fact()
            .and_then(|fact| fact.as_any().downcast_ref::<SecurityAlert>())
            .map(|alert| {
                let mut hasher = DefaultHasher::new();
                alert.location.hash(&mut hasher);
                hasher.finish()
            })
            .unwrap_or(0)
    })
}

/// Zero-copy key extractor for location from BiTuple (Customer, Transaction)
fn bituple_transaction_location_key() -> ZeroCopyKeyFn {
    Rc::new(|tuple: &dyn ZeroCopyFacts| {
        tuple.get_fact_ref(1) // Get Transaction from BiTuple
            .and_then(|fact| fact.as_any().downcast_ref::<Transaction>())
            .map(|tx| {
                let mut hasher = DefaultHasher::new();
                tx.location.hash(&mut hasher);
                hasher.finish()
            })
            .unwrap_or(0)
    })
}

/// Zero-copy predicate for high-value transactions
fn high_value_transaction_predicate() -> ZeroCopyPredicate {
    zero_copy_ops::field_check(|tx: &Transaction| tx.amount > 4_500_000)
}

/// Zero-copy predicate for inactive customers
fn inactive_customer_predicate() -> ZeroCopyPredicate {
    zero_copy_ops::field_check(|c: &Customer| matches!(c.status, CustomerStatus::Inactive))
}

/// Zero-copy predicate for high-risk customers
fn high_risk_customer_predicate() -> ZeroCopyPredicate {
    zero_copy_ops::field_check(|c: &Customer| matches!(c.risk_level, RiskLevel::High))
}

/// Zero-copy predicate for excessive transaction count
fn excessive_transaction_count_predicate() -> ZeroCopyPredicate {
    Rc::new(|tuple: &dyn ZeroCopyFacts| {
        tuple.get_fact_ref(1) // Count is the second fact in BiTuple
            .and_then(|fact| fact.as_any().downcast_ref::<usize>())
            .map_or(false, |count| *count > 25)
    })
}

/// Zero-copy impact function for high-value transactions
fn high_value_transaction_impact() -> ZeroCopyImpactFn<SimpleScore> {
    enhanced_zero_copy_ops::impact_function(|tuple: &dyn ZeroCopyFacts| {
        tuple.first_fact()
            .and_then(|fact| fact.as_any().downcast_ref::<Transaction>())
            .map(|tx| SimpleScore::new(tx.amount as f64 / 100_000.0))
            .unwrap_or_else(|| SimpleScore::new(0.0))
    })
}

/// Zero-copy impact function for excessive transactions
fn excessive_transactions_impact() -> ZeroCopyImpactFn<SimpleScore> {
    enhanced_zero_copy_ops::impact_function(|tuple: &dyn ZeroCopyFacts| {
        tuple.get_fact_ref(1)
            .and_then(|fact| fact.as_any().downcast_ref::<usize>())
            .map(|count| SimpleScore::new((*count as f64 - 25.0) * 10.0))
            .unwrap_or_else(|| SimpleScore::new(0.0))
    })
}

/// Zero-copy impact function for transactions in alerted locations
fn alerted_location_impact() -> ZeroCopyImpactFn<SimpleScore> {
    enhanced_zero_copy_ops::impact_function(|tuple: &dyn ZeroCopyFacts| {
        tuple.get_fact_ref(1) // SecurityAlert is the second fact
            .and_then(|fact| fact.as_any().downcast_ref::<SecurityAlert>())
            .map(|alert| SimpleScore::new(100.0 * alert.severity as f64))
            .unwrap_or_else(|| SimpleScore::new(0.0))
    })
}

/// Zero-copy impact function for fixed penalties
fn fixed_penalty_impact(penalty: f64) -> ZeroCopyImpactFn<SimpleScore> {
    enhanced_zero_copy_ops::impact_function(move |_tuple: &dyn ZeroCopyFacts| {
        SimpleScore::new(penalty)
    })
}

// --- Pure Zero-Copy Constraint Definitions ---

fn build_constraints_with_zero_copy() -> Result<Session<SimpleScore>> {
    // Setup with optimized limits for zero-copy operations
    let limits = ResourceLimits {
        max_tuples: 50_000_000,
        max_operations_per_batch: 50_000_000,
        max_memory_mb: 40960,
        max_cascade_depth: 200_000,
        max_facts_per_type: 20_000_000,
    };
    
    let builder = zero_copy_builder_with_limits::<SimpleScore>(limits);

    // Constraint 1: High-value transactions using pure zero-copy API
    let high_value_constraint = builder
        .for_each::<Transaction>()
        .filter_zero_copy(high_value_transaction_predicate())
        .penalize_zero_copy("high_value_transaction", high_value_transaction_impact());

    // Constraint 2: Excessive transactions per customer using zero-copy grouping
    let excessive_transactions_constraint = builder
        .for_each::<Transaction>()
        .group_by_zero_copy(
            transaction_customer_key(),
            Collectors::count(),
        )
        .filter_zero_copy(excessive_transaction_count_predicate())
        .penalize_zero_copy("excessive_transactions_per_customer", excessive_transactions_impact());

    // Constraint 3: Transactions in alerted locations using zero-copy joins
    let alerted_location_constraint = builder
        .for_each::<Transaction>()
        .join_zero_copy(
            builder.for_each::<SecurityAlert>(),
            JoinerType::Equal,
            transaction_location_key(),
            alert_location_key(),
        )
        .penalize_zero_copy("transaction_in_alerted_location", alerted_location_impact());

    // Constraint 4: Inactive customer transactions using zero-copy filter chains
    let inactive_customer_constraint = builder
        .for_each::<Customer>()
        .filter_zero_copy(inactive_customer_predicate())
        .join_zero_copy(
            builder.for_each::<Transaction>(),
            JoinerType::Equal,
            customer_id_key(),
            transaction_customer_key(),
        )
        .penalize_zero_copy("inactive_customer_transaction", fixed_penalty_impact(500.0));

    // Constraint 5: High-risk transactions without alerts using zero-copy conditional joins
    let high_risk_no_alert_constraint = builder
        .for_each::<Customer>()
        .filter_zero_copy(high_risk_customer_predicate())
        .join_zero_copy(
            builder.for_each::<Transaction>(),
            JoinerType::Equal,
            customer_id_key(),
            transaction_customer_key(),
        )
        .if_not_exists_zero_copy(
            builder.for_each::<SecurityAlert>(),
            bituple_transaction_location_key(),
            alert_location_key(),
        )
        .penalize_zero_copy("high_risk_transaction_without_alert", fixed_penalty_impact(1000.0));

    // Build the session with all constraints
    let session = builder
        .constraint("high_value_transaction", 1.0, move || high_value_constraint.clone())
        .constraint("excessive_transactions_per_customer", 1.0, move || excessive_transactions_constraint.clone())
        .constraint("transaction_in_alerted_location", 1.0, move || alerted_location_constraint.clone())
        .constraint("inactive_customer_transaction", 1.0, move || inactive_customer_constraint.clone())
        .constraint("high_risk_transaction_without_alert", 1.0, move || high_risk_no_alert_constraint.clone())
        .build()?;

    Ok(session)
}

// --- Data Generation ---

fn generate_data(
    num_customers: usize,
    num_transactions: usize,
    num_locations: usize,
) -> (Vec<Customer>, Vec<Transaction>, Vec<SecurityAlert>) {
    let mut rng = rand::thread_rng();

    // Generate locations
    let locations: Vec<String> = (0..num_locations)
        .map(|i| format!("location_{}", i))
        .collect();

    // Generate customers
    let customers: Vec<Customer> = (0..num_customers)
        .map(|i| Customer {
            id: i as i32,
            risk_level: match rng.gen_range(0..3) {
                0 => RiskLevel::Low,
                1 => RiskLevel::Medium,
                _ => RiskLevel::High,
            },
            status: if rng.gen_bool(0.95) {
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
            customer_id: rng.gen_range(0..num_customers) as i32,
            amount: rng.gen_range(100..50_000_000), // 1 cent to $500,000 in cents
            location: locations[rng.gen_range(0..locations.len())].clone(),
        })
        .collect();

    // Generate security alerts for a subset of locations
    let num_alerts = num_locations / 4;
    let alerted_locations: Vec<&String> = locations.choose_multiple(&mut rng, num_alerts).collect();
    let alerts: Vec<SecurityAlert> = alerted_locations
        .into_iter()
        .map(|loc| SecurityAlert {
            location: loc.clone(),
            severity: rng.gen_range(1..=5),
        })
        .collect();

    (customers, transactions, alerts)
}

// --- Performance Testing ---

fn run_performance_benchmark() -> Result<()> {
    println!("### Starting Pure Zero-Copy API Performance Test ###");

    // Configuration
    const NUM_CUSTOMERS: usize = 10_000;
    const NUM_TRANSACTIONS: usize = 10_000_000;
    const NUM_LOCATIONS: usize = 1_000;

    // 1. Setup Phase
    let setup_start = Instant::now();
    let mut session = build_constraints_with_zero_copy()?;
    let setup_duration = setup_start.elapsed();

    // 2. Data Generation Phase
    let data_start = Instant::now();
    let (customers, transactions, alerts) = generate_data(NUM_CUSTOMERS, NUM_TRANSACTIONS, NUM_LOCATIONS);
    let total_facts = customers.len() + transactions.len() + alerts.len();
    let data_gen_duration = data_start.elapsed();

    // Debug information
    let high_value_count = transactions.iter().filter(|tx| tx.amount > 4_500_000).count();
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

    // 3. Processing Phase using pure zero-copy operations
    println!("Processing facts using pure zero-copy API...");
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
    println!("\n=== PURE ZERO-COPY API PERFORMANCE RESULTS ===");
    
    println!("\n🚀 Performance Metrics:");
    println!("┌─────────────────────────────────┬─────────────────────┐");
    println!("│ Metric                          │ Value               │");
    println!("├─────────────────────────────────┼─────────────────────┤");
    println!("│ Total Facts Processed          │ {:>19} │", format!("{:}", total_facts));
    println!("│ Setup Time                      │ {:>15.4} s │", setup_duration.as_secs_f64());
    println!("│ Data Generation Time            │ {:>15.4} s │", data_gen_duration.as_secs_f64());
    println!("│ **Zero-Copy Processing Time**   │ **{:>11.4} s** │", processing_duration.as_secs_f64());
    println!("│ Total Time                      │ {:>15.4} s │", total_duration.as_secs_f64());
    println!("│ **Zero-Copy Throughput**        │ **{:>9.0} facts/s** │", facts_per_second);
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
    println!("│ Generation Counter              │ {:>19} │", format!("{:}", stats.arena_stats.generation_counter));
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
             total_facts as f64 / stats.total_nodes as f64);

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