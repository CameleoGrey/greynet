use greynet::{prelude::*, zero_copy_builder_with_limits};
// Import zero-copy API
use greynet::streams_zero_copy::{ZeroCopyJoinOps, ZeroCopyStreamOps};
use greynet::streams_zero_copy::zero_copy_ops;
use greynet::tuple::ZeroCopyFacts;
use greynet::{Collectors, JoinerType};
use greynet::stream_def::ConstraintRecipe;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use std::rc::Rc;
use std::any::Any;
use uuid::Uuid;
use rand::prelude::*;

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
    pub amount: i64, // Amount in cents (for precision while avoiding floating point hashing issues)
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

// --- Pure Zero-Copy Constraint Definitions ---

fn define_constraints_zero_copy(builder: &ConstraintBuilder<SimpleScore>) -> Vec<ConstraintRecipe<SimpleScore>> {
    let mut recipes = Vec::new();

    // Constraint 1: High-value transactions (amount > 45000) - Pure Zero-Copy
    let constraint1 = builder
        .for_each::<Transaction>()
        .filter_zero_copy(zero_copy_ops::field_check(|tx: &Transaction| tx.amount > 4_500_000))
        .penalize("high_value_transaction", |tuple: &AnyTuple| {
            // Zero-copy access to extract amount
            tuple.first_fact()
                .and_then(|fact| fact.as_any().downcast_ref::<Transaction>())
                .map(|tx| SimpleScore::new(tx.amount as f64 / 100_000.0))
                .unwrap_or_else(|| SimpleScore::new(0.0))
        });
    recipes.push(constraint1);

    // Constraint 2: Excessive transactions per customer - Zero-Copy Group By
    let constraint2 = builder
        .for_each::<Transaction>()
        .group_by_zero_copy(
            zero_copy_ops::field_key(|tx: &Transaction| tx.customer_id),
            Collectors::count(),
        )
        .filter_zero_copy(Rc::new(|tuple: &dyn ZeroCopyFacts| {
            // Zero-copy access to count result (second fact in BiTuple)
            tuple.get_fact_ref(1)
                .and_then(|fact| fact.as_any().downcast_ref::<usize>())
                .map_or(false, |count| *count > 25)
        }))
        .penalize("excessive_transactions_per_customer", |tuple: &AnyTuple| {
            tuple.get_fact_ref(1)
                .and_then(|fact| fact.as_any().downcast_ref::<usize>())
                .map(|count| SimpleScore::new((*count as f64 - 25.0) * 10.0))
                .unwrap_or_else(|| SimpleScore::new(0.0))
        });
    recipes.push(constraint2);

    // Constraint 3: Transactions in alerted locations - Pure Zero-Copy Join
    let constraint3 = builder
        .for_each::<Transaction>()
        .join_zero_copy(
            builder.for_each::<SecurityAlert>(),
            JoinerType::Equal,
            // FIX: Manually hash the location field instead of returning a reference.
            Rc::new(|tuple: &dyn ZeroCopyFacts| {
                tuple.first_fact()
                    .and_then(|fact| fact.as_any().downcast_ref::<Transaction>())
                    .map(|tx| {
                        let mut hasher = DefaultHasher::new();
                        tx.location.hash(&mut hasher);
                        hasher.finish()
                    })
                    .unwrap_or(0)
            }),
            // FIX: Manually hash the location field for the SecurityAlert as well.
            Rc::new(|tuple: &dyn ZeroCopyFacts| {
                tuple.first_fact()
                    .and_then(|fact| fact.as_any().downcast_ref::<SecurityAlert>())
                    .map(|alert| {
                        let mut hasher = DefaultHasher::new();
                        alert.location.hash(&mut hasher);
                        hasher.finish()
                    })
                    .unwrap_or(0)
            }),
        )
        .penalize("transaction_in_alerted_location", |tuple: &AnyTuple| {
            // Zero-copy access to SecurityAlert (second fact)
            tuple.get_fact_ref(1)
                .and_then(|fact| fact.as_any().downcast_ref::<SecurityAlert>())
                .map(|alert| SimpleScore::new(100.0 * alert.severity as f64))
                .unwrap_or_else(|| SimpleScore::new(0.0))
        });
    recipes.push(constraint3);

    // Constraint 4: Inactive customer transactions - Zero-Copy Filter Chain
    let constraint4 = builder
        .for_each::<Customer>()
        .filter_zero_copy(zero_copy_ops::field_check(|c: &Customer| matches!(c.status, CustomerStatus::Inactive)))
        .join_zero_copy(
            builder.for_each::<Transaction>(),
            JoinerType::Equal,
            zero_copy_ops::field_key(|c: &Customer| c.id),
            zero_copy_ops::field_key(|tx: &Transaction| tx.customer_id),
        )
        .penalize("inactive_customer_transaction", |_tuple: &AnyTuple| {
            SimpleScore::new(500.0)
        });
    recipes.push(constraint4);

    // Constraint 5: High-risk transactions without alert - Zero-Copy Conditional Join
    let constraint5 = builder
        .for_each::<Customer>()
        .filter_zero_copy(zero_copy_ops::field_check(|c: &Customer| matches!(c.risk_level, RiskLevel::High)))
        .join_zero_copy(
            builder.for_each::<Transaction>(),
            JoinerType::Equal,
            zero_copy_ops::field_key(|c: &Customer| c.id),
            zero_copy_ops::field_key(|tx: &Transaction| tx.customer_id),
        )
        .if_not_exists_zero_copy(
            builder.for_each::<SecurityAlert>(),
            // Zero-copy key extraction from (Customer, Transaction) tuple
            Rc::new(|tuple: &dyn ZeroCopyFacts| {
                tuple.get_fact_ref(1) // Get Transaction from BiTuple
                    .and_then(|fact| fact.as_any().downcast_ref::<Transaction>())
                    .map(|tx| {
                        let mut hasher = DefaultHasher::new();
                        tx.location.hash(&mut hasher);
                        hasher.finish()
                    })
                    .unwrap_or(0)
            }),
            // FIX: Manually hash the location field here as well.
            Rc::new(|tuple: &dyn ZeroCopyFacts| {
                tuple.first_fact()
                    .and_then(|fact| fact.as_any().downcast_ref::<SecurityAlert>())
                    .map(|alert| {
                        let mut hasher = DefaultHasher::new();
                        alert.location.hash(&mut hasher);
                        hasher.finish()
                    })
                    .unwrap_or(0)
            }),
        )
        .penalize("high_risk_transaction_without_alert", |_tuple: &AnyTuple| {
            SimpleScore::new(1000.0)
        });
    recipes.push(constraint5);

    recipes
}

// Helper function for manual hashing when needed
fn hash_location(location: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    location.hash(&mut hasher);
    hasher.finish()
}

// --- Data Generation (Same as before) ---

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

    // Generate transactions (amounts in cents to avoid floating point issues)
    let transactions: Vec<Transaction> = (0..num_transactions)
        .map(|i| Transaction {
            id: i as i32,
            customer_id: rng.random_range(0..num_customers) as i32,
            amount: rng.random_range(100..50_000_000), // 1 cent to $500,000 in cents
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

// --- Main Test Runner ---

fn main() -> Result<()> {
    println!("### Starting Rust Greynet Stress Test (Pure Zero-Copy API) ###");

    // Configuration
    const NUM_CUSTOMERS: usize = 10_000;
    const NUM_TRANSACTIONS: usize = 10_000_000;
    const NUM_LOCATIONS: usize = 1_000;

    // 1. Setup Phase - Using zero-copy optimized limits
    let setup_start = Instant::now();
    
    let limits = ResourceLimits {
        max_tuples: 50_000_000,
        max_operations_per_batch: 50_000_000,
        max_memory_mb: 40960,
        max_cascade_depth: 200_000,
        max_facts_per_type: 20_000_000,
    };
    
    // Create builder with SIMD optimization enabled
    let builder = zero_copy_builder_with_limits::<SimpleScore>(limits)
        .with_simd_optimization(false);
    
    // Define all constraints using pure zero-copy API
    let constraint_recipes = define_constraints_zero_copy(&builder);
    
    // FIXED: Proper constraint builder usage pattern
    let constraint_builder = builder
        .constraint("high_value_transaction", 1.0, {
            let recipe = constraint_recipes[0].clone();
            move || recipe.clone()
        })
        .constraint("excessive_transactions_per_customer", 1.0, {
            let recipe = constraint_recipes[1].clone();
            move || recipe.clone()
        })
        .constraint("transaction_in_alerted_location", 1.0, {
            let recipe = constraint_recipes[2].clone();
            move || recipe.clone()
        })
        .constraint("inactive_customer_transaction", 1.0, {
            let recipe = constraint_recipes[3].clone();
            move || recipe.clone()
        })
        .constraint("high_risk_transaction_without_alert", 1.0, {
            let recipe = constraint_recipes[4].clone();
            move || recipe.clone()
        });

    let mut session = constraint_builder.build()?;
    let setup_duration = setup_start.elapsed();

    // 2. Data Generation Phase
    let data_start = Instant::now();
    let (customers, transactions, alerts) = generate_data(NUM_CUSTOMERS, NUM_TRANSACTIONS, NUM_LOCATIONS);
    let total_facts = customers.len() + transactions.len() + alerts.len();
    let data_gen_duration = data_start.elapsed();

    // Debug: Check transaction amount distribution
    let high_value_count = transactions.iter().filter(|tx| tx.amount > 4_500_000).count();
    println!("Debug: Transactions with amount > 45,000: {} out of {} ({:.2}%)", 
             high_value_count, transactions.len(), 
             high_value_count as f64 / transactions.len() as f64 * 100.0);
    
    // Debug: Check customer and alert distribution
    let inactive_customers = customers.iter().filter(|c| matches!(c.status, CustomerStatus::Inactive)).count();
    let high_risk_customers = customers.iter().filter(|c| matches!(c.risk_level, RiskLevel::High)).count();
    println!("Debug: {} inactive customers ({:.2}%), {} high-risk customers ({:.2}%)", 
             inactive_customers, inactive_customers as f64 / customers.len() as f64 * 100.0,
             high_risk_customers, high_risk_customers as f64 / customers.len() as f64 * 100.0);
    println!("Debug: {} security alerts for {} locations ({:.2}%)", 
             alerts.len(), NUM_LOCATIONS, alerts.len() as f64 / NUM_LOCATIONS as f64 * 100.0);
    
    let min_amount = transactions.iter().map(|tx| tx.amount).min().unwrap_or(0);
    let max_amount = transactions.iter().map(|tx| tx.amount).max().unwrap_or(0);
    println!("Debug: Amount range: ${:.2} to ${:.2}", 
             min_amount as f64 / 100.0, max_amount as f64 / 100.0);

    // 3. Processing Phase - Using SIMD-optimized bulk insertion
    println!("Inserting facts and processing rules using zero-copy batch operations...");
    let processing_start = Instant::now();

    // Use SIMD-optimized batch insertion for maximum performance
    session.insert_batch(customers)?;
    session.insert_batch(transactions)?;
    session.insert_batch(alerts)?;

    // Flush all operations and get final score
    session.flush()?;
    let final_score = session.get_score()?;
    let constraint_matches = session.get_constraint_matches()?;
    
    let processing_duration = processing_start.elapsed();
    let total_duration = setup_start.elapsed();

    // 4. Calculate metrics
    let facts_per_second = if processing_duration.as_secs_f64() > 0.0 {
        total_facts as f64 / processing_duration.as_secs_f64()
    } else {
        f64::INFINITY
    };

    // 5. Get session statistics
    let stats = session.get_statistics();

    // 6. Reporting
    println!("\n--- Zero-Copy Stress Test Results ---");
    println!("\n#### Performance Summary");
    println!("| Metric                         | Value               |");
    println!("|--------------------------------|---------------------|");
    println!("| Total Facts Processed          | {:}         |", total_facts);
    println!("| Setup Time (Build Network)     | {:.4} s      |", setup_duration.as_secs_f64());
    println!("| Data Generation Time           | {:.4} s      |", data_gen_duration.as_secs_f64());
    println!("| **Processing Time (Zero-Copy)** | **{:.4} s** |", processing_duration.as_secs_f64());
    println!("| Total Time                     | {:.4} s      |", total_duration.as_secs_f64());
    println!("| **Zero-Copy Throughput** | **{:.2} facts/sec** |", facts_per_second);

    println!("\n#### Memory Usage Summary");
    println!("| Metric                         | Value               |");
    println!("|--------------------------------|---------------------|");
    println!("| Memory Usage Estimate         | {} MB        |", stats.memory_usage_mb);
    println!("| Total Arena Slots              | {:}         |", stats.arena_stats.total_slots);
    println!("| Live Tuples                    | {:}         |", stats.arena_stats.live_tuples);
    println!("| Dead Tuples                    | {:}         |", stats.arena_stats.dead_tuples);

    println!("\n#### Engine Output");
    println!("- **Final Score:** {:?}", final_score);
    
    let total_matches: usize = constraint_matches.values().map(|v| v.len()).sum();
    println!("- **Total Constraint Matches:** {}", total_matches);
    
    for (constraint_id, matches) in constraint_matches.iter() {
        println!("  - `{}`: {} matches", constraint_id, matches.len());
    }

    println!("\n#### Network Statistics");
    println!("- **Total Nodes:** {}", stats.total_nodes);
    println!("- **Scoring Nodes:** {}", stats.scoring_nodes);
    println!("- **Generation Counter:** {}", stats.arena_stats.generation_counter);

    // Validate consistency
    session.validate_consistency()?;
    println!("\n✅ Zero-copy consistency validation passed!");

    // Zero-copy specific performance insights
    println!("\n#### Zero-Copy API Performance Insights");
    println!("- Used pure zero-copy stream operations for maximum performance");
    println!("- SIMD-optimized bulk fact insertion");
    println!("- Zero-allocation fact access during constraint evaluation");
    println!("- Optimized field extractors with zero-copy tuple access");

    Ok(())
}