use greynet::prelude::*;
use greynet::{Collectors, JoinerType};
use greynet::stream_def::ConstraintRecipe;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use std::rc::Rc;
use std::any::Any;
use uuid::Uuid;
use rand::prelude::*;
use rand::rng;

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

// --- Constraint Definitions ---

fn define_constraints(builder: &ConstraintBuilder<SimpleScore>) -> Vec<ConstraintRecipe<SimpleScore>> {
    let mut recipes = Vec::new();

    // Constraint 1: High-value transactions (matches Java: amount > 45000)
    let constraint1 = builder
        .for_each::<Transaction>()
        .filter(Rc::new(|tuple: &AnyTuple| {
            if let AnyTuple::Uni(uni_tuple) = tuple {
                if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                    tx.amount > 4_500_000 // 45000 * 100 (stored in cents)
                } else {
                    false
                }
            } else {
                false
            }
        }))
        .penalize("high_value_transaction", |tuple: &AnyTuple| {
            if let AnyTuple::Uni(uni_tuple) = tuple {
                if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                    SimpleScore::new(tx.amount as f64 / 100_000.0) // Convert cents to dollars then divide by 1000
                } else {
                    SimpleScore::new(0.0)
                }
            } else {
                SimpleScore::new(0.0)
            }
        });
    recipes.push(constraint1);

    // Constraint 2: Excessive transactions per customer (matches Java exactly)
    let constraint2 = builder
        .for_each::<Transaction>()
        .group_by(
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                        tx.customer_id as u64
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
            Collectors::count(),
        )
        .filter(Rc::new(|tuple: &AnyTuple| {
            if let AnyTuple::Bi(bi_tuple) = tuple {
                if let Some(count) = bi_tuple.fact_b.as_any().downcast_ref::<usize>() {
                    *count > 25
                } else {
                    false
                }
            } else {
                false
            }
        }))
        .penalize("excessive_transactions_per_customer", |tuple: &AnyTuple| {
            if let AnyTuple::Bi(bi_tuple) = tuple {
                if let Some(count) = bi_tuple.fact_b.as_any().downcast_ref::<usize>() {
                    SimpleScore::new((*count as f64 - 25.0) * 10.0)
                } else {
                    SimpleScore::new(0.0)
                }
            } else {
                SimpleScore::new(0.0)
            }
        });
    recipes.push(constraint2);

    // Constraint 3: Transactions in alerted locations (matches Java exactly)
    let constraint3 = builder
        .for_each::<Transaction>()
        .join(
            builder.for_each::<SecurityAlert>(),
            JoinerType::Equal,
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                        hash_string(&tx.location)
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(alert) = uni_tuple.fact_a.as_any().downcast_ref::<SecurityAlert>() {
                        hash_string(&alert.location)
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
        )
        .penalize("transaction_in_alerted_location", |tuple: &AnyTuple| {
            if let AnyTuple::Bi(bi_tuple) = tuple {
                if let Some(alert) = bi_tuple.fact_b.as_any().downcast_ref::<SecurityAlert>() {
                    SimpleScore::new(100.0 * alert.severity as f64)
                } else {
                    SimpleScore::new(0.0)
                }
            } else {
                SimpleScore::new(0.0)
            }
        });
    recipes.push(constraint3);

    // Constraint 4: Inactive customer transactions (matches Java exactly)
    let constraint4 = builder
        .for_each::<Customer>()
        .filter(Rc::new(|tuple: &AnyTuple| {
            if let AnyTuple::Uni(uni_tuple) = tuple {
                if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                    matches!(customer.status, CustomerStatus::Inactive)
                } else {
                    false
                }
            } else {
                false
            }
        }))
        .join(
            builder.for_each::<Transaction>(),
            JoinerType::Equal,
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                        customer.id as u64
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                        tx.customer_id as u64
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
        )
        .penalize("inactive_customer_transaction", |_tuple: &AnyTuple| {
            SimpleScore::new(500.0)
        });
    recipes.push(constraint4);

    // Constraint 5: High-risk transactions without alert (matches Java ifNotExists logic)
    let constraint5 = builder
        .for_each::<Customer>()
        .filter(Rc::new(|tuple: &AnyTuple| {
            if let AnyTuple::Uni(uni_tuple) = tuple {
                if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                    matches!(customer.risk_level, RiskLevel::High)
                } else {
                    false
                }
            } else {
                false
            }
        }))
        .join(
            builder.for_each::<Transaction>(),
            JoinerType::Equal,
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                        customer.id as u64
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                        tx.customer_id as u64
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
        )
        .if_not_exists(
            builder.for_each::<SecurityAlert>(),
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Bi(bi_tuple) = tuple {
                    if let Some(tx) = bi_tuple.fact_b.as_any().downcast_ref::<Transaction>() {
                        hash_string(&tx.location)
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
            Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(alert) = uni_tuple.fact_a.as_any().downcast_ref::<SecurityAlert>() {
                        hash_string(&alert.location)
                    } else {
                        0
                    }
                } else {
                    0
                }
            }),
        )
        .penalize("high_risk_transaction_without_alert", |_tuple: &AnyTuple| {
            SimpleScore::new(1000.0) // Match Java penalty exactly
        });
    recipes.push(constraint5);

    recipes
}

// Helper function to hash strings consistently
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// --- Data Generation ---

fn generate_data(
    num_customers: usize,
    num_transactions: usize,
    num_locations: usize,
) -> (Vec<Customer>, Vec<Transaction>, Vec<SecurityAlert>) {
    let mut rng = rng();

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
    println!("### Starting Rust Greynet Stress Test ###");

    // Configuration
    const NUM_CUSTOMERS: usize = 10_000;
    const NUM_TRANSACTIONS: usize = 10_000_000; // Reduced from Python for initial test
    const NUM_LOCATIONS: usize = 1_000;

    // 1. Setup Phase
    let setup_start = Instant::now();
    
    let limits = ResourceLimits {
        max_tuples: 50_000_000,
        max_operations_per_batch: 50_000_000,
        max_memory_mb: 40960,
        max_cascade_depth: 20000,
        max_facts_per_type: 20_000_000,
    };
    
    let builder = builder_with_limits::<SimpleScore>(limits);
    
    // Define all constraints
    let constraint_recipes = define_constraints(&builder);
    
    // Add constraints to builder with weights
    let mut constraint_builder = builder;
    constraint_builder
        .constraint("high_value_transaction", 1.0, || constraint_recipes[0].clone())
        .constraint("excessive_transactions_per_customer", 1.0, || constraint_recipes[1].clone())
        .constraint("transaction_in_alerted_location", 1.0, || constraint_recipes[2].clone())
        .constraint("inactive_customer_transaction", 1.0, || constraint_recipes[3].clone())
        .constraint("high_risk_transaction_without_alert", 1.0, || constraint_recipes[4].clone());

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

    // 3. Processing Phase
    println!("Inserting facts and processing rules...");
    let processing_start = Instant::now();

    // Insert customers
    for customer in customers {
        session.insert(customer)?;
    }

    // Insert transactions  
    for transaction in transactions {
        session.insert(transaction)?;
    }

    // Insert alerts
    for alert in alerts {
        session.insert(alert)?;
    }

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
    println!("\n--- Stress Test Results ---");
    println!("\n#### Performance Summary");
    println!("| Metric                         | Value               |");
    println!("|--------------------------------|---------------------|");
    println!("| Total Facts Processed          | {:}         |", total_facts);
    println!("| Setup Time (Build Network)     | {:.4} s      |", setup_duration.as_secs_f64());
    println!("| Data Generation Time           | {:.4} s      |", data_gen_duration.as_secs_f64());
    println!("| **Processing Time (Insert+Flush)** | **{:.4} s**      |", processing_duration.as_secs_f64());
    println!("| Total Time                     | {:.4} s      |", total_duration.as_secs_f64());
    println!("| **Throughput**                 | **{:.2} facts/sec** |", facts_per_second);

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
    println!("\n✅ Consistency validation passed!");

    Ok(())
}