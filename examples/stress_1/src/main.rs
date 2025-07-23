// Greynet Rust Stress Test
// ========================
// 
// This is a Rust translation of the Python Greynet stress test that demonstrates
// the constraint satisfaction engine's capabilities with a realistic dataset.
//
// ## How to Run
// 1. Add this file to your Rust project
// 2. Update Cargo.toml with the dependencies listed below
// 3. Ensure the greynet library is available (adjust import paths as needed)
// 4. Run with: `cargo run --bin stress_test --release` (for performance testing)
//
// ## What it does
// - Generates 10,000 customers, 100,000 transactions, and 1,000 locations
// - Applies 5 different constraint rules that test various engine features:
//   * Simple filtering and scoring
//   * Aggregation with groupBy operations  
//   * Complex joins between multiple fact types
//   * Conditional existence checks (if_not_exists)
// - Measures performance metrics: setup time, processing time, throughput
// - Reports constraint violations and final score
//
// ## Expected Performance
// On a modern machine, this should process ~50,000-100,000+ facts/second
// with peak memory usage under 1GB, depending on hardware.

// Cargo.toml dependencies needed:
// [dependencies]
// uuid = { version = "1.0", features = ["v4", "v5"] }
// rand = "0.8"
// greynet = { path = "." }  # Adjust path as needed

use greynet::*;
use greynet::constraint_builder::ConstraintBuilder;
use greynet::collectors::Collectors;
use greynet::score::SimpleScore;
use greynet::joiner::JoinerType;
use greynet::fact::GreynetFact;
use greynet::tuple::AnyTuple;

use std::collections::HashMap;
use std::time::Instant;
use std::rc::Rc;
use uuid::Uuid;
use rand::prelude::*;

// --- Data Definitions ---

#[derive(Debug, Clone)]
struct Customer {
    id: u64,
    risk_level: String, // 'low', 'medium', 'high'
    status: String,     // 'active', 'inactive'
}

impl Customer {
    fn new(id: u64, risk_level: String, status: String) -> Self {
        Self { id, risk_level, status }
    }
}

impl GreynetFact for Customer {
    fn fact_id(&self) -> Uuid {
        // Create deterministic UUID based on customer data
        let namespace = Uuid::parse_str("12345678-1234-5678-9012-123456789abc").unwrap();
        Uuid::new_v5(&namespace, format!("customer_{}", self.id).as_bytes())
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        if let Some(other_customer) = other.as_any().downcast_ref::<Customer>() {
            self.id == other_customer.id
        } else {
            false
        }
    }

    fn hash_fact(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.id.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone)]
struct Transaction {
    id: u64,
    customer_id: u64,
    amount: f64,
    location: String,
}

impl Transaction {
    fn new(id: u64, customer_id: u64, amount: f64, location: String) -> Self {
        Self { id, customer_id, amount, location }
    }
}

impl GreynetFact for Transaction {
    fn fact_id(&self) -> Uuid {
        let namespace = Uuid::parse_str("87654321-4321-8765-2109-876543210fed").unwrap();
        Uuid::new_v5(&namespace, format!("transaction_{}", self.id).as_bytes())
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        if let Some(other_tx) = other.as_any().downcast_ref::<Transaction>() {
            self.id == other_tx.id
        } else {
            false
        }
    }

    fn hash_fact(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.id.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone)]
struct SecurityAlert {
    location: String,
    severity: u32, // 1 to 5
}

impl SecurityAlert {
    fn new(location: String, severity: u32) -> Self {
        Self { location, severity }
    }
}

impl GreynetFact for SecurityAlert {
    fn fact_id(&self) -> Uuid {
        let namespace = Uuid::parse_str("abcdef12-3456-7890-abcd-ef1234567890").unwrap();
        Uuid::new_v5(&namespace, format!("alert_{}_{}", self.location, self.severity).as_bytes())
    }

    fn clone_fact(&self) -> Box<dyn GreynetFact> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn eq_fact(&self, other: &dyn GreynetFact) -> bool {
        if let Some(other_alert) = other.as_any().downcast_ref::<SecurityAlert>() {
            self.location == other_alert.location && self.severity == other_alert.severity
        } else {
            false
        }
    }

    fn hash_fact(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.location.hash(&mut hasher);
        self.severity.hash(&mut hasher);
        hasher.finish()
    }
}

// --- Constraint Definitions ---

fn define_constraints(builder: &ConstraintBuilder<SimpleScore>) {
    // Constraint 1: Simple filter for high-value transactions
    builder.constraint("high_value_transaction", 1.0, || {
        builder.for_each::<Transaction>()
            .filter(Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                        tx.amount > 45000.0
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
                        SimpleScore::new(tx.amount / 1000.0)
                    } else {
                        SimpleScore::new(0.0)
                    }
                } else {
                    SimpleScore::new(0.0)
                }
            })
    });

    // Constraint 2: Group transactions by customer and check for excessive activity
    builder.constraint("excessive_transactions_per_customer", 1.0, || {
        builder.for_each::<Transaction>()
            .group_by(
                Rc::new(|tuple: &AnyTuple| {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                            tx.customer_id
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }),
                Collectors::count()
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
            })
    });

    // Constraint 3: Join transactions with security alerts on location
    builder.constraint("transaction_in_alerted_location", 1.0, || {
        builder.for_each::<Transaction>()
            .join(
                builder.for_each::<SecurityAlert>(),
                JoinerType::Equal,  
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                            use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
                            let mut hasher = DefaultHasher::new();
                            tx.location.hash(&mut hasher);
                            hasher.finish()
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }),
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(alert) = uni_tuple.fact_a.as_any().downcast_ref::<SecurityAlert>() {
                            use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
                            let mut hasher = DefaultHasher::new();
                            alert.location.hash(&mut hasher);
                            hasher.finish()
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                })
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
            })
    });

    // Constraint 4: Join to find transactions from inactive customers
    builder.constraint("inactive_customer_transaction", 1.0, || {
        builder.for_each::<Customer>()
            .filter(Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                        customer.status == "inactive"
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
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                            customer.id
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }),
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                            tx.customer_id
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                })
            )
            .penalize("inactive_customer_transaction", |_tuple: &AnyTuple| {
                SimpleScore::new(500.0)
            })
    });

    // Constraint 5: Complex rule using if_not_exists
    // Penalize if a high-risk customer has a transaction in a location
    // that does NOT have a security alert
    builder.constraint("high_risk_transaction_without_alert", 1.0, || {
        builder.for_each::<Customer>()
            .filter(Rc::new(|tuple: &AnyTuple| {
                if let AnyTuple::Uni(uni_tuple) = tuple {
                    if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                        customer.risk_level == "high"
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
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(customer) = uni_tuple.fact_a.as_any().downcast_ref::<Customer>() {
                            customer.id
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }),
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(tx) = uni_tuple.fact_a.as_any().downcast_ref::<Transaction>() {
                            tx.customer_id
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                })
            )
            .if_not_exists(
                builder.for_each::<SecurityAlert>(),
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Bi(bi_tuple) = tuple {
                        if let Some(tx) = bi_tuple.fact_b.as_any().downcast_ref::<Transaction>() {
                            use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
                            let mut hasher = DefaultHasher::new();
                            tx.location.hash(&mut hasher);
                            hasher.finish()
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }),
                Rc::new(|tuple: &AnyTuple| -> u64 {
                    if let AnyTuple::Uni(uni_tuple) = tuple {
                        if let Some(alert) = uni_tuple.fact_a.as_any().downcast_ref::<SecurityAlert>() {
                            use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
                            let mut hasher = DefaultHasher::new();
                            alert.location.hash(&mut hasher);
                            hasher.finish()
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                })
            )
            .penalize("high_risk_transaction_without_alert", |_tuple: &AnyTuple| {
                SimpleScore::new(1000.0)
            })
    });
}

// --- Data Generation ---

struct TestData {
    customers: Vec<Customer>,
    transactions: Vec<Transaction>,
    alerts: Vec<SecurityAlert>,
}

fn generate_data(num_customers: usize, num_transactions: usize, num_locations: usize) -> TestData {
    println!("Generating test data...");
    let mut rng = rand::thread_rng();
    
    let locations: Vec<String> = (0..num_locations)
        .map(|i| format!("location_{}", i))
        .collect();
    
    let customers: Vec<Customer> = (0..num_customers)
        .map(|i| {
            let risk_level = ["low", "medium", "high"].choose(&mut rng).unwrap().to_string();
            let status = if rng.gen_bool(0.95) { "active" } else { "inactive" }.to_string();
            Customer::new(i as u64, risk_level, status)
        })
        .collect();
    
    let transactions: Vec<Transaction> = (0..num_transactions)
        .map(|i| {
            let customer_id = rng.gen_range(0..num_customers) as u64;
            let amount = rng.gen_range(1.0..50000.0);
            let location = locations.choose(&mut rng).unwrap().clone();
            Transaction::new(i as u64, customer_id, amount, location)
        })
        .collect();
    
    // Create alerts for a subset of locations
    let num_alerted_locations = std::cmp::max(1, num_locations / 4);
    let alerted_locations: Vec<String> = locations
        .choose_multiple(&mut rng, num_alerted_locations)
        .cloned()
        .collect();
    
    let alerts: Vec<SecurityAlert> = alerted_locations
        .into_iter()
        .map(|loc| {
            let severity = rng.gen_range(1..=5);
            SecurityAlert::new(loc, severity)
        })
        .collect();
    
    TestData {
        customers,
        transactions,
        alerts,
    }
}

// --- Main Test Runner ---

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("### Starting Rule Engine Stress Test (Rust Version) ###");
    
    // Configuration
    const NUM_CUSTOMERS: usize = 10_000;
    const NUM_TRANSACTIONS: usize = 100_000;
    const NUM_LOCATIONS: usize = 1_000;
    
    // 1. Setup Phase & Initial State
    let time_start_setup = Instant::now();
    let builder = ConstraintBuilder::<SimpleScore>::new();
    define_constraints(&builder);
    let mut session = builder.build();
    let time_end_setup = Instant::now();
    
    // 2. Data Generation Phase
    let time_start_data = Instant::now();
    let data = generate_data(NUM_CUSTOMERS, NUM_TRANSACTIONS, NUM_LOCATIONS);
    let total_facts = data.customers.len() + data.transactions.len() + data.alerts.len();
    let time_end_data = Instant::now();
    
    // 3. Processing Phase
    println!("Inserting facts and processing rules...");
    let time_start_processing = Instant::now();
    
    // Insert all facts
    for customer in &data.customers {
        session.insert(customer.clone())?;
    }
    for transaction in &data.transactions {
        session.insert(transaction.clone())?;
    }
    for alert in &data.alerts {
        session.insert(alert.clone())?;
    }
    
    let final_score = session.get_score()?;
    let matches = session.get_constraint_matches()?;
    let time_end_processing = Instant::now();
    
    // 4. Calculate durations
    let setup_duration = time_end_setup.duration_since(time_start_setup);
    let data_gen_duration = time_end_data.duration_since(time_start_data);
    let processing_duration = time_end_processing.duration_since(time_start_processing);
    let total_duration = time_end_processing.duration_since(time_start_setup);
    
    // 5. Performance Metrics
    let facts_per_second = if processing_duration.as_secs_f64() > 0.0 {
        total_facts as f64 / processing_duration.as_secs_f64()
    } else {
        f64::INFINITY
    };
    
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
    
    // Memory usage (basic process memory info)
    if let Ok(usage) = get_memory_usage() {
        println!("\n#### Memory Usage Summary");
        println!("| Metric                         | Value               |");
        println!("|--------------------------------|---------------------|");
        println!("| Process Peak Memory Usage      | {:.2} MB        |", usage / 1024.0 / 1024.0);
    }
    
    println!("\n#### Engine Output");
    println!("- **Final Score:** {:?}", final_score);
    let total_matches: usize = matches.values().map(|v| v.len()).sum();
    println!("- **Total Constraint Matches:** {}", total_matches);
    
    let mut sorted_matches: Vec<_> = matches.iter().collect();
    sorted_matches.sort_by_key(|&(k, _)| k);
    
    for (constraint_id, match_list) in sorted_matches {
        println!("  - `{}`: {} matches", constraint_id, match_list.len());
    }
    
    Ok(())
}

// Helper function to get memory usage (simplified)
fn get_memory_usage() -> Result<f64, Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        let contents = fs::read_to_string("/proc/self/status")?;
        for line in contents.lines() {
            if line.starts_with("VmPeak:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let kb: f64 = parts[1].parse()?;
                    return Ok(kb * 1024.0); // Convert KB to bytes
                }
            }
        }
    }
    
    #[cfg(target_os = "windows")]
    {
        // On Windows, we'll use a simpler approach
        // This is a placeholder - in practice you might use Windows APIs
        return Ok(0.0);
    }
    
    #[cfg(target_os = "macos")]
    {
        // On macOS, we'll use a simpler approach
        // This is a placeholder - in practice you might use system APIs
        return Ok(0.0);
    }
    
    Ok(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fact_creation() {
        let customer = Customer::new(1, "high".to_string(), "active".to_string());
        let transaction = Transaction::new(1, 1, 50000.0, "location_1".to_string());
        let alert = SecurityAlert::new("location_1".to_string(), 5);
        
        // Test that facts have unique IDs
        assert_ne!(customer.fact_id(), transaction.fact_id());
        assert_ne!(customer.fact_id(), alert.fact_id());
        assert_ne!(transaction.fact_id(), alert.fact_id());
    }
    
    #[test]
    fn test_constraint_system() {
        let builder = ConstraintBuilder::<SimpleScore>::new();
        define_constraints(&builder);
        let mut session = builder.build();
        
        // Add some test data
        let customer = Customer::new(1, "high".to_string(), "active".to_string());
        let transaction = Transaction::new(1, 1, 50000.0, "location_1".to_string());
        
        session.insert(customer).expect("Failed to insert customer");
        session.insert(transaction).expect("Failed to insert transaction");
        
        let score = session.get_score().expect("Failed to get score");
        println!("Test score: {:?}", score);
        
        // The score should be non-zero since we have a high-value transaction
        assert!(score.simple_value > 0.0);
    }
}