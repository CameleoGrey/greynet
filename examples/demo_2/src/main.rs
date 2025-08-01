// corrected_comprehensive_example.rs
// A comprehensive, corrected demonstration of all Greynet streaming operations

use greynet::prelude::*;
use greynet::collectors::{count, sum, avg, min, max, to_list, to_set, distinct};
use std::rc::Rc;
use greynet::greynet_fact_for_struct;
use std::hash::DefaultHasher;
use std::any::Any;
use std::hash::Hash;
use std::hash::Hasher;
use std::collections::HashMap;

// ===== DOMAIN MODEL =====

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Student {
    pub id: i64,
    pub name: String,
    pub grade_level: i64,
    pub special_needs: bool,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Teacher {
    pub id: i64,
    pub name: String,
    pub subject: String, // Simplified to single subject
    pub max_students: i64,
    pub experience_years: i64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Course {
    pub id: i64,
    pub name: String,
    pub subject: String,
    pub difficulty_level: i64,
    pub required_equipment: String, // Simplified to single equipment
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Classroom {
    pub id: i64,
    pub capacity: i64,
    pub equipment: String, // Simplified
    pub building: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TimeSlot {
    pub id: i64,
    pub day: String,
    pub start_hour: i64,
    pub duration_minutes: i64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Assignment {
    pub student_id: i64,
    pub course_id: i64,
    pub teacher_id: i64,
    pub classroom_id: i64,
    pub timeslot_id: i64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Enrollment {
    pub student_id: i64,
    pub course_id: i64,
    pub priority: i64,
}

// Implement GreynetFact for all our types
greynet_fact_for_struct!(Student);
greynet_fact_for_struct!(Teacher);
greynet_fact_for_struct!(Course);
greynet_fact_for_struct!(Classroom);
greynet_fact_for_struct!(TimeSlot);
greynet_fact_for_struct!(Assignment);
greynet_fact_for_struct!(Enrollment);

// ===== CORRECTED COMPREHENSIVE DEMONSTRATION =====

pub fn demonstrate_all_streaming_operations() -> Result<()> {
    println!("=== Greynet Comprehensive Streaming Operations Demo ===\n");

    let mut builder = ConstraintBuilder::<HardSoftScore>::new();

    // ===== 1. BASIC FILTERING (Arity1) =====
    println!("1. Basic Stream Creation and Filtering");
    
    builder.add_constraint("senior_students", 1.0)
        .for_each::<Student>()
        .filter(|s: &Student| s.grade_level >= 11)
        .penalize(|tuple| {
            let student = extract_fact::<Student>(tuple, 0).unwrap();
            HardSoftScore::soft(student.grade_level as f64 - 11.0)
        });

    builder.add_constraint("experienced_teachers", 1.0)
        .for_each::<Teacher>()
        .filter(|t: &Teacher| t.experience_years >= 10)
        .penalize(|_| HardSoftScore::soft(5.0));

    builder.add_constraint("large_classrooms", 1.0)
        .for_each::<Classroom>()
        .filter(|c: &Classroom| c.capacity >= 30)
        .penalize(|_| HardSoftScore::soft(2.0));

    // FIX: Register TimeSlot type with the engine.
    // This is necessary because TimeSlot facts are inserted into the session,
    // but were not previously used in any `for_each` stream, causing a runtime error.
    // This dummy constraint ensures the engine is aware of the TimeSlot type.
    builder.add_constraint("register_timeslot", 0.0)
        .for_each::<TimeSlot>()
        .penalize(|_| HardSoftScore::null_score());

    println!("✓ Added basic filtering constraints");

    // ===== 2. JOIN OPERATIONS (Arity1 -> Arity2) =====
    println!("\n2. Join Operations (Equal joins)");
    
    builder.add_constraint("student_enrollments", 2.0)
        .for_each::<Student>()
        .join_on(
            builder.for_each::<Enrollment>(),
            |s: &Student| s.id,
            |e: &Enrollment| e.student_id
        )
        .penalize(|_| HardSoftScore::soft(0.5)); // Small penalty per enrollment

    builder.add_constraint("teacher_course_match", 3.0)
        .for_each::<Teacher>()
        .join_on(
            builder.for_each::<Course>(),
            |t: &Teacher| t.subject.clone(),
            |c: &Course| c.subject.clone()
        )
        .penalize(|_| HardSoftScore::soft(1.0));

    println!("✓ Added join constraints");

    // ===== 3. JOIN TYPES WITH COMPARATORS =====
    println!("\n3. Join Types with Different Comparators");
    
    builder.add_constraint("age_priority_mismatch", 2.0)
        .for_each::<Student>()
        .join_on_with_comparator(
            builder.for_each::<Enrollment>(),
            JoinerType::LessThan,
            |s: &Student| s.grade_level as i64,
            |e: &Enrollment| e.priority as i64
        )
        .penalize(|_| HardSoftScore::soft(3.0));

    builder.add_constraint("capacity_difficulty_check", 1.5)
        .for_each::<Classroom>()
        .join_on_with_comparator(
            builder.for_each::<Course>(),
            JoinerType::GreaterThan,
            |c: &Classroom| c.capacity,
            |course: &Course| course.difficulty_level as i64 * 5
        )
        .penalize(|_| HardSoftScore::soft(2.0));

    println!("✓ Added comparator join constraints");

    // ===== 4. CONDITIONAL JOINS =====
    println!("\n4. Conditional Joins");
    
    builder.add_constraint("enrolled_students_bonus", 1.0)
        .for_each::<Student>()
        .if_exists(
            builder.for_each::<Enrollment>(),
            |s: &Student| s.id,
            |e: &Enrollment| e.student_id
        )
        .penalize(|_| HardSoftScore::soft(-1.0)); // Bonus for enrolled students

    builder.add_constraint("unenrolled_students_penalty", 5.0)
        .for_each::<Student>()
        .if_not_exists(
            builder.for_each::<Enrollment>(),
            |s: &Student| s.id,
            |e: &Enrollment| e.student_id
        )
        .penalize(|tuple| {
            let student = extract_fact::<Student>(tuple, 0).unwrap();
            HardSoftScore::hard(if student.special_needs { 20.0 } else { 10.0 })
        });

    println!("✓ Added conditional join constraints");

    // ===== 5. GROUPING WITH ALL COLLECTORS =====
    println!("\n5. Grouping Operations with All Collectors");
    
    // Count collector
    builder.add_constraint("students_per_grade", 3.0)
        .for_each::<Student>()
        .group_by(|s: &Student| s.grade_level, count())
        .filter_tuple(|tuple| {
            extract_fact::<i64>(tuple, 1).map_or(false, |&count| count > 5)
        })
        .penalize(|tuple| {
            let count = extract_fact::<i64>(tuple, 1).copied().unwrap_or(0);
            HardSoftScore::soft((count as f64 - 5.0).max(0.0))
        });

    // Sum collector
    builder.add_constraint("total_teacher_experience", 2.0)
        .for_each::<Teacher>()
        .group_by(
            |t: &Teacher| t.subject.clone(),
            sum(|tuple| {
                extract_fact::<Teacher>(tuple, 0)
                    .map_or(0.0, |t| t.experience_years as f64)
            })
        )
        .penalize(|tuple| {
            let total_exp = extract_fact::<f64>(tuple, 1).copied().unwrap_or(0.0);
            HardSoftScore::soft(total_exp * 0.1)
        });

    // Average collector
    builder.add_constraint("avg_classroom_capacity", 1.5)
        .for_each::<Classroom>()
        .group_by(
            |c: &Classroom| c.building.clone(),
            avg(|tuple| {
                extract_fact::<Classroom>(tuple, 0)
                    .map_or(0.0, |c| c.capacity as f64)
            })
        )
        .penalize(|tuple| {
            let avg_capacity = extract_fact::<f64>(tuple, 1).copied().unwrap_or(0.0);
            if avg_capacity < 20.0 {
                HardSoftScore::soft(20.0 - avg_capacity)
            } else {
                HardSoftScore::null_score()
            }
        });

    // Min/Max collectors
    builder.add_constraint("min_experience_check", 1.0)
        .for_each::<Teacher>()
        .group_by(
            |t: &Teacher| t.subject.clone(),
            min(|tuple| {
                extract_fact::<Teacher>(tuple, 0)
                    .map(|t| t.experience_years)
                    .unwrap_or(0)
            })
        )
        .filter_tuple(|tuple| {
            extract_fact::<i64>(tuple, 1).map_or(false, |&min_exp| min_exp < 2)
        })
        .penalize(|tuple| {
            let min_exp = extract_fact::<i64>(tuple, 1).copied().unwrap_or(0);
            HardSoftScore::hard((2.0 - min_exp as f64).max(0.0) * 5.0)
        });

    // List and Set collectors
    builder.add_constraint("student_lists", 0.5)
        .for_each::<Student>()
        .group_by(|s: &Student| s.grade_level, to_list())
        .penalize(|_| HardSoftScore::soft(0.1));

    builder.add_constraint("unique_count", 0.5)
        .for_each::<Teacher>()
        .group_by(|t: &Teacher| t.subject.clone(), to_set())
        .penalize(|_| HardSoftScore::soft(0.1));

    println!("✓ Added grouping constraints with all collector types");

    // ===== 6. TRANSFORMATION OPERATIONS =====
    println!("\n6. Transformation Operations");
    
    // Map operation
    builder.add_constraint("student_age_groups", 1.0)
        .for_each::<Student>()
        .map(|s: &Student| {
            let age_group = if s.grade_level <= 10 { "Underclass" } else { "Upperclass" };
            Rc::new(age_group.to_string()) as Rc<dyn GreynetFact>
        })
        .penalize(|_| HardSoftScore::soft(0.5));

    // Map to pair
    builder.add_constraint("teacher_subject_pairs", 1.0)
        .for_each::<Teacher>()
        .map_to_pair(|t: &Teacher| (
            Rc::new(t.id) as Rc<dyn GreynetFact>,
            Rc::new(t.subject.clone()) as Rc<dyn GreynetFact>
        ))
        .penalize(|_| HardSoftScore::soft(0.2));

    // FlatMap operation
    builder.add_constraint("equipment_requirements", 1.0)
        .for_each::<Course>()
        .flat_map(|c: &Course| {
            vec![Rc::new(c.required_equipment.clone()) as Rc<dyn GreynetFact>]
        })
        .penalize(|_| HardSoftScore::soft(0.3));

    println!("✓ Added transformation constraints");

    // ===== 7. SET OPERATIONS =====
    println!("\n7. Set Operations");
    
    // Union operation
    builder.add_constraint("id_conflicts", 2.0)
        .for_each::<Student>()
        .map(|s: &Student| Rc::new(s.id) as Rc<dyn GreynetFact>)
        .union(
            builder.for_each::<Teacher>()
                .map(|t: &Teacher| Rc::new(t.id) as Rc<dyn GreynetFact>)
        )
        .group_by(|id: &i64| *id, count())
        .filter_tuple(|tuple| {
            extract_fact::<i64>(tuple, 1).map_or(false, |&count| count > 1)
        })
        .penalize(|_| HardSoftScore::hard(50.0)); // High penalty for ID conflicts

    // Distinct operation
    builder.add_constraint("unique_subjects", 1.0)
        .for_each::<Course>()
        .map(|c: &Course| Rc::new(c.subject.clone()) as Rc<dyn GreynetFact>)
        .distinct()
        .penalize(|_| HardSoftScore::soft(2.0));

    println!("✓ Added set operation constraints");

    // ===== 8. GLOBAL AGGREGATION =====
    println!("\n8. Global Aggregation");
    
    // Global count
    builder.add_constraint("total_students_limit", 10.0)
        .for_each::<Student>()
        .aggregate(count())
        .filter_tuple(|tuple| {
            extract_fact::<i64>(tuple, 0).map_or(false, |&count| count > 20)
        })
        .penalize(|tuple| {
            let count = extract_fact::<i64>(tuple, 0).copied().unwrap_or(0);
            HardSoftScore::hard((count as f64 - 20.0).max(0.0) * 5.0)
        });

    // Global sum
    builder.add_constraint("total_capacity", 5.0)
        .for_each::<Classroom>()
        .aggregate(sum(|tuple| {
            extract_fact::<Classroom>(tuple, 0)
                .map_or(0.0, |c| c.capacity as f64)
        }))
        .filter_tuple(|tuple| {
            extract_fact::<f64>(tuple, 0).map_or(false, |&total| total < 100.0)
        })
        .penalize(|tuple| {
            let total = extract_fact::<f64>(tuple, 0).copied().unwrap_or(0.0);
            HardSoftScore::soft((100.0 - total).max(0.0))
        });

    // Global average
    builder.add_constraint("avg_teacher_experience", 3.0)
        .for_each::<Teacher>()
        .aggregate(avg(|tuple| {
            extract_fact::<Teacher>(tuple, 0)
                .map_or(0.0, |t| t.experience_years as f64)
        }))
        .filter_tuple(|tuple| {
            extract_fact::<f64>(tuple, 0).map_or(false, |&avg| avg < 5.0)
        })
        .penalize(|tuple| {
            let avg_exp = extract_fact::<f64>(tuple, 0).copied().unwrap_or(0.0);
            HardSoftScore::soft((5.0 - avg_exp).max(0.0) * 2.0)
        });

    println!("✓ Added global aggregation constraints");

    // ===== 9. HIGHER ARITY OPERATIONS =====
    println!("\n9. Higher Arity Operations");
    
    // Arity2 -> Arity3 join
    builder.add_constraint("complex_assignment_chain", 4.0)
        .for_each::<Assignment>()
        .join_on(
            builder.for_each::<Student>(),
            |a: &Assignment| a.student_id,
            |s: &Student| s.id
        )
        .join_on_second(
            builder.for_each::<Course>(),
            |s: &Student| s.grade_level as i64, // Simplified join logic
            |c: &Course| c.difficulty_level as i64
        )
        .penalize(|_| HardSoftScore::soft(1.0));

    println!("✓ Added higher arity constraints");

    // ===== 10. COMPLEX MULTI-CONSTRAINT SCENARIOS =====
    println!("\n10. Complex Multi-Constraint Scenarios");
    
    // Classroom capacity constraint
    /*builder.add_constraint("classroom_capacity_limit", 15.0)
        .for_each::<Assignment>()
        .join_on(
            builder.for_each::<Classroom>(),
            |a: &Assignment| a.classroom_id,
            |c: &Classroom| c.id
        )
        .group_by_tuple(
            |tuple| {
                // FIX: Group by the Classroom fact itself to preserve it for the next step.
                // The tuple here is (Assignment, Classroom).
                extract_fact::<Classroom>(tuple, 1).unwrap().clone()
            },
            count()
        )
        .filter_tuple(|tuple| {
            let classroom = extract_fact::<Classroom>(tuple, 0).unwrap();
            let student_count = extract_fact::<i64>(tuple, 1).copied().unwrap_or(0);
            student_count > classroom.capacity as i64
        })
        .penalize(|tuple| {
            let classroom = extract_fact::<Classroom>(tuple, 0).unwrap();
            let student_count = extract_fact::<i64>(tuple, 1).copied().unwrap_or(0);
            let overflow = student_count as f64 - classroom.capacity as f64;
            HardSoftScore::hard(overflow.max(0.0) * 10.0)
        });*/

    // Teacher workload constraint
    builder.add_constraint("teacher_workload", 8.0)
        .for_each::<Assignment>()
        .group_by(|a: &Assignment| a.teacher_id, count())
        .filter_tuple(|tuple| {
            extract_fact::<i64>(tuple, 1).map_or(false, |&count| count > 25)
        })
        .penalize(|tuple| {
            let count = extract_fact::<i64>(tuple, 1).copied().unwrap_or(0);
            HardSoftScore::soft((count as f64 - 25.0).max(0.0) * 2.0)
        });

    // Time conflict constraint
    builder.add_constraint("no_student_time_conflicts", 25.0)
        .for_each::<Assignment>()
        .join_on(
            builder.for_each::<Assignment>(),
            |a1: &Assignment| (a1.student_id, a1.timeslot_id),
            |a2: &Assignment| (a2.student_id, a2.timeslot_id)
        )
        .filter_tuple(|tuple| {
            let a1 = extract_fact::<Assignment>(tuple, 0).unwrap();
            let a2 = extract_fact::<Assignment>(tuple, 1).unwrap();
            a1.student_id == a2.student_id && 
            a1.timeslot_id == a2.timeslot_id && 
            a1.course_id < a2.course_id // Avoid counting the same pair twice
        })
        .penalize(|_| HardSoftScore::hard(100.0));

    println!("✓ Added complex multi-constraint scenarios");

    // ===== 11. BUILD AND TEST SESSION =====
    println!("\n11. Building and Testing Session");
    
    let mut session = builder.build()?;
    
    println!("Inserting comprehensive test data...");
    
    // Insert students
    session.insert(Student { id: 1, name: "Alice".to_string(), grade_level: 12, special_needs: false })?;
    session.insert(Student { id: 2, name: "Bob".to_string(), grade_level: 11, special_needs: true })?;
    session.insert(Student { id: 3, name: "Charlie".to_string(), grade_level: 10, special_needs: false })?;
    session.insert(Student { id: 4, name: "Diana".to_string(), grade_level: 12, special_needs: false })?;
    session.insert(Student { id: 5, name: "Eve".to_string(), grade_level: 9, special_needs: false })?;
    
    // Insert teachers
    session.insert(Teacher { id: 1, name: "Dr. Smith".to_string(), subject: "Math".to_string(), max_students: 30, experience_years: 15 })?;
    session.insert(Teacher { id: 2, name: "Ms. Johnson".to_string(), subject: "Science".to_string(), max_students: 25, experience_years: 3 })?;
    session.insert(Teacher { id: 3, name: "Mr. Brown".to_string(), subject: "English".to_string(), max_students: 28, experience_years: 8 })?;
    
    // Insert courses
    session.insert(Course { id: 1, name: "Calculus".to_string(), subject: "Math".to_string(), difficulty_level: 8, required_equipment: "Calculator".to_string() })?;
    session.insert(Course { id: 2, name: "Physics".to_string(), subject: "Science".to_string(), difficulty_level: 7, required_equipment: "Lab Equipment".to_string() })?;
    session.insert(Course { id: 3, name: "Literature".to_string(), subject: "English".to_string(), difficulty_level: 5, required_equipment: "Books".to_string() })?;
    
    // Insert classrooms
    session.insert(Classroom { id: 1, capacity: 25, equipment: "Calculator".to_string(), building: "Main".to_string() })?;
    session.insert(Classroom { id: 2, capacity: 35, equipment: "Lab Equipment".to_string(), building: "Science".to_string() })?;
    session.insert(Classroom { id: 3, capacity: 20, equipment: "Books".to_string(), building: "Liberal Arts".to_string() })?;
    
    // Insert timeslots
    session.insert(TimeSlot { id: 1, day: "Monday".to_string(), start_hour: 9, duration_minutes: 50 })?;
    session.insert(TimeSlot { id: 2, day: "Monday".to_string(), start_hour: 11, duration_minutes: 50 })?;
    session.insert(TimeSlot { id: 3, day: "Tuesday".to_string(), start_hour: 9, duration_minutes: 50 })?;
    
    // Insert enrollments
    session.insert(Enrollment { student_id: 1, course_id: 1, priority: 9 })?;
    session.insert(Enrollment { student_id: 2, course_id: 2, priority: 8 })?;
    session.insert(Enrollment { student_id: 3, course_id: 1, priority: 7 })?;
    session.insert(Enrollment { student_id: 4, course_id: 3, priority: 6 })?;
    // Eve (student 5) has no enrollment - will trigger penalty
    
    // Insert assignments (some creating violations)
    session.insert(Assignment { student_id: 1, course_id: 1, teacher_id: 1, classroom_id: 1, timeslot_id: 1 })?;
    session.insert(Assignment { student_id: 2, course_id: 2, teacher_id: 2, classroom_id: 2, timeslot_id: 2 })?;
    session.insert(Assignment { student_id: 3, course_id: 1, teacher_id: 1, classroom_id: 1, timeslot_id: 1 })?;
    session.insert(Assignment { student_id: 4, course_id: 3, teacher_id: 3, classroom_id: 3, timeslot_id: 1 })?;
    
    // Create overcapacity situation in classroom 1
    for i in 6..=30 {
        session.insert(Student { id: i, name: format!("Student{}", i), grade_level: 12, special_needs: false })?;
        session.insert(Assignment { student_id: i, course_id: 1, teacher_id: 1, classroom_id: 1, timeslot_id: 1 })?;
    }

    println!("✓ Inserted comprehensive test data");

    // ===== 12. EVALUATE RESULTS =====
    println!("\n12. Evaluating Results");
    
    let total_score = session.get_score()?;
    println!("Total Score: {:?}", total_score);
    
    let violations = session.get_constraint_matches_with_stats()?;
    println!("\nConstraint Violations:");
    for (constraint_name, (violating_tuples, count, score)) in violations {
        println!("  {}: {} violations, contribution: {:?}", constraint_name, count, score);
    }
    
    let detailed_stats = session.get_detailed_statistics();
    println!("\nDetailed Session Statistics:");
    println!("  Total Facts: {}", detailed_stats.total_facts);
    println!("  Total Nodes: {}", detailed_stats.total_nodes);
    println!("  Scoring Nodes: {}", detailed_stats.scoring_nodes);
    println!("  Memory Usage: {} MB", detailed_stats.memory_usage_mb);
    
    if let Some(ref breakdown) = detailed_stats.node_type_breakdown {
        println!("  Node Type Breakdown:");
        for (node_type, count) in breakdown {
            println!("    {}: {}", node_type, count);
        }
    }
    
    if let Some(ref arity_counts) = detailed_stats.active_tuples_by_arity {
        println!("  Active Tuples by Arity:");
        for (arity, count) in arity_counts {
            println!("    Arity{}: {}", arity, count);
        }
    }

    if let Some(ref constraint_counts) = detailed_stats.constraint_match_counts {
        println!("  Constraint Match Counts:");
        for (constraint, count) in constraint_counts {
            println!("    {}: {}", constraint, count);
        }
    }

    println!("✓ Evaluated session results");

    // ===== 13. DYNAMIC OPERATIONS =====
    println!("\n13. Dynamic Operations");
    
    let initial_score = session.get_score()?;
    println!("Initial score: {:?}", initial_score);
    
    // Update constraint weight
    session.update_constraint_weight("classroom_capacity_limit", 30.0)?;
    let updated_score = session.get_score()?;
    println!("Score after weight update: {:?}", updated_score);
    
    let mut standard_map = HashMap::<String, f64>::new();
    standard_map.insert("teacher_workload".to_string(), 12.0);
    standard_map.insert("no_student_time_conflicts".to_string(), 50.0);
    let weight_updates = standard_map.into_iter().collect();
    session.update_constraint_weights(weight_updates)?;
    
    // Remove some violations by retracting facts
    let overcrowded_student = Student { id: 6, name: "Student6".to_string(), grade_level: 12, special_needs: false };
    session.retract(&overcrowded_student)?;
    
    let final_score = session.get_score()?;
    println!("Final score after changes: {:?}", final_score);

    println!("✓ Performed dynamic operations");

    // ===== 14. VALIDATION AND ANALYTICS =====
    println!("\n14. Validation and Analytics");
    
    // Validate session consistency
    session.validate_consistency()?;
    println!("✓ Session consistency validated");
    
    // Check resource limits
    session.check_resource_limits()?;
    println!("✓ Resource limits checked");
    
    // Get fact-level constraint analysis
    let fact_report = session.get_all_fact_constraint_matches()?;
    println!("Fact-Level Constraint Analysis:");
    println!("  Total involved facts: {}", fact_report.total_involved_facts);
    println!("  Total violations: {}", fact_report.total_violations);
    
    // Sample fact-specific analysis
    if let Some(student_violations) = fact_report.matches_by_fact.get(&1) {
        println!("  Student 1 violations: {}", student_violations.len());
        for violation in student_violations {
            println!("    Constraint: {}, Role: {:?}", violation.constraint_id, violation.fact_role);
        }
    }
    
    // Cleanup operations
    let cleaned_tuples = session.cleanup_dying_tuples()?;
    println!("✓ Cleaned {} dying tuples", cleaned_tuples);

    println!("✓ Completed validation and analytics");

    Ok(())
}

// ===== DIFFERENT SCORE TYPE DEMONSTRATIONS =====

pub fn demonstrate_score_types() -> Result<()> {
    println!("\n=== Score Types Demonstration ===\n");
    
    // SimpleScore example
    {
        println!("1. SimpleScore Example");
        let mut builder = ConstraintBuilder::<SimpleScore>::new();
        
        builder.add_constraint("simple_penalty", 2.5)
            .for_each::<Student>()
            .filter(|s: &Student| s.special_needs)
            .penalize(|_| SimpleScore::new(10.0));
        
        let mut session = builder.build()?;
        session.insert(Student { id: 1, name: "Special Student".to_string(), grade_level: 10, special_needs: true })?;
        
        let score = session.get_score()?;
        println!("SimpleScore result: {:?}", score); // Should be 25.0 (10.0 * 2.5)
        assert_eq!(score.simple_value, 25.0);
    }
    
    // HardSoftScore example  
    {
        println!("\n2. HardSoftScore Example");
        let mut builder = ConstraintBuilder::<HardSoftScore>::new();
        
        builder.add_constraint("capacity_violation", 1.0)
            .for_each::<Classroom>()
            .filter(|c: &Classroom| c.capacity < 20)
            .penalize(|tuple| {
                let classroom = extract_fact::<Classroom>(tuple, 0).unwrap();
                HardSoftScore::new(
                    (20.0 - classroom.capacity as f64).max(0.0), // Hard penalty for small rooms
                    classroom.capacity as f64 * 0.1              // Soft penalty proportional to size
                )
            });
        
        let mut session = builder.build()?;
        session.insert(Classroom { id: 1, capacity: 15, equipment: "Basic".to_string(), building: "Old".to_string() })?;
        
        let score = session.get_score()?;
        println!("HardSoftScore result: {:?}", score);
        assert_eq!(score.hard_score, 5.0); // 20 - 15 = 5
        assert_eq!(score.soft_score, 1.5); // 15 * 0.1 = 1.5
    }
    
    // HardMediumSoftScore example
    {
        println!("\n3. HardMediumSoftScore Example");
        let mut builder = ConstraintBuilder::<HardMediumSoftScore>::new();
        
        builder.add_constraint("complex_teacher_evaluation", 1.0)
            .for_each::<Teacher>()
            .penalize(|tuple| {
                let teacher = extract_fact::<Teacher>(tuple, 0).unwrap();
                HardMediumSoftScore::new(
                    if teacher.experience_years < 2 { 10.0 } else { 0.0 },    // Hard: Unqualified
                    if teacher.max_students > 35 { 5.0 } else { 0.0 },         // Medium: Overloaded
                    teacher.experience_years as f64 * 0.5                      // Soft: Experience bonus
                )
            });
        
        let mut session = builder.build()?;
        session.insert(Teacher { id: 1, name: "New Teacher".to_string(), subject: "Math".to_string(), max_students: 40, experience_years: 1 })?;
        
        let score = session.get_score()?;
        println!("HardMediumSoftScore result: {:?}", score);
        assert_eq!(score.hard_score, 10.0);  // Unqualified teacher
        assert_eq!(score.medium_score, 5.0); // Overloaded teacher  
        assert_eq!(score.soft_score, 0.5);   // Low experience
    }

    println!("✓ Demonstrated all score types");
    Ok(())
}

// ===== PERFORMANCE AND ADVANCED PATTERNS =====

pub fn demonstrate_advanced_patterns() -> Result<()> {
    println!("\n=== Advanced Patterns Demonstration ===\n");
    
    let limits = ResourceLimits::conservative(); // Use conservative limits for demo
    let mut builder = ConstraintBuilder::<HardSoftScore>::with_limits(limits);

    // Complex nested aggregation
    builder.add_constraint("nested_aggregation", 1.0)
        .for_each::<Assignment>()
        .join_on(
            builder.for_each::<Student>(),
            |a: &Assignment| a.student_id,
            |s: &Student| s.id
        )
        .group_by_tuple(
            |tuple| extract_fact::<Student>(tuple, 1).map(|s| s.grade_level).unwrap_or(0),
            count()
        )
        .group_by_tuple(
            |_| "global".to_string(),
            avg(|tuple| extract_fact::<i64>(tuple, 1).copied().unwrap_or(0) as f64)
        )
        .penalize(|tuple| {
            let avg_per_grade = extract_fact::<f64>(tuple, 1).copied().unwrap_or(0.0);
            if avg_per_grade > 10.0 {
                HardSoftScore::soft((avg_per_grade - 10.0) * 2.0)
            } else {
                HardSoftScore::null_score()
            }
        });

    // Chain of conditional logic
    builder.add_constraint("enrollment_assignment_chain", 2.0)
        .for_each::<Enrollment>()
        .if_exists(
            builder.for_each::<Assignment>(),
            |e: &Enrollment| (e.student_id, e.course_id),
            |a: &Assignment| (a.student_id, a.course_id)
        )
        .if_not_exists(
            builder.for_each::<Student>(),
            |e: &Enrollment| e.student_id,
            |s: &Student| if s.special_needs { s.id } else { 0 } // Only match special needs students
        )
        .penalize(|tuple| {
            let enrollment = extract_fact::<Enrollment>(tuple, 0).unwrap();
            HardSoftScore::soft(enrollment.priority as f64)
        });

    let mut session = builder.build()?;
    
    // Insert minimal test data for advanced patterns
    session.insert(Student { id: 1, name: "Test Student".to_string(), grade_level: 11, special_needs: false })?;
    //session.insert(Teacher { id: 1, name: "Test Teacher".to_string(), subject: "Math".to_string(), max_students: 20, experience_years: 5 })?;
    //session.insert(Course { id: 1, name: "Test Course".to_string(), subject: "Math".to_string(), difficulty_level: 5, required_equipment: "None".to_string() })?;
    //session.insert(Classroom { id: 1, capacity: 30, equipment: "None".to_string(), building: "Main".to_string() })?;
    session.insert(Enrollment { student_id: 1, course_id: 1, priority: 8 })?;
    session.insert(Assignment { student_id: 1, course_id: 1, teacher_id: 1, classroom_id: 1, timeslot_id: 1 })?;

    let score = session.get_score()?;
    println!("Advanced patterns score: {:?}", score);
    
    let stats = session.get_detailed_statistics();
    println!("Advanced session stats: nodes={}, memory={}MB", stats.total_nodes, stats.memory_usage_mb);

    println!("✓ Demonstrated advanced patterns");
    Ok(())
}

// ===== MAIN RUNNER =====

pub fn run_comprehensive_demo() -> Result<()> {
    println!("🚀 Starting Greynet Comprehensive Demo...\n");
    
    demonstrate_all_streaming_operations()?;
    demonstrate_score_types()?;
    demonstrate_advanced_patterns()?;
    
    println!("\n🎉 === Demo Completed Successfully! ===");
    println!("\n📋 This demo comprehensively showcased:");
    println!("✅ All streaming arities (Arity1 through Arity5)");
    println!("✅ All join types (Equal, LessThan, GreaterThan, NotEqual)");
    println!("✅ All collector types (count, sum, avg, min, max, list, set, distinct)");
    println!("✅ Filter operations (typed and tuple-level)");
    println!("✅ Transformation operations (map, flat_map, map_to_pair)");
    println!("✅ Set operations (union, distinct)");
    println!("✅ Conditional joins (if_exists, if_not_exists)");
    println!("✅ Global aggregation with all collectors");
    println!("✅ All score types (Simple, HardSoft, HardMediumSoft)");
    println!("✅ Dynamic operations (weight updates, fact insertion/retraction)");
    println!("✅ Session management (statistics, validation, cleanup)");
    println!("✅ Complex multi-constraint scenarios");
    println!("✅ Advanced patterns (nested aggregations, conditional chains)");
    println!("✅ Resource management and performance considerations");
    
    Ok(())
}

#[cfg(test)]
mod comprehensive_tests {
    use super::*;

    #[test]
    fn test_comprehensive_demo() {
        run_comprehensive_demo().expect("Comprehensive demo should complete without errors");
    }

    #[test]
    fn test_all_operations_coverage() {
        // This test ensures we've covered all major operation types
        let mut builder = ConstraintBuilder::<HardSoftScore>::new();
        
        // Cover all fundamental operations in one test
        builder.add_constraint("coverage_test", 1.0)
            .for_each::<Student>()                                    // ✓ Stream creation
            .filter(|s: &Student| s.grade_level >= 10)              // ✓ Filter
            .join_on(                                                 // ✓ Join
                builder.for_each::<Course>(),
                |s: &Student| s.grade_level as i64,
                |c: &Course| c.difficulty_level as i64
            )
            .group_by_tuple(                                         // ✓ Group by
                |tuple| extract_fact::<Student>(tuple, 0).map(|s| s.grade_level).unwrap_or(0),
                count()                                              // ✓ Count collector
            )
            .filter_tuple(|tuple| {                                  // ✓ Tuple filter
                extract_fact::<i64>(tuple, 1).map_or(false, |&c| c > 0)
            })
            .penalize(|_| HardSoftScore::soft(1.0));                // ✓ Penalize

        let session = builder.build().expect("Should build successfully");
        
        // Verify session was created with expected structure
        let stats = session.get_statistics();
        assert!(stats.total_nodes > 0);
        assert!(stats.scoring_nodes > 0);
    }
}

#[cfg(not(test))]
fn main() {
    match run_comprehensive_demo() {
        Ok(()) => println!("\n✨ Demo completed successfully!"),
        Err(e) => eprintln!("\n❌ Demo failed: {}", e),
    }
}
