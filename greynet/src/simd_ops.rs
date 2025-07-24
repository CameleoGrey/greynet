//! SIMD optimizations for bulk operations
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use crate::state::TupleState;
use crate::score::Score;

/// SIMD-optimized bulk tuple state operations
pub struct SimdOps;

impl SimdOps {
    /// Count tuples with specific state using SIMD when available
    #[inline]
    pub fn count_matching_tuples_simd(tuples: &[TupleState], target_state: TupleState) -> usize {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if is_x86_feature_detected!("avx2") {
                return Self::count_matching_tuples_avx2(tuples, target_state);
            }
            if is_x86_feature_detected!("sse2") {
                return Self::count_matching_tuples_sse2(tuples, target_state);
            }
        }
        
        // Fallback to scalar implementation
        tuples.iter().filter(|&&s| s == target_state).count()
    }

    /// Bulk update tuple states using SIMD
    #[inline]
    pub fn bulk_update_states_simd(
        tuples: &mut [TupleState], 
        from_state: TupleState, 
        to_state: TupleState
    ) -> usize {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if is_x86_feature_detected!("avx2") {
                return Self::bulk_update_states_avx2(tuples, from_state, to_state);
            }
        }
        
        // Fallback to scalar
        let mut count = 0;
        for tuple_state in tuples.iter_mut() {
            if *tuple_state == from_state {
                *tuple_state = to_state;
                count += 1;
            }
        }
        count
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn count_matching_tuples_avx2(tuples: &[TupleState], target_state: TupleState) -> usize {
        let target_byte = target_state as u8;
        let target_vec = _mm256_set1_epi8(target_byte as i8);
        let mut count = 0usize;
        
        let chunks = tuples.len() / 32;
        let remainder = tuples.len() % 32;
        
        // Process 32 bytes at a time with AVX2
        for i in 0..chunks {
            let offset = i * 32;
            let data_ptr = tuples.as_ptr().add(offset) as *const u8;
            let data_vec = _mm256_loadu_si256(data_ptr as *const __m256i);
            
            let cmp_result = _mm256_cmpeq_epi8(data_vec, target_vec);
            let mask = _mm256_movemask_epi8(cmp_result) as u32;
            count += mask.count_ones() as usize;
        }
        
        // Handle remainder with scalar code
        for i in (chunks * 32)..(chunks * 32 + remainder) {
            if tuples[i] == target_state {
                count += 1;
            }
        }
        
        count
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse2")]
    unsafe fn count_matching_tuples_sse2(tuples: &[TupleState], target_state: TupleState) -> usize {
        let target_byte = target_state as u8;
        let target_vec = _mm_set1_epi8(target_byte as i8);
        let mut count = 0usize;
        
        let chunks = tuples.len() / 16;
        let remainder = tuples.len() % 16;
        
        // Process 16 bytes at a time with SSE2
        for i in 0..chunks {
            let offset = i * 16;
            let data_ptr = tuples.as_ptr().add(offset) as *const u8;
            let data_vec = _mm_loadu_si128(data_ptr as *const __m128i);
            
            let cmp_result = _mm_cmpeq_epi8(data_vec, target_vec);
            let mask = _mm_movemask_epi8(cmp_result) as u32;
            count += mask.count_ones() as usize;
        }
        
        // Handle remainder
        for i in (chunks * 16)..(chunks * 16 + remainder) {
            if tuples[i] == target_state {
                count += 1;
            }
        }
        
        count
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn bulk_update_states_avx2(
        tuples: &mut [TupleState], 
        from_state: TupleState, 
        to_state: TupleState
    ) -> usize {
        let from_byte = from_state as u8;
        let to_byte = to_state as u8;
        let from_vec = _mm256_set1_epi8(from_byte as i8);
        let to_vec = _mm256_set1_epi8(to_byte as i8);
        let mut count = 0usize;
        
        let chunks = tuples.len() / 32;
        let remainder = tuples.len() % 32;
        
        for i in 0..chunks {
            let offset = i * 32;
            let data_ptr = tuples.as_mut_ptr().add(offset) as *mut u8;
            let data_vec = _mm256_loadu_si256(data_ptr as *const __m256i);
            
            let mask_vec = _mm256_cmpeq_epi8(data_vec, from_vec);
            let mask = _mm256_movemask_epi8(mask_vec) as u32;
            count += mask.count_ones() as usize;
            
            // Conditionally update matching elements
            let updated_vec = _mm256_blendv_epi8(data_vec, to_vec, mask_vec);
            _mm256_storeu_si256(data_ptr as *mut __m256i, updated_vec);
        }
        
        // Handle remainder
        for i in (chunks * 32)..(chunks * 32 + remainder) {
            if tuples[i] == from_state {
                tuples[i] = to_state;
                count += 1;
            }
        }
        
        count
    }
}

/// SIMD-optimized score operations
impl SimdOps {
    /// Sum scores using SIMD for f64 arrays
    #[inline]
    pub fn sum_scores_simd(scores: &[f64]) -> f64 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if is_x86_feature_detected!("avx") {
                return Self::sum_scores_avx(scores);
            }
            if is_x86_feature_detected!("sse2") {
                return Self::sum_scores_sse2(scores);
            }
        }
        
        // Fallback
        scores.iter().sum()
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx")]
    unsafe fn sum_scores_avx(scores: &[f64]) -> f64 {
        let mut sum_vec = _mm256_setzero_pd();
        let chunks = scores.len() / 4;
        let remainder = scores.len() % 4;
        
        // Process 4 f64 values at a time
        for i in 0..chunks {
            let offset = i * 4;
            let data_ptr = scores.as_ptr().add(offset);
            let data_vec = _mm256_loadu_pd(data_ptr);
            sum_vec = _mm256_add_pd(sum_vec, data_vec);
        }
        
        // Horizontal sum of the 4 elements in sum_vec
        let sum_high = _mm256_extractf128_pd(sum_vec, 1);
        let sum_low = _mm256_castpd256_pd128(sum_vec);
        let sum_combined = _mm_add_pd(sum_high, sum_low);
        let sum_final = _mm_hadd_pd(sum_combined, sum_combined);
        
        let mut result = 0.0;
        _mm_store_sd(&mut result, sum_final);
        
        // Add remainder
        for i in (chunks * 4)..(chunks * 4 + remainder) {
            result += scores[i];
        }
        
        result
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse2")]
    unsafe fn sum_scores_sse2(scores: &[f64]) -> f64 {
        let mut sum_vec = _mm_setzero_pd();
        let chunks = scores.len() / 2;
        let remainder = scores.len() % 2;
        
        for i in 0..chunks {
            let offset = i * 2;
            let data_ptr = scores.as_ptr().add(offset);
            let data_vec = _mm_loadu_pd(data_ptr);
            sum_vec = _mm_add_pd(sum_vec, data_vec);
        }
        
        let sum_final = _mm_hadd_pd(sum_vec, sum_vec);
        let mut result = 0.0;
        _mm_store_sd(&mut result, sum_final);
        
        // Add remainder
        for i in (chunks * 2)..(chunks * 2 + remainder) {
            result += scores[i];
        }
        
        result
    }
}