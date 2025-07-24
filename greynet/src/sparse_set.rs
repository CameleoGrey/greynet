//sparse_set.rs

#[derive(Debug)]
pub struct SparseSet<T> {
    pub dense: Vec<(usize, T)>,
    sparse: Vec<Option<usize>>,
}

impl<T: Copy> SparseSet<T> {
    pub fn new() -> Self {
        Self {
            dense: Vec::new(),
            sparse: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            dense: Vec::with_capacity(capacity),
            sparse: Vec::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, index: usize, value: T) {
        if index >= self.sparse.len() {
            self.sparse.resize(index + 1, None);
        }

        if let Some(dense_idx) = self.sparse[index] {
            self.dense[dense_idx].1 = value;
        } else {
            let dense_idx = self.dense.len();
            self.dense.push((index, value));
            self.sparse[index] = Some(dense_idx);
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<T> {
        self.sparse
            .get(index)
            .and_then(|&dense_idx| dense_idx)
            .map(|idx| self.dense[idx].1)
    }

    #[inline]
    pub fn remove(&mut self, index: usize) {
        if let Some(Some(dense_idx)) = self.sparse.get(index).copied() {
            let last_idx = self.dense.len() - 1;
            if dense_idx < last_idx {
                let (last_sparse_idx, _) = self.dense[last_idx];
                self.dense.swap(dense_idx, last_idx);
                self.sparse[last_sparse_idx] = Some(dense_idx);
            }
            self.dense.pop();
            self.sparse[index] = None;
        }
    }

    pub fn clear(&mut self) {
        self.dense.clear();
        self.sparse.clear();
    }
}
