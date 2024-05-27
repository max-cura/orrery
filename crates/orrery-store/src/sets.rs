#[derive(Debug, Clone)]
pub struct RowSet {
    pub(crate) table: usize,
    // INVARIANT: sorted
    pub(crate) rows: Vec<usize>,
}
impl RowSet {
    pub fn intersects(&self, rhs: &RowSet) -> bool {
        if self.table != rhs.table {
            return false;
        }
        if self.rows.is_empty() || rhs.rows.is_empty() {
            return false;
        }
        if self.rows.last().unwrap() < rhs.rows.first().unwrap() {
            return false;
        }
        let mut left = self.rows.iter();
        let mut li = left.next().unwrap();
        for rj in rhs.rows.iter() {
            while rj > li {
                li = match left.next() {
                    None => return false,
                    Some(x) => x,
                }
            }
            if rj == li {
                return true;
            }
        }
        false
    }
    pub fn union_(&mut self, rhs: &RowSet) {
        if self.rows.is_empty() {
            return;
        }
        let orig_len = self.rows.len();
        let mut i = 0;
        for rj in rhs.rows.iter() {
            while i < orig_len && rj > &self.rows[i] {
                i += 1;
                self.rows.push(*rj);
            }
            if rj == &self.rows[i] {
                i += 1;
            }
        }
        self.rows.sort_unstable()
    }
    pub fn subtract_(&mut self, rhs: &RowSet) {
        if self.rows.is_empty() || rhs.rows.is_empty() {
            return;
        }
        let mut j = 0;
        self.rows.retain(|&r| {
            while j < rhs.rows.len() && rhs.rows[j] < r {
                j += 1;
            }
            if j >= rhs.rows.len() {
                return true;
            }
            if rhs.rows[j] == r {
                j += 1;
                return false;
            } else {
                true
            }
        });
    }
    pub fn union_nonoverlapping_unchecked_(&mut self, rhs: &RowSet) {
        self.rows.extend_from_slice(&rhs.rows);
        self.rows.sort_unstable();
    }
}

#[derive(Debug, Clone)]
pub struct AccessSet {
    pub(crate) row_sets: Vec<RowSet>,
}

impl AccessSet {
    pub fn empty() -> AccessSet {
        AccessSet {
            row_sets: Vec::new(),
        }
    }
}

impl AccessSet {
    pub fn len(&self) -> usize {
        self.row_sets.iter().map(|rs| rs.rows.len()).sum()
    }
    pub fn intersects(&self, rhs: &AccessSet) -> bool {
        let mut left = self.row_sets.iter();
        let mut lc = left.next().unwrap();
        for rj in rhs.row_sets.iter() {
            while rj.table > lc.table {
                lc = match left.next() {
                    None => return false,
                    Some(x) => x,
                }
            }
            if rj.table == lc.table {
                if rj.intersects(lc) {
                    return true;
                }
            }
        }
        false
    }
    pub fn union_(&mut self, rhs: &AccessSet) {
        if self.row_sets.is_empty() {
            self.row_sets.extend_from_slice(&rhs.row_sets);
        }
        let orig_len = self.row_sets.len();
        let mut i = 0;
        for rj in rhs.row_sets.iter() {
            while i < orig_len && rj.table > self.row_sets[i].table {
                i += 1;
                self.row_sets.push(rj.clone());
            }
            if rj.table == self.row_sets[i].table {
                self.row_sets[i].union_(rj);
            }
        }
        self.row_sets.sort_unstable_by_key(|rs| rs.table);
    }
    pub fn union_nonoverlapping_unchecked_(&mut self, rhs: &AccessSet) {
        if self.row_sets.is_empty() {
            return;
        }
        let orig_len = self.row_sets.len();
        let mut i = 0;
        for rj in rhs.row_sets.iter() {
            while i < orig_len && rj.table > self.row_sets[i].table {
                i += 1;
                self.row_sets.push(rj.clone());
            }
            if rj.table == self.row_sets[i].table {
                self.row_sets[i].union_nonoverlapping_unchecked_(rj);
            }
        }
        self.row_sets.sort_unstable_by_key(|rs| rs.table);
    }
    pub fn subtract_(&mut self, rhs: &AccessSet) {
        if self.row_sets.is_empty() || rhs.row_sets.is_empty() {
            return;
        }
        let mut j = 0;
        self.row_sets.retain_mut(|r| {
            while j < rhs.row_sets.len() && rhs.row_sets[j].table < r.table {
                j += 1;
            }
            if j >= rhs.row_sets.len() {
                return true;
            }
            if rhs.row_sets[j].table == r.table {
                r.subtract_(&rhs.row_sets[j]);
                !r.rows.is_empty()
            } else {
                true
            }
        });
    }
    pub fn iter_keys(&self) -> AccessKeysIter {
        AccessKeysIter {
            inner: self,
            rs: 0,
            r: 0,
        }
    }
    // for debugging
    pub fn to_string(&self) -> String {
        self.iter_keys()
            .map(|(_, b)| b.to_string())
            .intersperse(" ".to_string())
            .collect::<String>()
    }
}

pub struct AccessKeysIter<'a> {
    inner: &'a AccessSet,
    rs: usize,
    r: usize,
}
impl<'a> Iterator for AccessKeysIter<'a> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rs < self.inner.row_sets.len() {
            let rs = &self.inner.row_sets[self.rs];
            let ret = (self.rs, rs.rows[self.r]);
            self.r += 1;
            if self.r >= rs.rows.len() {
                self.r = 0;
                self.rs += 1;
                if self.rs >= self.inner.row_sets.len() {
                    return None;
                }
            }
            Some(ret)
        } else {
            None
        }
    }
}
