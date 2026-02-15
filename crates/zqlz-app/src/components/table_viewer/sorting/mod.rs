//! Sorting module for table viewer
//!
//! Provides multi-column sorting with configurable null handling.

mod multi_sort;

pub use multi_sort::{MultiColumnSort, NullPosition, SortColumn};
