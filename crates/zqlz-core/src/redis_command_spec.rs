/// Redis command specification.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RedisCommandSpec {
    /// Command name (uppercase).
    pub name: String,
    /// Arity: positive = exact, negative = minimum (absolute value).
    pub arity: i32,
    /// Command group (string, list, hash, etc.).
    pub group: String,
    /// Brief description.
    pub summary: String,
    /// Whether this command is deprecated.
    pub deprecated: bool,
    /// Replacement command if deprecated.
    pub replaced_by: Option<String>,
}

impl RedisCommandSpec {
    /// Calculate minimum arguments from arity.
    /// Redis arity includes the command name, so subtract 1.
    pub fn min_args(&self) -> usize {
        if self.arity < 0 {
            (self.arity.abs() - 1) as usize
        } else {
            (self.arity - 1) as usize
        }
    }

    /// Calculate maximum arguments from arity. None means unlimited.
    pub fn max_args(&self) -> Option<usize> {
        if self.arity < 0 {
            None
        } else {
            Some((self.arity - 1) as usize)
        }
    }

    pub fn is_valid_arg_count(&self, count: usize) -> bool {
        let min = self.min_args();
        if count < min {
            return false;
        }
        if let Some(max) = self.max_args()
            && count > max
        {
            return false;
        }
        true
    }
}
