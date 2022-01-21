use std::fmt::Display;

use uuid::Uuid;

#[cfg(not(not_use_u128))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DevId(u128);

#[cfg(not(not_use_u128))]
impl DevId {
    pub fn new() -> Self {
        DevId(Uuid::new_v4().as_u128())
    }
}

impl Default for DevId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for DevId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
