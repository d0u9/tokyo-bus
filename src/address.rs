use std::fmt::Display;
use std::sync::Arc;

use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
    inner: String,
}

impl Address {
    pub fn new(addr: &str) -> Self {
        Self { inner: addr.to_owned() }
    }

    pub fn random() -> Self {
        Self { inner: Uuid::new_v4().to_string() }
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[derive(Debug, Clone)]
struct AddrInfoInner {
    addr: Address,
}

#[derive(Debug, Clone)]
pub struct AddrInfo {
    inner: Arc<AddrInfoInner>,
}

impl AddrInfo {
    pub fn new(addr: Address) -> Self {
        Self {
            inner: Arc::new(AddrInfoInner{ addr }),
        }
    }

    pub fn get_addr(&self) -> Address {
        self.inner.addr.clone()
    }
}

