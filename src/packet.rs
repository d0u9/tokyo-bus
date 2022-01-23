use std::fmt::Debug;

use super::address::Address;

#[derive(Debug, Clone)]
pub struct Packet<T> {
    payload: T,
    src: Address,
    dst: Address,
}

impl<T> Packet<T>
where
    T: Clone + Debug,
{
    pub fn new(data: T, src_addr: &Address, dst_addr: &Address) -> Self {
        Self {
            payload: data,
            src: src_addr.to_owned(),
            dst: dst_addr.to_owned(),
        }
    }

    pub fn into_inner(self) -> T {
        self.payload
    }

    pub fn into_tuple(self) -> (T, Address, Address) {
        let Self { payload, src, dst } = self;

        (payload, src, dst)
    }

    pub fn dst_addr_ref(&self) -> &Address {
        &self.dst
    }

    pub fn dst_addr(&self) -> Address {
        self.dst.clone()
    }

    pub fn src_addr_ref(&self) -> &Address {
        &self.src
    }

    pub fn src_addr(&self) -> Address {
        self.src.clone()
    }
}
