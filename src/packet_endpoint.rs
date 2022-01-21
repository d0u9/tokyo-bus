use std::convert::From;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::address::Address;
use super::wire::{Rx, Tx, Wire, Endpoint, EndpointError};
use super::packet::Packet;

#[cfg(test)]
#[path = "unit_tests/packet_endpoint_test.rs"]
mod test;

#[derive(Debug)]
pub enum PktEndpointErrKind {
    MisDelivery,
    Endpoint(EndpointError)
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PktEndpointError {
    kind: PktEndpointErrKind,
    msg: String,
}

impl PktEndpointError {
    pub fn misdelivery(this_addr: &Address, dst_addr: &Address) -> Self {
        Self {
            kind: PktEndpointErrKind::MisDelivery,
            msg: format!("packet has dst addr {} which doesn't match endpoint's addr {}", dst_addr, this_addr),
        }
    }
}

impl From<EndpointError> for PktEndpointError {
    fn from(err: EndpointError) -> Self {
        Self {
            msg: format!("endpoint err: {:?}", err),
            kind: PktEndpointErrKind::Endpoint(err),
        }
    }
}

#[derive(Debug)]
pub struct PktRx<T: Debug + Clone> {
    addr_info: Arc<AddrInfo>,
    inner: Rx<Packet<T>>,
}

impl<T> PktRx<T>
where
    T: Clone + Debug
{
    pub async fn recv_tuple(&mut self) -> Result<(T, Address, Address), PktEndpointError> {
        Ok(self.recv().await?.into_tuple())
    }

    pub async fn recv_tuple_timeout(&mut self, timeout: Duration) -> Result<(T, Address, Address), PktEndpointError> {
        Ok(self.recv_timeout(timeout).await?.into_tuple())
    }

    pub async fn recv_data(&mut self) -> Result<T, PktEndpointError> {
        Ok(self.recv().await?.into_inner())
    }

    pub async fn recv_data_timeout(&mut self, timeout: Duration) -> Result<T, PktEndpointError> {
        Ok(self.recv_timeout(timeout).await?.into_inner())
    }

    pub async fn recv(&mut self) -> Result<Packet<T>, PktEndpointError> {
        let this_addr = self.addr_info.get_addr();
        let pkt = self.inner.recv().await?;
        if *pkt.dst_addr_ref() != this_addr {
            return Err(PktEndpointError::misdelivery(&this_addr, pkt.dst_addr_ref()));
        }
        Ok(pkt)
    }

    pub async fn recv_timeout(&mut self, timeout: Duration) -> Result<Packet<T>, PktEndpointError> {
        let this_addr = self.addr_info.get_addr();
        let pkt = self.inner.recv_timeout(timeout).await?;
        if *pkt.dst_addr_ref() != this_addr {
            return Err(PktEndpointError::misdelivery(&this_addr, pkt.dst_addr_ref()));
        }
        Ok(pkt)
    }
}

#[derive(Debug)]
pub struct PktTx<T: Debug + Clone> {
    addr_info: Arc<AddrInfo>,
    inner: Tx<Packet<T>>,
}

impl<T> PktTx<T>
where
    T: Clone + Debug
{
    pub fn send(&self, pkt: Packet<T>) -> Result<(), PktEndpointError> {
        let _ = self.inner.send(pkt)?;
        Ok(())
    }

    pub fn send_data(&self, dst_addr: &Address, data: T) -> Result<(), PktEndpointError> {
        let src_addr = self.addr_info.get_addr();
        let pkt = Packet::new(data, &src_addr, dst_addr);
        self.send(pkt)
    }
}

#[derive(Debug, Clone)]
pub struct AddrInfo {
    addr: Address,
}

impl AddrInfo {
    fn new(addr: Address) -> Self {
        Self { addr, }
    }

    fn get_addr(self: &Arc<Self>) -> Address {
        self.addr.clone()
    }
}

pub struct PktEndpoint<T: Debug + Clone> {
    addr_info: Arc<AddrInfo>,
    inner: Endpoint<Packet<T>>,
}

impl<T> PktEndpoint<T>
where
    T: Clone + Debug
{
    pub fn split(self) -> Result<(PktTx<T>, PktRx<T>), PktEndpointError> {
        let (inner_tx, inner_rx) = self.inner.split()?;
        let tx = PktTx{
            addr_info: self.addr_info.clone(),
            inner: inner_tx,
        };
        let rx = PktRx {
            addr_info: self.addr_info,
            inner: inner_rx
        };
        Ok((tx, rx))
    }

    fn from(addr_info: Arc<AddrInfo>, ep: Endpoint<Packet<T>>) -> Self {
        Self {
            addr_info,
            inner: ep,
        }
    }
}

pub struct PktWire<T> {
    _phantom: PhantomData<T>
}

impl<T> PktWire<T>
where
    T: Clone + Debug
{
    pub fn endpoints(addr: Address) -> (PktEndpoint<T>, PktEndpoint<T>) {
        let addr_info = AddrInfo::new(addr);
        let addr_info = Arc::new(addr_info);
        let (ep0, ep1) = Wire::endpoints(); 
        let pkt_ep0 = PktEndpoint::from(addr_info.clone(), ep0);
        let pkt_ep1 = PktEndpoint::from(addr_info, ep1);
        (pkt_ep0, pkt_ep1)
    }
}
