use std::task::{Context, Poll};
use std::pin::Pin;
use std::convert::From;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

use log::debug;
use futures::StreamExt;
use tokio_stream::Stream;

use super::address::{AddrInfo, Address};
use super::packet::Packet;
use super::wire::{Endpoint, EndpointErrKind, EndpointError, Rx, Tx, Wire, RxStream};

#[cfg(test)]
#[path = "unit_tests/packet_endpoint_test.rs"]
mod test;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PktEndpointErrKind {
    MisDelivery,
    Endpoint(EndpointErrKind),
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
            msg: format!(
                "packet has dst addr {} which doesn't match endpoint's addr {}",
                dst_addr, this_addr
            ),
        }
    }

    pub fn err_kind(&self) -> PktEndpointErrKind {
        self.kind
    }

    pub fn err_msg(&self) -> &str {
        &self.msg
    }
}

impl From<EndpointError> for PktEndpointError {
    fn from(err: EndpointError) -> Self {
        Self {
            msg: format!("endpoint err: {:?}", err),
            kind: PktEndpointErrKind::Endpoint(err.kind()),
        }
    }
}

#[derive(Debug)]
pub struct PktRx<T: Debug + Clone> {
    inner: Rx<Packet<T>>,
}

impl<T> PktRx<T>
where
    T: Clone + Debug,
{
    pub async fn recv_tuple(&mut self) -> Result<(T, Address, Address), PktEndpointError> {
        Ok(self.recv().await?.into_tuple())
    }

    pub async fn recv_tuple_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(T, Address, Address), PktEndpointError> {
        Ok(self.recv_timeout(timeout).await?.into_tuple())
    }

    pub async fn recv_data(&mut self) -> Result<T, PktEndpointError> {
        Ok(self.recv().await?.into_inner())
    }

    pub async fn recv_data_timeout(&mut self, timeout: Duration) -> Result<T, PktEndpointError> {
        Ok(self.recv_timeout(timeout).await?.into_inner())
    }

    pub async fn recv(&mut self) -> Result<Packet<T>, PktEndpointError> {
        let pkt = self.inner.recv().await?;
        Ok(pkt)
    }

    pub async fn recv_timeout(&mut self, timeout: Duration) -> Result<Packet<T>, PktEndpointError> {
        let pkt = self.inner.recv_timeout(timeout).await?;
        Ok(pkt)
    }
}

#[derive(Debug)]
pub struct PktRxStream<T: Debug + Clone> {
    inner: RxStream<Packet<T>>,
}

impl<T> PktRxStream<T>
where
    T: 'static + Clone + Debug + Send,
{
    pub fn new(rx: PktRx<T>) -> Self {
        Self {
            inner: RxStream::new(rx.inner),
        }
    }
}

impl<T> Stream for PktRxStream<T>
where
    T: 'static + Clone + Debug + Send,
{
    type Item = Result<Packet<T>, PktEndpointError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = match self.inner.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(result)) => {
                result
            }
            _ => return Poll::Ready(None),
        };

        match result {
            Err(err) => {
                match err.kind() {
                    EndpointErrKind::Lagged(num) => {
                        debug!("[PktRxStream has lagged {} packets, retry", num);
                        Poll::Pending
                    }
                    _ => {
                        Poll::Ready(Some(Err(err.into())))
                    }
                }
            }
            Ok(pkt) => {
                Poll::Ready(Some(Ok(pkt)))
            }
        }
    }
}

#[derive(Debug)]
pub struct PktTx<T: Debug + Clone> {
    addr_info: AddrInfo,
    inner: Tx<Packet<T>>,
}

impl<T> PktTx<T>
where
    T: Clone + Debug,
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
pub struct PktEndpoint<T: Debug + Clone> {
    addr_info: AddrInfo,
    inner: Endpoint<Packet<T>>,
}

impl<T> PktEndpoint<T>
where
    T: Clone + Debug,
{
    pub fn split(self) -> Result<(PktTx<T>, PktRx<T>), PktEndpointError> {
        let (inner_tx, inner_rx) = self.inner.split()?;
        let tx = PktTx {
            addr_info: self.addr_info,
            inner: inner_tx,
        };
        let rx = PktRx { inner: inner_rx };
        Ok((tx, rx))
    }

    pub fn address(&self) -> Address {
        self.addr_info.get_addr()
    }

    fn from(addr_info: AddrInfo, ep: Endpoint<Packet<T>>) -> Self {
        Self {
            addr_info,
            inner: ep,
        }
    }
}

pub struct PktWire<T> {
    _phantom: PhantomData<T>,
}

impl<T> PktWire<T>
where
    T: Clone + Debug,
{
    pub fn endpoints(addr: Address) -> (PktEndpoint<T>, PktEndpoint<T>) {
        let addr_info = AddrInfo::new(addr);
        let (ep0, ep1) = Wire::endpoints();
        let pkt_ep0 = PktEndpoint::from(addr_info.clone(), ep0);
        let pkt_ep1 = PktEndpoint::from(addr_info, ep1);
        (pkt_ep0, pkt_ep1)
    }
}
