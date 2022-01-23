use std::clone::Clone;
use std::convert::From;
use std::default::Default;
use std::pin::Pin;
use std::fmt::Debug;
use std::ops::Drop;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use log::warn;
use log::debug;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::time::Duration;
use tokio_stream::wrappers::{self, BroadcastStream};
use tokio_stream::Stream;

use super::types::DevId;

#[cfg(test)]
#[path = "unit_tests/wire_test.rs"]
mod test;

pub type RawTx<T> = broadcast::Sender<T>;
pub type RawRx<T> = broadcast::Receiver<T>;
pub type RawRxStream<T> = BroadcastStream<T>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EndpointErrKind {
    Timeout,
    NoPeer,
    NoReceiver,
    Closed,
    Lagged(u64),
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct EndpointError {
    kind: EndpointErrKind,
    msg: String,
}

impl EndpointError {
    pub fn no_peer() -> Self {
        Self {
            kind: EndpointErrKind::NoPeer,
            msg: "no peer endpoint alive".to_string(),
        }
    }

    pub fn timeout(time: Duration) -> Self {
        Self {
            kind: EndpointErrKind::Timeout,
            msg: format!("{:?} timeout", time),
        }
    }

    pub fn closed() -> Self {
        Self {
            kind: EndpointErrKind::Closed,
            msg: "underlaying channel closed".to_string(),
        }
    }

    pub fn kind(&self) -> EndpointErrKind {
        self.kind
    }
}

impl From<broadcast::error::RecvError> for EndpointError {
    fn from(err: broadcast::error::RecvError) -> Self {
        let (kind, msg) = match &err {
            broadcast::error::RecvError::Lagged(num) => (
                EndpointErrKind::Lagged(*num),
                format!("underlaying channel lagged {} messages", num),
            ),
            broadcast::error::RecvError::Closed => (
                EndpointErrKind::Closed,
                "underlaying channel closed".to_string(),
            ),
        };

        Self { kind, msg }
    }
}

impl<T> From<broadcast::error::SendError<T>> for EndpointError
where
    T: Debug + Clone,
{
    fn from(err: broadcast::error::SendError<T>) -> Self {
        Self {
            kind: EndpointErrKind::NoReceiver,
            msg: format!("underlaying channel send error: {:?}", err),
        }
    }
}

impl From<wrappers::errors::BroadcastStreamRecvError> for EndpointError
{
    fn from(err: wrappers::errors::BroadcastStreamRecvError) -> Self {
        let (kind, msg) = match &err {
            wrappers::errors::BroadcastStreamRecvError::Lagged(num) => (
                EndpointErrKind::Lagged(*num),
                format!("underlaying channel lagged {} messages", num),
            ),
        };

        Self { kind, msg }
    }
}

#[derive(Debug)]
pub struct Rx<T: Debug + Clone> {
    _endpoint: Arc<Endpoint<T>>,
    inner: RawRx<T>,
    closed: bool,
}

impl<T> Rx<T>
where
    T: Clone + Debug,
{
    pub async fn recv(&mut self) -> Result<T, EndpointError> {
        (!self.closed)
            .then(|| ())
            .ok_or_else(EndpointError::closed)?;

        let result = self.inner.recv().await;
        if let Err(broadcast::error::RecvError::Closed) = &result {
            self.closed = true;
        }

        let val = result?;

        Ok(val)
    }

    pub async fn recv_timeout(&mut self, timeout: Duration) -> Result<T, EndpointError> {
        let timer = tokio::time::sleep(timeout);
        tokio::select! {
            result = self.recv() => {
                result
            }
            _ = timer => {
                Err(EndpointError::timeout(timeout))
            }
        }
    }
}

#[derive(Debug)]
pub struct RxStream<T: Debug + Clone> {
    _endpoint: Arc<Endpoint<T>>,
    inner: RawRxStream<T>,
}

impl<T> RxStream<T>
where
    T: 'static + Clone + Debug + Send,
{
    pub fn new(rx: Rx<T>) -> Self {
        Self {
            _endpoint: rx._endpoint,
            inner: RawRxStream::new(rx.inner),
        }
    }
}

impl<T> Stream for RxStream<T>
where
    T: 'static + Clone + Debug + Send,
{
    type Item = Result<T, EndpointError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(result)) => {
                match result {
                    Err(e) => {
                        debug!("[RxStream({}) has lagged {} packets, retry", self._endpoint.wire_id(), e);
                        Poll::Ready(Some(Err(e.into())))
                    }
                    Ok(val) => {
                        Poll::Ready(Some(Ok(val)))
                    }
                }
            }
            _ => Poll::Ready(None),
        }
    }
}

#[derive(Debug)]
pub struct Tx<T: Debug + Clone> {
    _endpoint: Arc<Endpoint<T>>,
    inner: RawTx<T>,
}

impl<T> Tx<T>
where
    T: Clone + Debug,
{
    pub fn send(&self, value: T) -> Result<(), EndpointError> {
        self.inner.send(value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum EndpointPeer {
    Peer0,
    Peer1,
}

impl EndpointPeer {
    pub fn another(&self) -> Self {
        match self {
            Self::Peer0 => Self::Peer1,
            Self::Peer1 => Self::Peer0,
        }
    }
}

#[derive(Debug)]
pub struct Endpoint<T>
where
    T: Clone + Debug,
{
    wire: Arc<Wire<T>>,
    peer: EndpointPeer,
}

impl<T> Endpoint<T>
where
    T: Clone + Debug,
{
    pub fn split(self) -> Result<(Tx<T>, Rx<T>), EndpointError> {
        let (raw_tx, raw_rx) = self.wire.endpoint_split(self.peer)?;

        let endpoint = Arc::new(self);
        let tx = Tx {
            _endpoint: endpoint.clone(),
            inner: raw_tx,
        };

        let rx = Rx {
            _endpoint: endpoint,
            inner: raw_rx,
            closed: false,
        };

        Ok((tx, rx))
    }

    pub fn get_peer(&self) -> Result<Endpoint<T>, EndpointError> {
        let peer = self.peer.another();
        let endpoint = self.wire.add_endpoint(peer);
        Ok(endpoint)
    }

    pub fn wire_ref(&self) -> &Arc<Wire<T>> {
        &self.wire
    }

    pub fn wire(&self) -> Arc<Wire<T>> {
        self.wire.clone()
    }

    pub fn wire_id(&self) -> DevId {
        self.wire.id
    }

    pub fn wire_peer_counter(&self) -> (usize, usize) {
        self.wire.get_peer_counter()
    }
}

impl<T> Clone for Endpoint<T>
where
    T: Clone + Debug,
{
    fn clone(&self) -> Self {
        let this_peer = self.peer;
        self.wire.add_endpoint(this_peer)
    }
}

impl<T> Drop for Endpoint<T>
where
    T: Clone + Debug,
{
    fn drop(&mut self) {
        let this_peer = self.peer;
        self.wire.drop_endpoint(this_peer);
    }
}

#[derive(Debug)]
struct PeerInfo<T> {
    counter: usize,
    tx_chan: Option<RawTx<T>>,
}

impl<T> PeerInfo<T>
where
    T: Clone + Debug,
{
    fn new(init_counter: usize) -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            counter: init_counter,
            tx_chan: Some(tx),
        }
    }
}

#[derive(Debug)]
pub struct WireInner<T> {
    peer0: PeerInfo<T>,
    peer1: PeerInfo<T>,
}

impl<T> Default for WireInner<T>
where
    T: Clone + Debug,
{
    fn default() -> Self {
        Self {
            peer0: PeerInfo::new(1),
            peer1: PeerInfo::new(1),
        }
    }
}

#[derive(Debug)]
pub struct Wire<T> {
    inner: Mutex<WireInner<T>>,
    id: DevId,
}

impl<T> Wire<T>
where
    T: Clone + Debug,
{
    pub fn endpoints() -> (Endpoint<T>, Endpoint<T>) {
        let wire = Arc::new(Self {
            inner: Mutex::new(WireInner::default()),
            id: DevId::new(),
        });

        let ep0 = Endpoint {
            peer: EndpointPeer::Peer0,
            wire: wire.clone(),
        };

        let ep1 = Endpoint {
            peer: EndpointPeer::Peer1,
            wire,
        };

        (ep0, ep1)
    }

    fn get_peer_counter(self: &Arc<Self>) -> (usize, usize) {
        let wire_inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(e) => {
                warn!("Locking wire returns an poisoned lock");
                e.into_inner()
            }
        };

        (wire_inner.peer0.counter, wire_inner.peer1.counter)
    }

    fn endpoint_split(
        self: &Arc<Self>,
        peer: EndpointPeer,
    ) -> Result<(RawTx<T>, RawRx<T>), EndpointError> {
        let wire_inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(e) => {
                warn!("Locking wire returns an poisoned lock");
                e.into_inner()
            }
        };

        let (this_tx, that_tx) = match peer {
            EndpointPeer::Peer0 => (
                wire_inner.peer0.tx_chan.as_ref(),
                wire_inner.peer1.tx_chan.as_ref(),
            ),
            EndpointPeer::Peer1 => (
                wire_inner.peer1.tx_chan.as_ref(),
                wire_inner.peer0.tx_chan.as_ref(),
            ),
        };

        match (this_tx, that_tx) {
            (Some(this), Some(that)) => Ok((this.clone(), that.subscribe())),
            _ => Err(EndpointError::no_peer()),
        }
    }

    fn add_endpoint(self: &Arc<Self>, peer: EndpointPeer) -> Endpoint<T> {
        let mut wire_inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(e) => {
                warn!("Locking wire returns an poisoned lock");
                e.into_inner()
            }
        };

        match peer {
            EndpointPeer::Peer0 => {
                wire_inner.peer0.counter += 1;
                if wire_inner.peer0.tx_chan.is_none() {
                    let (tx, _) = broadcast::channel(16);
                    wire_inner.peer0.tx_chan = Some(tx);
                }
            }
            EndpointPeer::Peer1 => {
                wire_inner.peer1.counter += 1;
                if wire_inner.peer1.tx_chan.is_none() {
                    let (tx, _) = broadcast::channel(16);
                    wire_inner.peer1.tx_chan = Some(tx);
                }
            }
        };

        Endpoint {
            peer,
            wire: self.clone(),
        }
    }

    fn drop_endpoint(self: &Arc<Self>, peer: EndpointPeer) {
        let mut wire_inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(e) => {
                warn!("Locking wire returns an poisoned lock");
                e.into_inner()
            }
        };

        let peer_info = match peer {
            EndpointPeer::Peer0 => &mut wire_inner.peer0,
            EndpointPeer::Peer1 => &mut wire_inner.peer1,
        };

        peer_info.counter -= 1;

        if peer_info.counter == 0 {
            peer_info.tx_chan = None;
        }
    }
}
