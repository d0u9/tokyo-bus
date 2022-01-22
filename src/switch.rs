use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;
use std::convert::From;
use std::fmt::Debug;
use std::collections::HashMap;

use futures::FutureExt;
use log::warn;
use log::debug;
use log::error;

use super::packet::Packet;
use super::wire::{Wire, Endpoint, EndpointError, EndpointErrKind, Tx, Rx};
use super::packet_endpoint::{PktEndpoint, PktTx, PktRx, PktEndpointErrKind, PktEndpointError};
use super::address::Address;

#[cfg(test)]
#[path = "unit_tests/switch_test.rs"]
mod test;

#[derive(Debug, Clone, Copy)]
pub enum SwitchErrKind {
    PktEndpoint(PktEndpointErrKind),
    ControlEndpoint(EndpointErrKind),
}

#[derive(Debug)]
pub struct SwitchError {
    kind: SwitchErrKind,
    msg: String,
}

impl From<PktEndpointError> for SwitchError {
    fn from(err: PktEndpointError) -> Self {
        Self {
            msg: format!("packet endpoint failed: {:?}", err),
            kind: SwitchErrKind::PktEndpoint(err.kind()),
        }
    }
}

impl From<EndpointError> for SwitchError {
    fn from(err: EndpointError) -> Self {
        Self {
            msg: format!("packet endpoint failed: {:?}", err),
            kind: SwitchErrKind::ControlEndpoint(err.kind()),
        }
    }
}

#[derive(Debug)]
struct Port<T: Clone + Debug> {
    tx_only: bool,
    tx: PktTx<T>,
    rx: PktRx<T>,
}

impl<T> Port<T>
where
    T: Clone + Debug
{
    pub fn new(tx: PktTx<T>, rx: PktRx<T>) -> Self {
        Self {
            tx_only: false,
            tx,
            rx,
        }
    }

    pub fn is_tx_only(&self) -> bool {
        self.tx_only
    }

    pub fn send_pkt(&self, pkt: Packet<T>) {
        if let Err(e) = self.tx.send(pkt) {
            error!("[Switch] port send packet failed: {:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct SwitchHandler {
    control_endpoint: Endpoint<CtrlMsg>,
}

impl SwitchHandler {
    pub fn non_clonable(self) -> Result<SwitchHandlerNonClonable, SwitchError> {
        let (tx, rx) = self.control_endpoint.split()?;

        Ok(SwitchHandlerNonClonable {
            tx,
            rx,
        })
    }

    pub fn shutdown_server(self) -> Result<(), SwitchError> {
        self.non_clonable()?.shutdown_server()
    }
}

pub struct SwitchHandlerNonClonable {
    tx: Tx<CtrlMsg>,
    rx: Rx<CtrlMsg>,
}

impl SwitchHandlerNonClonable {
    pub fn shutdown_server(self) -> Result<(), SwitchError> {
        let _ = self.tx.send(CtrlMsg::shutdown_server())?;
        Ok(())
    }
}

pub struct Switch<T: Debug + Clone> {
    control_endpoint: Endpoint<CtrlMsg>,
    ports: HashMap<Address, Port<T>>,
}

impl<T> Switch<T>
where
    T: Debug + Clone
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let (ctrl_ep0, _) = Wire::endpoints();
        Self {
            control_endpoint: ctrl_ep0,
            ports: HashMap::new(),
        }
    }

    pub fn attach_endpoint(&mut self, endpoint: PktEndpoint<T>) -> Result<(), SwitchError> {
        let addr = endpoint.address();

        let (tx, rx) = endpoint.split()?;
        let port = Port::new(tx, rx);
        self.ports.insert(addr, port);

        Ok(())
    }

    pub fn server(self) -> Result<(SwitchServer<T>, SwitchHandler), SwitchError> {
        let Self {
            control_endpoint,
            ports,
        } = self;

        let peer_control_endpoint = control_endpoint.get_peer()?;

        let handler = SwitchHandler {
            control_endpoint: peer_control_endpoint,
        };

        let (ctrl_tx, ctrl_rx) = control_endpoint.split()?;

        let poller = SwitchServer {
            status: SwitchServerStatus::new(ports.len()),
            ctrl_tx,
            ctrl_rx,
            ports,
        };

        Ok((poller, handler))
    }
}

struct PortsPollerInner<'a, T, Fut> {
    futures: Vec<(&'a Address, Fut)>,
    _phantom: PhantomData<T>,
}

impl<'a, T, Fut> Unpin for PortsPollerInner<'a, T, Fut> {}

impl<'a, T, Fut> Future for PortsPollerInner<'a, T, Fut>
where
    T: Clone + Debug,
    Fut: Future<Output=Result<Packet<T>, PktEndpointError>> + Unpin,
{
    type Output = (Address, Result<Packet<T>, PktEndpointError>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let polled = self.futures
            .iter_mut()
            .find_map(|entry| {
                let (addr, fut) = entry;
                match fut.poll_unpin(cx) {
                    Poll::Pending => None,
                    Poll::Ready(result) => Some((addr.to_owned(), result)),
                }
            });

        match polled {
            None => Poll::Pending,
            Some(ret) => Poll::Ready(ret),
        }
    }
}


struct PortsPoller<'a, T> {
    _pha: &'a PhantomData<T>,
}

impl<'a, T> PortsPoller<'a, T>
where
    T: Debug + Clone,
{
    async fn poll_ports(ports_map: &'a mut HashMap<Address, Port<T>>) -> (Address, Result<Packet<T>, PktEndpointError>) {
        let futures = ports_map
            .iter_mut()
            .filter(|(_, port)| !port.is_tx_only())
            .map(|(addr, port)| (addr, Box::pin(port.rx.recv())))
            .collect::<Vec<_>>();

        let inner = PortsPollerInner {
            futures,
            _phantom: PhantomData,
        };

        inner.await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SwitchServerStatus {
    ctrl_endabled: bool,
    rx_port_num: usize,
}

impl SwitchServerStatus {
    fn new(rx_port_num: usize) -> Self {
        Self {
            ctrl_endabled: true,
            rx_port_num,
        }
    }

    fn ports_poll_enabled(&self) -> bool {
        self.rx_port_num > 0
    }

    fn ctrl_poll_endabled(&self) -> bool {
        self.ctrl_endabled
    }
}

pub struct SwitchServer<T: Debug + Clone> {
    ctrl_tx: Tx<CtrlMsg>,
    ctrl_rx: Rx<CtrlMsg>,
    ports: HashMap<Address, Port<T>>,
    status: SwitchServerStatus,
}

impl<T> SwitchServer<T>
where
    T: Clone + Debug
{
    pub async fn serve_with_shutdown(self, signal: impl Future<Output=()>) {
        tokio::select! {
            _ = signal => { }
            _ = self.serve() => { }
        }
    }

    pub async fn serve(mut self) {
        loop {
            let status = self.status;
            let ports_poller = PortsPoller::poll_ports(&mut self.ports);
            tokio::select! {
                (addr, result) = ports_poller, if status.ports_poll_enabled()=> {
                    self.pkt_received(addr, result)
                }
                ctrl_data = self.ctrl_rx.recv(), if status.ctrl_poll_endabled() => {
                    if self.ctrl_received(ctrl_data) { break; }
                }
            }
        }
    }

    fn ctrl_received(&mut self, ctrl_recv: Result<CtrlMsg, EndpointError>) -> bool {
        let data = match ctrl_recv {
            Err(e) if e.kind() == EndpointErrKind::Closed => {
                self.status.ctrl_endabled = false;
                warn!("[Switch] no ctrl receiver exists, close ctrl endpoint");
                return false;
            }
            Err(e) => {
                debug!("[Switch] ctrl recv err: {:?}", e);
                return false;
            }

            Ok(data) => data
        };

        println!("ctrl data: {:?}", data);
        let request = match data {
            CtrlMsg::Reply(_) => {
                error!("[[BUG BUG BUG]]: switch handler sends a reply packet");
                return false;
            }
            // TODO: not implemented
            CtrlMsg::Err => { return false; }
            CtrlMsg::Request(request) => request,
        };

        self.ctrl_request_reply(request)
    }

    fn ctrl_request_reply(&mut self, request: CtrllMsgRequest) -> bool {
        match request {
            CtrllMsgRequest::ShutdownServer => {
                warn!("Shutting down switch server");
                true
            }
            CtrllMsgRequest::UNSPEC => { true }
        }
    }

    fn pkt_received(&mut self, addr: Address, pkt_received: Result<Packet<T>, PktEndpointError>) {
        match pkt_received {
            Err(ref e) if e.kind() == PktEndpointErrKind::Endpoint(EndpointErrKind::Closed) => {
                debug!("[Switch] port (addr={}) rx disabled", &addr);
                self.status.rx_port_num -= 1;
            }
            Err(ref e) => {
                debug!("[Switch] port (addr={}) rx recv err: {:?}", &addr, e);
            }
            Ok(pkt) => {
                if pkt.src_addr_ref() != &addr {
                    error!("[[BUG BUG BUG]]: received an packet but src addr doesn't match");
                    return;
                }
                self.pkt_switch(pkt);
            }
        }
    }

    fn pkt_switch(&mut self, pkt: Packet<T>) {
        let dst_ref = pkt.dst_addr_ref();
        if let Some(port) = self.ports.get(dst_ref) {
            port.send_pkt(pkt);
        } else {
            warn!("Packet has a dest addr {} which can not be found", dst_ref);
        }
    }
}

#[derive(Debug, Clone)]
pub enum CtrlMsg {
    Request(CtrllMsgRequest),
    Reply(CtrlMsgReply),
    Err,
}

impl CtrlMsg {
    pub fn shutdown_server() -> Self {
        Self::Request(CtrllMsgRequest::ShutdownServer)
    }
}

#[derive(Debug, Clone)]
pub enum CtrllMsgRequest {
    ShutdownServer,
    UNSPEC,
}

#[derive(Debug, Clone)]
pub enum CtrlMsgReply {
    Request,
    Response,
}
