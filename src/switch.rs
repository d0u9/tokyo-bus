use std::collections::HashMap;
use std::convert::From;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::FutureExt;
use log::debug;
use log::error;
use log::trace;
use log::warn;

use super::address::Address;
use super::packet::Packet;
use super::packet_endpoint::{
    PktEndpoint, PktEndpointErrKind, PktEndpointError, PktRx, PktTx, PktWire,
};
use super::wire::{Endpoint, EndpointErrKind, EndpointError, Rx, Tx, Wire};

#[cfg(test)]
#[path = "unit_tests/switch_test.rs"]
mod test;

#[derive(Debug, Clone, Copy)]
pub enum SwitchErrKind {
    TypeMismatch,
    PktEndpoint(PktEndpointErrKind),
    ControlEndpoint(EndpointErrKind),
}

#[derive(Debug, Clone)]
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

impl SwitchError {
    pub fn type_mismatch(msg: &str) -> Self {
        Self {
            kind: SwitchErrKind::TypeMismatch,
            msg: msg.to_owned(),
        }
    }
}

#[derive(Debug)]
struct Port<T: Clone + Debug> {
    switch_info: Arc<SwitchInfo>,
    tx_only: bool,
    tx: PktTx<T>,
    rx: PktRx<T>,
}

impl<T> Port<T>
where
    T: Clone + Debug,
{
    pub fn switch_name(&self) -> &str {
        &self.switch_info.name
    }

    pub fn new(tx: PktTx<T>, rx: PktRx<T>, switch_info: Arc<SwitchInfo>) -> Self {
        Self {
            switch_info,
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
            error!("[{}] port send packet failed: {:?}", self.switch_name(), e);
        }
    }
}

#[derive(Clone)]
pub struct SwitchHandler<T: Clone + Debug> {
    switch_info: Arc<SwitchInfo>,
    control_endpoint: Endpoint<CtrlMsg<T>>,
}

impl<T: Clone + Debug> SwitchHandler<T> {
    pub fn switch_name(&self) -> &str {
        &self.switch_info.name
    }

    pub fn non_clonable(self) -> Result<SwitchHandlerNonClonable<T>, SwitchError> {
        let (tx, rx) = self.control_endpoint.split()?;

        Ok(SwitchHandlerNonClonable { tx, rx })
    }

    pub fn shutdown_server(self) -> Result<(), SwitchError> {
        self.non_clonable()?.shutdown_server()
    }

    pub async fn new_endpoint(&self, addr: Address) -> Result<PktEndpoint<T>, SwitchError> {
        self.clone().non_clonable()?.new_endpoint(addr).await
    }
}

pub struct SwitchHandlerNonClonable<T: Clone + Debug> {
    tx: Tx<CtrlMsg<T>>,
    rx: Rx<CtrlMsg<T>>,
}

impl<T: Debug + Clone> SwitchHandlerNonClonable<T> {
    pub fn shutdown_server(self) -> Result<(), SwitchError> {
        let _ = self.tx.send(CtrlMsg::shutdown_server_request())?;
        Ok(())
    }

    pub async fn new_endpoint(&mut self, addr: Address) -> Result<PktEndpoint<T>, SwitchError> {
        self.tx.send(CtrlMsg::new_endpoint_request(addr))?;
        let reply_msg = self.rx.recv().await?.to_result()?;
        match reply_msg {
            CtrlMsgReply::NewEndpoint(ep) => Ok(ep),
            _ => {
                error!("[[BUG BUG BUG]]: request and reply ctrl msg doesn't match");
                Err(SwitchError::type_mismatch("reply msg doesn't match"))
            }
        }
    }
}

pub struct Switch<T: Debug + Clone> {
    control_endpoint: Endpoint<CtrlMsg<T>>,
    endpoints: HashMap<Address, PktEndpoint<T>>,
    name: Option<String>,
}

impl<T> Default for Switch<T>
where
    T: Debug + Clone,
{
    fn default() -> Self {
        let (ctrl_ep0, _) = Wire::endpoints();
        Self {
            control_endpoint: ctrl_ep0,
            endpoints: HashMap::new(),
            name: None,
        }
    }
}

impl<T> Switch<T>
where
    T: Debug + Clone,
{
    pub fn new_with_name(name: &str) -> Self {
        let mut switch = Self::default();
        switch.set_name(name);
        switch
    }

    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = Some(name.to_owned())
    }

    pub fn attach_endpoint(&mut self, endpoint: PktEndpoint<T>) -> Result<(), SwitchError> {
        let addr = endpoint.address();
        self.endpoints.insert(addr, endpoint);
        Ok(())
    }

    pub fn server(self) -> Result<(SwitchServer<T>, SwitchHandler<T>), SwitchError> {
        let Self {
            control_endpoint,
            endpoints,
            name,
        } = self;

        let peer_control_endpoint = control_endpoint.get_peer()?;
        let (ctrl_tx, ctrl_rx) = control_endpoint.split()?;

        let info = Arc::new(SwitchInfo {
            name: format!(
                "switch-{}",
                name.unwrap_or_else(|| "unnamedswitch".to_string())
            ),
        });

        let mut ports = HashMap::new();
        for (addr, ep) in endpoints {
            let (tx, rx) = ep.split()?;
            let port = Port::new(tx, rx, info.clone());
            ports.insert(addr, port);
        }

        let handler = SwitchHandler {
            switch_info: info.clone(),
            control_endpoint: peer_control_endpoint,
        };

        let poller = SwitchServer {
            info,
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
    Fut: Future<Output = Result<Packet<T>, PktEndpointError>> + Unpin,
{
    type Output = (Address, Result<Packet<T>, PktEndpointError>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let polled = self.futures.iter_mut().find_map(|entry| {
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
    async fn poll_ports(
        ports_map: &'a mut HashMap<Address, Port<T>>,
    ) -> (Address, Result<Packet<T>, PktEndpointError>) {
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
    shutdown_server: bool,
    ctrl_endabled: bool,
    rx_port_num: usize,
}

impl SwitchServerStatus {
    fn new(rx_port_num: usize) -> Self {
        Self {
            shutdown_server: false,
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

#[derive(Debug)]
struct SwitchInfo {
    name: String,
}

#[derive(Debug)]
pub struct SwitchServer<T: Debug + Clone> {
    info: Arc<SwitchInfo>,
    ctrl_tx: Tx<CtrlMsg<T>>,
    ctrl_rx: Rx<CtrlMsg<T>>,
    ports: HashMap<Address, Port<T>>,
    status: SwitchServerStatus,
}

impl<T> SwitchServer<T>
where
    T: Clone + Debug,
{
    pub fn name(&self) -> &str {
        &self.info.name
    }

    pub async fn serve_with_shutdown(self, signal: impl Future<Output = ()>) {
        tokio::select! {
            _ = signal => { }
            _ = self.serve() => { }
        }
    }

    pub async fn serve(mut self) {
        while !self.status.shutdown_server {
            let status = self.status;
            let ports_poller = PortsPoller::poll_ports(&mut self.ports);
            tokio::select! {
                (addr, result) = ports_poller, if status.ports_poll_enabled()=> {
                    self.pkt_received(addr, result)
                }
                ctrl_data = self.ctrl_rx.recv(), if status.ctrl_poll_endabled() => {
                    self.ctrl_received(ctrl_data)
                }
            }
        }
    }

    fn ctrl_received(&mut self, ctrl_recv: Result<CtrlMsg<T>, EndpointError>) {
        let data = match ctrl_recv {
            Err(e) if e.kind() == EndpointErrKind::Closed => {
                self.status.ctrl_endabled = false;
                warn!(
                    "[{}] no ctrl receiver exists, close ctrl endpoint",
                    self.name()
                );
                return;
            }
            Err(e) => {
                debug!("[{}] ctrl recv err: {:?}", self.name(), e);
                return;
            }

            Ok(data) => data,
        };

        debug!("[{}] Ctrl data received: {:?}", self.name(), data);
        let request = match data {
            CtrlMsg::Reply(_) => {
                error!("[[BUG BUG BUG]]: switch handler sends a reply packet");
                return;
            }
            // TODO: not implemented
            CtrlMsg::Error(_) => {
                return;
            }
            CtrlMsg::Request(request) => request,
        };

        self.ctrl_request_reply(request)
    }

    fn ctrl_request_reply(&mut self, request: CtrlMsgRequest) {
        let reply = match request {
            CtrlMsgRequest::ShutdownServer => {
                warn!("[{}] Shutting down switch server", self.name());
                self.status.shutdown_server = true;
                return;
            }
            CtrlMsgRequest::NewEndpoint(addr) => self
                .ctrl_new_endpoint(addr)
                .map(|v| CtrlMsgReply::NewEndpoint(v)),
        };

        let msg = match reply {
            Err(e) => CtrlMsg::Error(e),
            Ok(reply) => CtrlMsg::Reply(reply),
        };

        if let Err(e) = self.ctrl_tx.send(msg) {
            warn!("[{}] ctrl msg send failed: {:?}", self.name(), e);
        }
    }

    fn ctrl_new_endpoint(&mut self, addr: Address) -> Result<PktEndpoint<T>, SwitchError> {
        let (ep0, ep1) = PktWire::endpoints(addr.clone());
        self.ctrl_attach_endpoint(addr, ep0)?;
        self.status.rx_port_num += 1;
        Ok(ep1)
    }

    fn ctrl_attach_endpoint(
        &mut self,
        addr: Address,
        ep: PktEndpoint<T>,
    ) -> Result<(), SwitchError> {
        let (tx, rx) = ep.split()?;
        let port = Port::new(tx, rx, self.info.clone());
        self.ports.insert(addr, port);
        Ok(())
    }

    fn pkt_received(&mut self, addr: Address, pkt_received: Result<Packet<T>, PktEndpointError>) {
        trace!(
            "[{}] Port({}) receives new packet: {:?}",
            self.name(),
            addr,
            pkt_received
        );
        match pkt_received {
            Err(ref e) if e.kind() == PktEndpointErrKind::Endpoint(EndpointErrKind::Closed) => {
                debug!("[{}] port (addr={}) rx disabled", self.name(), &addr);
                self.status.rx_port_num -= 1;
            }
            Err(ref e) => {
                debug!(
                    "[{}] port (addr={}) rx recv err: {:?}",
                    self.name(),
                    &addr,
                    e
                );
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
            trace!("[{}] Packet is sent via port({})", self.name(), dst_ref);
            port.send_pkt(pkt);
        } else {
            warn!(
                "[{}] Packet has a dest addr {} which can not be found",
                self.name(),
                dst_ref
            );
        }
    }
}

#[derive(Debug, Clone)]
pub enum CtrlMsg<T: Debug + Clone> {
    Request(CtrlMsgRequest),
    Reply(CtrlMsgReply<T>),
    Error(SwitchError),
}

impl<T: Debug + Clone> CtrlMsg<T> {
    pub fn shutdown_server_request() -> Self {
        Self::Request(CtrlMsgRequest::ShutdownServer)
    }

    pub fn new_endpoint_request(addr: Address) -> Self {
        Self::Request(CtrlMsgRequest::NewEndpoint(addr))
    }

    pub fn to_result(self) -> Result<CtrlMsgReply<T>, SwitchError> {
        match self {
            CtrlMsg::Error(e) => Err(e),
            CtrlMsg::Reply(reply) => Ok(reply),
            _ => Err(SwitchError::type_mismatch(
                "ctrl msg doesn't has reply type",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CtrlMsgRequest {
    ShutdownServer,
    NewEndpoint(Address),
}

#[derive(Debug, Clone)]
pub enum CtrlMsgReply<T: Clone + Debug> {
    NewEndpoint(PktEndpoint<T>),
    Request,
    Response,
}
