use r2d2;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::io::Write;
use std::iter::Iterator;
use std::sync::Mutex;

#[cfg(feature = "ssl")]
use crate::cluster::{new_ssl_pool, ClusterSslConfig, NodeSslConfig, SslConnectionPool};
use crate::cluster::{
    new_tcp_pool, startup, CDRSSession, ClusterTcpConfig, ConnectionPool, GetCompressor,
    GetConnection, NodeTcpConfig, TcpConnectionPool,
};
use crate::error;
use crate::load_balancing::LoadBalancingStrategy;
use crate::transport::{CDRSTransport, TransportTcp};

use crate::authenticators::Authenticator;
use crate::cluster::SessionPager;
use crate::compression::Compression;
use crate::events::{new_listener, EventStream, EventStreamNonBlocking, Listener};
use crate::frame::events::{ServerEvent, SimpleServerEvent, TopologyChange, TopologyChangeType};
use crate::frame::parser::parse_frame;
use crate::frame::{Frame, IntoBytes};
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};

#[cfg(feature = "ssl")]
use crate::transport::TransportTls;
#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;

/// CDRS session that holds one pool of authorized connecitons per node.
/// `compression` field contains data compressor that will be used
/// for decompressing data received from Cassandra server.
pub struct Session<LB> {
    load_balancing: Mutex<LB>,
    event_stream: Option<Mutex<EventStreamNonBlocking>>,
    #[allow(dead_code)]
    pub compression: Compression,
}

impl<'a, LB> GetCompressor<'a> for Session<LB> {
    /// Returns compression that current session has.
    fn get_compressor(&self) -> Compression {
        self.compression.clone()
    }
}

impl<'a, LB: Sized> Session<LB> {
    /// Basing on current session returns new `SessionPager` that can be used
    /// for performing paged queries.
    pub fn paged<
        T: CDRSTransport + 'static,
        M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error>,
    >(
        &'a self,
        page_size: i32,
    ) -> SessionPager<'a, M, Session<LB>, T>
    where
        Session<LB>: CDRSSession<'static, T, M>,
    {
        return SessionPager::new(self, page_size);
    }
}

impl<
        T: CDRSTransport + Send + Sync + 'static,
        M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized,
    > GetConnection<T, M> for Session<LB>
{
    fn get_connection(&self) -> Option<r2d2::PooledConnection<M>> {
        if let Some(ref event_stream_mx) = self.event_stream {
            if let Ok(ref mut event_stream) = event_stream_mx.try_lock() {
                loop {
                    let next_event = event_stream.borrow_mut().next();

                    match next_event {
                        None => break,
                        Some(ServerEvent::TopologyChange(TopologyChange {
                            addr,
                            change_type: TopologyChangeType::RemovedNode,
                        })) => {
                            self.load_balancing
                                .lock()
                                .ok()?
                                .borrow_mut()
                                .remove_node(|pool| pool.get_addr() == addr.addr);
                        }
                        Some(_) => continue,
                    }
                }
            }
        }

        self.load_balancing
            .lock()
            .ok()?
            .borrow()
            .next()
            .and_then(|pool| pool.get_pool().get().ok())
    }
}

impl<
        'a,
        T: CDRSTransport + 'static,
        M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized,
    > QueryExecutor<T, M> for Session<LB>
{
}

impl<
        'a,
        T: CDRSTransport + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized,
        M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    > PrepareExecutor<T, M> for Session<LB>
{
}

impl<
        'a,
        T: CDRSTransport + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized,
        M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    > ExecExecutor<T, M> for Session<LB>
{
}

impl<
        'a,
        T: CDRSTransport + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized,
        M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    > BatchExecutor<T, M> for Session<LB>
{
}

impl<
        'a,
        T: CDRSTransport + 'static,
        M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized,
    > CDRSSession<'a, T, M> for Session<LB>
{
}

fn connect_static<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    mut load_balancing: LB,
    compression: Compression,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<TcpConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone())?;
        nodes.push(node_connection_pool);
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
    })
}

fn connect_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    mut load_balancing: LB,
    compression: Compression,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<TcpConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone())?;
        nodes.push(node_connection_pool);
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
    };

    let (listener, event_stream) = session.listen_non_blocking(
        event_src.addr,
        event_src.authenticator,
        vec![SimpleServerEvent::TopologyChange],
    )?;

    ::std::thread::spawn(move || listener.start(&Compression::None));

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub fn new<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_static(node_configs, load_balancing, Compression::None)
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
pub fn new_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_dynamic(node_configs, load_balancing, Compression::None, event_src)
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub fn new_snappy<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_static(node_configs, load_balancing, Compression::Snappy)
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
pub fn new_snappy_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_dynamic(node_configs, load_balancing, Compression::Snappy, event_src)
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub fn new_lz4<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_static(node_configs, load_balancing, Compression::Lz4)
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
pub fn new_lz4_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_dynamic(node_configs, load_balancing, Compression::Lz4, event_src)
}

impl<'a, L> Session<L> {
    /// Returns new event listener.
    pub fn listen<A: Authenticator + 'static + Sized>(
        &self,
        node: &str,
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<RefCell<TransportTcp>>, EventStream)> {
        let compression = self.get_compressor();
        let transport = TransportTcp::new(&node).map(RefCell::new)?;

        startup(&transport, &authenticator)?;

        let query_frame = Frame::new_req_register(events).into_cbytes();
        transport.borrow_mut().write(query_frame.as_slice())?;
        parse_frame(&transport, &compression)?;

        Ok(new_listener(transport))
    }

    pub fn listen_non_blocking<A: Authenticator + 'static + Sized>(
        &self,
        node: &str,
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<RefCell<TransportTcp>>, EventStreamNonBlocking)> {
        self.listen(node, authenticator, events).map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }
}

#[cfg(feature = "ssl")]
fn connect_ssl_static<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    mut load_balancing: LB,
    compression: Compression,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<SslConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_ssl_pool(node_config.clone())?;
        nodes.push(node_connection_pool);
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
    })
}

#[cfg(feature = "ssl")]
fn connect_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    mut load_balancing: LB,
    compression: Compression,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<SslConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_ssl_pool(node_config.clone())?;
        nodes.push(node_connection_pool);
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        compression,
    };

    let (listener, event_stream) = session.listen_non_blocking_ssl(
        (event_src.addr, &event_src.ssl_connector),
        event_src.authenticator,
        vec![SimpleServerEvent::TopologyChange],
    )?;

    ::std::thread::spawn(move || listener.start(&Compression::None));

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

/// Creates new SSL-based session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub fn new_ssl<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_static(node_configs, load_balancing, Compression::None)
}

/// Creates new SSL-based session that will perform queries without any compression. `Compression` type
/// can be changed at any time. Once received `TopologyChange` event from event source node it will adjust
/// a cluster - remove dead nodes.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * event source node SSL configuration.
#[cfg(feature = "ssl")]
pub fn new_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_dynamic(node_configs, load_balancing, Compression::None, event_src)
}

/// Creates new SSL-based session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub fn new_snappy_ssl<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_static(node_configs, load_balancing, Compression::Snappy)
}

/// Creates new SSL-based session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time. Once received `TopologyChange` event from event source node it will adjust
/// a cluster - remove dead nodes.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * event source node SSL configuration.
#[cfg(feature = "ssl")]
pub fn new_snappy_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_dynamic(node_configs, load_balancing, Compression::Snappy, event_src)
}

/// Creates new SSL-based session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub fn new_lz4_ssl<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_static(node_configs, load_balancing, Compression::Lz4)
}

/// Creates new SSL-based session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time. Once received `TopologyChange` event from event source node it will adjust
/// a cluster - remove dead nodes.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * event source node SSL configuration.
#[cfg(feature = "ssl")]
pub fn new_lz4_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_dynamic(node_configs, load_balancing, Compression::Lz4, event_src)
}

/// Returns new SSL-based event listener.
#[cfg(feature = "ssl")]
impl<'a, L> Session<L> {
    pub fn listen_ssl<A: Authenticator + 'static + Sized>(
        &self,
        node: (&str, &SslConnector),
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<RefCell<TransportTls>>, EventStream)> {
        let (addr_ref, ssl_connector_ref) = node;
        let compression = self.get_compressor();
        let transport = TransportTls::new(addr_ref, ssl_connector_ref).map(RefCell::new)?;

        startup(&transport, &authenticator)?;

        let query_frame = Frame::new_req_register(events).into_cbytes();
        transport.borrow_mut().write(query_frame.as_slice())?;
        parse_frame(&transport, &compression)?;

        Ok(new_listener(transport))
    }

    pub fn listen_non_blocking_ssl<A: Authenticator + 'static + Sized>(
        &self,
        node: (&str, &SslConnector),
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<RefCell<TransportTls>>, EventStreamNonBlocking)> {
        self.listen_ssl(node, authenticator, events).map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }
}
