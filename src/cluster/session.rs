use async_trait::async_trait;
use bb8;
use fnv::FnvHashMap;
use std::iter::Iterator;
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, sync::Mutex};

#[cfg(feature = "unstable-dynamic-cluster")]
use crate::cluster::NodeTcpConfig;
#[cfg(feature = "ssl")]
use crate::cluster::{new_ssl_pool, ClusterSslConfig, NodeSslConfig, SslConnectionPool};
use crate::cluster::{new_tcp_pool, startup, CDRSSession, ClusterTcpConfig, ConnectionPool, GetCompressor, GetConnection, TcpConnectionPool, ResponseCache};
use crate::error;
use crate::load_balancing::LoadBalancingStrategy;
use crate::transport::{CDRSTransport, TransportTcp};

use crate::authenticators::Authenticator;
use crate::cluster::SessionPager;
use crate::compression::Compression;
use crate::events::{new_listener, EventStream, EventStreamNonBlocking, Listener};
use crate::frame::events::{ServerEvent, SimpleServerEvent, StatusChange, StatusChangeType};
use crate::frame::parser::parse_frame;
use crate::frame::{Frame, IntoBytes, StreamId};
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};

#[cfg(feature = "ssl")]
use crate::transport::TransportTls;

/// CDRS session that holds one pool of authorized connecitons per node.
/// `compression` field contains data compressor that will be used
/// for decompressing data received from Cassandra server.
pub struct Session<LB> {
    load_balancing: Mutex<LB>,
    event_stream: Option<Mutex<EventStreamNonBlocking>>,
    responses: Mutex<FnvHashMap<StreamId, Frame>>,
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
        T: CDRSTransport + Unpin + 'static,
        M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error>,
    >(
        &'a mut self,
        page_size: i32,
    ) -> SessionPager<'a, M, Session<LB>, T>
    where
        Session<LB>: CDRSSession<'static, T, M>,
    {
        return SessionPager::new(self, page_size);
    }
}

#[async_trait]
impl<
        T: CDRSTransport + Send + Sync + 'static,
        M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized + Send + Sync,
    > GetConnection<T, M> for Session<LB>
{
    async fn get_connection(&self) -> Option<Arc<ConnectionPool<M>>> {
        if cfg!(feature = "unstable-dynamic-cluster") {
            if let Some(ref event_stream_mx) = self.event_stream {
                if let Ok(ref mut event_stream) = event_stream_mx.try_lock() {
                    loop {
                        let next_event = event_stream.next();

                        match next_event {
                            None => break,
                            Some(ServerEvent::StatusChange(StatusChange {
                                addr,
                                change_type: StatusChangeType::Down,
                            })) => {
                                self.load_balancing
                                    .lock()
                                    .await
                                    .remove_node(|pool| pool.get_addr() == addr.addr);
                            }
                            Some(_) => continue,
                        }
                    }
                }
            }
        }

        self.load_balancing
            .lock()
            .await
            .next()
    }
}

#[async_trait]
impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized + Send + Sync,
    > QueryExecutor<T, M> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized + Send + Sync,
        M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
    > PrepareExecutor<T, M> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized + Send + Sync,
        M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
    > ExecExecutor<T, M> for Session<LB>
{
}

#[async_trait]
impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized + Send + Sync,
        M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
    > BatchExecutor<T, M> for Session<LB>
{
}

impl<
        'a,
        T: CDRSTransport + Unpin + 'static,
        M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error> + Sized,
        LB: LoadBalancingStrategy<ConnectionPool<M>> + Sized + Send + Sync,
    > CDRSSession<'a, T, M> for Session<LB>
{
}

#[async_trait]
impl <LB> ResponseCache for Session<LB> where LB: Send {
    async fn match_or_cache_response(&self, stream_id: i16, frame: Frame) -> Option<Frame> {
        if frame.stream == stream_id {
            return Some(frame);
        }

        self.responses.lock().await.insert(frame.stream, frame);
        self.responses.lock().await.remove(&stream_id)
    }
}

async fn connect_static<A, LB>(
    node_configs: &ClusterTcpConfig<'_, A>,
    mut load_balancing: LB,
    compression: Compression,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<Arc<TcpConnectionPool<A>>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Mutex::new(FnvHashMap::default()),
        compression,
    })
}

#[cfg(feature = "unstable-dynamic-cluster")]
async fn connect_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    mut load_balancing: LB,
    compression: Compression,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<Arc<TcpConnectionPool<A>>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_tcp_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Mutex::new(FnvHashMap::default()),
        compression,
    };

    let (listener, event_stream) = session.listen_non_blocking(
        event_src.addr,
        event_src.authenticator,
        vec![SimpleServerEvent::StatusChange],
    ).await?;

    tokio::spawn(listener.start(&Compression::None));

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_static(node_configs, load_balancing, Compression::None).await
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn new_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_dynamic(node_configs, load_balancing, Compression::None, event_src).await
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new_snappy<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_static(node_configs, load_balancing, Compression::Snappy).await
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn new_snappy_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_dynamic(node_configs, load_balancing, Compression::Snappy, event_src).await
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub async fn new_lz4<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_static(node_configs, load_balancing, Compression::Lz4).await
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time. Once received topology change event, it will adjust an inner load
/// balancer.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * node address where to listen events
#[cfg(feature = "unstable-dynamic-cluster")]
pub async fn new_lz4_dynamic<'a, A, LB>(
    node_configs: &ClusterTcpConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeTcpConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
    connect_dynamic(node_configs, load_balancing, Compression::Lz4, event_src).await
}

impl<'a, L> Session<L> {
    /// Returns new event listener.
    pub async fn listen<A: Authenticator + 'static + Sized>(
        &self,
        node: &str,
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<Mutex<TransportTcp>>, EventStream)> {
        let compression = self.get_compressor();
        let transport = TransportTcp::new(&node).await.map(Mutex::new)?;

        startup(&transport, &authenticator).await?;

        let query_frame = Frame::new_req_register(events).into_cbytes();
        transport.lock().await.write(query_frame.as_slice()).await?;
        parse_frame(&transport, &compression).await?;

        Ok(new_listener(transport))
    }

    pub async fn listen_non_blocking<A: Authenticator + 'static + Sized>(
        &self,
        node: &str,
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<Mutex<TransportTcp>>, EventStreamNonBlocking)> {
        self.listen(node, authenticator, events).await.map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }
}

#[cfg(feature = "ssl")]
async fn connect_ssl_static<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    mut load_balancing: LB,
    compression: Compression,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<Arc<SslConnectionPool<A>>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_ssl_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    Ok(Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Mutex::new(FnvHashMap::default()),
        compression,
    })
}

#[cfg(feature = "ssl")]
async fn connect_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    mut load_balancing: LB,
    compression: Compression,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    let mut nodes: Vec<Arc<SslConnectionPool<A>>> = Vec::with_capacity(node_configs.0.len());

    for node_config in &node_configs.0 {
        let node_connection_pool = new_ssl_pool(node_config.clone()).await?;
        nodes.push(Arc::new(node_connection_pool));
    }

    load_balancing.init(nodes);

    let mut session = Session {
        load_balancing: Mutex::new(load_balancing),
        event_stream: None,
        responses: Mutex::new(FnvHashMap::default()),
        compression,
    };

    let (listener, event_stream) = session.listen_non_blocking_ssl(
        event_src.addr,
        event_src.authenticator,
        vec![SimpleServerEvent::TopologyChange],
    ).await?;

    tokio::spawn(listener.start(&Compression::None));

    session.event_stream = Some(Mutex::new(event_stream));

    Ok(session)
}

/// Creates new SSL-based session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub async fn new_ssl<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_static(node_configs, load_balancing, Compression::None).await
}

/// Creates new SSL-based session that will perform queries without any compression. `Compression` type
/// can be changed at any time. Once received `TopologyChange` event from event source node it will adjust
/// a cluster - remove dead nodes.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * event source node SSL configuration.
#[cfg(feature = "ssl")]
pub async fn new_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_dynamic(node_configs, load_balancing, Compression::None, event_src).await
}

/// Creates new SSL-based session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub async fn new_snappy_ssl<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_static(node_configs, load_balancing, Compression::Snappy).await
}

/// Creates new SSL-based session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time. Once received `TopologyChange` event from event source node it will adjust
/// a cluster - remove dead nodes.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * event source node SSL configuration.
#[cfg(feature = "ssl")]
pub async fn new_snappy_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_dynamic(node_configs, load_balancing, Compression::Snappy, event_src).await
}

/// Creates new SSL-based session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub async fn new_lz4_ssl<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_static(node_configs, load_balancing, Compression::Lz4).await
}

/// Creates new SSL-based session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time. Once received `TopologyChange` event from event source node it will adjust
/// a cluster - remove dead nodes.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
/// * event source node SSL configuration.
#[cfg(feature = "ssl")]
pub async fn new_lz4_ssl_dynamic<'a, A, LB>(
    node_configs: &ClusterSslConfig<'a, A>,
    load_balancing: LB,
    event_src: NodeSslConfig<'a, A>,
) -> error::Result<Session<LB>>
where
    A: Authenticator + 'static + Sized,
    LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
    connect_ssl_dynamic(node_configs, load_balancing, Compression::Lz4, event_src).await
}

/// Returns new SSL-based event listener.
#[cfg(feature = "ssl")]
impl<'a, L> Session<L> {
    pub async fn listen_ssl<A: Authenticator + 'static + Sized>(
        &self,
        addr_ref: &str,
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<Mutex<TransportTls>>, EventStream)> {
        let compression = self.get_compressor();
        let transport = TransportTls::new(addr_ref).await.map(Mutex::new)?;

        startup(&transport, &authenticator).await?;

        let query_frame = Frame::new_req_register(events).into_cbytes();
        transport.lock().await.write(query_frame.as_slice()).await?;
        parse_frame(&transport, &compression).await?;

        Ok(new_listener(transport))
    }

    pub async fn listen_non_blocking_ssl<A: Authenticator + 'static + Sized>(
        &self,
        node: &str,
        authenticator: A,
        events: Vec<SimpleServerEvent>,
    ) -> error::Result<(Listener<Mutex<TransportTls>>, EventStreamNonBlocking)> {
        self.listen_ssl(node, authenticator, events).await.map(|l| {
            let (listener, stream) = l;
            (listener, stream.into())
        })
    }
}
