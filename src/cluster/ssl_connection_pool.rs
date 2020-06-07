use async_trait::async_trait;
use bb8::{Builder, ManageConnection};
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;
use std::net::SocketAddr;

use crate::authenticators::Authenticator;
use crate::cluster::ConnectionPool;
use crate::cluster::{startup, NodeSslConfig};
use crate::compression::Compression;
use crate::error;
use crate::frame::parser::parse_frame;
use crate::frame::{Frame, IntoBytes};
use crate::transport::TransportTls;

/// Shortcut for `bb8::Pool` type of SSL-based CDRS connections.
pub type SslConnectionPool<A> = ConnectionPool<SslConnectionsManager<A>>;

/// `bb8::Pool` of SSL-based CDRS connections.
///
/// Used internally for SSL Session for holding connections to a specific Cassandra node.
pub async fn new_ssl_pool<'a, A: Authenticator + Send + Sync + 'static>(
    node_config: NodeSslConfig<'a, A>,
) -> error::Result<SslConnectionPool<A>> {
    let manager = SslConnectionsManager::new(
        node_config.addr,
        node_config.authenticator,
    );

    let pool = Builder::new()
        .max_size(node_config.max_size)
        .min_idle(node_config.min_idle)
        .max_lifetime(node_config.max_lifetime)
        .idle_timeout(node_config.idle_timeout)
        .connection_timeout(node_config.connection_timeout)
        .build(manager)
        .await
        .map_err(|err| error::Error::from(err.to_string()))?;

    Ok(SslConnectionPool::new(
        pool,
        node_config
            .addr
            .parse::<SocketAddr>()
            .map_err(|err| error::Error::from(err.to_string()))?,
    ))
}

/// `bb8` connection manager.
#[derive(Debug)]
pub struct SslConnectionsManager<A> {
    addr: String,
    auth: A,
}

impl<A> SslConnectionsManager<A> {
    pub fn new<S: ToString>(addr: S, auth: A) -> Self {
        SslConnectionsManager {
            addr: addr.to_string(),
            auth,
        }
    }
}

#[async_trait]
impl<A: Authenticator + 'static + Send + Sync> ManageConnection for SslConnectionsManager<A> {
    type Connection = Mutex<TransportTls>;
    type Error = error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = Mutex::new(TransportTls::new(&self.addr).await?);
        startup(&transport, &self.auth).await?;

        Ok(transport)
    }

    async fn is_valid(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        let options_frame = Frame::new_req_options().into_cbytes();
        conn.lock().await.write(options_frame.as_slice()).await?;

        parse_frame(&conn, &Compression::None {}).await.map(|_| conn)
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
