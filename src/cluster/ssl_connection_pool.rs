use openssl::ssl::SslConnector;
use r2d2::{Builder, ManageConnection, Pool};
use std::cell::RefCell;
use std::error::Error;
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::authenticators::Authenticator;
use crate::cluster::ConnectionPool;
use crate::cluster::{startup, NodeSslConfig};
use crate::compression::Compression;
use crate::error;
use crate::frame::parser::parse_frame;
use crate::frame::{Frame, IntoBytes};
use crate::transport::CDRSTransport;
use crate::transport::TransportTls;

/// Shortcut for `r2d2::Pool` type of SSL-based CDRS connections.
pub type SslConnectionPool<A> = ConnectionPool<SslConnectionsManager<A>>;

/// `r2d2::Pool` of SSL-based CDRS connections.
///
/// Used internally for SSL Session for holding connections to a specific Cassandra node.
pub fn new_ssl_pool<'a, A: Authenticator + Send + Sync + 'static>(
    node_config: NodeSslConfig<'a, A>,
) -> error::Result<SslConnectionPool<A>> {
    let manager = SslConnectionsManager::new(
        node_config.addr,
        node_config.authenticator,
        node_config.ssl_connector,
    );

    let pool = Builder::new()
        .max_size(node_config.max_size)
        .min_idle(node_config.min_idle)
        .max_lifetime(node_config.max_lifetime)
        .idle_timeout(node_config.idle_timeout)
        .connection_timeout(node_config.connection_timeout)
        .build(manager)
        .map_err(|err| error::Error::from(err.description()))?;

    let addr = node_config
        .addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| error::Error::from("Cannot parse address"))?;

    Ok(SslConnectionPool::new(pool, addr))
}

/// `r2d2` connection manager.
pub struct SslConnectionsManager<A> {
    addr: String,
    ssl_connector: SslConnector,
    auth: A,
}

impl<A> SslConnectionsManager<A> {
    pub fn new<S: ToString>(addr: S, auth: A, ssl_connector: SslConnector) -> Self {
        SslConnectionsManager {
            addr: addr.to_string(),
            auth,
            ssl_connector,
        }
    }
}

impl<A: Authenticator + 'static + Send + Sync> ManageConnection for SslConnectionsManager<A> {
    type Connection = RefCell<TransportTls>;
    type Error = error::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = RefCell::new(TransportTls::new(&self.addr, &self.ssl_connector)?);
        startup(&transport, &self.auth)?;

        Ok(transport)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let options_frame = Frame::new_req_options().into_cbytes();
        conn.borrow_mut().write(options_frame.as_slice())?;

        parse_frame(conn, &Compression::None {}).map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.borrow().is_alive()
    }
}
