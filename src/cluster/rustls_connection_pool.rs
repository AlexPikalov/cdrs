use r2d2::{Builder, ManageConnection};

use std::net;
use std::sync::Arc;
use std::io::Write;
use std::error::Error;
use core::cell::RefCell;

use crate::cluster::{startup, NodeRustlsConfig};
use crate::authenticators::Authenticator;
use crate::cluster::ConnectionPool;
use crate::compression::Compression;
use crate::frame::parser::parse_frame;
use crate::frame::{Frame, IntoBytes};
use crate::transport::{CDRSTransport, TransportRustls};
use crate::error;

pub type RustlsConnectionPool<A> = ConnectionPool<RustlsConnectionsManager<A>>;

/// `r2d2::Pool` of SSL-based CDRS connections.
///
/// Used internally for SSL Session for holding connections to a specific Cassandra node.
pub fn new_rustls_pool<A: Authenticator + Send + Sync + 'static>(node_config: NodeRustlsConfig<A>) -> error::Result<RustlsConnectionPool<A>> {
    let manager = RustlsConnectionsManager::new(
        node_config.addr,
        node_config.dns_name,
        node_config.config,
        node_config.authenticator,
    );

    let pool = Builder::new()
        .max_size(node_config.max_size)
        .min_idle(node_config.min_idle)
        .max_lifetime(node_config.max_lifetime)
        .idle_timeout(node_config.idle_timeout)
        .connection_timeout(node_config.connection_timeout)
        .build(manager)
        .map_err(|err| error::Error::from(err.description()))?;

    Ok(RustlsConnectionPool::new(pool, node_config.addr))
}

/// `r2d2` connection manager.
pub struct RustlsConnectionsManager<A> {
    addr: net::SocketAddr,
    dns_name: webpki::DNSName,
    config: Arc<rustls::ClientConfig>,
    auth: A,
}

impl<A> RustlsConnectionsManager<A> {
    #[inline]
    pub fn new(addr: net::SocketAddr, dns_name: webpki::DNSName, config: Arc<rustls::ClientConfig>, auth: A) -> Self {
        Self {
            addr,
            dns_name,
            config,
            auth,
        }
    }
}

impl<A: Authenticator + 'static + Send + Sync> ManageConnection for RustlsConnectionsManager<A> {
    type Connection = RefCell<TransportRustls>;
    type Error = error::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = RefCell::new(TransportRustls::new(self.addr, self.dns_name.clone(), self.config.clone())?);
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
