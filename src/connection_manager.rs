//! This modules contains an implementation of [r2d2](https://github.com/sfackler/r2d2)
//! functionality of connection pools. To get more details about creating r2d2 pools
//! please refer to original documentation.
use client::{CDRS, Session, QueryBuilder};
use error::{Error as CError};
use authenticators::Authenticator;
use compression::Compression;
use r2d2;

#[cfg(not(feature = "ssl"))]
use transport::Transport;

/// [r2d2](https://github.com/sfackler/r2d2) `ManageConnection` for non-SSL
/// transport
#[cfg(not(features = "ssl"))]
pub struct ConnectionManager<T> {
    transport: Transport,
    authenticator: T,
    compression: Compression
}

impl<T: Authenticator + Send + Sync + 'static> ConnectionManager<T> {
    /// Creates a new instance of `ConnectionManager`.
    /// It requires transport, authenticator and compression as inputs.
    pub fn new(transport: Transport, authenticator: T, compression: Compression)
        -> ConnectionManager<T> {
        ConnectionManager {
            transport: transport,
            authenticator: authenticator,
            compression: compression
        }
    }
}

impl<T: Authenticator + Send + Sync + 'static> r2d2::ManageConnection for ConnectionManager<T> {
    type Connection = Session<T>;
    type Error = CError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = try!(self.transport.try_clone());
        let compression = self.compression.clone();
        let cdrs = CDRS::new(transport, self.authenticator.clone());

        cdrs.start(compression)
    }

    fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        let query = QueryBuilder::new("DESCRIBE keyspaces;").finalize();

        connection.query(query, false, false).map(|_| (()))
    }

    fn has_broken(&self, _connection: &mut Self::Connection) -> bool {
        false
    }
}
