//! This modules contains an implementation of [r2d2](https://github.com/sfackler/r2d2)
//! functionality of connection pools. To get more details about creating r2d2 pools
//! please refer to original documentation.
use client::{Session, CDRS};
use error::Error as CError;
use authenticators::Authenticator;
use compression::Compression;
use r2d2;
use transport::CDRSTransport;

/// [r2d2](https://github.com/sfackler/r2d2) `ManageConnection`.
pub struct ConnectionManager<T, X> {
    transport: X,
    authenticator: T,
    compression: Compression,
}

impl<T: Authenticator + Send + Sync + 'static, X: CDRSTransport + Send + Sync + 'static>
    ConnectionManager<T, X> {
    /// Creates a new instance of `ConnectionManager`.
    /// It requires transport, authenticator and compression as inputs.
    pub fn new(transport: X,
               authenticator: T,
               compression: Compression)
               -> ConnectionManager<T, X> {
        ConnectionManager { transport: transport,
                            authenticator: authenticator,
                            compression: compression, }
    }
}

impl<T: Authenticator + Send + Sync + 'static,
     X: CDRSTransport + Send + Sync + 'static> r2d2::ManageConnection for ConnectionManager<T, X> {
    type Connection = Session<T, X>;
    type Error = CError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport = try!(self.transport.try_clone());
        let compression = self.compression;
        let cdrs = CDRS::new(transport, self.authenticator.clone());

        cdrs.start(compression)
    }

    fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        if connection.is_connected() {
            Ok(())
        } else {
            Err("Connection to DB was dropped".into())
        }
    }

    fn has_broken(&self, _connection: &mut Self::Connection) -> bool {
        false
    }
}
