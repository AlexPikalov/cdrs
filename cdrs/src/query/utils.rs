use std::cell::RefCell;
use std::ops::Deref;

use cassandra_proto::{compression::Compressor, error, frame::parser::parse_frame, frame::Frame};
use r2d2;

use crate::transport::CDRSTransport;

pub fn from_connection<M, T>(
  conn: &r2d2::PooledConnection<M>,
  compressor: &impl Compressor,
) -> error::Result<Frame>
where
  T: CDRSTransport + 'static,
  M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
{
  parse_frame(conn.deref(), compressor)
}
