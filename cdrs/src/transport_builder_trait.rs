use std::io;

use crate::transport::CDRSTransport;

pub trait CDRSTransportBuilder<T: CDRSTransport>: Sized {
  fn create(&self) -> io::Result<T>;
}
