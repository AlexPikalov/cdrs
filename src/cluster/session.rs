use std::io;

use error;
use transport::{CDRSTransport, TransportTcp};
use load_balancing::LoadBalancingStrategy;
use cluster::{CDRSSession, GetCompressor, GetTransport, SessionPager};

use authenticators::Authenticator;
use compression::Compression;
use frame::{Frame, IntoBytes, Opcode};
use frame::parser::parse_frame;
use query::{ExecExecutor, PrepareExecutor, QueryExecutor};

#[cfg(feature = "ssl")]
use transport::TransportTls;
#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;


pub struct Session<LB, A> {
  load_balancing: LB,
  #[allow(dead_code)] authenticator: A,
  pub compression: Compression,
}

impl<'a, LB, A> GetCompressor<'a> for Session<LB, A> {
  fn get_compressor(&self) -> Compression {
    self.compression.clone()
  }
}

impl<'a, LB: Sized, A: Authenticator + 'a + Sized> Session<LB, A> {
  pub fn paged<T: CDRSTransport>(&'a mut self,
                                 page_size: i32)
                                 -> SessionPager<'a, Session<LB, A>, T>
    where Session<LB, A>: CDRSSession<'a, T>
  {
    return SessionPager::new(self, page_size);
  }

  fn startup<'b, T: CDRSTransport>(transport: &'b mut T,
                                   session_authenticator: &'b A)
                                   -> error::Result<()> {
    let ref mut compression = Compression::None;
    let startup_frame = Frame::new_req_startup(compression.as_str()).into_cbytes();

    try!(transport.write(startup_frame.as_slice()));
    let start_response = try!(parse_frame(transport, compression));

    if start_response.opcode == Opcode::Ready {
      return Ok(());
    }

    if start_response.opcode == Opcode::Authenticate {
      let body = start_response.get_body()?;
      let authenticator = body.get_authenticator()
                              .expect(
        "Cassandra Server did communicate that it needed password
                authentication but the  auth schema was missing in the body response",
      );

      // This creates a new scope; avoiding a clone
      // and we check whether
      // 1. any authenticators has been passed in by client and if not send error back
      // 2. authenticator is provided by the client and `auth_scheme` presented by
      //      the server and client are same if not send error back
      // 3. if it falls through it means the preliminary conditions are true

      let auth_check =
        session_authenticator.get_cassandra_name()
                     .ok_or(error::Error::General("No authenticator was provided".to_string()))
                     .map(|auth| {
                       if authenticator != auth {
                         let io_err = io::Error::new(
              io::ErrorKind::NotFound,
              format!(
                "Unsupported type of authenticator. {:?} got,
                             but {} is supported.",
                authenticator, authenticator
              ),
            );
                         return Err(error::Error::Io(io_err));
                       }
                       Ok(())
                     });

      if let Err(err) = auth_check {
        return Err(err);
      }

      let auth_token_bytes = session_authenticator.get_auth_token().into_cbytes();
      try!(transport.write(Frame::new_req_auth_response(auth_token_bytes).into_cbytes()
                                                                         .as_slice()));
      try!(parse_frame(transport, compression));

      return Ok(());
    }

    unimplemented!();
  }
}

impl<'a,
     T: CDRSTransport + 'a,
     LB: LoadBalancingStrategy<T> + Sized,
     A: Authenticator + Sized> GetTransport<'a, T> for Session<LB, A> {
  fn get_transport(&mut self) -> Option<&mut T> {
    self.load_balancing.next()
  }
}

impl<'a,
     T: CDRSTransport + 'a,
     LB: LoadBalancingStrategy<T> + Sized,
     A: Authenticator + Sized> QueryExecutor<'a, T> for Session<LB, A> {
}

impl<'a,
     T: CDRSTransport + 'a,
     LB: LoadBalancingStrategy<T> + Sized,
     A: Authenticator + Sized> PrepareExecutor<'a, T> for Session<LB, A> {
}

impl<'a,
     T: CDRSTransport + 'a,
     LB: LoadBalancingStrategy<T> + Sized,
     A: Authenticator + Sized> ExecExecutor<'a, T> for Session<LB, A> {
}

impl<'a,
     T: CDRSTransport + 'a,
     LB: LoadBalancingStrategy<T> + Sized,
     A: Authenticator + Sized> CDRSSession<'a, T> for Session<LB, A> {
}

impl<'a, LB: LoadBalancingStrategy<TransportTcp> + Sized, A: Authenticator + 'a + Sized>
  Session<LB, A> {
  pub fn new(addrs: &Vec<&str>,
             mut load_balancing: LB,
             authenticator: A)
             -> error::Result<Session<LB, A>> {
    let mut nodes: Vec<TransportTcp> = Vec::with_capacity(addrs.len());

    for addr in addrs {
      let mut transport = TransportTcp::new(&addr)?;
      Self::startup(&mut transport, &authenticator)?;
      nodes.push(transport);
    }

    load_balancing.init(nodes);

    Ok(Session { load_balancing,
                 authenticator,
                 compression: Compression::None, })
  }

  pub fn new_snappy(addrs: &Vec<&str>,
                    mut load_balancing: LB,
                    authenticator: A)
                    -> error::Result<Session<LB, A>> {
    let mut nodes: Vec<TransportTcp> = Vec::with_capacity(addrs.len());

    for addr in addrs {
      let mut transport = TransportTcp::new(&addr)?;
      Self::startup(&mut transport, &authenticator)?;
      nodes.push(transport);
    }

    load_balancing.init(nodes);

    Ok(Session { load_balancing,
                 authenticator,
                 compression: Compression::Snappy, })
  }

  pub fn new_lz4(addrs: &Vec<&str>,
                 mut load_balancing: LB,
                 authenticator: A)
                 -> error::Result<Session<LB, A>> {
    let mut nodes: Vec<TransportTcp> = Vec::with_capacity(addrs.len());

    for addr in addrs {
      let mut transport = TransportTcp::new(&addr)?;
      Self::startup(&mut transport, &authenticator)?;
      nodes.push(transport);
    }

    load_balancing.init(nodes);

    Ok(Session { load_balancing,
                 authenticator,
                 compression: Compression::Lz4, })
  }
}

#[cfg(feature = "ssl")]
impl<'a, LB: LoadBalancingStrategy<TransportTls> + Sized, A: Authenticator + 'a + Sized>
  Session<LB, A> {
  pub fn new_ssl(addrs: &Vec<&str>,
                 mut load_balancing: LB,
                 authenticator: A,
                 ssl_connector: &SslConnector)
                 -> error::Result<Session<LB, A>> {
    let mut nodes: Vec<TransportTls> = Vec::with_capacity(addrs.len());

    for addr in addrs {
      let mut transport = TransportTls::new(&addr, ssl_connector)?;
      Self::startup(&mut transport, &authenticator)?;
      nodes.push(transport);
    }

    load_balancing.init(nodes);

    Ok(Session { load_balancing,
                 authenticator,
                 compression: Compression::None, })
  }

  pub fn new_snappy_ssl(addrs: &Vec<&str>,
                        mut load_balancing: LB,
                        authenticator: A,
                        ssl_connector: &SslConnector)
                        -> error::Result<Session<LB, A>> {
    let mut nodes: Vec<TransportTls> = Vec::with_capacity(addrs.len());

    for addr in addrs {
      let mut transport = TransportTls::new(&addr, ssl_connector)?;
      Self::startup(&mut transport, &authenticator)?;
      nodes.push(transport);
    }

    load_balancing.init(nodes);

    Ok(Session { load_balancing,
                 authenticator,
                 compression: Compression::Snappy, })
  }

  pub fn new_lz4_ssl(addrs: &Vec<&str>,
                     mut load_balancing: LB,
                     authenticator: A,
                     ssl_connector: &SslConnector)
                     -> error::Result<Session<LB, A>> {
    let mut nodes: Vec<TransportTls> = Vec::with_capacity(addrs.len());

    for addr in addrs {
      let mut transport = TransportTls::new(&addr, ssl_connector)?;
      Self::startup(&mut transport, &authenticator)?;
      nodes.push(transport);
    }

    load_balancing.init(nodes);

    Ok(Session { load_balancing,
                 authenticator,
                 compression: Compression::Lz4, })
  }
}
