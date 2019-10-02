use std::cell::RefCell;
use std::error::Error;
use std::iter::Iterator;
use std::sync::mpsc::{channel, Receiver, Sender};

use cassandra_proto::{
  error,
  frame::events::{
    SchemaChange as FrameSchemaChange, ServerEvent as FrameServerEvent,
    SimpleServerEvent as FrameSimpleServerEvent,
  },
  frame::parser::parse_frame,
};

use crate::compression::Compression;
use crate::transport::CDRSTransport;

/// Full Server Event which includes all details about occured change.
pub type ServerEvent = FrameServerEvent;

/// Simplified Server event. It should be used to represent an event
/// which consumer wants listen to.
pub type SimpleServerEvent = FrameSimpleServerEvent;

/// Reexport of `FrameSchemaChange`.
pub type SchemaChange = FrameSchemaChange;

/// Factory function which returns a `Listener` and related `EventStream.`
///
/// `Listener` provides only one function `start` to start listening. It
/// blocks a thread so should be moved into a separate one to no release
/// main thread.
///
/// `EventStream` is an iterator which returns new events once they come.
/// It is similar to `Receiver::iter`.
pub fn new_listener<X>(transport: X) -> (Listener<X>, EventStream) {
  let (tx, rx) = channel();
  let listener = Listener {
    transport: transport,
    tx: tx,
  };
  let stream = EventStream { rx: rx };
  (listener, stream)
}

/// `Listener` provides only one function `start` to start listening. It
/// blocks a thread so should be moved into a separate one to no release
/// main thread.

pub struct Listener<X> {
  transport: X,
  tx: Sender<ServerEvent>,
}

impl<X: CDRSTransport + 'static> Listener<RefCell<X>> {
  /// It starts a process of listening to new events. Locks a frame.
  pub fn start(self, compressor: &Compression) -> error::Result<()> {
    loop {
      let event_opt = parse_frame(&self.transport, compressor)?
        .get_body()?
        .into_server_event();

      let event = if event_opt.is_some() {
        // unwrap is safe as we've checked that event_opt.is_some()
        event_opt.unwrap().event as ServerEvent
      } else {
        continue;
      };
      match self.tx.send(event) {
        Err(err) => return Err(error::Error::General(err.description().to_string())),
        _ => continue,
      }
    }
  }
}

/// `EventStream` is an iterator which returns new events once they come.
/// It is similar to `Receiver::iter`.
pub struct EventStream {
  rx: Receiver<ServerEvent>,
}

impl Iterator for EventStream {
  type Item = ServerEvent;

  fn next(&mut self) -> Option<Self::Item> {
    self.rx.recv().ok()
  }
}
