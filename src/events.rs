use std::sync::mpsc::{Sender, Receiver, channel};
use std::iter::Iterator;

use std::error::Error;
use error;
#[cfg(not(feature="ssl"))]
use transport::Transport;
#[cfg(feature="ssl")]
use transport_ssl::Transport;
use frame::events::{
    ServerEvent as FrameServerEvent,
    SimpleServerEvent as FrameSimpleServerEvent,
    SchemaChange as FrameSchemaChange
};
use frame::parser::parse_frame;
use compression::Compression;

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
pub fn new_listener(transport: Transport) -> (Listener, EventStream) {
    let (tx, rx) = channel();
    let listener = Listener {
        transport: transport,
        tx: tx
    };
    let stream = EventStream { rx: rx };
    (listener, stream)
}

/// `Listener` provides only one function `start` to start listening. It
/// blocks a thread so should be moved into a separate one to no release
/// main thread.
pub struct Listener {
    transport: Transport,
    tx: Sender<ServerEvent>
}

impl Listener {
    /// It starts a process of listening to new events. Locks a frame.
    pub fn start(&mut self, compressor: &Compression) -> error::Result<()> {
        loop {
            let event_opt = try!(parse_frame(&mut self.transport, compressor))
                .get_body()
                .into_server_event();

            let event = if event_opt.is_some() {
                event_opt.unwrap().event as ServerEvent
            } else {
                continue;
            };
            match self.tx.send(event) {
                Err(err) => return Err(error::Error::General(err.description().to_string())),
                _ => continue
            }
        }
    }
}

/// `EventStream` is an iterator which returns new events once they come.
/// It is similar to `Receiver::iter`.
pub struct EventStream {
    rx: Receiver<ServerEvent>
}

impl Iterator for EventStream {
    type Item = ServerEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}
