use std::sync::mpsc::{Sender, Receiver, channel};
use std::iter::Iterator;

use std::error::Error;
use error;
use frame::events::{
    ServerEvent as FrameServerEvent,
    SimpleServerEvent as FrameSimpleServerEvent,
    SchemaChange as FrameSchemaChange
};
use frame::Frame;
use frame::parser::parse_frame;
use compression::Compression;
use transport::CDRSTransport;

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
        tx: tx
    };
    let stream = EventStream { rx: rx };
    (listener, stream)
}

/// `Listener` provides only one function `start` to start listening. It
/// blocks a thread so should be moved into a separate one to no release
/// main thread.
pub struct Listener<X> {
    transport: X,
    tx: Sender<Frame>
}

impl <X:CDRSTransport> Listener <X> {
    /// It starts a process of listening to new events. Locks a frame.
    pub fn start(&mut self, compressor: &Compression) -> error::Result<()> {
        loop {
            match self.tx.send(try!(parse_frame(&mut self.transport, compressor))) {
                Err(err) => return Err(error::Error::General(err.description().to_string())),
                _ => continue
            }
        }
    }
}

/// `EventStream` is an iterator which returns new events once they come.
/// It is similar to `Receiver::iter`.
pub struct EventStream {
    rx: Receiver<Frame>
}

impl Iterator for EventStream {
    type Item = Frame;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}
