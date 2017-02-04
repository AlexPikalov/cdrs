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
use frame::Frame;
use frame::parser::parse_frame;
use compression::Compression;

pub type ServerEvent = FrameServerEvent;
pub type SimpleServerEvent = FrameSimpleServerEvent;
pub type SchemaChange = FrameSchemaChange;

pub fn new_listener(transport: Transport) -> (Listener, EventStream) {
    let (tx, rx) = channel();
    let listener = Listener {
        transport: transport,
        tx: tx
    };
    let stream = EventStream { rx: rx };
    (listener, stream)
}

pub struct Listener {
    transport: Transport,
    tx: Sender<Frame>
}

impl Listener {
    pub fn start(&mut self, compressor: &Compression) -> error::Result<()> {
        loop {
            match self.tx.send(try!(parse_frame(&mut self.transport, compressor))) {
                Err(err) => return Err(error::Error::General(err.description().to_string())),
                _ => continue
            }
        }
    }
}

pub struct EventStream {
    rx: Receiver<Frame>
}

impl Iterator for EventStream {
    type Item = Frame;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}
