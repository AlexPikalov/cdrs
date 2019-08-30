use rand;

use crate::types::*;
use crate::frame::*;

/// Struct that represents a body of a frame of type `prepare`
#[derive(Debug)]
pub struct BodyReqPrepare {
    query: CStringLong,
}

impl BodyReqPrepare {
    /// Creates new body of a frame of type `prepare` that prepares query `query`.
    pub fn new(query: String) -> BodyReqPrepare {
        BodyReqPrepare { query: CStringLong::new(query), }
    }
}

impl IntoBytes for BodyReqPrepare {
    fn into_cbytes(&self) -> Vec<u8> {
        self.query.into_cbytes()
    }
}

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_prepare(query: String, flags: Vec<Flag>) -> Frame {
        let version = Version::Request;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Prepare;
        let body = BodyReqPrepare::new(query);

        Frame { version: version,
                flags: flags,
                stream: stream,
                opcode: opcode,
                body: body.into_cbytes(),
                // for request frames it's always None
                tracing_id: None,
                warnings: vec![], }
    }
}
