use super::super::IntoBytes;
use frame::*;

/// The structure which represents a body of a frame of type `options`.
#[derive(Debug)]
pub struct BodyReqOptions;

impl BodyReqOptions {
    /// Creates new body of a frame of type `options`
    pub fn new() -> BodyReqOptions {
        return BodyReqOptions{};
    }
}

impl IntoBytes for BodyReqOptions {
    fn into_cbytes(&self) -> Vec<u8> {
        return vec![];
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `options`.
    pub fn new_req_options() -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::Options;
        let body = BodyReqOptions::new();

        return Frame {
            version: version,
            flags: vec![flag],
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes(),
            // for request frames it's always None
            tracing_id: None,
            warnings: vec![]
        };
    }
}
