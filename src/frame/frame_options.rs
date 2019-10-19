use rand;

use crate::frame::*;

/// The structure which represents a body of a frame of type `options`.
#[derive(Debug, Default)]
pub struct BodyReqOptions;

impl IntoBytes for BodyReqOptions {
    fn into_cbytes(&self) -> Vec<u8> {
        vec![]
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `options`.
    pub fn new_req_options() -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Options;
        let body: BodyReqOptions = Default::default();

        Frame {
            version: version,
            flags: vec![flag],
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes(),
            // for request frames it's always None
            tracing_id: None,
            warnings: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_options() {
        let frame = Frame::new_req_options();
        assert_eq!(frame.version, Version::Request);
        assert_eq!(frame.opcode, Opcode::Options);
        assert_eq!(frame.body, vec![]);
    }
}
