use types::*;
use super::*;
use super::super::IntoBytes;

/// Struct that represents a body of a frame of type `prepare`
pub struct BodyReqPrepare {
    query: CString
}

impl BodyReqPrepare {
    /// Creates new body of a frame of type `prepare` that prepares query `query`.
    pub fn new(query: String) -> BodyReqPrepare {
        return BodyReqPrepare {
            query: query as CString
        }
    }
}

impl IntoBytes for BodyReqPrepare {
    fn into_cbytes(&self) -> Vec<u8> {
        return self.query.into_cbytes();
    }
}

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_prepare(query: String) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::Query;
        let body = BodyReqPrepare::new(query);

        return Frame {
            version: version,
            flag: flag,
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes()
        };
    }
}
