use types::*;
use frame::*;
use super::super::IntoBytes;
use super::frame_query::ParamsReqQuery;

/// The structure that represents a body of a frame of type `execute`.
pub struct BodyReqExecute {
    /// Id of prepared query
    id: CBytesShort,
    /// Query paramaters which have the same meaning as one for `query`
    query_parameters: ParamsReqQuery
}

impl BodyReqExecute {
    /// The method which creates new instance of `BodyReqExecute`
    pub fn new(id: CBytesShort, query_parameters: ParamsReqQuery) -> BodyReqExecute {
        return BodyReqExecute {
            id: id,
            query_parameters: query_parameters
        };
    }
}

impl IntoBytes for BodyReqExecute {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        v.extend_from_slice(self.id.into_cbytes().as_slice());
        v.extend_from_slice(self.query_parameters.into_cbytes().as_slice());
        return v;
    }
}

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_execute(id: CBytesShort, query_parameters: ParamsReqQuery) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::Execute;
        let body = BodyReqExecute::new(id, query_parameters);

        return Frame {
            version: version,
            flag: flag,
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes()
        };
    }
}
