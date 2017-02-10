use types::*;
use frame::*;
use IntoBytes;
use frame::frame_query::ParamsReqQuery;

/// The structure that represents a body of a frame of type `execute`.
#[derive(Debug)]
pub struct BodyReqExecute {
    /// Id of prepared query
    id: CBytesShort,
    /// Query paramaters which have the same meaning as one for `query`
    query_parameters: ParamsReqQuery,
}

impl BodyReqExecute {
    /// The method which creates new instance of `BodyReqExecute`
    pub fn new(id: CBytesShort, query_parameters: ParamsReqQuery) -> BodyReqExecute {
        BodyReqExecute {
            id: id,
            query_parameters: query_parameters,
        }
    }
}

impl IntoBytes for BodyReqExecute {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        v.extend_from_slice(self.id.into_cbytes().as_slice());
        v.extend_from_slice(self.query_parameters.into_cbytes().as_slice());
        v
    }
}

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_execute(id: CBytesShort,
                           query_parameters: ParamsReqQuery,
                           flags: Vec<Flag>)
                           -> Frame {
        let version = Version::Request;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::Execute;
        debug!("prepared statement id{:?} getting executed  with parameters  {:?}",
               id,
               query_parameters);
        let body = BodyReqExecute::new(id, query_parameters);

        Frame {
            version: version,
            flags: flags,
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes(),
            // for request frames it's always None
            tracing_id: None,
            warnings: vec![],
        }
    }
}
