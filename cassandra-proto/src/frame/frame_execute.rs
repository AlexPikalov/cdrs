use rand;

use crate::types::*;
use crate::frame::*;
use crate::query::QueryParams;

/// The structure that represents a body of a frame of type `execute`.
#[derive(Debug)]
pub struct BodyReqExecute<'a> {
    /// Id of prepared query
    id: &'a CBytesShort,
    /// Query paramaters which have the same meaning as one for `query`
    /// TODO: clarify if it is QueryParams or its shortened variant
    query_parameters: QueryParams,
}

impl<'a> BodyReqExecute<'a> {
    /// The method which creates new instance of `BodyReqExecute`
    pub fn new(id: &CBytesShort, query_parameters: QueryParams) -> BodyReqExecute {
        BodyReqExecute { id: id,
                         query_parameters: query_parameters, }
    }
}

impl<'a> IntoBytes for BodyReqExecute<'a> {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        v.extend_from_slice(self.id.into_cbytes().as_slice());
        v.extend_from_slice(self.query_parameters.into_cbytes().as_slice());
        v
    }
}

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_execute(id: &CBytesShort,
                           query_parameters: QueryParams,
                           flags: Vec<Flag>)
                           -> Frame {
        let version = Version::Request;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Execute;
        debug!("prepared statement id{:?} getting executed  with parameters  {:?}",
               id, query_parameters);
        let body = BodyReqExecute::new(id, query_parameters);

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
