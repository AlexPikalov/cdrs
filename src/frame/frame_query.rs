#![warn(missing_docs)]
//! Contains Query Frame related functionality.
use rand;
use frame::*;
use consistency::Consistency;
use types::*;
use query::{Query, QueryFlags, QueryParams, QueryValues};

/// Structure which represents body of Query request
#[derive(Debug)]
pub struct BodyReqQuery {
    /// Query string.
    pub query: CStringLong,
    /// Query parameters.
    pub query_params: QueryParams,
}

impl BodyReqQuery {
    // Fabric function that produces Query request body.
    fn new(query: String,
           consistency: Consistency,
           values: Option<QueryValues>,
           with_names: Option<bool>,
           page_size: Option<i32>,
           paging_state: Option<CBytes>,
           serial_consistency: Option<Consistency>,
           timestamp: Option<i64>)
           -> BodyReqQuery {
        // query flags
        let mut flags: Vec<QueryFlags> = vec![];
        if values.is_some() {
            flags.push(QueryFlags::Value);
        }
        if with_names.unwrap_or(false) {
            flags.push(QueryFlags::WithNamesForValues);
        }
        if page_size.is_some() {
            flags.push(QueryFlags::PageSize);
        }
        if serial_consistency.is_some() {
            flags.push(QueryFlags::WithSerialConsistency);
        }
        if timestamp.is_some() {
            flags.push(QueryFlags::WithDefaultTimestamp);
        }

        BodyReqQuery { query: CStringLong::new(query),
                       query_params: QueryParams { consistency,
                                                   flags,
                                                   with_names,
                                                   values,
                                                   page_size,
                                                   paging_state,
                                                   serial_consistency,
                                                   timestamp, }, }
    }
}

impl IntoBytes for BodyReqQuery {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        v.extend_from_slice(self.query.clone().into_cbytes().as_slice());
        v.extend_from_slice(self.query_params.into_cbytes().as_slice());
        v
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_query(query: String,
                         consistency: Consistency,
                         values: Option<QueryValues>,
                         with_names: Option<bool>,
                         page_size: Option<i32>,
                         paging_state: Option<CBytes>,
                         serial_consistency: Option<Consistency>,
                         timestamp: Option<i64>,
                         flags: Vec<Flag>)
                         -> Frame {
        let version = Version::Request;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Query;
        let body = BodyReqQuery::new(query,
                                     consistency,
                                     values,
                                     with_names,
                                     page_size,
                                     paging_state,
                                     serial_consistency,
                                     timestamp);

        Frame { version: version,
                flags: flags,
                stream: stream,
                opcode: opcode,
                body: body.into_cbytes(),
                // for request frames it's always None
                tracing_id: None,
                warnings: vec![], }
    }

    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_query(query: Query, flags: Vec<Flag>) -> Frame {
        Frame::new_req_query(query.query,
                             query.params.consistency,
                             query.params.values,
                             query.params.with_names,
                             query.params.page_size,
                             query.params.paging_state,
                             query.params.serial_consistency,
                             query.params.timestamp,
                             flags)
    }
}
