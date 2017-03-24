#![warn(missing_docs)]
//! Contains Query Frame related functionality.
use super::*;
use consistency::Consistency;
use {AsByte, IntoBytes};
use types::*;
use types::value::*;

/// Structure which represents body of Query request
#[derive(Debug)]
pub struct BodyReqQuery {
    /// Query string.
    pub query: CStringLong,
    /// Query parameters.
    pub query_params: ParamsReqQuery,
}

impl BodyReqQuery {
    // Fabric function that produces Query request body.
    fn new(query: String,
           consistency: Consistency,
           values: Option<Vec<Value>>,
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

        BodyReqQuery {
            query: CStringLong::new(query),
            query_params: ParamsReqQuery {
                consistency: consistency,
                flags: flags,
                values: values,
                page_size: page_size,
                paging_state: paging_state,
                serial_consistency: serial_consistency,
                timestamp: timestamp,
            },
        }
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

/// Parameters of Query request.
#[derive(Debug)]
pub struct ParamsReqQuery {
    /// Cassandra consistency level.
    pub consistency: Consistency,
    /// Array of query flags.
    pub flags: Vec<QueryFlags>,
    /// Array of values.
    pub values: Option<Vec<Value>>,
    /// Page size.
    pub page_size: Option<i32>,
    /// Array of bytes which represents paging state.
    pub paging_state: Option<CBytes>,
    /// Serial `Consistency`.
    pub serial_consistency: Option<Consistency>,
    /// Timestamp.
    pub timestamp: Option<i64>,
}

impl ParamsReqQuery {
    /// Sets values of Query request params.
    pub fn set_values(&mut self, values: Vec<Value>) {
        self.flags.push(QueryFlags::Value);
        self.values = Some(values);
    }

    fn flags_as_byte(&self) -> u8 {
        self.flags.iter().fold(0, |acc, flag| acc | flag.as_byte())
    }

    #[allow(dead_code)]
    fn parse_query_flags(byte: u8) -> Vec<QueryFlags> {
        let mut flags: Vec<QueryFlags> = vec![];

        if QueryFlags::has_value(byte) {
            flags.push(QueryFlags::Value);
        }
        if QueryFlags::has_skip_metadata(byte) {
            flags.push(QueryFlags::SkipMetadata);
        }
        if QueryFlags::has_page_size(byte) {
            flags.push(QueryFlags::PageSize);
        }
        if QueryFlags::has_with_paging_state(byte) {
            flags.push(QueryFlags::WithPagingState);
        }
        if QueryFlags::has_with_serial_consistency(byte) {
            flags.push(QueryFlags::WithSerialConsistency);
        }
        if QueryFlags::has_with_default_timestamp(byte) {
            flags.push(QueryFlags::WithDefaultTimestamp);
        }
        if QueryFlags::has_with_names_for_values(byte) {
            flags.push(QueryFlags::WithNamesForValues);
        }

        flags
    }
}

impl IntoBytes for ParamsReqQuery {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];

        v.extend_from_slice(self.consistency.into_cbytes().as_slice());
        v.push(self.flags_as_byte());
        if QueryFlags::has_value(self.flags_as_byte()) {
            // XXX clone
            let values = self.values.clone().unwrap();
            v.extend_from_slice(to_short(values.len() as i16).as_slice());
            for val in values.iter() {
                v.extend_from_slice(val.into_cbytes().as_slice());
            }
        }
        if QueryFlags::has_with_paging_state(self.flags_as_byte()) {
            // XXX clone
            v.extend_from_slice(self.paging_state.clone().unwrap().into_cbytes().as_slice());
        }
        if QueryFlags::has_with_serial_consistency(self.flags_as_byte()) {
            // XXX clone
            v.extend_from_slice(self.serial_consistency.clone().unwrap().into_cbytes().as_slice());
        }
        if QueryFlags::has_with_default_timestamp(self.flags_as_byte()) {
            v.extend_from_slice(to_bigint(self.timestamp.unwrap()).as_slice());
        }

        v
    }
}

const FLAGS_VALUE: u8 = 0x01;
const FLAGS_SKIP_METADATA: u8 = 0x02;
const WITH_PAGE_SIZE: u8 = 0x04;
const WITH_PAGING_STATE: u8 = 0x08;
const WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const WITH_NAME_FOR_VALUES: u8 = 0x40;

/// Cassandra Query Flags.
#[derive(Clone, Debug)]
pub enum QueryFlags {
    /// If set indicates that Query Params contains value.
    Value,
    /// If set indicates that Query Params does not contain metadata.
    SkipMetadata,
    /// If set indicates that Query Params contains page size.
    PageSize,
    /// If set indicates that Query Params contains paging state.
    WithPagingState,
    /// If set indicates that Query Params contains serial consistency.
    WithSerialConsistency,
    /// If set indicates that Query Params contains default timestamp.
    WithDefaultTimestamp,
    /// If set indicates that Query Params values are named ones.
    WithNamesForValues,
}

impl QueryFlags {
    #[doc(hidden)]
    pub fn has_value(byte: u8) -> bool {
        (byte & FLAGS_VALUE) != 0
    }

    #[doc(hidden)]
    pub fn set_value(byte: u8) -> u8 {
        byte | FLAGS_VALUE
    }

    #[doc(hidden)]
    pub fn has_skip_metadata(byte: u8) -> bool {
        (byte & FLAGS_SKIP_METADATA) != 0
    }

    #[doc(hidden)]
    pub fn set_skip_metadata(byte: u8) -> u8 {
        byte | FLAGS_SKIP_METADATA
    }

    #[doc(hidden)]
    pub fn has_page_size(byte: u8) -> bool {
        (byte & WITH_PAGE_SIZE) != 0
    }

    #[doc(hidden)]
    pub fn set_page_size(byte: u8) -> u8 {
        byte | WITH_PAGE_SIZE
    }

    #[doc(hidden)]
    pub fn has_with_paging_state(byte: u8) -> bool {
        (byte & WITH_PAGING_STATE) != 0
    }

    #[doc(hidden)]
    pub fn set_with_paging_state(byte: u8) -> u8 {
        byte | WITH_PAGING_STATE
    }

    #[doc(hidden)]
    pub fn has_with_serial_consistency(byte: u8) -> bool {
        (byte & WITH_SERIAL_CONSISTENCY) != 0
    }

    #[doc(hidden)]
    pub fn set_with_serial_consistency(byte: u8) -> u8 {
        byte | WITH_SERIAL_CONSISTENCY
    }

    #[doc(hidden)]
    pub fn has_with_default_timestamp(byte: u8) -> bool {
        (byte & WITH_DEFAULT_TIMESTAMP) != 0
    }

    #[doc(hidden)]
    pub fn set_with_default_timestamp(byte: u8) -> u8 {
        byte | WITH_DEFAULT_TIMESTAMP
    }

    #[doc(hidden)]
    pub fn has_with_names_for_values(byte: u8) -> bool {
        (byte & WITH_NAME_FOR_VALUES) != 0
    }

    #[doc(hidden)]
    pub fn set_with_names_for_values(byte: u8) -> u8 {
        byte | WITH_NAME_FOR_VALUES
    }
}

impl AsByte for QueryFlags {
    fn as_byte(&self) -> u8 {
        match *self {
            QueryFlags::Value => FLAGS_VALUE,
            QueryFlags::SkipMetadata => FLAGS_SKIP_METADATA,
            QueryFlags::PageSize => WITH_PAGE_SIZE,
            QueryFlags::WithPagingState => WITH_PAGING_STATE,
            QueryFlags::WithSerialConsistency => WITH_SERIAL_CONSISTENCY,
            QueryFlags::WithDefaultTimestamp => WITH_DEFAULT_TIMESTAMP,
            QueryFlags::WithNamesForValues => WITH_NAME_FOR_VALUES,
        }
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// **Note:** This function should be used internally for building query request frames.
    pub fn new_req_query<'a>(query: String,
                             consistency: Consistency,
                             values: Option<Vec<Value>>,
                             with_names: Option<bool>,
                             page_size: Option<i32>,
                             paging_state: Option<CBytes>,
                             serial_consistency: Option<Consistency>,
                             timestamp: Option<i64>,
                             flags: Vec<Flag>)
                             -> Frame {
        let version = Version::Request;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::Query;
        let body = BodyReqQuery::new(query,
                                     consistency,
                                     values,
                                     with_names,
                                     page_size,
                                     paging_state,
                                     serial_consistency,
                                     timestamp);

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
