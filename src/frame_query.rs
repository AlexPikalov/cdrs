use super::consistency::Consistency;
use super::{AsByte, IntoBytes};
use super::value::Value;

pub struct BodyReqQuery {
    pub query: Vec<u8>,
    pub query_params: ParamsReqQuery
}

pub struct ParamsReqQuery {
    pub consistency: Consistency,
    pub flags: Vec<QueryFlags>,
    pub values: Vec<Value>
}

impl ParamsReqQuery {
    pub fn set_values(&mut self, values: Vec<Value>) {
        self.flags.push(QueryFlags::Value);
        self.values = values;
    }

    fn flags_as_byte(&self) -> u8 {
        return self.flags.iter().fold(0, |acc, flag| acc | flag.as_byte());
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

        return flags;
    }
}

impl IntoBytes for ParamsReqQuery {
    fn into_bytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];

        v.extend_from_slice(self.consistency.into_bytes().as_slice());
        v.push(self.flags_as_byte());
        for val in self.values.iter() {
            v.extend_from_slice(val.into_bytes().as_slice());
        }

        return v;
    }
}

pub const FLAGS_VALUE: u8 = 0x01;
pub const FLAGS_SKIP_METADATA: u8 = 0x02;
pub const WITH_PAGE_SIZE: u8 = 0x04;
pub const WITH_PAGING_STATE: u8 = 0x08;
pub const WITH_SERIAL_CONSISTENCY: u8 = 0x10;
pub const WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
pub const WITH_NAME_FOR_VALUES: u8 = 0x40;

pub enum QueryFlags {
    Value,
    SkipMetadata,
    PageSize,
    WithPagingState,
    WithSerialConsistency,
    WithDefaultTimestamp,
    WithNamesForValues
}

impl QueryFlags {
    pub fn has_value(byte: u8) -> bool {
        return (byte & FLAGS_VALUE) != 0;
    }

    pub fn set_value(byte: u8) -> u8 {
        return byte | FLAGS_VALUE;
    }

    pub fn has_skip_metadata(byte: u8) -> bool {
        return (byte & FLAGS_SKIP_METADATA) != 0;
    }

    pub fn has_page_size(byte: u8) -> bool {
        return (byte & WITH_PAGE_SIZE) != 0;
    }

    pub fn has_with_paging_state(byte: u8) -> bool {
        return (byte & WITH_PAGING_STATE) != 0;
    }

    pub fn has_with_serial_consistency(byte: u8) -> bool {
        return (byte & WITH_SERIAL_CONSISTENCY) != 0;
    }

    pub fn has_with_default_timestamp(byte: u8) -> bool {
        return (byte & WITH_DEFAULT_TIMESTAMP) != 0;
    }

    pub fn has_with_names_for_values(byte: u8) -> bool {
        return (byte & WITH_NAME_FOR_VALUES) != 0;
    }
}

impl AsByte for QueryFlags {
    fn as_byte(&self) -> u8 {
        return match *self {
            QueryFlags::Value => FLAGS_VALUE,
            QueryFlags::SkipMetadata => FLAGS_SKIP_METADATA,
            QueryFlags::PageSize => WITH_PAGE_SIZE,
            QueryFlags::WithPagingState => WITH_PAGING_STATE,
            QueryFlags::WithSerialConsistency => WITH_SERIAL_CONSISTENCY,
            QueryFlags::WithDefaultTimestamp => WITH_DEFAULT_TIMESTAMP,
            QueryFlags::WithNamesForValues => WITH_NAME_FOR_VALUES,
        };
    }
}
