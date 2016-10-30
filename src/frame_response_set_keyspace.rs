use std::io::Cursor;
use super::FromBytes;
use super::FromCursor;
use super::types::*;

pub struct BodyResResultSetKeyspace {
    pub body: CString
}

impl BodyResResultSetKeyspace {
    pub fn new(body: CString) -> BodyResResultSetKeyspace {
        return BodyResResultSetKeyspace {
            body: body
        }
    }
}

impl FromBytes for BodyResResultSetKeyspace {
    /// Returns BodyResResultSetKeyspace with body provided via bytes
    /// Bytes is Cassandra's [string]
    fn from_bytes(bytes: Vec<u8>) -> BodyResResultSetKeyspace {
        return BodyResResultSetKeyspace::new(CString::from_bytes(bytes));
    }
}

impl FromCursor for BodyResResultSetKeyspace {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResResultSetKeyspace {
        return BodyResResultSetKeyspace::new(CString::from_cursor(&mut cursor));
    }
}
