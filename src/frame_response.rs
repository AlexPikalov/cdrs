use std::io::Cursor;

use super::IntoBytes;
use super::FromBytes;
use super::FromCursor;
use super::types::*;
use super::frame_response_void::*;
use super::frame_response_rows::*;
use super::frame_response_set_keyspace::*;

/// `ResultKind` is enum which represents types of result.
pub enum ResultKind {
    /// Void result.
    Void,
    /// Rows result.
    Rows,
    /// Set keyspace result.
    SetKeyspace,
    /// Prepeared result.
    Prepared,
    /// Schema change result.
    SchemaChange
}

impl IntoBytes for ResultKind {
    fn into_cbytes(&self) -> Vec<u8> {
        return match *self {
            ResultKind::Void => to_int(0x0001),
            ResultKind::Rows => to_int(0x0002),
            ResultKind::SetKeyspace => to_int(0x0003),
            ResultKind::Prepared => to_int(0x0004),
            ResultKind::SchemaChange => to_int(0x0005)
        }
    }
}

impl FromBytes for ResultKind {
    fn from_bytes(bytes: Vec<u8>) -> ResultKind {
        return match from_bytes(bytes.clone()) {
            0x0001 => ResultKind::Void,
            0x0002 => ResultKind::Rows,
            0x0003 => ResultKind::SetKeyspace,
            0x0004 => ResultKind::Prepared,
            0x0005 => ResultKind::SchemaChange,
            _ => {
                error!("Unexpected Cassandra result kind: {:?}", bytes);
                panic!("Unexpected Cassandra result kind: {:?}", bytes);
            }
        };
    }
}

/// ResponseBody is a generalized enum that represents all types of responses. Each of enum
/// option wraps related body type.
pub enum ResponseBody {
    /// Void response body. It's an empty stuct.
    Void(BodyResResultVoid),
    /// Rows response body. It represents a body of response which contains rows.
    Rows(BodyResResultRows),
    /// Set keyspace body. It represents a body of set_keyspace query and usually contains
    /// a name of just set namespace.
    SetKeyspace(BodyResResultSetKeyspace)
}

impl ResponseBody {
    pub fn parse_body(body_bytes: Vec<u8>, result_kind: ResultKind) -> ResponseBody {
        let mut cursor = Cursor::new(body_bytes);
        return match result_kind {
            ResultKind::Void => ResponseBody::Void(BodyResResultVoid::from_cursor(&mut cursor)),
            ResultKind::Rows => ResponseBody::Rows(BodyResResultRows::from_cursor(&mut cursor)),
            ResultKind::SetKeyspace => ResponseBody::SetKeyspace(BodyResResultSetKeyspace::from_cursor(&mut cursor)),
            _ => unimplemented!()
        };
    }
}
