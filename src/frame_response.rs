use std::io::Cursor;
use super::FromCursor;
use super::frame_response_result::*;
use super::frame::Opcode;

#[derive(Debug)]
pub enum ResponseBody {
    Error,
    Startup,
    Ready(BodyResResultVoid),
    Authenticate,
    Options,
    Supported,
    Query,
    Result(ResResultBody),
    Prepare,
    Execute,
    Register,
    Event,
    Batch,
    AuthChallenge,
    AuthResponse,
    AuthSuccess
}

impl ResponseBody {
    pub fn from(bytes: Vec<u8>, response_type: &Opcode) -> ResponseBody {
        let mut cursor: Cursor<Vec<u8>> = Cursor::new(bytes);
        return match response_type {
            &Opcode::Error => unimplemented!(),
            &Opcode::Startup => unimplemented!(),
            &Opcode::Ready => ResponseBody::Ready(BodyResResultVoid::from_cursor(&mut cursor)),
            &Opcode::Authenticate => unimplemented!(),
            &Opcode::Options => unimplemented!(),
            &Opcode::Supported => unimplemented!(),
            &Opcode::Query => unimplemented!(),
            &Opcode::Result => ResponseBody::Result(ResResultBody::from_cursor(&mut cursor)),
            &Opcode::Prepare => unimplemented!(),
            &Opcode::Execute => unimplemented!(),
            &Opcode::Register => unimplemented!(),
            &Opcode::Event => unimplemented!(),
            &Opcode::Batch => unimplemented!(),
            &Opcode::AuthChallenge => unimplemented!(),
            &Opcode::AuthResponse => unimplemented!(),
            &Opcode::AuthSuccess => unimplemented!()
        }
    }
}

// use std::io::Cursor;
//
// use super::IntoBytes;
// use super::FromBytes;
// use super::FromCursor;
// use super::types::*;
// use super::frame_response_void::*;
// use super::frame_response_rows::*;
// use super::frame_response_set_keyspace::*;
//
// /// `ResultKind` is enum which represents types of result.
// pub enum ResultKind {
//     /// Void result.
//     Void,
//     /// Rows result.
//     Rows,
//     /// Set keyspace result.
//     SetKeyspace,
//     /// Prepeared result.
//     Prepared,
//     /// Schema change result.
//     SchemaChange
// }
//
// impl IntoBytes for ResultKind {
//     fn into_cbytes(&self) -> Vec<u8> {
//         return match *self {
//             ResultKind::Void => to_int(0x0001),
//             ResultKind::Rows => to_int(0x0002),
//             ResultKind::SetKeyspace => to_int(0x0003),
//             ResultKind::Prepared => to_int(0x0004),
//             ResultKind::SchemaChange => to_int(0x0005)
//         }
//     }
// }
//
// impl FromBytes for ResultKind {
//     fn from_bytes(bytes: Vec<u8>) -> ResultKind {
//         return match from_bytes(bytes.clone()) {
//             0x0001 => ResultKind::Void,
//             0x0002 => ResultKind::Rows,
//             0x0003 => ResultKind::SetKeyspace,
//             0x0004 => ResultKind::Prepared,
//             0x0005 => ResultKind::SchemaChange,
//             _ => {
//                 error!("Unexpected Cassandra result kind: {:?}", bytes);
//                 panic!("Unexpected Cassandra result kind: {:?}", bytes);
//             }
//         };
//     }
// }
//
// /// ResponseBody is a generalized enum that represents all types of responses. Each of enum
// /// option wraps related body type.
// pub enum ResResultBody {
//     /// Void response body. It's an empty stuct.
//     Void(BodyResResultVoid),
//     /// Rows response body. It represents a body of response which contains rows.
//     Rows(BodyResResultRows),
//     /// Set keyspace body. It represents a body of set_keyspace query and usually contains
//     /// a name of just set namespace.
//     SetKeyspace(BodyResResultSetKeyspace)
// }
//
// impl ResResultBody {
//     pub fn parse_body(body_bytes: Vec<u8>, result_kind: ResultKind) -> ResResultBody {
//         let mut cursor = Cursor::new(body_bytes);
//         return match result_kind {
//             ResultKind::Void => ResResultBody::Void(BodyResResultVoid::from_cursor(&mut cursor)),
//             ResultKind::Rows => ResResultBody::Rows(BodyResResultRows::from_cursor(&mut cursor)),
//             ResultKind::SetKeyspace => ResResultBody::SetKeyspace(BodyResResultSetKeyspace::from_cursor(&mut cursor)),
//             _ => unimplemented!()
//         };
//     }
// }
