use std::io::Cursor;
use std::slice::Iter;
use super::super::types::*;
use super::super::FromCursor;
use super::frame_result::*;
use super::super::frame::*;
use super::super::error::CDRSError;
use super::frame_supported::*;

#[derive(Debug)]
pub enum ResponseBody {
    Error(CDRSError),
    Startup,
    Ready(BodyResResultVoid),
    Authenticate,
    Options,
    Supported(BodyResSupported),
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
            &Opcode::Error => ResponseBody::Error(CDRSError::from_cursor(&mut cursor)),
            // request frame
            &Opcode::Startup => unreachable!(),
            &Opcode::Ready => ResponseBody::Ready(BodyResResultVoid::from_cursor(&mut cursor)),
            &Opcode::Authenticate => unimplemented!(),
            // request frame
            &Opcode::Options => unreachable!(),
            &Opcode::Supported => ResponseBody::Supported(BodyResSupported::from_cursor(&mut cursor)),
            // request frame
            &Opcode::Query => unreachable!(),
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

    pub fn as_rows_iter(&self) -> Option<Iter<Vec<CBytes>>> {
        match self {
            &ResponseBody::Result(ref res) => {
                match res {
                    &ResResultBody::Rows(ref rows) => Some(rows.rows_content.iter()),
                    _ => None
                }
            },
            _ => None
        }
    }

    pub fn as_cols(&self) -> Option<&BodyResResultRows> {
        match self {
            &ResponseBody::Result(ref res) => {
                match res {
                    &ResResultBody::Rows(ref rows) => Some(rows),
                    _ => None
                }
            },
            _ => None
        }
    }
}
