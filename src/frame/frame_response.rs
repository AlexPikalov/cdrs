use std::io::Cursor;
use std::slice::Iter;

use FromCursor;
use types::*;
use frame::*;
use frame::frame_result::*;
use frame::frame_error::CDRSError;
use frame::frame_supported::*;
use frame::frame_auth_challenge::*;
use frame::frame_authenticate::BodyResAuthenticate;
use frame::frame_auth_success::BodyReqAuthSuccess;
use types::rows::Row;

#[derive(Debug)]
pub enum ResponseBody {
    Error(CDRSError),
    Startup,
    Ready(BodyResResultVoid),
    Authenticate(BodyResAuthenticate),
    Options,
    Supported(BodyResSupported),
    Query,
    Result(ResResultBody),
    Prepare,
    Execute,
    Register,
    Event,
    Batch,
    AuthChallenge(BodyResAuthChallenge),
    AuthResponse,
    AuthSuccess(BodyReqAuthSuccess)
}

impl ResponseBody {
    pub fn from(bytes: Vec<u8>, response_type: &Opcode) -> ResponseBody {
        let mut cursor: Cursor<Vec<u8>> = Cursor::new(bytes);
        return match response_type {
            &Opcode::Error => ResponseBody::Error(CDRSError::from_cursor(&mut cursor)),
            // request frame
            &Opcode::Startup => unreachable!(),
            &Opcode::Ready => ResponseBody::Ready(BodyResResultVoid::from_cursor(&mut cursor)),
            &Opcode::Authenticate => ResponseBody::Authenticate(BodyResAuthenticate::from_cursor(&mut cursor)),
            // request frame
            &Opcode::Options => unreachable!(),
            &Opcode::Supported => ResponseBody::Supported(BodyResSupported::from_cursor(&mut cursor)),
            // request frame
            &Opcode::Query => unreachable!(),
            &Opcode::Result => ResponseBody::Result(ResResultBody::from_cursor(&mut cursor)),
            // request frames
            &Opcode::Prepare => unreachable!(),
            &Opcode::Execute => unreachable!(),
            &Opcode::Register => unreachable!(),
            &Opcode::Event => unreachable!(),
            &Opcode::Batch => unreachable!(),
            &Opcode::AuthChallenge => ResponseBody::AuthChallenge(BodyResAuthChallenge::from_cursor(&mut cursor)),
            // request frame
            &Opcode::AuthResponse => unreachable!(),
            &Opcode::AuthSuccess => ResponseBody::AuthSuccess(BodyReqAuthSuccess::from_cursor(&mut cursor))
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

    pub fn into_rows(self) -> Option<Vec<Row>> {
        match self {
            ResponseBody::Result(res) => res.into_rows(),
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

    pub fn get_authenticator(&self) -> Option<String> {
        match self {
            &ResponseBody::Authenticate(ref auth) => {
                Some(auth.data.clone().into_plain())
            },
            _ => None
        }
    }
}
