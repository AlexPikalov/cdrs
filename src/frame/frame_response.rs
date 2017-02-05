use std::io::Cursor;

use FromCursor;
use frame::Opcode;
use frame::frame_result::{
    BodyResResultVoid,
    BodyResResultPrepared,
    BodyResResultRows,
    BodyResResultSetKeyspace,
    ResResultBody
};
use frame::frame_event::BodyResEvent;
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
    Event(BodyResEvent),
    Batch,
    AuthChallenge(BodyResAuthChallenge),
    AuthResponse,
    AuthSuccess(BodyReqAuthSuccess)
}

impl ResponseBody {
    pub fn from(bytes: Vec<u8>, response_type: &Opcode) -> ResponseBody {
        let mut cursor: Cursor<Vec<u8>> = Cursor::new(bytes);
        match response_type {
            // request frames
            &Opcode::Startup => unreachable!(),
            &Opcode::Options => unreachable!(),
            &Opcode::Query => unreachable!(),
            &Opcode::Prepare => unreachable!(),
            &Opcode::Execute => unreachable!(),
            &Opcode::Register => unreachable!(),
            &Opcode::Batch => unreachable!(),
            &Opcode::AuthResponse => unreachable!(),

            // response frames
            &Opcode::Error => ResponseBody::Error(CDRSError::from_cursor(&mut cursor)),
            &Opcode::Ready => ResponseBody::Ready(BodyResResultVoid::from_cursor(&mut cursor)),
            &Opcode::Authenticate => ResponseBody::Authenticate(
                BodyResAuthenticate::from_cursor(&mut cursor)
            ),
            &Opcode::Supported => ResponseBody::Supported(
                BodyResSupported::from_cursor(&mut cursor)
            ),
            &Opcode::Result => ResponseBody::Result(ResResultBody::from_cursor(&mut cursor)),
            &Opcode::Event => ResponseBody::Event(BodyResEvent::from_cursor(&mut cursor)),
            &Opcode::AuthChallenge => ResponseBody::AuthChallenge(
                BodyResAuthChallenge::from_cursor(&mut cursor)
            ),
            &Opcode::AuthSuccess => ResponseBody::AuthSuccess(
                BodyReqAuthSuccess::from_cursor(&mut cursor)
            )
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

    /// It unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// PREPARE query. If frame body is not of type `Result` this method returns `None`.
    pub fn into_prepared(self) -> Option<BodyResResultPrepared> {
        match self {
            ResponseBody::Result(res) => res.into_prepared(),
            _ => None
        }
    }

    /// It unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// use keyspace query. If frame body is not of type `Result` this method returns `None`.
    pub fn into_set_keyspace(self) -> Option<BodyResResultSetKeyspace> {
        match self {
            ResponseBody::Result(res) => res.into_set_keyspace(),
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
