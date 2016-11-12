use std::io::Cursor;
use super::FromCursor;
use super::frame_result::*;
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
