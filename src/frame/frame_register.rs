use crate::frame::events::SimpleServerEvent;
use crate::frame::*;
use crate::types::{CString, CStringList};

/// The structure which represents a body of a frame of type `options`.
pub struct BodyReqRegister {
    pub events: Vec<SimpleServerEvent>,
}

impl IntoBytes for BodyReqRegister {
    fn into_cbytes(&self) -> Vec<u8> {
        let events_string_list = CStringList {
            list: self
                .events
                .iter()
                .map(|event| CString::new(event.as_string()))
                .collect(),
        };
        events_string_list.into_cbytes()
    }
}

// Frame implementation related to BodyReqRegister

impl Frame {
    /// Creates new frame of type `REGISTER`.
    pub fn new_req_register(events: Vec<SimpleServerEvent>) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        let opcode = Opcode::Register;
        let register_body = BodyReqRegister { events: events };

        Frame::new(version, vec![flag], opcode, register_body.into_cbytes(), None, vec![])
    }
}
