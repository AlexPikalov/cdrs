use std::collections::HashMap;

use super::frame::*;
use super::{IntoBytes};
use super::to_n_bytes;

pub struct BodyReqStartup {
    pub map: HashMap<String, String>
}

impl BodyReqStartup {
    pub fn new(compression: Option<String>) -> BodyReqStartup {
        let mut map = HashMap::new();
        map.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        if let Some(c) = compression {
            map.insert("COMPRESSION".to_string(), c);
        }
        return BodyReqStartup {
            map: map
        };
    }

    // TODO rid of it
    pub fn map_len(&self) -> u64 {
        let mut l: usize = 0;
        for (key, val) in self.map.iter() {
            // [short], string, [short], string
            l += key.len() + SHORT_LEN + val.len() + SHORT_LEN;
        }
        return l as u64;
    }

    // should be [u8; 2]
    /// Number of key-value pairs
    pub fn num(&self) -> Vec<u8> {
        return to_n_bytes(self.map.len() as u64, SHORT_LEN);
    }
}

impl IntoBytes for BodyReqStartup {
    fn into_bytes(&self) -> Vec<u8> {
        let mut v = vec![];
        // push number of key-value pairs
        v.extend_from_slice(&self.num().as_slice());
        for (key, val) in self.map.iter() {
            // push key len
            v.extend_from_slice(to_n_bytes(key.len() as u64, SHORT_LEN).as_slice());
            // push key itself
            v.extend_from_slice(key.as_bytes());
            // push val len
            v.extend_from_slice(to_n_bytes(val.len() as u64, SHORT_LEN).as_slice());
            // push val itself
            v.extend_from_slice(val.as_bytes());
        }
        return v;
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    pub fn new_req_startup<'a>(compression: Option<String>) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::Startup;
        let body = BodyReqStartup::new(compression);

        return Frame {
            version: version,
            flag: flag,
            stream: stream,
            opcode: opcode,
            body: body.into_bytes()
        };
    }
}
