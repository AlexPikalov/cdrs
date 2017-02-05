use std::collections::HashMap;

use frame::*;
use types::*;
use IntoBytes;

const CQL_VERSION: &'static str = "CQL_VERSION";
const CQL_VERSION_VAL: &'static str = "3.0.0";
const COMPRESSION: &'static str = "COMPRESSION";

#[derive(Debug)]
pub struct BodyReqStartup<'a> {
    pub map: HashMap<&'static str, &'a str>
}

impl<'a> BodyReqStartup<'a> {
    pub fn new<'b>(compression: Option<&'b str>) -> BodyReqStartup<'b> {
        let mut map = HashMap::new();
        map.insert(CQL_VERSION, CQL_VERSION_VAL);
        if let Some(c) = compression {
            map.insert(COMPRESSION, c);
        }
        BodyReqStartup {
            map: map
        }
    }

    // should be [u8; 2]
    /// Number of key-value pairs
    pub fn num(&self) -> Vec<u8> {
        to_short(self.map.len() as u64)
    }
}

impl<'a> IntoBytes for BodyReqStartup<'a> {
    fn into_cbytes(&self) -> Vec<u8> {
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
        v
    }
}

// Frame implementation related to BodyReqStartup

impl Frame {
    /// Creates new frame of type `startup`.
    pub fn new_req_startup(compression: Option<&str>) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        // sync client
        let stream: u64 = 0;
        let opcode = Opcode::Startup;
        let body = BodyReqStartup::new(compression);

        Frame {
            version: version,
            flags: vec![flag],
            stream: stream,
            opcode: opcode,
            body: body.into_cbytes(),
            // for request frames it's always None
            tracing_id: None,
            warnings: vec![]
        }
    }
}
