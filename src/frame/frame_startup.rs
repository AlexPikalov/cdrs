use std::collections::HashMap;

use rand;

use crate::frame::*;
use crate::types::to_short;

const CQL_VERSION: &'static str = "CQL_VERSION";
const CQL_VERSION_VAL: &'static str = "3.0.0";
const COMPRESSION: &'static str = "COMPRESSION";

#[derive(Debug)]
pub struct BodyReqStartup<'a> {
    pub map: HashMap<&'static str, &'a str>,
}

impl<'a> BodyReqStartup<'a> {
    pub fn new<'b>(compression: Option<&'b str>) -> BodyReqStartup<'b> {
        let mut map = HashMap::new();
        map.insert(CQL_VERSION, CQL_VERSION_VAL);
        if let Some(c) = compression {
            map.insert(COMPRESSION, c);
        }
        BodyReqStartup { map: map }
    }

    // should be [u8; 2]
    // Number of key-value pairs
    fn num(&self) -> Vec<u8> {
        to_short(self.map.len() as i16)
    }
}

impl<'a> IntoBytes for BodyReqStartup<'a> {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v = vec![];
        // push number of key-value pairs
        v.extend_from_slice(&self.num().as_slice());
        for (key, val) in self.map.iter() {
            // push key len
            v.extend_from_slice(to_short(key.len() as i16).as_slice());
            // push key itself
            v.extend_from_slice(key.as_bytes());
            // push val len
            v.extend_from_slice(to_short(val.len() as i16).as_slice());
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
        let stream = rand::random::<u16>();
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
            warnings: vec![],
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::{Flag, Frame, Opcode, Version};

    #[test]
    fn new_body_req_startup_some_compression() {
        let compression = "test_compression";
        let body = BodyReqStartup::new(Some(compression));
        assert_eq!(body.map.get("CQL_VERSION"), Some(&"3.0.0"));
        assert_eq!(body.map.get("COMPRESSION"), Some(&compression));
        assert_eq!(body.map.len(), 2);
    }

    #[test]
    fn new_body_req_startup_none_compression() {
        let body = BodyReqStartup::new(None);
        assert_eq!(body.map.get("CQL_VERSION"), Some(&"3.0.0"));
        assert_eq!(body.map.len(), 1);
    }

    #[test]
    fn new_req_startup() {
        let compression = Some("test_compression");
        let frame = Frame::new_req_startup(compression);
        assert_eq!(frame.version, Version::Request);
        assert_eq!(frame.flags, vec![Flag::Ignore]);
        assert_eq!(frame.opcode, Opcode::Startup);
        assert_eq!(frame.tracing_id, None);
        assert_eq!(frame.warnings, vec![] as Vec<String>);
    }
}
