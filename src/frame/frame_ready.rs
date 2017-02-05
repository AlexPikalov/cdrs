use std::convert::From;
use super::super::IntoBytes;

#[derive(Debug)]
pub struct BodyResReady;

impl BodyResReady {
    pub fn new() -> BodyResReady {
        BodyResReady{}
    }
}

impl From<Vec<u8>> for BodyResReady {
    fn from(_vec: Vec<u8>) -> BodyResReady {
        BodyResReady{}
    }
}

impl IntoBytes for BodyResReady {
    fn into_cbytes(&self) -> Vec<u8> {
        vec![]
    }
}
