use std::convert::From;
use super::super::IntoBytes;

pub struct BodyResReady;

impl BodyResReady {
    pub fn new() -> BodyResReady {
        return BodyResReady{};
    }
}

impl From<Vec<u8>> for BodyResReady {
    fn from(_vec: Vec<u8>) -> BodyResReady {
        return BodyResReady{};
    }
}

impl IntoBytes for BodyResReady {
    fn into_cbytes(&self) -> Vec<u8> {
        return vec![];
    }
}
