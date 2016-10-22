use std::convert::From;
use super::IntoBytes;

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
    fn into_bytes(&self) -> Vec<u8> {
        return vec![];
    }
}
