use super::FromBytes;

/// Body of a response of type Void
pub struct BodyResVoid {}

impl BodyResVoid {
    pub fn new() -> BodyResVoid {
        return BodyResVoid {};
    }
}

impl FromBytes for BodyResVoid {
    fn from_bytes(_bytes: Vec<u8>) -> BodyResVoid {
        // as it's empty by definition just create BodyResVoid
        return BodyResVoid::new();
    }
}
