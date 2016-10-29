use super::FromBytes;

/// Body of a response of type Void
pub struct BodyResResultVoid {}

impl BodyResResultVoid {
    pub fn new() -> BodyResResultVoid {
        return BodyResResultVoid {};
    }
}

impl FromBytes for BodyResResultVoid {
    fn from_bytes(_bytes: Vec<u8>) -> BodyResResultVoid {
        // as it's empty by definition just create BodyResVoid
        return BodyResResultVoid::new();
    }
}
