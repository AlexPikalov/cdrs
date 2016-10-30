use std::io::Cursor;
use super::FromBytes;
use super::FromCursor;

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

impl FromCursor for BodyResResultVoid {
    fn from_cursor(mut _cursor: &mut Cursor<Vec<u8>>) -> BodyResResultVoid {
        return BodyResResultVoid::new();
    }
}
