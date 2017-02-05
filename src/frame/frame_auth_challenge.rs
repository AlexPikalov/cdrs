use std::io::Cursor;
use FromCursor;
use types::CBytes;

/// A server authentication challenge.
#[derive(Debug)]
pub struct BodyResAuthChallenge {
    pub data: CBytes
}

impl FromCursor for BodyResAuthChallenge {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResAuthChallenge {
        BodyResAuthChallenge {
            data: CBytes::from_cursor(&mut cursor)
        }
    }
}
