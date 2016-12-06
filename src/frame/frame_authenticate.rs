use std::io::Cursor;
use FromCursor;
use types::CString;

/// A server authentication challenge.
#[derive(Debug)]
pub struct BodyResAuthenticate {
    pub data: CString
}

impl FromCursor for BodyResAuthenticate {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResAuthenticate {
        return BodyResAuthenticate {
            data: CString::from_cursor(&mut cursor)
        };
    }
}
