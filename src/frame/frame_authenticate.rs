use std::io::Cursor;

use crate::error;
use crate::frame::FromCursor;
use crate::types::CString;

/// A server authentication challenge.
#[derive(Debug)]
pub struct BodyResAuthenticate {
    pub data: CString,
}

impl FromCursor for BodyResAuthenticate {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResAuthenticate> {
        Ok(BodyResAuthenticate {
            data: CString::from_cursor(&mut cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn body_res_authenticate() {
        // string "abcde"
        let data = [0, 5, 97, 98, 99, 100, 101];
        let mut cursor: Cursor<&[u8]> = Cursor::new(&data);
        let body = BodyResAuthenticate::from_cursor(&mut cursor).unwrap();
        assert_eq!(body.data.as_str(), "abcde");
    }
}
