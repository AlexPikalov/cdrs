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
        BodyResAuthenticate {
            data: CString::from_cursor(&mut cursor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use FromCursor;

    #[test]
    fn body_res_authenticate() {
        // string "abcde"
        let data: Vec<u8> = vec![0, 5, 97, 98, 99, 100, 101];
        let mut cursor = Cursor::new(data);
        let body = BodyResAuthenticate::from_cursor(&mut cursor);
        assert_eq!(body.data.as_str(), "abcde");
    }
}
