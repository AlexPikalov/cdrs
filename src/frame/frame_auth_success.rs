use std::io::Cursor;

use crate::error;
use crate::frame::FromCursor;

/// `BodyReqAuthSuccess` is a frame that represents a successfull authentication response.
#[derive(Debug, PartialEq)]
pub struct BodyReqAuthSuccess {}

impl FromCursor for BodyReqAuthSuccess {
    fn from_cursor(mut _cursor: &mut Cursor<&[u8]>) -> error::Result<BodyReqAuthSuccess> {
        Ok(BodyReqAuthSuccess {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn test_name() {
        let rnd_bytes = [4, 5, 3, 8, 4, 6, 5, 0, 3, 7, 2];
        let mut cursor: Cursor<&[u8]> = Cursor::new(&rnd_bytes);
        let body = BodyReqAuthSuccess::from_cursor(&mut cursor).unwrap();
        assert_eq!(body, BodyReqAuthSuccess {});
    }
}
