use FromCursor;
use std::io::Cursor;

/// `BodyReqAuthSuccess` is a frame that represents a successfull authentication response.
#[derive(Debug)]
pub struct BodyReqAuthSuccess {}

impl FromCursor for BodyReqAuthSuccess {
    fn from_cursor(mut _cursor: &mut Cursor<Vec<u8>>) -> BodyReqAuthSuccess {
        return BodyReqAuthSuccess {};
    }
}
