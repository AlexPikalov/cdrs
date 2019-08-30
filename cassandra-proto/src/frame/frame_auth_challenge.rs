use std::io::Cursor;

use crate::frame::FromCursor;
use crate::error;
use crate::types::CBytes;

/// Server authentication challenge.
#[derive(Debug)]
pub struct BodyResAuthChallenge {
    pub data: CBytes,
}

impl FromCursor for BodyResAuthChallenge {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResAuthChallenge> {
        CBytes::from_cursor(&mut cursor).map(|data| BodyResAuthChallenge { data: data })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::*;
    use crate::frame::traits::FromCursor;

    #[test]
    fn body_res_auth_challenge_from_cursor() {
        let few_bytes = &[0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut cursor: Cursor<&[u8]> = Cursor::new(few_bytes);
        let body = BodyResAuthChallenge::from_cursor(&mut cursor).unwrap();
        assert_eq!(body.data.into_plain().unwrap(),
                   vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
