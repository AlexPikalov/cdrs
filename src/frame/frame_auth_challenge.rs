use std::io::Cursor;

use frame::FromCursor;
use error;
use types::CBytes;

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
    use super::*;
    use FromCursor;
    use std::io::Cursor;

    #[test]
    fn body_res_auth_challenge_from_cursor() {
        let few_bytes = &[0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut cursor: Cursor<&[u8]> = Cursor::new(few_bytes);
        let body = BodyResAuthChallenge::from_cursor(&mut cursor).unwrap();
        assert_eq!(body.data.into_plain().unwrap(),
                   vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
