use std::io::Cursor;
use FromCursor;
use types::CBytes;

/// Server authentication challenge.
#[derive(Debug)]
pub struct BodyResAuthChallenge {
    pub data: CBytes,
}

impl FromCursor for BodyResAuthChallenge {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResAuthChallenge {
        BodyResAuthChallenge { data: CBytes::from_cursor(&mut cursor) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use FromCursor;
    use std::io::Cursor;

    #[test]
    fn body_res_auth_challenge_from_cursor() {
        let few_bytes: Vec<u8> = vec![0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut cursor = Cursor::new(few_bytes);
        let body = BodyResAuthChallenge::from_cursor(&mut cursor);
        assert_eq!(body.data.into_plain(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
