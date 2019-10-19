use std::collections::HashMap;
use std::io::Cursor;

use crate::error;
use crate::frame::FromCursor;
use crate::types::{cursor_next_value, try_from_bytes, CString, CStringList, SHORT_LEN};

#[derive(Debug)]
pub struct BodyResSupported {
    pub data: HashMap<String, Vec<String>>,
}

impl FromCursor for BodyResSupported {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResSupported> {
        let l =
            try_from_bytes(cursor_next_value(&mut cursor, SHORT_LEN as u64)?.as_slice())? as usize;
        let mut data: HashMap<String, Vec<String>> = HashMap::with_capacity(l);
        for _ in 0..l {
            let name = CString::from_cursor(&mut cursor)?.into_plain();
            let val = CStringList::from_cursor(&mut cursor)?.into_plain();
            data.insert(name, val);
        }

        Ok(BodyResSupported { data: data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::FromCursor;
    use std::io::Cursor;

    #[test]
    fn test_name() {
        let bytes = [
            0, 1, // n options
            // 1-st option
            0, 2, 97, 98, // key [string] "ab"
            0, 2, 0, 1, 97, 0, 1, 98, /* value ["a", "b"] */
        ];
        let mut cursor: Cursor<&[u8]> = Cursor::new(&bytes);
        let options = BodyResSupported::from_cursor(&mut cursor).unwrap().data;
        assert_eq!(options.len(), 1);
        let option_ab = options.get(&"ab".to_string()).unwrap();
        assert_eq!(option_ab[0], "a".to_string());
        assert_eq!(option_ab[1], "b".to_string());
    }
}
