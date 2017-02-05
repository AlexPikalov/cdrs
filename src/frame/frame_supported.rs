use std::collections::HashMap;
use std::io::Cursor;
use super::super::FromCursor;
use types::*;

#[derive(Debug)]
pub struct BodyResSupported {
    pub data: HashMap<String, Vec<String>>
}

impl FromCursor for BodyResSupported {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> BodyResSupported {
        let l = from_bytes(cursor_next_value(&mut cursor, SHORT_LEN as u64)) as i16;
        let acc: HashMap<String, Vec<String>> = HashMap::new();
        let map = (0..l).fold(acc, |mut m, _| {
            let name = CString::from_cursor(&mut cursor).into_plain();
            let val = CStringList::from_cursor(&mut cursor).into_plain();
            m.insert(name, val);
            return m;
        });

        BodyResSupported {
            data: map
        }
    }
}
