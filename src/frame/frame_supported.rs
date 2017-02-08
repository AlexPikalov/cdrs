use std::collections::HashMap;
use std::io::Cursor;
use FromCursor;
use types::{SHORT_LEN, cursor_next_value, from_bytes, CString, CStringList};

#[derive(Debug)]
pub struct BodyResSupported {
    pub data: HashMap<String, Vec<String>>,
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

        BodyResSupported { data: map }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use FromCursor;
    use super::*;

    #[test]
    fn test_name() {
        let bytes = vec![0,
                         1, // n options
                         // 1-st option
                         0,
                         2,
                         97,
                         98, // key [string] "ab"
                         0,
                         2,
                         0,
                         1,
                         97,
                         0,
                         1,
                         98 /* value ["a", "b"] */];
        let mut cursor = Cursor::new(bytes);
        let options = BodyResSupported::from_cursor(&mut cursor).data;
        assert_eq!(options.len(), 1);
        let option_ab = options.get(&"ab".to_string()).unwrap();
        assert_eq!(option_ab[0], "a".to_string());
        assert_eq!(option_ab[1], "b".to_string());
    }
}
