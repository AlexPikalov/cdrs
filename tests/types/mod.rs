use std::io::Cursor;
use cdrs::*;
use cdrs::types::*;

// CString
#[test]
fn test_cstring_new() {
    let foo = "foo".to_string();
    let _ = CString::new(foo);
}

#[test]
fn test_cstring_as_str() {
    let foo = "foo".to_string();
    let cstring = CString::new(foo);

    assert_eq!(cstring.as_str(), "foo");
}

#[test]
fn test_cstring_into_plain() {
    let foo = "foo".to_string();
    let cstring = CString::new(foo);

    assert_eq!(cstring.into_plain(), "foo".to_string());
}

#[test]
fn test_cstring_into_cbytes() {
    let foo = "foo".to_string();
    let cstring = CString::new(foo);

    assert_eq!(cstring.into_cbytes(), vec![0, 3, 102, 111, 111]);
}

#[test]
fn test_cstring_from_cursor() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 3, 102, 111, 111, 0]);
    let cstring = CString::from_cursor(&mut cursor);
    println!("{:?}", &cursor);
    assert_eq!(cstring.as_str(), "foo");
}

// CStringLong
#[test]
fn test_cstringlong_new() {
    let foo = "foo".to_string();
    let _ = CStringLong::new(foo);
}

#[test]
fn test_cstringlong_as_str() {
    let foo = "foo".to_string();
    let cstring = CStringLong::new(foo);

    assert_eq!(cstring.as_str(), "foo");
}

#[test]
fn test_cstringlong_into_plain() {
    let foo = "foo".to_string();
    let cstring = CStringLong::new(foo);

    assert_eq!(cstring.into_plain(), "foo".to_string());
}

#[test]
fn test_cstringlong_into_cbytes() {
    let foo = "foo".to_string();
    let cstring = CStringLong::new(foo);

    assert_eq!(cstring.into_cbytes(), vec![0, 0, 0, 3, 102, 111, 111]);
}

#[test]
fn test_cstringlong_from_cursor() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 0, 0, 3, 102, 111, 111, 0]);
    let cstring = CStringLong::from_cursor(&mut cursor);
    println!("{:?}", &cursor);
    assert_eq!(cstring.as_str(), "foo");
}

// CStringList
#[test]
fn test_cstringlist() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 2, 0, 3, 102, 111, 111, 0, 3, 102, 111, 111]);
    let list = CStringList::from_cursor(&mut cursor);
    let plain = list.into_plain();
    assert_eq!(plain.len(), 2);
    for s in plain.iter() {
        assert_eq!(s.as_str(), "foo");
    }
}

// CBytes
#[test]
fn test_cbytes_new() {
    let bytes_vec: Vec<u8> = vec![1, 2, 3];
    let _ = CBytes::new(bytes_vec);
}

#[test]
fn test_cbytes_into_plain() {
    let cbytes = CBytes::new(vec![1, 2, 3]);
    assert_eq!(cbytes.into_plain(), vec![1, 2, 3]);
}

#[test]
fn test_cbytes_from_cursor() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 0, 0, 3, 1, 2, 3]);
    let cbytes = CBytes::from_cursor(&mut cursor);
    assert_eq!(cbytes.into_plain(), vec![1, 2, 3]);
}

#[test]
fn test_cbytes_into_cbytes() {
    let bytes_vec: Vec<u8> = vec![1, 2, 3];
    let cbytes = CBytes::new(bytes_vec);
    assert_eq!(cbytes.into_cbytes(), vec![0, 0, 0, 3, 1, 2, 3]);
}

// CBytesShort
#[test]
fn test_cbytesshort_new() {
    let bytes_vec: Vec<u8> = vec![1, 2, 3];
    let _ = CBytesShort::new(bytes_vec);
}

#[test]
fn test_cbytesshort_into_plain() {
    let cbytes = CBytesShort::new(vec![1, 2, 3]);
    assert_eq!(cbytes.into_plain(), vec![1, 2, 3]);
}

#[test]
fn test_cbytesshort_from_cursor() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 3, 1, 2, 3]);
    let cbytes = CBytesShort::from_cursor(&mut cursor);
    assert_eq!(cbytes.into_plain(), vec![1, 2, 3]);
}

#[test]
fn test_cbytesshort_into_cbytes() {
    let bytes_vec: Vec<u8> = vec![1, 2, 3];
    let cbytes = CBytesShort::new(bytes_vec);
    assert_eq!(cbytes.into_cbytes(), vec![0, 3, 1, 2, 3]);
}

// CInt
#[test]
fn test_cint_from_cursor() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 0, 0, 5]);
    let i = CInt::from_cursor(&mut cursor);
    assert_eq!(i, 5);
}

// CIntShort
#[test]
fn test_cintshort_from_cursor() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 5]);
    let i = CIntShort::from_cursor(&mut cursor);
    assert_eq!(i, 5);
}

// cursor_next_value
#[test]
fn test_cursor_next_value() {
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![0, 1, 2, 3, 4]);
    let l: u64 = 3;
    let val = cursor_next_value(&mut cursor, l);
    assert_eq!(val, vec![0, 1, 2]);
}
