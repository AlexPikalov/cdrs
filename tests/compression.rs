extern crate cdrs;
use cdrs::compression::Compression;

#[test]
fn test_compression_from_str() {
    let lz4 = "lz4";
    assert_eq!(Compression::from(lz4), Compression::Lz4);
    let snappy = "snappy";
    assert_eq!(Compression::from(snappy), Compression::Snappy);
    let none = "x";
    assert_eq!(Compression::from(none), Compression::None);
}

#[test]
fn test_compression_from_string() {
    let lz4 = "lz4".to_string();
    assert_eq!(Compression::from(lz4), Compression::Lz4);
    let snappy = "snappy".to_string();
    assert_eq!(Compression::from(snappy), Compression::Snappy);
    let none = "x".to_string();
    assert_eq!(Compression::from(none), Compression::None);
}

#[test]
fn test_compression_encode_snappy() {
    let snappy_compression = Compression::Snappy;
    let bytes = String::from("Hello World").into_bytes().to_vec();
    let bytes2 = String::from("Hello World Hello World").into_bytes().to_vec();
    snappy_compression.encode(bytes.clone()).expect("Should work without exceptions");
}

#[test]
fn test_compression_decode_snappy() {
    let snappy_compression = Compression::Snappy;
    let bytes = String::from("Hello World").into_bytes().to_vec();
    let encoded = snappy_compression.encode(bytes.clone()).unwrap();
    assert_eq!(snappy_compression.decode(encoded).unwrap(), bytes);
}
