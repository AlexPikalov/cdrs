extern crate cdrs;
use std::io::Cursor;
use cdrs::{IntoBytes, FromBytes, FromCursor};
use cdrs::consistency::Consistency;

#[test]
fn test_consistency_into_cbytes() {
    assert_eq!(Consistency::Any.into_cbytes(), vec![0, 0]);
    assert_eq!(Consistency::One.into_cbytes(), vec![0, 1]);
    assert_eq!(Consistency::Two.into_cbytes(), vec![0, 2]);
    assert_eq!(Consistency::Three.into_cbytes(), vec![0, 3]);
    assert_eq!(Consistency::Quorum.into_cbytes(), vec![0, 4]);
    assert_eq!(Consistency::All.into_cbytes(), vec![0, 5]);
    assert_eq!(Consistency::LocalQuorum.into_cbytes(), vec![0, 6]);
    assert_eq!(Consistency::EachQuorum.into_cbytes(), vec![0, 7]);
    assert_eq!(Consistency::Serial.into_cbytes(), vec![0, 8]);
    assert_eq!(Consistency::LocalSerial.into_cbytes(), vec![0, 9]);
    assert_eq!(Consistency::LocalOne.into_cbytes(), vec![0, 10]);
}

#[test]
fn test_consistency_from() {
    assert_eq!(Consistency::from(0), Consistency::Any);
    assert_eq!(Consistency::from(1), Consistency::One);
    assert_eq!(Consistency::from(2), Consistency::Two);
    assert_eq!(Consistency::from(3), Consistency::Three);
    assert_eq!(Consistency::from(4), Consistency::Quorum);
    assert_eq!(Consistency::from(5), Consistency::All);
    assert_eq!(Consistency::from(6), Consistency::LocalQuorum);
    assert_eq!(Consistency::from(7), Consistency::EachQuorum);
    assert_eq!(Consistency::from(8), Consistency::Serial);
    assert_eq!(Consistency::from(9), Consistency::LocalSerial);
    assert_eq!(Consistency::from(10), Consistency::LocalOne);
}

#[test]
fn test_consistency_from_bytes() {
    assert_eq!(Consistency::from_bytes(vec![0, 0]), Consistency::Any);
    assert_eq!(Consistency::from_bytes(vec![0, 1]), Consistency::One);
    assert_eq!(Consistency::from_bytes(vec![0, 2]), Consistency::Two);
    assert_eq!(Consistency::from_bytes(vec![0, 3]), Consistency::Three);
    assert_eq!(Consistency::from_bytes(vec![0, 4]), Consistency::Quorum);
    assert_eq!(Consistency::from_bytes(vec![0, 5]), Consistency::All);
    assert_eq!(Consistency::from_bytes(vec![0, 6]), Consistency::LocalQuorum);
    assert_eq!(Consistency::from_bytes(vec![0, 7]), Consistency::EachQuorum);
    assert_eq!(Consistency::from_bytes(vec![0, 8]), Consistency::Serial);
    assert_eq!(Consistency::from_bytes(vec![0, 9]), Consistency::LocalSerial);
    assert_eq!(Consistency::from_bytes(vec![0, 10]), Consistency::LocalOne);
}

#[test]
fn test_consistency_from_cursor() {
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 0])), Consistency::Any);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 1])), Consistency::One);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 2])), Consistency::Two);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 3])), Consistency::Three);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 4])), Consistency::Quorum);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 5])), Consistency::All);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 6])), Consistency::LocalQuorum);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 7])), Consistency::EachQuorum);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 8])), Consistency::Serial);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 9])), Consistency::LocalSerial);
    assert_eq!(Consistency::from_cursor(&mut Cursor::new(vec![0, 10])), Consistency::LocalOne);
}
